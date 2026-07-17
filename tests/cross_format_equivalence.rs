//! Cross-format equivalence harness.
//!
//! Loads a synthetic table exercising every payload family,
//! constructs the equivalent wire event for `pg_walstream` and
//! `wal2json`, digests each through the unified
//! [`DiffSetBuilder::digest`](sqlite_diff_rs::DiffSetBuilder::digest)
//!
//! This is the static analog of the Postgres-in-docker harness the
//! plan doc originally sketched. It exercises the same invariant
//! (same source data through two wire formats produces the same
//! SQLite session output) without a container dep.

#![cfg(all(feature = "wal2json", feature = "pg-walstream"))]

extern crate alloc;

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::hash::{Hash, Hasher};

use sqlite_diff_rs::pg_walstream::{ColumnValue, EventType, PgWalstream};
use sqlite_diff_rs::wal2json::{Action, Column, MessageV2, Wal2Json};
use sqlite_diff_rs::{
    DynTable, NamedColumns, PatchSet, SchemaWithPK, SimpleTable, TypeMap, Value, WireColumnTypes,
    WireSchema, WireType,
};

/// Newtype around [`SimpleTable`] that answers per-column semantic
/// [`WireType`] queries. Delegates every schema method to the inner
/// `SimpleTable`.
#[derive(Debug, Clone)]
struct UsersTable {
    inner: SimpleTable,
    wire_types: Vec<WireType>,
    pg_type_names: Vec<Arc<str>>,
}

impl PartialEq for UsersTable {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl Eq for UsersTable {}

impl Hash for UsersTable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl DynTable for UsersTable {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn number_of_columns(&self) -> usize {
        self.inner.number_of_columns()
    }
    fn write_pk_flags(&self, buf: &mut [u8]) {
        self.inner.write_pk_flags(buf);
    }
}

impl SchemaWithPK for UsersTable {
    fn number_of_primary_keys(&self) -> usize {
        self.inner.number_of_primary_keys()
    }
    fn primary_key_index(&self, col_idx: usize) -> Option<usize> {
        self.inner.primary_key_index(col_idx)
    }
    fn extract_pk<S, B>(
        &self,
        values: &impl sqlite_diff_rs::schema::IndexableValues<Text = S, Binary = B>,
    ) -> Vec<Value<S, B>>
    where
        S: Clone,
        B: Clone,
    {
        self.inner.extract_pk(values)
    }
}

impl NamedColumns for UsersTable {
    fn column_index(&self, column_name: &str) -> Option<usize> {
        NamedColumns::column_index(&self.inner, column_name)
    }
}

impl WireColumnTypes for UsersTable {
    fn column_type(&self, column_index: usize) -> WireType {
        self.wire_types[column_index]
    }
}

/// Static schema container. Every table our tests digest against
/// lives here. `get` walks each stored table's [`DynTable::name`] to
/// find a match.
#[derive(Debug, Clone)]
struct AppSchema {
    users: UsersTable,
}

impl WireSchema for AppSchema {
    type Table = UsersTable;
    fn get(&self, table_name: &str) -> Option<&Self::Table> {
        (table_name == self.users.name()).then_some(&self.users)
    }
}

/// Row with (id BIGINT, active BOOLEAN, handle TEXT, credits BIGINT, price NUMERIC, ts TIMESTAMPTZ).
#[allow(clippy::type_complexity)]
fn scenario_scalar_row() -> (
    AppSchema,
    Vec<(&'static str, ColumnValue, serde_json::Value)>,
) {
    let inner = SimpleTable::new(
        "users",
        &["id", "active", "handle", "credits", "price", "ts"],
        &[0],
    );
    let wire_types: Vec<WireType> = alloc::vec![
        WireType::Int,
        WireType::Bool,
        WireType::Text,
        WireType::Int,
        WireType::Decimal,
        WireType::TimestampTz,
    ];
    let pg_type_names: Vec<Arc<str>> = alloc::vec![
        Arc::from("bigint"),
        Arc::from("boolean"),
        Arc::from("text"),
        Arc::from("bigint"),
        Arc::from("numeric"),
        Arc::from("timestamp with time zone"),
    ];
    let schema = AppSchema {
        users: UsersTable {
            inner,
            wire_types,
            pg_type_names,
        },
    };
    // (column name, pg_walstream ColumnValue, wal2json JSON value).
    let vals: Vec<(&'static str, ColumnValue, serde_json::Value)> = alloc::vec![
        (
            "id",
            ColumnValue::text("42"),
            serde_json::Value::Number(42_i64.into())
        ),
        (
            "active",
            ColumnValue::text("t"),
            serde_json::Value::Bool(true)
        ),
        (
            "handle",
            ColumnValue::text("alice"),
            serde_json::Value::String("alice".into()),
        ),
        (
            "credits",
            ColumnValue::text("100"),
            serde_json::Value::Number(100_i64.into()),
        ),
        (
            "price",
            ColumnValue::text("12.34"),
            serde_json::Value::String("12.34".into()),
        ),
        (
            "ts",
            ColumnValue::text("2024-01-15 10:30:00+00"),
            serde_json::Value::String("2024-01-15 10:30:00+00".into()),
        ),
    ];
    (schema, vals)
}

fn row_data(cols: &[(&'static str, ColumnValue)]) -> pg_walstream::RowData {
    let mut row = pg_walstream::RowData::new();
    for (name, value) in cols {
        row.push(Arc::from(*name), value.clone());
    }
    row
}

fn columns_from_vals(
    schema: &AppSchema,
    vals: &[(&'static str, ColumnValue, serde_json::Value)],
) -> Vec<Column> {
    vals.iter()
        .map(|(name, _, value)| {
            let idx = NamedColumns::column_index(&schema.users, name).unwrap();
            Column {
                name: (*name).to_string(),
                type_name: schema.users.pg_type_names[idx].as_ref().to_string(),
                value: value.clone(),
            }
        })
        .collect()
}

#[test]
fn cross_format_insert_produces_byte_equal_patchset() {
    let (schema, vals) = scenario_scalar_row();

    let pg_event = EventType::Insert {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 1,
        data: row_data(
            &vals
                .iter()
                .map(|(n, cv, _)| (*n, cv.clone()))
                .collect::<Vec<_>>(),
        ),
    };

    let w2j_msg = MessageV2 {
        action: Action::I,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: Some(columns_from_vals(&schema, &vals)),
        identity: None,
        lsn: None,
    };

    let pg_types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    let w2j_types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();

    let pg_patchset = PatchSet::<UsersTable, String, Vec<u8>>::new()
        .digest(&pg_event, &schema, &pg_types)
        .unwrap();
    let w2j_patchset = PatchSet::<UsersTable, String, Vec<u8>>::new()
        .digest(&w2j_msg, &schema, &w2j_types)
        .unwrap();

    let pg_bytes: Vec<u8> = pg_patchset.build();
    let w2j_bytes: Vec<u8> = w2j_patchset.build();
    assert_eq!(
        pg_bytes, w2j_bytes,
        "pg_walstream and wal2json digests diverge on the same insert"
    );
}

#[test]
fn cross_format_delete_produces_byte_equal_patchset() {
    let (schema, vals) = scenario_scalar_row();

    let pg_event = EventType::Delete {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 1,
        old_data: row_data(
            &vals
                .iter()
                .map(|(n, cv, _)| (*n, cv.clone()))
                .collect::<Vec<_>>(),
        ),
        replica_identity: pg_walstream::ReplicaIdentity::Default,
        key_columns: alloc::vec![Arc::from("id")],
    };

    let w2j_msg = MessageV2 {
        action: Action::D,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: None,
        identity: Some(columns_from_vals(&schema, &vals)),
        lsn: None,
    };

    let pg_types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    let w2j_types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();

    let pg_patchset = PatchSet::<UsersTable, String, Vec<u8>>::new()
        .digest(&pg_event, &schema, &pg_types)
        .unwrap();
    let w2j_patchset = PatchSet::<UsersTable, String, Vec<u8>>::new()
        .digest(&w2j_msg, &schema, &w2j_types)
        .unwrap();

    let pg_bytes: Vec<u8> = pg_patchset.build();
    let w2j_bytes: Vec<u8> = w2j_patchset.build();
    assert_eq!(
        pg_bytes, w2j_bytes,
        "pg_walstream and wal2json delete digests diverge"
    );
}

#[test]
fn cross_format_update_produces_byte_equal_patchset() {
    let (schema, mut vals) = scenario_scalar_row();

    // Old values: current vals. New values: change handle "alice" -> "alicia".
    let old_vals: Vec<(&'static str, ColumnValue, serde_json::Value)> = vals.clone();
    for (name, cv, json) in &mut vals {
        if *name == "handle" {
            *cv = ColumnValue::text("alicia");
            *json = serde_json::Value::String("alicia".into());
        }
    }

    let pg_event = EventType::Update {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 1,
        old_data: Some(row_data(
            &old_vals
                .iter()
                .map(|(n, cv, _)| (*n, cv.clone()))
                .collect::<Vec<_>>(),
        )),
        new_data: row_data(
            &vals
                .iter()
                .map(|(n, cv, _)| (*n, cv.clone()))
                .collect::<Vec<_>>(),
        ),
        replica_identity: pg_walstream::ReplicaIdentity::Default,
        key_columns: alloc::vec![Arc::from("id")],
    };

    let w2j_msg = MessageV2 {
        action: Action::U,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: Some(columns_from_vals(&schema, &vals)),
        identity: Some(columns_from_vals(&schema, &old_vals)),
        lsn: None,
    };

    let pg_types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    let w2j_types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();

    let pg_patchset = PatchSet::<UsersTable, String, Vec<u8>>::new()
        .digest(&pg_event, &schema, &pg_types)
        .unwrap();
    let w2j_patchset = PatchSet::<UsersTable, String, Vec<u8>>::new()
        .digest(&w2j_msg, &schema, &w2j_types)
        .unwrap();

    let pg_bytes: Vec<u8> = pg_patchset.build();
    let w2j_bytes: Vec<u8> = w2j_patchset.build();
    assert_eq!(
        pg_bytes, w2j_bytes,
        "pg_walstream and wal2json update digests diverge"
    );
}

#[test]
fn wal2json_v1_and_v2_insert_produce_byte_equal_patchset() {
    use sqlite_diff_rs::wal2json::ChangeV1;

    let (schema, vals) = scenario_scalar_row();

    let v2_msg = MessageV2 {
        action: Action::I,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: Some(columns_from_vals(&schema, &vals)),
        identity: None,
        lsn: None,
    };

    let v1_change = ChangeV1 {
        kind: "insert".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        columnnames: vals.iter().map(|(n, _, _)| (*n).to_string()).collect(),
        columntypes: vals
            .iter()
            .map(|(n, _, _)| {
                let idx = NamedColumns::column_index(&schema.users, n).unwrap();
                schema.users.pg_type_names[idx].as_ref().to_string()
            })
            .collect(),
        columnvalues: vals.iter().map(|(_, _, v)| v.clone()).collect(),
        oldkeys: None,
    };

    let w2j_types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();

    let v2_patchset = PatchSet::<UsersTable, String, Vec<u8>>::new()
        .digest(&v2_msg, &schema, &w2j_types)
        .unwrap();
    let v1_patchset = PatchSet::<UsersTable, String, Vec<u8>>::new()
        .digest(&v1_change, &schema, &w2j_types)
        .unwrap();

    let v2_bytes: Vec<u8> = v2_patchset.build();
    let v1_bytes: Vec<u8> = v1_patchset.build();
    assert_eq!(
        v2_bytes, v1_bytes,
        "wal2json v1 and v2 digests diverge on the same insert"
    );
}
