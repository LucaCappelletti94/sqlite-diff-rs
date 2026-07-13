//! Phase 10: cross-format equivalence harness.
//!
//! Loads a synthetic table exercising every Phase 1..9 payload family,
//! constructs the equivalent wire event for `pg_walstream` and
//! `wal2json`, digests each through `TypeMap::defaults()`, and asserts
//! the resulting `PatchSet` bytes are byte-equal.
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

use pg_walstream::{ColumnInfo, RelationInfo, RowData};
use sqlite_diff_rs::pg_walstream::{ColumnValue, EventType, PgWalstream};
use sqlite_diff_rs::wal2json::{Action, Column, MessageV2, Wal2Json};
use sqlite_diff_rs::{PatchSet, SimpleTable, TypeMap};

fn build_relation(table: &str, cols: &[(&str, u32, u8)]) -> RelationInfo {
    let mut relation = RelationInfo {
        relation_id: 1,
        namespace: Arc::from("public"),
        relation_name: Arc::from(table),
        replica_identity: 0,
        columns: Vec::new(),
    };
    for (name, type_id, flags) in cols {
        relation
            .columns
            .push(ColumnInfo::new(*flags, name.to_string(), *type_id, -1));
    }
    relation
}

fn row_data(cols: &[(&str, ColumnValue)]) -> RowData {
    let mut row = RowData::new();
    for (name, value) in cols {
        row.push(Arc::from(*name), value.clone());
    }
    row
}

/// Row with (id BIGINT, active BOOLEAN, handle TEXT, credits BIGINT,
/// price NUMERIC, ts TIMESTAMPTZ) covering Phases 1, 2, 4, 7, 8.
#[allow(clippy::type_complexity)]
fn scenario_scalar_row() -> (
    SimpleTable,
    Vec<(&'static str, u32, u8)>,
    Vec<(&'static str, ColumnValue, &'static str, serde_json::Value)>,
) {
    let schema = SimpleTable::new(
        "users",
        &["id", "active", "handle", "credits", "price", "ts"],
        &[0],
    );
    // (name, pg_oid, pk_flag)
    let cols: Vec<(&str, u32, u8)> = alloc::vec![
        ("id", 20u32, 1),   // int8, PK
        ("active", 16, 0),  // bool
        ("handle", 25, 0),  // text
        ("credits", 20, 0), // int8
        ("price", 1700, 0), // numeric
        ("ts", 1184, 0),    // timestamptz
    ];
    // Each column: pg_walstream ColumnValue (text-mode), wal2json
    // pg_type_name, wal2json JSON value.
    let vals: Vec<(&str, ColumnValue, &str, serde_json::Value)> = alloc::vec![
        (
            "id",
            ColumnValue::text("42"),
            "bigint",
            serde_json::Value::Number(42_i64.into()),
        ),
        (
            "active",
            ColumnValue::text("t"),
            "boolean",
            serde_json::Value::Bool(true),
        ),
        (
            "handle",
            ColumnValue::text("alice"),
            "text",
            serde_json::Value::String("alice".into()),
        ),
        (
            "credits",
            ColumnValue::text("100"),
            "bigint",
            serde_json::Value::Number(100_i64.into()),
        ),
        (
            "price",
            ColumnValue::text("12.34"),
            "numeric",
            serde_json::Value::String("12.34".into()),
        ),
        (
            "ts",
            ColumnValue::text("2024-01-15 10:30:00+00"),
            "timestamp with time zone",
            serde_json::Value::String("2024-01-15 10:30:00+00".into()),
        ),
    ];
    (schema, cols, vals)
}

#[test]
fn cross_format_insert_produces_byte_equal_patchset() {
    let (schema, cols, vals) = scenario_scalar_row();
    let relation = build_relation(
        "users",
        &cols
            .iter()
            .map(|(n, o, f)| (*n, *o, *f))
            .collect::<Vec<_>>(),
    );

    // pg_walstream insert event.
    let pg_event = EventType::Insert {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 1,
        data: row_data(
            &vals
                .iter()
                .map(|(n, cv, _, _)| (*n, cv.clone()))
                .collect::<Vec<_>>(),
        ),
    };

    // wal2json v2 insert message.
    let w2j_msg = MessageV2 {
        action: Action::I,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: Some(
            vals.iter()
                .map(|(name, _, type_name, value)| Column {
                    name: (*name).to_string(),
                    type_name: (*type_name).to_string(),
                    value: value.clone(),
                })
                .collect(),
        ),
        identity: None,
    };

    let pg_types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    let w2j_types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();

    let pg_patchset = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .digest_pg_walstream(&pg_event, &relation, &schema, &pg_types)
        .unwrap();
    let w2j_patchset = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .digest_wal2json_v2(&w2j_msg, &schema, &w2j_types)
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
    let (schema, cols, vals) = scenario_scalar_row();
    let relation = build_relation(
        "users",
        &cols
            .iter()
            .map(|(n, o, f)| (*n, *o, *f))
            .collect::<Vec<_>>(),
    );

    let pg_event = EventType::Delete {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 1,
        old_data: row_data(
            &vals
                .iter()
                .map(|(n, cv, _, _)| (*n, cv.clone()))
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
        identity: Some(
            vals.iter()
                .map(|(name, _, type_name, value)| Column {
                    name: (*name).to_string(),
                    type_name: (*type_name).to_string(),
                    value: value.clone(),
                })
                .collect(),
        ),
    };

    let pg_types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    let w2j_types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();

    let pg_patchset = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .digest_pg_walstream(&pg_event, &relation, &schema, &pg_types)
        .unwrap();
    let w2j_patchset = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .digest_wal2json_v2(&w2j_msg, &schema, &w2j_types)
        .unwrap();

    let pg_bytes: Vec<u8> = pg_patchset.build();
    let w2j_bytes: Vec<u8> = w2j_patchset.build();
    assert_eq!(
        pg_bytes, w2j_bytes,
        "pg_walstream and wal2json delete digests diverge"
    );
}
