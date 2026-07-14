//! Tests for `pg_walstream` wire event digestion via `DiffSetBuilder::digest`.
//!
//! Exercises the `Digestable` impls on `EventType` for both
//! `ChangesetFormat` and `PatchsetFormat`, covering every operation kind,
//! error paths, and no-op events that should be silently ignored.

#![cfg(feature = "pg-walstream")]

extern crate alloc;

use alloc::sync::Arc;
use alloc::vec::Vec;

use sqlite_diff_rs::pg_walstream::{
    ColumnValue, ConversionError, EventType, Oid, PgWalstream, RowData,
};
use sqlite_diff_rs::{
    ChangeSet, DecodeError, DynTable, NamedColumns, PatchSet, SchemaWithPK, SimpleTable, TypeMap,
    Value, WireColumnTypes, WireSchema,
};

// ---------------------------------------------------------------------------
// Test schema: a simple `users` table with known PG OIDs.
// ---------------------------------------------------------------------------

/// Minimal schema implementing `WireColumnTypes<PgWalstream>` with
/// hard-coded OIDs matching the crate's `TypeMap::defaults()` keys.
#[derive(Debug, Clone)]
struct TestSchema {
    users: TestUsersTable,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TestUsersTable(SimpleTable);

impl DynTable for TestUsersTable {
    fn name(&self) -> &str {
        self.0.name()
    }
    fn number_of_columns(&self) -> usize {
        self.0.number_of_columns()
    }
    fn write_pk_flags(&self, buf: &mut [u8]) {
        self.0.write_pk_flags(buf);
    }
}

impl SchemaWithPK for TestUsersTable {
    fn extract_pk<S: Clone, B: Clone>(
        &self,
        values: &impl sqlite_diff_rs::IndexableValues<Text = S, Binary = B>,
    ) -> Vec<Value<S, B>> {
        self.0.extract_pk(values)
    }
    fn number_of_primary_keys(&self) -> usize {
        self.0.number_of_primary_keys()
    }
    fn primary_key_index(&self, col: usize) -> Option<usize> {
        self.0.primary_key_index(col)
    }
}

impl NamedColumns for TestUsersTable {
    fn column_index(&self, name: &str) -> Option<usize> {
        self.0.column_index(name)
    }
}

impl WireColumnTypes<PgWalstream> for TestUsersTable {
    fn column_type_key(&self, column_index: usize) -> Oid {
        // id -> int4 (OID 23), name -> text (OID 25), active -> bool (OID 16)
        match column_index {
            0 => 23,
            1 => 25,
            2 => 16,
            _ => panic!("column {column_index} out of range"),
        }
    }
}

impl WireSchema<PgWalstream> for TestSchema {
    type Table = TestUsersTable;
    fn get(&self, table_name: &str) -> Option<&Self::Table> {
        if table_name == "users" {
            Some(&self.users)
        } else {
            None
        }
    }
}

fn test_schema() -> TestSchema {
    TestSchema {
        users: TestUsersTable(SimpleTable::new("users", &["id", "name", "active"], &[0])),
    }
}

fn default_adapter() -> TypeMap<PgWalstream, String, Vec<u8>> {
    TypeMap::defaults()
}

fn row_data(id: i64, name: &str, active: bool) -> RowData {
    let mut data = RowData::new();
    data.push(Arc::from("id"), ColumnValue::text(&id.to_string()));
    data.push(Arc::from("name"), ColumnValue::text(name));
    data.push(
        Arc::from("active"),
        ColumnValue::text(if active { "t" } else { "f" }),
    );
    data
}

// -- ChangesetFormat: Insert, Update, Delete --------------------------------

#[test]
fn pg_changeset_insert() {
    let schema = test_schema();
    let adapter = default_adapter();
    let data = row_data(1, "Alice", true);

    let event = EventType::Insert {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 1,
        data,
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&event, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = cs.build();
    assert!(!bytes.is_empty(), "changeset must contain data");
    assert_eq!(bytes[0], b'T', "changeset marker must be 'T'");
}

#[test]
fn pg_changeset_update() {
    let schema = test_schema();
    let adapter = default_adapter();
    let old = row_data(1, "Alice", true);
    let new = row_data(1, "Alicia", true);

    let event = EventType::Update {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 1,
        old_data: Some(old),
        new_data: new,
        replica_identity: pg_walstream::ReplicaIdentity::Default,
        key_columns: alloc::vec![Arc::from("id")],
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&event, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = cs.build();
    assert!(!bytes.is_empty(), "changeset must contain data");
    assert_eq!(bytes[0], b'T', "changeset marker must be 'T'");
}

#[test]
fn pg_changeset_delete() {
    let schema = test_schema();
    let adapter = default_adapter();
    let data = row_data(1, "Alice", true);

    let event = EventType::Delete {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 1,
        old_data: data,
        replica_identity: pg_walstream::ReplicaIdentity::Default,
        key_columns: alloc::vec![Arc::from("id")],
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&event, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = cs.build();
    assert!(!bytes.is_empty(), "changeset must contain data");
    assert_eq!(bytes[0], b'T', "changeset marker must be 'T'");
}

// -- PatchsetFormat: Insert, Update, Delete ---------------------------------

#[test]
fn pg_patchset_insert() {
    let schema = test_schema();
    let adapter = default_adapter();
    let data = row_data(1, "Alice", true);

    let event = EventType::Insert {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 1,
        data,
    };

    let ps: PatchSet<TestUsersTable, String, Vec<u8>> =
        PatchSet::new().digest(&event, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = ps.build();
    assert!(!bytes.is_empty(), "patchset must contain data");
    assert_eq!(bytes[0], b'P', "patchset marker must be 'P'");
}

#[test]
fn pg_patchset_update() {
    let schema = test_schema();
    let adapter = default_adapter();
    let old = row_data(1, "Alice", true);
    let new = row_data(1, "Alicia", true);

    let event = EventType::Update {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 1,
        old_data: Some(old),
        new_data: new,
        replica_identity: pg_walstream::ReplicaIdentity::Default,
        key_columns: alloc::vec![Arc::from("id")],
    };

    let ps: PatchSet<TestUsersTable, String, Vec<u8>> =
        PatchSet::new().digest(&event, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = ps.build();
    assert!(!bytes.is_empty(), "patchset must contain data");
    assert_eq!(bytes[0], b'P', "patchset marker must be 'P'");
}

#[test]
fn pg_patchset_delete() {
    let schema = test_schema();
    let adapter = default_adapter();
    let data = row_data(1, "Alice", true);

    let event = EventType::Delete {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 1,
        old_data: data,
        replica_identity: pg_walstream::ReplicaIdentity::Default,
        key_columns: alloc::vec![Arc::from("id")],
    };

    let ps: PatchSet<TestUsersTable, String, Vec<u8>> =
        PatchSet::new().digest(&event, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = ps.build();
    assert!(!bytes.is_empty(), "patchset must contain data");
    assert_eq!(bytes[0], b'P', "patchset marker must be 'P'");
}

// -- Error paths -----------------------------------------------------------

#[test]
fn pg_table_not_found_is_error() {
    let schema = test_schema();
    let adapter = default_adapter();
    let data = row_data(1, "Alice", true);

    let event = EventType::Insert {
        schema: Arc::from("public"),
        table: Arc::from("nonexistent"),
        relation_oid: 1,
        data,
    };

    let result: Result<ChangeSet<TestUsersTable, String, Vec<u8>>, ConversionError> =
        ChangeSet::new().digest(&event, &schema, &adapter);
    match result {
        Err(ConversionError::TableNotFound(n)) => assert_eq!(n, "nonexistent"),
        Err(other) => panic!("expected TableNotFound, got {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn pg_column_not_found_is_error() {
    let schema = test_schema();
    let adapter = default_adapter();
    let mut data = RowData::new();
    data.push(Arc::from("id"), ColumnValue::text("1"));
    // Use a column name that doesn't exist in the schema
    data.push(Arc::from("missing_col"), ColumnValue::text("val"));

    let event = EventType::Insert {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 1,
        data,
    };

    let result: Result<ChangeSet<TestUsersTable, String, Vec<u8>>, ConversionError> =
        ChangeSet::new().digest(&event, &schema, &adapter);
    match result {
        Err(ConversionError::ColumnNotFound(n)) => assert!(n.contains("missing_col")),
        Err(other) => panic!("expected ColumnNotFound, got {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn pg_decode_error_is_propagated() {
    // Use an empty TypeMap so every decode fails with NoDecoderForType.
    let adapter: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::new();
    let schema = test_schema();
    let data = row_data(1, "Alice", true);

    let event = EventType::Insert {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 1,
        data,
    };

    let result: Result<ChangeSet<TestUsersTable, String, Vec<u8>>, ConversionError> =
        ChangeSet::new().digest(&event, &schema, &adapter);
    match result {
        Err(ConversionError::Decode(DecodeError::NoDecoderForType { column })) => {
            assert!(!column.is_empty());
        }
        Err(other) => panic!("expected Decode(NoDecoderForType), got {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn pg_missing_old_data_still_works_for_changeset_update() {
    let schema = test_schema();
    let adapter = default_adapter();
    let new = row_data(1, "Alice", true);

    // pg_walstream Update events carry old_data as Option.
    // When it's None for a changeset, the builder falls through
    // to `set_new` for columns without old data — this is fine.
    let event = EventType::Update {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 1,
        old_data: None,
        new_data: new,
        replica_identity: pg_walstream::ReplicaIdentity::Default,
        key_columns: alloc::vec![Arc::from("id")],
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&event, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = cs.build();
    assert!(
        !bytes.is_empty(),
        "changeset should produce output with None old_data"
    );
}

// -- No-op handling --------------------------------------------------------

#[test]
fn pg_update_with_no_old_data_patchset_still_works() {
    let schema = test_schema();
    let new = row_data(1, "Alice", true);

    let event = EventType::Update {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 1,
        old_data: None,
        new_data: new,
        replica_identity: pg_walstream::ReplicaIdentity::Default,
        key_columns: alloc::vec![Arc::from("id")],
    };

    let ps: PatchSet<TestUsersTable, String, Vec<u8>> = PatchSet::new()
        .digest(&event, &schema, &default_adapter())
        .unwrap();
    let bytes: Vec<u8> = ps.build();
    assert!(
        !bytes.is_empty(),
        "patchset update with no old data must produce output"
    );
}
