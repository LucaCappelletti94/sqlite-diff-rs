//! Tests for `maxwell` wire event digestion via `DiffSetBuilder::digest`.
//!
//! Exercises the `Digestable` impls on `Message` for both
//! `ChangesetFormat` and `PatchsetFormat`, covering every operation kind,
//! error paths, and the `--include_types` type-key stripping behavior.

#![cfg(feature = "maxwell")]

extern crate alloc;

use alloc::collections::BTreeMap;
use alloc::sync::Arc;
use alloc::vec::Vec;

use sqlite_diff_rs::maxwell::{ConversionError, Maxwell, MaxwellColumn, Message, OpType};
use sqlite_diff_rs::{
    ChangeSet, DecodeError, DynTable, NamedColumns, PatchSet, SchemaWithPK, SimpleTable, TypeMap,
    Value, WireColumnTypes, WireSchema,
};

// ---------------------------------------------------------------------------
// Test schema
// ---------------------------------------------------------------------------

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

impl WireColumnTypes<Maxwell> for TestUsersTable {
    fn column_type_key(&self, column_index: usize) -> Arc<str> {
        // id -> int, name -> varchar, active -> tinyint(1)
        match column_index {
            0 => Arc::from("int"),
            1 => Arc::from("varchar"),
            2 => Arc::from("tinyint(1)"),
            _ => panic!("column {column_index} out of range"),
        }
    }
}

impl WireSchema<Maxwell> for TestSchema {
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

fn default_adapter() -> TypeMap<Maxwell, String, Vec<u8>> {
    TypeMap::defaults()
}

fn data_map(id: i64, name: &str, active: bool) -> BTreeMap<String, serde_json::Value> {
    let mut map = BTreeMap::new();
    map.insert(
        "id".to_string(),
        serde_json::Value::Number(serde_json::Number::from(id)),
    );
    map.insert(
        "name".to_string(),
        serde_json::Value::String(name.to_string()),
    );
    map.insert("active".to_string(), serde_json::Value::Bool(active));
    map
}

fn message(
    op_type: OpType,
    data: BTreeMap<String, serde_json::Value>,
    old: Option<BTreeMap<String, serde_json::Value>>,
) -> Message {
    Message {
        database: "testdb".to_string(),
        table: "users".to_string(),
        op_type,
        ts: None,
        xid: None,
        commit: None,
        position: None,
        server_id: None,
        thread_id: None,
        primary_key: None,
        primary_key_columns: None,
        data,
        old,
        columns_types: None,
    }
}

// -- ChangesetFormat: Insert, Update, Delete --------------------------------

#[test]
fn maxwell_changeset_insert() {
    let schema = test_schema();
    let adapter = default_adapter();
    let data = data_map(1, "Alice", true);
    let msg = message(OpType::Insert, data, None);

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&msg, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = cs.build();
    assert!(!bytes.is_empty(), "changeset must contain data");
    assert_eq!(bytes[0], b'T', "changeset marker must be 'T'");
}

#[test]
fn maxwell_changeset_update() {
    let schema = test_schema();
    let adapter = default_adapter();
    let new_data = data_map(1, "Alicia", true);
    let old_data = data_map(1, "Alice", true);
    let msg = message(OpType::Update, new_data, Some(old_data));

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&msg, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = cs.build();
    assert!(!bytes.is_empty(), "changeset must contain data");
    assert_eq!(bytes[0], b'T', "changeset marker must be 'T'");
}

#[test]
fn maxwell_changeset_delete() {
    let schema = test_schema();
    let adapter = default_adapter();
    let data = data_map(1, "Alice", true);
    let msg = message(OpType::Delete, data, None);

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&msg, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = cs.build();
    assert!(!bytes.is_empty(), "changeset must contain data");
    assert_eq!(bytes[0], b'T', "changeset marker must be 'T'");
}

// -- PatchsetFormat: Insert, Update, Delete ---------------------------------

#[test]
fn maxwell_patchset_insert() {
    let schema = test_schema();
    let adapter = default_adapter();
    let data = data_map(1, "Alice", true);
    let msg = message(OpType::Insert, data, None);

    let ps: PatchSet<TestUsersTable, String, Vec<u8>> =
        PatchSet::new().digest(&msg, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = ps.build();
    assert!(!bytes.is_empty(), "patchset must contain data");
    assert_eq!(bytes[0], b'P', "patchset marker must be 'P'");
}

#[test]
fn maxwell_patchset_update() {
    let schema = test_schema();
    let adapter = default_adapter();
    let new_data = data_map(1, "Alicia", true);
    let msg = message(OpType::Update, new_data, None);

    let ps: PatchSet<TestUsersTable, String, Vec<u8>> =
        PatchSet::new().digest(&msg, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = ps.build();
    assert!(!bytes.is_empty(), "patchset must contain data");
    assert_eq!(bytes[0], b'P', "patchset marker must be 'P'");
}

#[test]
fn maxwell_patchset_delete() {
    let schema = test_schema();
    let adapter = default_adapter();
    let data = data_map(1, "Alice", true);
    let msg = message(OpType::Delete, data, None);

    let ps: PatchSet<TestUsersTable, String, Vec<u8>> =
        PatchSet::new().digest(&msg, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = ps.build();
    assert!(!bytes.is_empty(), "patchset must contain data");
    assert_eq!(bytes[0], b'P', "patchset marker must be 'P'");
}

// -- Error paths -----------------------------------------------------------

#[test]
fn maxwell_table_not_found_is_error() {
    let schema = test_schema();
    let adapter = default_adapter();
    let data = data_map(1, "Alice", true);

    let msg = Message {
        database: "testdb".to_string(),
        table: "nonexistent".to_string(),
        op_type: OpType::Insert,
        ts: None,
        xid: None,
        commit: None,
        position: None,
        server_id: None,
        thread_id: None,
        primary_key: None,
        primary_key_columns: None,
        data,
        old: None,
        columns_types: None,
    };

    let result: Result<ChangeSet<TestUsersTable, String, Vec<u8>>, ConversionError> =
        ChangeSet::new().digest(&msg, &schema, &adapter);
    match result {
        Err(ConversionError::TableNotFound(n)) => assert_eq!(n, "nonexistent"),
        Err(other) => panic!("expected TableNotFound, got {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn maxwell_column_not_found_is_error() {
    let schema = test_schema();
    let adapter = default_adapter();

    let mut data = BTreeMap::new();
    data.insert(
        "missing_col".to_string(),
        serde_json::Value::Number(serde_json::Number::from(1_i64)),
    );
    let msg = message(OpType::Insert, data, None);

    let result: Result<ChangeSet<TestUsersTable, String, Vec<u8>>, ConversionError> =
        ChangeSet::new().digest(&msg, &schema, &adapter);
    match result {
        Err(ConversionError::ColumnNotFound(n)) => assert!(n.contains("missing_col")),
        Err(other) => panic!("expected ColumnNotFound, got {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn maxwell_decode_error_is_propagated() {
    let adapter: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::new();
    let schema = test_schema();
    let data = data_map(1, "Alice", true);
    let msg = message(OpType::Insert, data, None);

    let result: Result<ChangeSet<TestUsersTable, String, Vec<u8>>, ConversionError> =
        ChangeSet::new().digest(&msg, &schema, &adapter);
    match result {
        Err(ConversionError::Decode(DecodeError::NoDecoderForType { column })) => {
            assert!(!column.is_empty());
        }
        Err(other) => panic!("expected Decode(NoDecoderForType), got {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn maxwell_changeset_update_without_old_is_ok() {
    // Maxwell updates carry `old` as optional. When absent, the builder
    // falls through to `set_new` for all columns — this is fine.
    let schema = test_schema();
    let adapter = default_adapter();
    let new_data = data_map(1, "Alice", true);
    let msg = message(OpType::Update, new_data, None);

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&msg, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = cs.build();
    assert!(
        !bytes.is_empty(),
        "changeset must produce output without old data"
    );
}

// -- WireSource type_key stripping -----------------------------------------

#[test]
fn maxwell_type_key_strips_paren_suffix() {
    // Verify that `WireSource::type_key` strips parenthesized modifiers
    // from MySQL type names, except for `tinyint(1)`.
    use sqlite_diff_rs::WireSource;

    // varchar(255) -> varchar
    let payload = MaxwellColumn {
        column_name: "c",
        mysql_type: Some("varchar(255)"),
        value: &serde_json::Value::Null,
    };
    assert_eq!(
        Maxwell::type_key(&payload).as_ref(),
        "varchar",
        "varchar(255) should strip to varchar"
    );

    // decimal(10,2) -> decimal
    let payload = MaxwellColumn {
        column_name: "c",
        mysql_type: Some("decimal(10,2)"),
        value: &serde_json::Value::Null,
    };
    assert_eq!(
        Maxwell::type_key(&payload).as_ref(),
        "decimal",
        "decimal(10,2) should strip to decimal"
    );

    // tinyint(1) -> tinyint(1) (preserved for bool detection)
    let payload = MaxwellColumn {
        column_name: "c",
        mysql_type: Some("tinyint(1)"),
        value: &serde_json::Value::Null,
    };
    assert_eq!(
        Maxwell::type_key(&payload).as_ref(),
        "tinyint(1)",
        "tinyint(1) should be preserved"
    );

    // Empty mysql_type -> ""
    let payload = MaxwellColumn {
        column_name: "c",
        mysql_type: None,
        value: &serde_json::Value::Null,
    };
    assert_eq!(
        Maxwell::type_key(&payload).as_ref(),
        "",
        "None mysql_type should produce empty string"
    );
}
