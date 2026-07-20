//! Tests for `wal2json` wire event digestion via `DiffSetBuilder::digest`.
//!
//! Exercises the `Digestable` impls on `MessageV2` and `ChangeV1` for both
//! `ChangesetFormat` and `PatchsetFormat`, covering every operation kind,
//! error paths, and no-op actions (B, C, T, M) that should be ignored.

#![cfg(feature = "wal2json")]

extern crate alloc;

use alloc::vec::Vec;

use sqlite_diff_rs::wal2json::{
    Action, ChangeV1, Column, ConversionError, MessageV2, OldKeys, Wal2Json, parse_v2,
};
use sqlite_diff_rs::{
    ChangeSet, ChangesetOp, DecodeError, DynTable, NamedColumns, PatchSet, SchemaWithPK,
    SimpleTable, TypeMap, Value, WireColumnTypes, WireSchema, WireType,
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

impl WireColumnTypes for TestUsersTable {
    fn column_type(&self, column_index: usize) -> WireType {
        // id -> Int, name -> Text, active -> Bool
        match column_index {
            0 => WireType::Int,
            1 => WireType::Text,
            2 => WireType::Bool,
            _ => panic!("column {column_index} out of range"),
        }
    }
}

impl WireSchema for TestSchema {
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

fn default_adapter() -> TypeMap<Wal2Json, String, Vec<u8>> {
    TypeMap::defaults()
}

fn column(name: &str, type_name: &str, value: serde_json::Value) -> Column {
    Column {
        name: name.to_string(),
        type_name: type_name.to_string(),
        value,
    }
}

fn int_col(name: &str, val: i64) -> Column {
    column(
        name,
        "integer",
        serde_json::Value::Number(serde_json::Number::from(val)),
    )
}

fn text_col(name: &str, val: &str) -> Column {
    column(name, "text", serde_json::Value::String(val.to_string()))
}

fn bool_col(name: &str, val: bool) -> Column {
    column(name, "boolean", serde_json::Value::Bool(val))
}

fn all_columns(id: i64, name: &str, active: bool) -> Vec<Column> {
    alloc::vec![
        int_col("id", id),
        text_col("name", name),
        bool_col("active", active),
    ]
}

fn all_values(id: i64, name: &str, active: bool) -> Vec<serde_json::Value> {
    alloc::vec![
        serde_json::Value::Number(serde_json::Number::from(id)),
        serde_json::Value::String(name.to_string()),
        serde_json::Value::Bool(active),
    ]
}

// -- MessageV2: ChangesetFormat --------------------------------------------

#[test]
fn w2j_v2_changeset_insert() {
    let schema = test_schema();
    let adapter = default_adapter();

    let msg = MessageV2 {
        action: Action::I,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: Some(all_columns(1, "Alice", true)),
        identity: None,
        lsn: None,
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&msg, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = cs.build();
    assert!(!bytes.is_empty(), "changeset must contain data");
    assert_eq!(bytes[0], b'T', "changeset marker must be 'T'");
}

#[test]
fn w2j_v2_changeset_update() {
    let schema = test_schema();
    let adapter = default_adapter();

    let msg = MessageV2 {
        action: Action::U,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: Some(all_columns(1, "Alicia", true)),
        identity: Some(all_columns(1, "Alice", true)),
        lsn: None,
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&msg, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = cs.build();
    assert!(!bytes.is_empty(), "changeset must contain data");
    assert_eq!(bytes[0], b'T', "changeset marker must be 'T'");
}

#[test]
fn w2j_v2_changeset_delete() {
    let schema = test_schema();
    let adapter = default_adapter();

    let msg = MessageV2 {
        action: Action::D,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: None,
        identity: Some(all_columns(1, "Alice", true)),
        lsn: None,
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&msg, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = cs.build();
    assert!(!bytes.is_empty(), "changeset must contain data");
    assert_eq!(bytes[0], b'T', "changeset marker must be 'T'");
}

// -- MessageV2: PatchsetFormat ---------------------------------------------

#[test]
fn w2j_v2_patchset_insert() {
    let schema = test_schema();
    let adapter = default_adapter();

    let msg = MessageV2 {
        action: Action::I,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: Some(all_columns(1, "Alice", true)),
        identity: None,
        lsn: None,
    };

    let ps: PatchSet<TestUsersTable, String, Vec<u8>> =
        PatchSet::new().digest(&msg, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = ps.build();
    assert!(!bytes.is_empty(), "patchset must contain data");
    assert_eq!(bytes[0], b'P', "patchset marker must be 'P'");
}

#[test]
fn w2j_v2_patchset_update() {
    let schema = test_schema();
    let adapter = default_adapter();

    let msg = MessageV2 {
        action: Action::U,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: Some(all_columns(1, "Alicia", true)),
        identity: Some(all_columns(1, "Alice", true)),
        lsn: None,
    };

    let ps: PatchSet<TestUsersTable, String, Vec<u8>> =
        PatchSet::new().digest(&msg, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = ps.build();
    assert!(!bytes.is_empty(), "patchset must contain data");
    assert_eq!(bytes[0], b'P', "patchset marker must be 'P'");
}

#[test]
fn w2j_v2_patchset_delete() {
    let schema = test_schema();
    let adapter = default_adapter();

    let msg = MessageV2 {
        action: Action::D,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: None,
        identity: Some(all_columns(1, "Alice", true)),
        lsn: None,
    };

    let ps: PatchSet<TestUsersTable, String, Vec<u8>> =
        PatchSet::new().digest(&msg, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = ps.build();
    assert!(!bytes.is_empty(), "patchset must contain data");
    assert_eq!(bytes[0], b'P', "patchset marker must be 'P'");
}

// -- ChangeV1: ChangesetFormat ---------------------------------------------

#[test]
fn w2j_v1_changeset_insert() {
    let schema = test_schema();
    let adapter = default_adapter();

    let change = ChangeV1 {
        kind: "insert".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        columnnames: alloc::vec!["id".to_string(), "name".to_string(), "active".to_string()],
        columntypes: alloc::vec![
            "integer".to_string(),
            "text".to_string(),
            "boolean".to_string(),
        ],
        columnvalues: all_values(1, "Alice", true),
        oldkeys: None,
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&change, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = cs.build();
    assert!(!bytes.is_empty(), "changeset must contain data");
    assert_eq!(bytes[0], b'T', "changeset marker must be 'T'");
}

#[test]
fn w2j_v1_changeset_update() {
    let schema = test_schema();
    let adapter = default_adapter();

    let change = ChangeV1 {
        kind: "update".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        columnnames: alloc::vec!["id".to_string(), "name".to_string(), "active".to_string()],
        columntypes: alloc::vec![
            "integer".to_string(),
            "text".to_string(),
            "boolean".to_string(),
        ],
        columnvalues: all_values(1, "Alicia", true),
        oldkeys: Some(sqlite_diff_rs::wal2json::OldKeys {
            keynames: alloc::vec!["id".to_string()],
            keytypes: alloc::vec!["integer".to_string()],
            keyvalues: alloc::vec![serde_json::Value::Number(serde_json::Number::from(1_i64),)],
        }),
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&change, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = cs.build();
    assert!(!bytes.is_empty(), "changeset must contain data");
    assert_eq!(bytes[0], b'T', "changeset marker must be 'T'");
}

#[test]
fn w2j_v1_changeset_delete() {
    let schema = test_schema();
    let adapter = default_adapter();

    let change = ChangeV1 {
        kind: "delete".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        columnnames: alloc::vec!["id".to_string(), "name".to_string(), "active".to_string()],
        columntypes: alloc::vec![
            "integer".to_string(),
            "text".to_string(),
            "boolean".to_string(),
        ],
        columnvalues: all_values(1, "Alice", true),
        oldkeys: Some(sqlite_diff_rs::wal2json::OldKeys {
            keynames: alloc::vec!["id".to_string()],
            keytypes: alloc::vec!["integer".to_string()],
            keyvalues: alloc::vec![serde_json::Value::Number(serde_json::Number::from(1_i64),)],
        }),
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&change, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = cs.build();
    assert!(!bytes.is_empty(), "changeset must contain data");
    assert_eq!(bytes[0], b'T', "changeset marker must be 'T'");
}

// -- ChangeV1: PatchsetFormat ----------------------------------------------

#[test]
fn w2j_v1_patchset_insert() {
    let schema = test_schema();
    let adapter = default_adapter();

    let change = ChangeV1 {
        kind: "insert".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        columnnames: alloc::vec!["id".to_string(), "name".to_string(), "active".to_string()],
        columntypes: alloc::vec![
            "integer".to_string(),
            "text".to_string(),
            "boolean".to_string(),
        ],
        columnvalues: all_values(1, "Alice", true),
        oldkeys: None,
    };

    let ps: PatchSet<TestUsersTable, String, Vec<u8>> =
        PatchSet::new().digest(&change, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = ps.build();
    assert!(!bytes.is_empty(), "patchset must contain data");
    assert_eq!(bytes[0], b'P', "patchset marker must be 'P'");
}

#[test]
fn w2j_v1_patchset_update() {
    let schema = test_schema();
    let adapter = default_adapter();

    let change = ChangeV1 {
        kind: "update".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        columnnames: alloc::vec!["id".to_string(), "name".to_string(), "active".to_string()],
        columntypes: alloc::vec![
            "integer".to_string(),
            "text".to_string(),
            "boolean".to_string(),
        ],
        columnvalues: all_values(1, "Alicia", true),
        oldkeys: Some(sqlite_diff_rs::wal2json::OldKeys {
            keynames: alloc::vec!["id".to_string()],
            keytypes: alloc::vec!["integer".to_string()],
            keyvalues: alloc::vec![serde_json::Value::Number(serde_json::Number::from(1_i64),)],
        }),
    };

    let ps: PatchSet<TestUsersTable, String, Vec<u8>> =
        PatchSet::new().digest(&change, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = ps.build();
    assert!(!bytes.is_empty(), "patchset must contain data");
    assert_eq!(bytes[0], b'P', "patchset marker must be 'P'");
}

#[test]
fn w2j_v1_patchset_delete() {
    let schema = test_schema();
    let adapter = default_adapter();

    let change = ChangeV1 {
        kind: "delete".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        columnnames: alloc::vec!["id".to_string(), "name".to_string(), "active".to_string()],
        columntypes: alloc::vec![
            "integer".to_string(),
            "text".to_string(),
            "boolean".to_string(),
        ],
        columnvalues: all_values(1, "Alice", true),
        oldkeys: Some(sqlite_diff_rs::wal2json::OldKeys {
            keynames: alloc::vec!["id".to_string()],
            keytypes: alloc::vec!["integer".to_string()],
            keyvalues: alloc::vec![serde_json::Value::Number(serde_json::Number::from(1_i64),)],
        }),
    };

    let ps: PatchSet<TestUsersTable, String, Vec<u8>> =
        PatchSet::new().digest(&change, &schema, &adapter).unwrap();
    let bytes: Vec<u8> = ps.build();
    assert!(!bytes.is_empty(), "patchset must contain data");
    assert_eq!(bytes[0], b'P', "patchset marker must be 'P'");
}

// -- Error paths -----------------------------------------------------------

#[test]
fn w2j_table_not_found_is_error() {
    let schema = test_schema();
    let adapter = default_adapter();

    let msg = MessageV2 {
        action: Action::I,
        schema: Some("public".to_string()),
        table: Some("nonexistent".to_string()),
        columns: Some(all_columns(1, "Alice", true)),
        identity: None,
        lsn: None,
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
fn w2j_missing_columns_is_error_for_insert() {
    let schema = test_schema();
    let adapter = default_adapter();

    let msg = MessageV2 {
        action: Action::I,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: None,
        identity: None,
        lsn: None,
    };

    let result: Result<ChangeSet<TestUsersTable, String, Vec<u8>>, ConversionError> =
        ChangeSet::new().digest(&msg, &schema, &adapter);
    match result {
        Err(ConversionError::MissingColumns) => {}
        Err(other) => panic!("expected MissingColumns, got {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn w2j_missing_identity_is_error_for_delete() {
    let schema = test_schema();
    let adapter = default_adapter();

    let msg = MessageV2 {
        action: Action::D,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: None,
        identity: None,
        lsn: None,
    };

    let result: Result<ChangeSet<TestUsersTable, String, Vec<u8>>, ConversionError> =
        ChangeSet::new().digest(&msg, &schema, &adapter);
    match result {
        Err(ConversionError::MissingColumns) => {}
        Err(other) => panic!("expected MissingColumns, got {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn w2j_column_not_found_is_error() {
    let schema = test_schema();
    let adapter = default_adapter();

    let msg = MessageV2 {
        action: Action::I,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: Some(alloc::vec![column(
            "missing_col",
            "integer",
            serde_json::Value::Number(serde_json::Number::from(1_i64))
        )]),
        identity: None,
        lsn: None,
    };

    let result: Result<ChangeSet<TestUsersTable, String, Vec<u8>>, ConversionError> =
        ChangeSet::new().digest(&msg, &schema, &adapter);
    match result {
        Err(ConversionError::ColumnNotFound(n)) => assert!(n.contains("missing_col")),
        Err(other) => panic!("expected ColumnNotFound, got {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn w2j_decode_error_is_propagated() {
    let adapter: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::new();
    let schema = test_schema();

    let msg = MessageV2 {
        action: Action::I,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: Some(all_columns(1, "Alice", true)),
        identity: None,
        lsn: None,
    };

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
fn w2j_v1_missing_oldkeys_is_error_for_patchset_delete() {
    let schema = test_schema();
    let adapter = default_adapter();

    let change = ChangeV1 {
        kind: "delete".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        columnnames: alloc::vec!["id".to_string()],
        columntypes: alloc::vec!["integer".to_string()],
        columnvalues: alloc::vec![serde_json::Value::Number(serde_json::Number::from(1_i64),)],
        oldkeys: None,
    };

    let result: Result<PatchSet<TestUsersTable, String, Vec<u8>>, ConversionError> =
        PatchSet::new().digest(&change, &schema, &adapter);
    match result {
        Err(ConversionError::MissingColumns) => {}
        Err(other) => panic!("expected MissingColumns, got {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

// -- No-op handling --------------------------------------------------------

#[test]
fn w2j_v2_no_table_is_ignored() {
    let schema = test_schema();
    let adapter = default_adapter();

    let msg = MessageV2 {
        action: Action::I,
        schema: Some("public".to_string()),
        table: None,
        columns: Some(all_columns(1, "Alice", true)),
        identity: None,
        lsn: None,
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&msg, &schema, &adapter).unwrap();
    assert!(
        cs.build().is_empty(),
        "no-table message must produce empty output"
    );
}

#[test]
fn w2j_v2_begin_commit_truncate_message_are_ignored() {
    let schema = test_schema();
    let adapter = default_adapter();

    for action in [Action::B, Action::C, Action::T, Action::M] {
        let msg = MessageV2 {
            action,
            schema: Some("public".to_string()),
            table: Some("users".to_string()),
            columns: Some(all_columns(1, "Alice", true)),
            identity: None,
            lsn: None,
        };

        let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
            ChangeSet::new().digest(&msg, &schema, &adapter).unwrap();
        assert!(
            cs.build().is_empty(),
            "non-DML action {action:?} must be ignored"
        );
    }
}

#[test]
fn w2j_v1_unknown_kind_is_ignored() {
    let schema = test_schema();
    let adapter = default_adapter();

    let change = ChangeV1 {
        kind: "unknown".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        columnnames: alloc::vec![],
        columntypes: alloc::vec![],
        columnvalues: alloc::vec![],
        oldkeys: None,
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&change, &schema, &adapter).unwrap();
    assert!(
        cs.build().is_empty(),
        "unknown kind must produce empty output"
    );
}

// -- MessageV2 lsn field ---------------------------------------------------

#[test]
fn w2j_v2_lsn_present_parses() {
    let json = r#"{"action":"I","schema":"public","table":"users","lsn":"0/16B2270","columns":[{"name":"id","type":"integer","value":1}]}"#;
    let msg = parse_v2(json).unwrap();
    assert_eq!(msg.lsn.as_deref(), Some("0/16B2270"));
}

#[test]
fn w2j_v2_lsn_absent_defaults_none() {
    let json = r#"{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1}]}"#;
    let msg = parse_v2(json).unwrap();
    assert_eq!(msg.lsn, None);
}

#[test]
fn w2j_v2_lsn_does_not_affect_digest() {
    let schema = test_schema();
    let adapter = default_adapter();

    let without = MessageV2 {
        action: Action::I,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: Some(all_columns(1, "Alice", true)),
        identity: None,
        lsn: None,
    };
    let with = MessageV2 {
        lsn: Some("0/16B2270".to_string()),
        ..without.clone()
    };

    let cs_without: ChangeSet<TestUsersTable, String, Vec<u8>> = ChangeSet::new()
        .digest(&without, &schema, &adapter)
        .unwrap();
    let cs_with: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&with, &schema, &adapter).unwrap();
    assert_eq!(
        cs_without.build(),
        cs_with.build(),
        "changeset output must be identical regardless of lsn"
    );

    let ps_without: PatchSet<TestUsersTable, String, Vec<u8>> =
        PatchSet::new().digest(&without, &schema, &adapter).unwrap();
    let ps_with: PatchSet<TestUsersTable, String, Vec<u8>> =
        PatchSet::new().digest(&with, &schema, &adapter).unwrap();
    assert_eq!(
        ps_without.build(),
        ps_with.build(),
        "patchset output must be identical regardless of lsn"
    );
}

// -- Changeset UPDATE captures the old-row image ---------------------------
//
// The changeset format stores old and new per column, so the digest must read
// the wal2json old image (v2 `identity`, v1 `oldkeys`), not only the new
// values. A primary-key change depends on the old key reaching the WHERE
// clause.

#[test]
fn w2j_v2_changeset_update_captures_old_pk_on_key_change() {
    let schema = test_schema();
    let adapter = default_adapter();

    // id changes 1 -> 2, identity carries the full old row.
    let msg = MessageV2 {
        action: Action::U,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: Some(all_columns(2, "Alice", true)),
        identity: Some(all_columns(1, "Alice", true)),
        lsn: None,
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&msg, &schema, &adapter).unwrap();
    let ops: Vec<_> = cs.iter().collect();
    assert_eq!(ops.len(), 1);
    match &ops[0] {
        ChangesetOp::Update { values, .. } => {
            assert_eq!(
                values[0].0,
                Some(Value::Integer(1)),
                "old key must be captured"
            );
            assert_eq!(
                values[0].1,
                Some(Value::Integer(2)),
                "new key must be present"
            );
        }
        other => panic!("expected update, got {other:?}"),
    }
}

#[test]
fn w2j_v2_changeset_update_captures_full_old_image() {
    let schema = test_schema();
    let adapter = default_adapter();

    // name changes, identity is the full old row (REPLICA IDENTITY FULL).
    let msg = MessageV2 {
        action: Action::U,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: Some(all_columns(1, "Alicia", true)),
        identity: Some(all_columns(1, "Alice", true)),
        lsn: None,
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&msg, &schema, &adapter).unwrap();
    let ops: Vec<_> = cs.iter().collect();
    match &ops[0] {
        ChangesetOp::Update { values, .. } => {
            assert_eq!(values[0].0, Some(Value::Integer(1)), "old id captured");
            assert_eq!(
                values[1].0,
                Some(Value::Text("Alice".to_string())),
                "old name captured"
            );
            assert_eq!(
                values[1].1,
                Some(Value::Text("Alicia".to_string())),
                "new name present"
            );
        }
        other => panic!("expected update, got {other:?}"),
    }
}

#[test]
fn w2j_v2_changeset_update_default_identity_captures_pk_only() {
    let schema = test_schema();
    let adapter = default_adapter();

    // name changes, identity carries only the primary key (default identity).
    let msg = MessageV2 {
        action: Action::U,
        schema: Some("public".to_string()),
        table: Some("users".to_string()),
        columns: Some(all_columns(1, "Alicia", true)),
        identity: Some(alloc::vec![int_col("id", 1)]),
        lsn: None,
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&msg, &schema, &adapter).unwrap();
    let ops: Vec<_> = cs.iter().collect();
    match &ops[0] {
        ChangesetOp::Update { values, .. } => {
            // Old primary key captured so the WHERE predicate can render.
            assert_eq!(values[0].0, Some(Value::Integer(1)), "old pk captured");
            // Non-key column absent from identity: old stays None, new set.
            assert_eq!(
                values[1].0, None,
                "non-key old absent under default identity"
            );
            assert_eq!(values[1].1, Some(Value::Text("Alicia".to_string())));
        }
        other => panic!("expected update, got {other:?}"),
    }
}

#[test]
fn w2j_v1_changeset_update_captures_old_pk_from_oldkeys() {
    let schema = test_schema();
    let adapter = default_adapter();

    // id changes 1 -> 2, oldkeys carries the old primary key.
    let change = ChangeV1 {
        kind: "update".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        columnnames: alloc::vec!["id".to_string(), "name".to_string(), "active".to_string()],
        columntypes: alloc::vec![
            "integer".to_string(),
            "text".to_string(),
            "boolean".to_string(),
        ],
        columnvalues: all_values(2, "Alice", true),
        oldkeys: Some(OldKeys {
            keynames: alloc::vec!["id".to_string()],
            keytypes: alloc::vec!["integer".to_string()],
            keyvalues: alloc::vec![serde_json::Value::Number(serde_json::Number::from(1_i64))],
        }),
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&change, &schema, &adapter).unwrap();
    let ops: Vec<_> = cs.iter().collect();
    match &ops[0] {
        ChangesetOp::Update { values, .. } => {
            assert_eq!(values[0].0, Some(Value::Integer(1)), "old key from oldkeys");
            assert_eq!(values[0].1, Some(Value::Integer(2)), "new key present");
            assert_eq!(values[1].0, None, "non-key old absent from oldkeys");
        }
        other => panic!("expected update, got {other:?}"),
    }
}

#[test]
fn w2j_v1_changeset_update_non_key_captures_old_pk() {
    // A non-key update: oldkeys carries only the PK, name changes.
    let schema = test_schema();
    let adapter = default_adapter();

    let change = ChangeV1 {
        kind: "update".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        columnnames: alloc::vec!["id".to_string(), "name".to_string(), "active".to_string()],
        columntypes: alloc::vec![
            "integer".to_string(),
            "text".to_string(),
            "boolean".to_string(),
        ],
        columnvalues: all_values(1, "Alicia", true),
        oldkeys: Some(OldKeys {
            keynames: alloc::vec!["id".to_string()],
            keytypes: alloc::vec!["integer".to_string()],
            keyvalues: alloc::vec![serde_json::Value::Number(serde_json::Number::from(1_i64))],
        }),
    };

    let cs: ChangeSet<TestUsersTable, String, Vec<u8>> =
        ChangeSet::new().digest(&change, &schema, &adapter).unwrap();
    let ops: Vec<_> = cs.iter().collect();
    match &ops[0] {
        ChangesetOp::Update { values, .. } => {
            assert_eq!(values[0].0, Some(Value::Integer(1)), "old pk from oldkeys");
            assert_eq!(values[1].0, None, "non-key old absent from oldkeys");
            assert_eq!(values[1].1, Some(Value::Text("Alicia".to_string())));
        }
        other => panic!("expected update, got {other:?}"),
    }
}
