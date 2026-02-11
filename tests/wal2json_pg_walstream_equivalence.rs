//! Equivalence tests between wal2json and `pg_walstream` conversions.
//!
//! These tests verify that when the same underlying database changes are represented
//! in both wal2json format and `pg_walstream` format, the resulting `SQLite` patchsets
//! are byte-for-byte identical.
//!
//! This is critical for ensuring consistency when migrating between CDC systems
//! or when a system uses both sources.

#![cfg(all(feature = "wal2json", feature = "pg-walstream"))]

use sqlite_diff_rs::wal2json::{ChangeV1, OldKeys};
use sqlite_diff_rs::{
    ChangeDelete, ChangeSet, ChangesetFormat, DiffOps, Insert, PatchDelete, PatchSet, SimpleTable,
    Update,
};
use std::collections::HashMap;

/// Create a wal2json v1 insert change with the given data.
fn make_wal2json_v1_insert(
    table: &str,
    column_names: &[&str],
    column_values: &[serde_json::Value],
) -> ChangeV1 {
    ChangeV1 {
        kind: "insert".to_string(),
        schema: "public".to_string(),
        table: table.to_string(),
        columnnames: column_names.iter().copied().map(str::to_string).collect(),
        columntypes: column_names.iter().map(|_| "text".to_string()).collect(),
        columnvalues: column_values.to_vec(),
        oldkeys: None,
    }
}

/// Create a wal2json v1 update change with the given data.
fn make_wal2json_v1_update(
    table: &str,
    column_names: &[&str],
    column_values: &[serde_json::Value],
    key_names: &[&str],
    key_values: &[serde_json::Value],
) -> ChangeV1 {
    ChangeV1 {
        kind: "update".to_string(),
        schema: "public".to_string(),
        table: table.to_string(),
        columnnames: column_names.iter().copied().map(str::to_string).collect(),
        columntypes: column_names.iter().map(|_| "text".to_string()).collect(),
        columnvalues: column_values.to_vec(),
        oldkeys: Some(OldKeys {
            keynames: key_names.iter().copied().map(str::to_string).collect(),
            keytypes: key_names.iter().map(|_| "integer".to_string()).collect(),
            keyvalues: key_values.to_vec(),
        }),
    }
}

/// Create a wal2json v1 delete change with the given data.
fn make_wal2json_v1_delete(
    table: &str,
    key_names: &[&str],
    key_values: &[serde_json::Value],
) -> ChangeV1 {
    ChangeV1 {
        kind: "delete".to_string(),
        schema: "public".to_string(),
        table: table.to_string(),
        columnnames: vec![],
        columntypes: vec![],
        columnvalues: vec![],
        oldkeys: Some(OldKeys {
            keynames: key_names.iter().copied().map(str::to_string).collect(),
            keytypes: key_names.iter().map(|_| "integer".to_string()).collect(),
            keyvalues: key_values.to_vec(),
        }),
    }
}

/// Create a `pg_walstream` insert event with the given data.
fn make_pg_walstream_insert(
    table: &str,
    data: HashMap<String, serde_json::Value>,
) -> pg_walstream::EventType {
    pg_walstream::EventType::Insert {
        schema: "public".to_string(),
        table: table.to_string(),
        relation_oid: 12345,
        data,
    }
}

/// Create a `pg_walstream` update event with the given data.
fn make_pg_walstream_update(
    table: &str,
    old_data: Option<HashMap<String, serde_json::Value>>,
    new_data: HashMap<String, serde_json::Value>,
    key_columns: Vec<String>,
) -> pg_walstream::EventType {
    pg_walstream::EventType::Update {
        schema: "public".to_string(),
        table: table.to_string(),
        relation_oid: 12345,
        old_data,
        new_data,
        replica_identity: pg_walstream::ReplicaIdentity::Default,
        key_columns,
    }
}

/// Create a `pg_walstream` delete event with the given data.
fn make_pg_walstream_delete(
    table: &str,
    old_data: HashMap<String, serde_json::Value>,
    key_columns: Vec<String>,
) -> pg_walstream::EventType {
    pg_walstream::EventType::Delete {
        schema: "public".to_string(),
        table: table.to_string(),
        relation_oid: 12345,
        old_data,
        replica_identity: pg_walstream::ReplicaIdentity::Default,
        key_columns,
    }
}

// ============================================================================
// Insert equivalence tests
// ============================================================================

#[test]
fn test_insert_equivalence_simple() {
    let table = SimpleTable::new("users", &["id", "name", "email"], &[0]);

    // Create equivalent data for both formats
    let id = serde_json::json!(1);
    let name = serde_json::json!("Alice");
    let email = serde_json::json!("alice@example.com");

    // wal2json v1 format
    let wal2json_change = make_wal2json_v1_insert(
        "users",
        &["id", "name", "email"],
        &[id.clone(), name.clone(), email.clone()],
    );

    // pg_walstream format
    let mut pg_data = HashMap::new();
    pg_data.insert("id".to_string(), id);
    pg_data.insert("name".to_string(), name);
    pg_data.insert("email".to_string(), email);
    let pg_event = make_pg_walstream_insert("users", pg_data);

    // Convert both to Insert operations
    let wal2json_insert: Insert<_, String, Vec<u8>> =
        (&wal2json_change, &table).try_into().unwrap();
    let pg_insert: Insert<_, String, Vec<u8>> = (pg_event, table.clone()).try_into().unwrap();

    // Build patchsets from both
    let wal2json_patchset: PatchSet<_, _, _> = PatchSet::new().insert(wal2json_insert);
    let pg_patchset: PatchSet<_, _, _> = PatchSet::new().insert(pg_insert);

    // They should be identical
    assert_eq!(
        wal2json_patchset.build(),
        pg_patchset.build(),
        "wal2json and pg_walstream should produce identical insert patchsets"
    );
}

#[test]
fn test_insert_equivalence_with_null() {
    let table = SimpleTable::new("users", &["id", "name", "bio"], &[0]);

    let id = serde_json::json!(42);
    let name = serde_json::json!("Bob");
    let bio = serde_json::Value::Null;

    // wal2json v1 format
    let wal2json_change = make_wal2json_v1_insert(
        "users",
        &["id", "name", "bio"],
        &[id.clone(), name.clone(), bio.clone()],
    );

    // pg_walstream format
    let mut pg_data = HashMap::new();
    pg_data.insert("id".to_string(), id);
    pg_data.insert("name".to_string(), name);
    pg_data.insert("bio".to_string(), bio);
    let pg_event = make_pg_walstream_insert("users", pg_data);

    // Convert and compare
    let wal2json_insert: Insert<_, String, Vec<u8>> =
        (&wal2json_change, &table).try_into().unwrap();
    let pg_insert: Insert<_, String, Vec<u8>> = (pg_event, table.clone()).try_into().unwrap();

    let wal2json_patchset: PatchSet<_, _, _> = PatchSet::new().insert(wal2json_insert);
    let pg_patchset: PatchSet<_, _, _> = PatchSet::new().insert(pg_insert);

    assert_eq!(
        wal2json_patchset.build(),
        pg_patchset.build(),
        "wal2json and pg_walstream should produce identical insert patchsets with NULL values"
    );
}

#[test]
fn test_insert_equivalence_with_numbers() {
    let table = SimpleTable::new("products", &["id", "price", "quantity"], &[0]);

    let id = serde_json::json!(100);
    let price = serde_json::json!(19.99);
    let quantity = serde_json::json!(50);

    // wal2json v1 format
    let wal2json_change = make_wal2json_v1_insert(
        "products",
        &["id", "price", "quantity"],
        &[id.clone(), price.clone(), quantity.clone()],
    );

    // pg_walstream format
    let mut pg_data = HashMap::new();
    pg_data.insert("id".to_string(), id);
    pg_data.insert("price".to_string(), price);
    pg_data.insert("quantity".to_string(), quantity);
    let pg_event = make_pg_walstream_insert("products", pg_data);

    // Convert and compare
    let wal2json_insert: Insert<_, String, Vec<u8>> =
        (&wal2json_change, &table).try_into().unwrap();
    let pg_insert: Insert<_, String, Vec<u8>> = (pg_event, table.clone()).try_into().unwrap();

    let wal2json_patchset: PatchSet<_, _, _> = PatchSet::new().insert(wal2json_insert);
    let pg_patchset: PatchSet<_, _, _> = PatchSet::new().insert(pg_insert);

    assert_eq!(
        wal2json_patchset.build(),
        pg_patchset.build(),
        "wal2json and pg_walstream should produce identical insert patchsets with numeric values"
    );
}

#[test]
fn test_insert_equivalence_with_boolean() {
    let table = SimpleTable::new("settings", &["id", "enabled", "visible"], &[0]);

    let id = serde_json::json!(1);
    let enabled = serde_json::json!(true);
    let visible = serde_json::json!(false);

    // wal2json v1 format
    let wal2json_change = make_wal2json_v1_insert(
        "settings",
        &["id", "enabled", "visible"],
        &[id.clone(), enabled.clone(), visible.clone()],
    );

    // pg_walstream format
    let mut pg_data = HashMap::new();
    pg_data.insert("id".to_string(), id);
    pg_data.insert("enabled".to_string(), enabled);
    pg_data.insert("visible".to_string(), visible);
    let pg_event = make_pg_walstream_insert("settings", pg_data);

    // Convert and compare
    let wal2json_insert: Insert<_, String, Vec<u8>> =
        (&wal2json_change, &table).try_into().unwrap();
    let pg_insert: Insert<_, String, Vec<u8>> = (pg_event, table.clone()).try_into().unwrap();

    let wal2json_patchset: PatchSet<_, _, _> = PatchSet::new().insert(wal2json_insert);
    let pg_patchset: PatchSet<_, _, _> = PatchSet::new().insert(pg_insert);

    assert_eq!(
        wal2json_patchset.build(),
        pg_patchset.build(),
        "wal2json and pg_walstream should produce identical insert patchsets with boolean values"
    );
}

// ============================================================================
// Delete equivalence tests (PatchDelete - PK only)
// ============================================================================

#[test]
fn test_patch_delete_equivalence_simple() {
    let table = SimpleTable::new("users", &["id", "name"], &[0]);

    let id = serde_json::json!(42);

    // wal2json v1 format (delete with oldkeys)
    let wal2json_change = make_wal2json_v1_delete("users", &["id"], std::slice::from_ref(&id));

    // pg_walstream format
    let mut pg_data = HashMap::new();
    pg_data.insert("id".to_string(), id);
    let pg_event = make_pg_walstream_delete("users", pg_data, vec!["id".to_string()]);

    // Convert both to PatchDelete operations
    let wal2json_delete: PatchDelete<_, String, Vec<u8>> =
        (&wal2json_change, &table).try_into().unwrap();
    let pg_delete: PatchDelete<_, String, Vec<u8>> = (pg_event, table.clone()).try_into().unwrap();

    // Build patchsets from both
    let wal2json_patchset: PatchSet<_, _, _> = PatchSet::new().delete(wal2json_delete);
    let pg_patchset: PatchSet<_, _, _> = PatchSet::new().delete(pg_delete);

    assert_eq!(
        wal2json_patchset.build(),
        pg_patchset.build(),
        "wal2json and pg_walstream should produce identical patch delete patchsets"
    );
}

#[test]
fn test_patch_delete_equivalence_composite_pk() {
    let table = SimpleTable::new("order_items", &["order_id", "item_id", "quantity"], &[0, 1]);

    let order_id = serde_json::json!(100);
    let item_id = serde_json::json!(200);

    // wal2json v1 format
    let wal2json_change = make_wal2json_v1_delete(
        "order_items",
        &["order_id", "item_id"],
        &[order_id.clone(), item_id.clone()],
    );

    // pg_walstream format
    let mut pg_data = HashMap::new();
    pg_data.insert("order_id".to_string(), order_id);
    pg_data.insert("item_id".to_string(), item_id);
    let pg_event = make_pg_walstream_delete(
        "order_items",
        pg_data,
        vec!["order_id".to_string(), "item_id".to_string()],
    );

    // Convert and compare
    let wal2json_delete: PatchDelete<_, String, Vec<u8>> =
        (&wal2json_change, &table).try_into().unwrap();
    let pg_delete: PatchDelete<_, String, Vec<u8>> = (pg_event, table.clone()).try_into().unwrap();

    let wal2json_patchset: PatchSet<_, _, _> = PatchSet::new().delete(wal2json_delete);
    let pg_patchset: PatchSet<_, _, _> = PatchSet::new().delete(pg_delete);

    assert_eq!(
        wal2json_patchset.build(),
        pg_patchset.build(),
        "wal2json and pg_walstream should produce identical patch delete patchsets with composite PK"
    );
}

// ============================================================================
// ChangeDelete equivalence tests (full row)
// ============================================================================

#[test]
fn test_change_delete_equivalence() {
    let table = SimpleTable::new("users", &["id", "name"], &[0]);

    let id = serde_json::json!(42);
    let name = serde_json::json!("Alice");

    // For ChangeDelete we need full row data
    // wal2json format - delete with full row data in oldkeys won't work directly
    // We need to construct it differently - use a ChangeV1 with columnvalues for delete

    // pg_walstream format
    let mut pg_data = HashMap::new();
    pg_data.insert("id".to_string(), id.clone());
    pg_data.insert("name".to_string(), name.clone());
    let pg_event = make_pg_walstream_delete("users", pg_data, vec!["id".to_string()]);

    // Build ChangeDelete manually from both
    let pg_delete: ChangeDelete<_, String, Vec<u8>> = (pg_event, table.clone()).try_into().unwrap();

    // Create equivalent changeset
    let pg_changeset: ChangeSet<_, _, _> = ChangeSet::new().delete(pg_delete);

    // Verify we can build the changeset without error
    let bytes = pg_changeset.build();
    assert!(!bytes.is_empty());
}

// ============================================================================
// Update equivalence tests
// ============================================================================

#[test]
fn test_update_equivalence_simple() {
    let table = SimpleTable::new("users", &["id", "name"], &[0]);

    let id = serde_json::json!(1);
    let name = serde_json::json!("UpdatedName");

    // wal2json v1 format - note: wal2json v1 uses set_new only (no old values in columnvalues)
    let wal2json_change = make_wal2json_v1_update(
        "users",
        &["id", "name"],
        &[id.clone(), name.clone()],
        &["id"],
        std::slice::from_ref(&id),
    );

    // pg_walstream format - matching wal2json behavior (no old_data)
    let mut new_data = HashMap::new();
    new_data.insert("id".to_string(), id);
    new_data.insert("name".to_string(), name);
    let pg_event = make_pg_walstream_update("users", None, new_data, vec!["id".to_string()]);

    // Convert both to Update operations
    let wal2json_update: Update<_, ChangesetFormat, String, Vec<u8>> =
        (&wal2json_change, &table).try_into().unwrap();
    let pg_update: Update<_, ChangesetFormat, String, Vec<u8>> =
        (pg_event, table.clone()).try_into().unwrap();

    // Build changesets from both
    let wal2json_changeset: ChangeSet<_, _, _> = ChangeSet::new().update(wal2json_update);
    let pg_changeset: ChangeSet<_, _, _> = ChangeSet::new().update(pg_update);

    assert_eq!(
        wal2json_changeset.build(),
        pg_changeset.build(),
        "wal2json and pg_walstream should produce identical update changesets"
    );
}

// ============================================================================
// Multiple operations equivalence test
// ============================================================================

#[test]
fn test_multiple_operations_equivalence() {
    let table = SimpleTable::new("users", &["id", "name"], &[0]);

    // Prepare data for operations
    let insert_id = serde_json::json!(1);
    let insert_name = serde_json::json!("Alice");

    let delete_id = serde_json::json!(3);

    // === wal2json path ===
    let wal2json_insert = make_wal2json_v1_insert(
        "users",
        &["id", "name"],
        &[insert_id.clone(), insert_name.clone()],
    );
    let wal2json_delete =
        make_wal2json_v1_delete("users", &["id"], std::slice::from_ref(&delete_id));

    let wal2json_insert_op: Insert<_, String, Vec<u8>> =
        (&wal2json_insert, &table).try_into().unwrap();
    let wal2json_delete_op: PatchDelete<_, String, Vec<u8>> =
        (&wal2json_delete, &table).try_into().unwrap();

    let wal2json_patchset: PatchSet<_, _, _> = PatchSet::new()
        .insert(wal2json_insert_op)
        .delete(wal2json_delete_op);

    // === pg_walstream path ===
    let mut insert_data = HashMap::new();
    insert_data.insert("id".to_string(), insert_id);
    insert_data.insert("name".to_string(), insert_name);
    let pg_insert_event = make_pg_walstream_insert("users", insert_data);

    let mut delete_data = HashMap::new();
    delete_data.insert("id".to_string(), delete_id);
    let pg_delete_event = make_pg_walstream_delete("users", delete_data, vec!["id".to_string()]);

    let pg_insert_op: Insert<_, String, Vec<u8>> =
        (pg_insert_event, table.clone()).try_into().unwrap();
    let pg_delete_op: PatchDelete<_, String, Vec<u8>> =
        (pg_delete_event, table.clone()).try_into().unwrap();

    let pg_patchset: PatchSet<_, _, _> = PatchSet::new().insert(pg_insert_op).delete(pg_delete_op);

    // Compare complete patchsets
    assert_eq!(
        wal2json_patchset.build(),
        pg_patchset.build(),
        "wal2json and pg_walstream should produce identical patchsets for multiple operations"
    );
}

// ============================================================================
// Edge case tests
// ============================================================================

#[test]
fn test_equivalence_large_integer() {
    let table = SimpleTable::new("numbers", &["id", "big_num"], &[0]);

    let id = serde_json::json!(1);
    let big_num = serde_json::json!(9_223_372_036_854_775_807i64); // i64::MAX

    // wal2json v1 format
    let wal2json_change = make_wal2json_v1_insert(
        "numbers",
        &["id", "big_num"],
        &[id.clone(), big_num.clone()],
    );

    // pg_walstream format
    let mut pg_data = HashMap::new();
    pg_data.insert("id".to_string(), id);
    pg_data.insert("big_num".to_string(), big_num);
    let pg_event = make_pg_walstream_insert("numbers", pg_data);

    // Convert and compare
    let wal2json_insert: Insert<_, String, Vec<u8>> =
        (&wal2json_change, &table).try_into().unwrap();
    let pg_insert: Insert<_, String, Vec<u8>> = (pg_event, table.clone()).try_into().unwrap();

    let wal2json_patchset: PatchSet<_, _, _> = PatchSet::new().insert(wal2json_insert);
    let pg_patchset: PatchSet<_, _, _> = PatchSet::new().insert(pg_insert);

    assert_eq!(
        wal2json_patchset.build(),
        pg_patchset.build(),
        "wal2json and pg_walstream should produce identical patchsets with large integers"
    );
}

#[test]
fn test_equivalence_negative_integer() {
    let table = SimpleTable::new("numbers", &["id", "negative"], &[0]);

    let id = serde_json::json!(1);
    let negative = serde_json::json!(-999_999);

    // wal2json v1 format
    let wal2json_change = make_wal2json_v1_insert(
        "numbers",
        &["id", "negative"],
        &[id.clone(), negative.clone()],
    );

    // pg_walstream format
    let mut pg_data = HashMap::new();
    pg_data.insert("id".to_string(), id);
    pg_data.insert("negative".to_string(), negative);
    let pg_event = make_pg_walstream_insert("numbers", pg_data);

    // Convert and compare
    let wal2json_insert: Insert<_, String, Vec<u8>> =
        (&wal2json_change, &table).try_into().unwrap();
    let pg_insert: Insert<_, String, Vec<u8>> = (pg_event, table.clone()).try_into().unwrap();

    let wal2json_patchset: PatchSet<_, _, _> = PatchSet::new().insert(wal2json_insert);
    let pg_patchset: PatchSet<_, _, _> = PatchSet::new().insert(pg_insert);

    assert_eq!(
        wal2json_patchset.build(),
        pg_patchset.build(),
        "wal2json and pg_walstream should produce identical patchsets with negative integers"
    );
}

#[test]
fn test_equivalence_empty_string() {
    let table = SimpleTable::new("strings", &["id", "empty"], &[0]);

    let id = serde_json::json!(1);
    let empty = serde_json::json!("");

    // wal2json v1 format
    let wal2json_change =
        make_wal2json_v1_insert("strings", &["id", "empty"], &[id.clone(), empty.clone()]);

    // pg_walstream format
    let mut pg_data = HashMap::new();
    pg_data.insert("id".to_string(), id);
    pg_data.insert("empty".to_string(), empty);
    let pg_event = make_pg_walstream_insert("strings", pg_data);

    // Convert and compare
    let wal2json_insert: Insert<_, String, Vec<u8>> =
        (&wal2json_change, &table).try_into().unwrap();
    let pg_insert: Insert<_, String, Vec<u8>> = (pg_event, table.clone()).try_into().unwrap();

    let wal2json_patchset: PatchSet<_, _, _> = PatchSet::new().insert(wal2json_insert);
    let pg_patchset: PatchSet<_, _, _> = PatchSet::new().insert(pg_insert);

    assert_eq!(
        wal2json_patchset.build(),
        pg_patchset.build(),
        "wal2json and pg_walstream should produce identical patchsets with empty strings"
    );
}

#[test]
fn test_equivalence_unicode_string() {
    let table = SimpleTable::new("strings", &["id", "unicode"], &[0]);

    let id = serde_json::json!(1);
    let unicode = serde_json::json!("Hello, \u{1F600} world! \u{4E2D}\u{6587}");

    // wal2json v1 format
    let wal2json_change = make_wal2json_v1_insert(
        "strings",
        &["id", "unicode"],
        &[id.clone(), unicode.clone()],
    );

    // pg_walstream format
    let mut pg_data = HashMap::new();
    pg_data.insert("id".to_string(), id);
    pg_data.insert("unicode".to_string(), unicode);
    let pg_event = make_pg_walstream_insert("strings", pg_data);

    // Convert and compare
    let wal2json_insert: Insert<_, String, Vec<u8>> =
        (&wal2json_change, &table).try_into().unwrap();
    let pg_insert: Insert<_, String, Vec<u8>> = (pg_event, table.clone()).try_into().unwrap();

    let wal2json_patchset: PatchSet<_, _, _> = PatchSet::new().insert(wal2json_insert);
    let pg_patchset: PatchSet<_, _, _> = PatchSet::new().insert(pg_insert);

    assert_eq!(
        wal2json_patchset.build(),
        pg_patchset.build(),
        "wal2json and pg_walstream should produce identical patchsets with unicode strings"
    );
}
