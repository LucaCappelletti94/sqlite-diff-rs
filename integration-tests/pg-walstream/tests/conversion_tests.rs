//! Integration tests for pg_walstream event conversion to sqlite-diff-rs changesets.
//!
//! These tests verify that pg_walstream EventType and ChangeEvent can be correctly
//! converted to sqlite-diff-rs Insert, Update, ChangeDelete, and PatchDelete operations.

use pg_walstream_integration_tests::{
    hashmap, make_delete_event, make_insert_event, make_update_event, wrap_in_change_event,
};
use serde_json::json;
use sqlite_diff_rs::pg_walstream::ConversionError;
use sqlite_diff_rs::{ChangeDelete, ChangesetFormat, Insert, PatchDelete, SimpleTable, Update};

// ============================================================================
// Insert conversion tests
// ============================================================================

#[test]
fn test_insert_simple() {
    let table = SimpleTable::new("users", &["id", "name", "email"], &[0]);
    let event = make_insert_event(
        "public",
        "users",
        hashmap! {
            "id" => json!(1),
            "name" => json!("Alice"),
            "email" => json!("alice@example.com"),
        },
    );

    let _insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
}

#[test]
fn test_insert_with_null() {
    let table = SimpleTable::new("users", &["id", "name", "bio"], &[0]);
    let event = make_insert_event(
        "public",
        "users",
        hashmap! {
            "id" => json!(1),
            "name" => json!("Bob"),
            "bio" => serde_json::Value::Null,
        },
    );

    let _insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
}

#[test]
fn test_insert_with_float() {
    let table = SimpleTable::new("products", &["id", "price"], &[0]);
    let event = make_insert_event(
        "public",
        "products",
        hashmap! {
            "id" => json!(1),
            "price" => json!(19.99),
        },
    );

    let _insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
}

#[test]
fn test_insert_with_boolean() {
    let table = SimpleTable::new("settings", &["id", "enabled"], &[0]);
    let event = make_insert_event(
        "public",
        "settings",
        hashmap! {
            "id" => json!(1),
            "enabled" => json!(true),
        },
    );

    let _insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
}

// ============================================================================
// Update conversion tests
// ============================================================================

#[test]
fn test_update_with_old_data() {
    let table = SimpleTable::new("users", &["id", "name"], &[0]);
    let event = make_update_event(
        "public",
        "users",
        Some(hashmap! {
            "id" => json!(1),
            "name" => json!("Alice"),
        }),
        hashmap! {
            "id" => json!(1),
            "name" => json!("Alicia"),
        },
        vec!["id".into()],
    );

    let _update: Update<_, ChangesetFormat, String, Vec<u8>> = (event, table).try_into().unwrap();
}

#[test]
fn test_update_without_old_data() {
    let table = SimpleTable::new("users", &["id", "name"], &[0]);
    let event = make_update_event(
        "public",
        "users",
        None, // No old data (replica identity = nothing)
        hashmap! {
            "id" => json!(1),
            "name" => json!("Bob"),
        },
        vec!["id".into()],
    );

    let _update: Update<_, ChangesetFormat, String, Vec<u8>> = (event, table).try_into().unwrap();
}

// ============================================================================
// Delete conversion tests
// ============================================================================

#[test]
fn test_delete_changeset() {
    let table = SimpleTable::new("users", &["id", "name", "email"], &[0]);
    let event = make_delete_event(
        "public",
        "users",
        hashmap! {
            "id" => json!(42),
            "name" => json!("Charlie"),
            "email" => json!("charlie@example.com"),
        },
        vec!["id".into()],
    );

    let _delete: ChangeDelete<_, String, Vec<u8>> = (event, table).try_into().unwrap();
}

#[test]
fn test_delete_patchset() {
    let table = SimpleTable::new("users", &["id", "name"], &[0]);
    let event = make_delete_event(
        "public",
        "users",
        hashmap! {
            "id" => json!(99),
            "name" => json!("Dave"),
        },
        vec!["id".into()],
    );

    let _delete: PatchDelete<_, String, Vec<u8>> = (event, table).try_into().unwrap();
}

// ============================================================================
// ChangeEvent wrapper tests
// ============================================================================

#[test]
fn test_change_event_insert() {
    let table = SimpleTable::new("logs", &["id", "message"], &[0]);
    let event_type = make_insert_event(
        "public",
        "logs",
        hashmap! {
            "id" => json!(1),
            "message" => json!("Hello, world!"),
        },
    );
    let change_event = wrap_in_change_event(event_type, 0x1234_5678);

    let _insert: Insert<_, String, Vec<u8>> = (change_event, table).try_into().unwrap();
}

// ============================================================================
// Error handling tests
// ============================================================================

#[test]
fn test_table_mismatch_error() {
    let table = SimpleTable::new("products", &["id", "name"], &[0]);
    let event = make_insert_event(
        "public",
        "users", // Wrong table name
        hashmap! {
            "id" => json!(1),
        },
    );

    let result: Result<Insert<_, String, Vec<u8>>, _> = (event, table).try_into();
    assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
}

#[test]
fn test_column_not_found_error() {
    let table = SimpleTable::new("users", &["id", "name"], &[0]);
    let event = make_insert_event(
        "public",
        "users",
        hashmap! {
            "id" => json!(1),
            "unknown_column" => json!("value"),
        },
    );

    let result: Result<Insert<_, String, Vec<u8>>, _> = (event, table).try_into();
    assert!(matches!(result, Err(ConversionError::ColumnNotFound(_))));
}

#[test]
fn test_invalid_event_type_error() {
    let table = SimpleTable::new("users", &["id", "name"], &[0]);
    let event = make_insert_event(
        "public",
        "users",
        hashmap! {
            "id" => json!(1),
        },
    );

    // Try to convert Insert to Delete
    let result: Result<ChangeDelete<_, String, Vec<u8>>, _> = (event, table).try_into();
    assert!(matches!(result, Err(ConversionError::InvalidEventType(_))));
}

#[test]
fn test_unsupported_json_array() {
    let table = SimpleTable::new("data", &["id", "tags"], &[0]);
    let event = make_insert_event(
        "public",
        "data",
        hashmap! {
            "id" => json!(1),
            "tags" => json!(["a", "b", "c"]), // Arrays not supported
        },
    );

    let result: Result<Insert<_, String, Vec<u8>>, _> = (event, table).try_into();
    assert!(matches!(result, Err(ConversionError::UnsupportedType(_))));
}

#[test]
fn test_unsupported_json_object() {
    let table = SimpleTable::new("data", &["id", "metadata"], &[0]);
    let event = make_insert_event(
        "public",
        "data",
        hashmap! {
            "id" => json!(1),
            "metadata" => json!({"key": "value"}), // Objects not supported
        },
    );

    let result: Result<Insert<_, String, Vec<u8>>, _> = (event, table).try_into();
    assert!(matches!(result, Err(ConversionError::UnsupportedType(_))));
}

// ============================================================================
// Complex data type tests
// ============================================================================

#[test]
fn test_large_integer() {
    let table = SimpleTable::new("numbers", &["id", "big_num"], &[0]);
    let event = make_insert_event(
        "public",
        "numbers",
        hashmap! {
            "id" => json!(1),
            "big_num" => json!(9_223_372_036_854_775_807i64), // i64::MAX
        },
    );

    let _insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
}

#[test]
fn test_negative_integer() {
    let table = SimpleTable::new("numbers", &["id", "negative"], &[0]);
    let event = make_insert_event(
        "public",
        "numbers",
        hashmap! {
            "id" => json!(1),
            "negative" => json!(-42),
        },
    );

    let _insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
}

#[test]
fn test_empty_string() {
    let table = SimpleTable::new("strings", &["id", "empty"], &[0]);
    let event = make_insert_event(
        "public",
        "strings",
        hashmap! {
            "id" => json!(1),
            "empty" => json!(""),
        },
    );

    let _insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
}

#[test]
fn test_unicode_string() {
    let table = SimpleTable::new("strings", &["id", "unicode"], &[0]);
    let event = make_insert_event(
        "public",
        "strings",
        hashmap! {
            "id" => json!(1),
            "unicode" => json!("Hello, \u{1F600} world! \u{4E2D}\u{6587}"),
        },
    );

    let _insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
}

// ============================================================================
// Composite primary key tests
// ============================================================================

#[test]
fn test_composite_pk_insert() {
    let table = SimpleTable::new("order_items", &["order_id", "item_id", "quantity"], &[0, 1]);
    let event = make_insert_event(
        "public",
        "order_items",
        hashmap! {
            "order_id" => json!(100),
            "item_id" => json!(200),
            "quantity" => json!(5),
        },
    );

    let _insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
}

#[test]
fn test_composite_pk_delete() {
    let table = SimpleTable::new("order_items", &["order_id", "item_id", "quantity"], &[0, 1]);
    let event = make_delete_event(
        "public",
        "order_items",
        hashmap! {
            "order_id" => json!(100),
            "item_id" => json!(200),
            "quantity" => json!(5),
        },
        vec!["order_id".into(), "item_id".into()],
    );

    let _delete: PatchDelete<_, String, Vec<u8>> = (event, table).try_into().unwrap();
}
