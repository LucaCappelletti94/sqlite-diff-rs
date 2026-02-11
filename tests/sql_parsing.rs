//! Integration tests for SQL digestion into `DiffSetBuilder` via `digest_sql`.
//!
//! These tests verify that `DiffSetBuilder::digest_sql` correctly parses SQL
//! DML statements (INSERT, UPDATE, DELETE) and populates the patchset builder.
//! Schemas are created manually via `SimpleTable::new`.

#![cfg(feature = "testing")]

use sqlite_diff_rs::testing::assert_patchset_sql_parity;
use sqlite_diff_rs::{PatchSet, SimpleTable};

/// Helper: create a `PatchSet` with tables pre-registered.
fn patchset_with(tables: &[SimpleTable]) -> PatchSet<SimpleTable, String, Vec<u8>> {
    let mut ps = PatchSet::new();
    for t in tables {
        ps.add_table(t);
    }
    ps
}

// =============================================================================
// Basic parsing tests
// =============================================================================

#[test]
fn test_digest_simple_insert() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    let mut ps = patchset_with(&[users]);
    ps.digest_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');")
        .unwrap();
    assert_eq!(ps.len(), 1);
    assert!(!ps.is_empty());
    assert!(!ps.build().is_empty());
}

#[test]
fn test_digest_simple_delete() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    let mut ps = patchset_with(&[users]);
    ps.digest_sql("DELETE FROM users WHERE id = 1;").unwrap();
    assert_eq!(ps.len(), 1);
}

#[test]
fn test_digest_simple_update() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    let mut ps = patchset_with(&[users]);
    ps.digest_sql("UPDATE users SET name = 'Bob' WHERE id = 1;")
        .unwrap();
    assert_eq!(ps.len(), 1);
}

#[test]
fn test_digest_multiple_tables() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    let posts = SimpleTable::new("posts", &["id", "user_id", "content"], &[0]);
    let mut ps = patchset_with(&[users, posts]);
    ps.digest_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');")
        .unwrap();
    ps.digest_sql("INSERT INTO posts (id, user_id, content) VALUES (1, 1, 'Hello World');")
        .unwrap();
    assert_eq!(ps.len(), 2);
}

#[test]
fn test_digest_mixed_operations() {
    let users = SimpleTable::new("users", &["id", "name", "age"], &[0]);
    let mut ps = patchset_with(&[users]);
    ps.digest_sql(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);\
         INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);\
         UPDATE users SET age = 31 WHERE id = 1;\
         DELETE FROM users WHERE id = 2;",
    )
    .unwrap();
    // INSERT(1) + UPDATE(1) = INSERT(1) with updated values
    // INSERT(2) + DELETE(2) = cancelled out
    assert_eq!(ps.len(), 1);
}

// =============================================================================
// Error handling tests
// =============================================================================

#[test]
fn test_table_not_registered_error() {
    let mut ps: PatchSet<SimpleTable, String, Vec<u8>> = PatchSet::new();
    let result = ps.digest_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');");
    assert!(result.is_err());
}

#[test]
fn test_invalid_sql_error() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    let mut ps = patchset_with(&[users]);
    let result = ps.digest_sql("THIS IS NOT VALID SQL");
    assert!(result.is_err());
}

#[test]
fn test_create_table_rejected() {
    let mut ps: PatchSet<SimpleTable, String, Vec<u8>> = PatchSet::new();
    let result = ps.digest_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    assert!(result.is_err());
}

#[test]
fn test_unknown_column_error() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    let mut ps = patchset_with(&[users]);
    let result = ps.digest_sql("INSERT INTO users (id, nonexistent) VALUES (1, 'Alice');");
    assert!(result.is_err());
}

// =============================================================================
// Complex value types tests
// =============================================================================

#[test]
fn test_digest_integer_values() {
    let numbers = SimpleTable::new("numbers", &["id", "value"], &[0]);
    let mut ps = patchset_with(&[numbers]);
    ps.digest_sql(
        "INSERT INTO numbers (id, value) VALUES (1, 42);\
         INSERT INTO numbers (id, value) VALUES (2, -100);\
         INSERT INTO numbers (id, value) VALUES (3, 0);",
    )
    .unwrap();
    assert_eq!(ps.len(), 3);
}

#[test]
fn test_digest_real_values() {
    let floats = SimpleTable::new("floats", &["id", "value"], &[0]);
    let mut ps = patchset_with(&[floats]);
    ps.digest_sql(
        "INSERT INTO floats (id, value) VALUES (1, 3.14);\
         INSERT INTO floats (id, value) VALUES (2, -2.5);",
    )
    .unwrap();
    assert_eq!(ps.len(), 2);
}

#[test]
fn test_digest_text_values() {
    let texts = SimpleTable::new("texts", &["id", "value"], &[0]);
    let mut ps = patchset_with(&[texts]);
    ps.digest_sql(
        "INSERT INTO texts (id, value) VALUES (1, 'hello');\
         INSERT INTO texts (id, value) VALUES (2, 'world');\
         INSERT INTO texts (id, value) VALUES (3, '');",
    )
    .unwrap();
    assert_eq!(ps.len(), 3);
}

#[test]
fn test_digest_null_values() {
    let nullable = SimpleTable::new("nullable", &["id", "value"], &[0]);
    let mut ps = patchset_with(&[nullable]);
    ps.digest_sql("INSERT INTO nullable (id, value) VALUES (1, NULL);")
        .unwrap();
    assert_eq!(ps.len(), 1);
}

#[test]
fn test_digest_blob_values() {
    let blobs = SimpleTable::new("blobs", &["id", "data"], &[0]);
    let mut ps = patchset_with(&[blobs]);
    ps.digest_sql("INSERT INTO blobs (id, data) VALUES (1, X'DEADBEEF');")
        .unwrap();
    assert_eq!(ps.len(), 1);
}

// =============================================================================
// Composite primary key tests
// =============================================================================

#[test]
fn test_digest_composite_pk_insert() {
    let composite = SimpleTable::new("composite", &["a", "b", "value"], &[0, 1]);
    let mut ps = patchset_with(&[composite]);
    ps.digest_sql("INSERT INTO composite (a, b, value) VALUES (1, 2, 'test');")
        .unwrap();
    assert_eq!(ps.len(), 1);
}

#[test]
fn test_digest_composite_pk_delete() {
    let composite = SimpleTable::new("composite", &["a", "b", "value"], &[0, 1]);
    let mut ps = patchset_with(&[composite]);
    ps.digest_sql("DELETE FROM composite WHERE a = 1 AND b = 2;")
        .unwrap();
    assert_eq!(ps.len(), 1);
}

#[test]
fn test_digest_composite_pk_update() {
    let composite = SimpleTable::new("composite", &["a", "b", "value"], &[0, 1]);
    let mut ps = patchset_with(&[composite]);
    ps.digest_sql("UPDATE composite SET value = 'updated' WHERE a = 1 AND b = 2;")
        .unwrap();
    assert_eq!(ps.len(), 1);
}

// =============================================================================
// Operation consolidation tests
// =============================================================================

#[test]
fn test_insert_then_delete_cancels() {
    let t = SimpleTable::new("t", &["id", "v"], &[0]);
    let mut ps = patchset_with(&[t]);
    ps.digest_sql(
        "INSERT INTO t (id, v) VALUES (1, 'a');\
         DELETE FROM t WHERE id = 1;",
    )
    .unwrap();
    // INSERT + DELETE on same PK should cancel out
    assert_eq!(ps.len(), 0);
}

#[test]
fn test_insert_then_update_becomes_insert() {
    let t = SimpleTable::new("t", &["id", "v"], &[0]);
    let mut ps = patchset_with(&[t]);
    ps.digest_sql(
        "INSERT INTO t (id, v) VALUES (1, 'a');\
         UPDATE t SET v = 'b' WHERE id = 1;",
    )
    .unwrap();
    // INSERT + UPDATE = INSERT with updated values
    assert_eq!(ps.len(), 1);
}

#[test]
fn test_update_then_update_consolidates() {
    let t = SimpleTable::new("t", &["id", "v"], &[0]);
    let mut ps = patchset_with(&[t]);
    ps.digest_sql(
        "UPDATE t SET v = 'a' WHERE id = 1;\
         UPDATE t SET v = 'b' WHERE id = 1;",
    )
    .unwrap();
    // UPDATE + UPDATE = single UPDATE
    assert_eq!(ps.len(), 1);
}

// =============================================================================
// Edge cases
// =============================================================================

#[test]
fn test_empty_sql() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    let mut ps = patchset_with(&[users]);
    ps.digest_sql("").unwrap();
    assert!(ps.is_empty());
}

#[test]
fn test_semicolons_only() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    let mut ps = patchset_with(&[users]);
    ps.digest_sql(";;;").unwrap();
    assert!(ps.is_empty());
}

#[test]
fn test_multiple_digest_calls() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    let mut ps = patchset_with(&[users]);
    ps.digest_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');")
        .unwrap();
    ps.digest_sql("INSERT INTO users (id, name) VALUES (2, 'Bob');")
        .unwrap();
    assert_eq!(ps.len(), 2);
}

#[test]
fn test_delete_rejects_non_pk_in_where() {
    let users = SimpleTable::new("users", &["id", "name", "status"], &[0]);
    let mut ps = patchset_with(&[users]);
    let result = ps.digest_sql("DELETE FROM users WHERE id = 1 AND status = 'active'");
    assert!(result.is_err());
}

#[test]
fn test_negative_numbers() {
    let t = SimpleTable::new("t", &["a", "b"], &[0]);
    let mut ps = patchset_with(&[t]);
    ps.digest_sql("INSERT INTO t (a, b) VALUES (-42, -3.14);")
        .unwrap();
    assert_eq!(ps.len(), 1);
}

// =============================================================================
// Bit-parity tests against rusqlite (patchset only)
// =============================================================================

#[test]
fn parity_single_insert() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    assert_patchset_sql_parity(
        &[users],
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
        ],
    );
}

#[test]
fn parity_insert_and_update() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    assert_patchset_sql_parity(
        &[users],
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "UPDATE users SET name = 'Alicia' WHERE id = 1",
        ],
    );
}

#[test]
fn parity_multi_table() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    let posts = SimpleTable::new("posts", &["id", "title"], &[0]);
    assert_patchset_sql_parity(
        &[users, posts],
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)",
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "INSERT INTO posts (id, title) VALUES (1, 'Hello')",
        ],
    );
}

#[test]
fn parity_composite_pk() {
    let order_items =
        SimpleTable::new("order_items", &["order_id", "item_id", "quantity"], &[0, 1]);
    assert_patchset_sql_parity(
        &[order_items],
        &[
            "CREATE TABLE order_items (order_id INTEGER, item_id INTEGER, quantity INTEGER, PRIMARY KEY (order_id, item_id))",
            "INSERT INTO order_items (order_id, item_id, quantity) VALUES (1, 100, 5)",
            "INSERT INTO order_items (order_id, item_id, quantity) VALUES (1, 101, 3)",
        ],
    );
}
