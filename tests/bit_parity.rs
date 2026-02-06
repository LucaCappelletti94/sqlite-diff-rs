//! Bit-for-bit parity tests between our library and rusqlite's session extension.
//!
//! These tests verify that our changeset/patchset binary output is **byte-identical**
//! to what SQLite's session extension produces for the same sequence of operations.
//!
//! Previous tests only compared at the semantic level (via `ParsedDiffSet::PartialEq`),
//! which uses `HashMap` comparison and thus ignores:
//! - Table ordering differences
//! - Row ordering differences within a table
//!
//! This file catches both classes of bug by comparing raw `Vec<u8>` output.
#![cfg(feature = "testing")]

use sqlite_diff_rs::testing::{assert_bit_parity, assert_fromstr_bit_parity, parse_schema};
use sqlite_diff_rs::{
    ChangeDelete, ChangeSet, ChangesetFormat, Insert, PatchSet, PatchsetFormat, Update,
};

use sqlparser::ast::CreateTable;

// =============================================================================
// Single-table, single-operation tests
// =============================================================================

#[test]
fn bit_parity_single_insert() {
    let sql = "\
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\
        INSERT INTO users (id, name) VALUES (1, 'Alice');";
    assert_fromstr_bit_parity(sql);
}

#[test]
fn bit_parity_single_insert_integer_only() {
    let sql = "\
        CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER);\
        INSERT INTO nums (id, val) VALUES (1, 42);";
    assert_fromstr_bit_parity(sql);
}

#[test]
fn bit_parity_single_insert_with_null() {
    let sql = "\
        CREATE TABLE items (id INTEGER PRIMARY KEY, description TEXT, price REAL);\
        INSERT INTO items (id, description, price) VALUES (1, NULL, 9.99);";
    assert_fromstr_bit_parity(sql);
}

#[test]
fn bit_parity_single_insert_with_real() {
    let sql = "\
        CREATE TABLE measurements (id INTEGER PRIMARY KEY, value REAL);\
        INSERT INTO measurements (id, value) VALUES (1, 3.14);";
    assert_fromstr_bit_parity(sql);
}

#[test]
fn bit_parity_single_insert_empty_string() {
    let sql = "\
        CREATE TABLE strings (id INTEGER PRIMARY KEY, value TEXT);\
        INSERT INTO strings (id, value) VALUES (1, '');";
    assert_fromstr_bit_parity(sql);
}

// =============================================================================
// Multiple rows in a single table (tests row ordering)
// =============================================================================

#[test]
fn bit_parity_two_inserts_same_table() {
    let sql = "\
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\
        INSERT INTO users (id, name) VALUES (1, 'Alice');\
        INSERT INTO users (id, name) VALUES (2, 'Bob');";
    assert_fromstr_bit_parity(sql);
}

#[test]
fn bit_parity_three_inserts_same_table() {
    let sql = "\
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);\
        INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);\
        INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);\
        INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);";
    assert_fromstr_bit_parity(sql);
}

#[test]
fn bit_parity_insert_then_update() {
    let sql = "\
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\
        INSERT INTO users (id, name) VALUES (1, 'Alice');\
        UPDATE users SET name = 'Alicia' WHERE id = 1;";
    assert_fromstr_bit_parity(sql);
}

#[test]
fn bit_parity_insert_then_delete_cancel() {
    let sql = "\
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\
        INSERT INTO users (id, name) VALUES (1, 'Alice');\
        DELETE FROM users WHERE id = 1;";
    assert_fromstr_bit_parity(sql);
}

#[test]
fn bit_parity_two_inserts_one_deleted() {
    let sql = "\
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\
        INSERT INTO users (id, name) VALUES (1, 'Alice');\
        INSERT INTO users (id, name) VALUES (2, 'Bob');\
        DELETE FROM users WHERE id = 1;";
    assert_fromstr_bit_parity(sql);
}

// =============================================================================
// Multi-table tests (tests table ordering)
// =============================================================================

#[test]
fn bit_parity_two_tables() {
    let sql = "\
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\
        CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT);\
        INSERT INTO users (id, name) VALUES (1, 'Alice');\
        INSERT INTO posts (id, title) VALUES (1, 'Hello');";
    assert_fromstr_bit_parity(sql);
}

#[test]
fn bit_parity_two_tables_reverse_order() {
    // Insert into posts first, then users
    let sql = "\
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\
        CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT);\
        INSERT INTO posts (id, title) VALUES (1, 'Hello');\
        INSERT INTO users (id, name) VALUES (1, 'Alice');";
    assert_fromstr_bit_parity(sql);
}

#[test]
fn bit_parity_three_tables() {
    let sql = "\
        CREATE TABLE alpha (id INTEGER PRIMARY KEY, val TEXT);\
        CREATE TABLE beta (id INTEGER PRIMARY KEY, val TEXT);\
        CREATE TABLE gamma (id INTEGER PRIMARY KEY, val TEXT);\
        INSERT INTO alpha (id, val) VALUES (1, 'a');\
        INSERT INTO beta (id, val) VALUES (1, 'b');\
        INSERT INTO gamma (id, val) VALUES (1, 'c');";
    assert_fromstr_bit_parity(sql);
}

#[test]
fn bit_parity_table_cancel_and_readd() {
    let sql = "\
        CREATE TABLE table_a (id INTEGER PRIMARY KEY, val TEXT);\
        CREATE TABLE table_b (id INTEGER PRIMARY KEY, val TEXT);\
        INSERT INTO table_a (id, val) VALUES (1, 'a1');\
        INSERT INTO table_b (id, val) VALUES (1, 'b1');\
        DELETE FROM table_a WHERE id = 1;\
        INSERT INTO table_a (id, val) VALUES (2, 'a2');";
    assert_fromstr_bit_parity(sql);
}

#[test]
fn bit_parity_cancelled_table_excluded() {
    let sql = "\
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\
        CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT);\
        INSERT INTO users (id, name) VALUES (1, 'Alice');\
        DELETE FROM users WHERE id = 1;\
        INSERT INTO posts (id, title) VALUES (1, 'Hello');";
    assert_fromstr_bit_parity(sql);
}

// =============================================================================
// Composite primary keys
// =============================================================================

#[test]
fn bit_parity_composite_pk() {
    let sql = "\
        CREATE TABLE order_items (\
            order_id INTEGER,\
            item_id INTEGER,\
            quantity INTEGER,\
            PRIMARY KEY (order_id, item_id)\
        );\
        INSERT INTO order_items (order_id, item_id, quantity) VALUES (1, 100, 5);\
        INSERT INTO order_items (order_id, item_id, quantity) VALUES (1, 101, 3);";
    assert_fromstr_bit_parity(sql);
}

// =============================================================================
// Builder API parity (not going through FromStr)
// =============================================================================

#[test]
fn bit_parity_builder_single_insert() {
    let schema = parse_schema("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

    let changeset: ChangeSet<&CreateTable> = ChangeSet::new().insert(
        Insert::from(&schema)
            .set(0, 1i64)
            .unwrap()
            .set(1, "Alice")
            .unwrap(),
    );
    let our_changeset: Vec<u8> = changeset.into();

    let patchset: PatchSet<&CreateTable> = PatchSet::new().insert(
        Insert::from(&schema)
            .set(0, 1i64)
            .unwrap()
            .set(1, "Alice")
            .unwrap(),
    );
    let our_patchset: Vec<u8> = patchset.into();

    assert_bit_parity(
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
        ],
        &our_changeset,
        &our_patchset,
    );
}

#[test]
fn bit_parity_builder_two_inserts() {
    let schema = parse_schema("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

    let changeset: ChangeSet<&CreateTable> = ChangeSet::new()
        .insert(
            Insert::from(&schema)
                .set(0, 1i64)
                .unwrap()
                .set(1, "Alice")
                .unwrap(),
        )
        .insert(
            Insert::from(&schema)
                .set(0, 2i64)
                .unwrap()
                .set(1, "Bob")
                .unwrap(),
        );
    let our_changeset: Vec<u8> = changeset.into();

    let patchset: PatchSet<&CreateTable> = PatchSet::new()
        .insert(
            Insert::from(&schema)
                .set(0, 1i64)
                .unwrap()
                .set(1, "Alice")
                .unwrap(),
        )
        .insert(
            Insert::from(&schema)
                .set(0, 2i64)
                .unwrap()
                .set(1, "Bob")
                .unwrap(),
        );
    let our_patchset: Vec<u8> = patchset.into();

    assert_bit_parity(
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "INSERT INTO users (id, name) VALUES (2, 'Bob')",
        ],
        &our_changeset,
        &our_patchset,
    );
}

#[test]
fn bit_parity_builder_insert_then_update() {
    let schema = parse_schema("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

    let changeset: ChangeSet<&CreateTable> = ChangeSet::new()
        .insert(
            Insert::from(&schema)
                .set(0, 1i64)
                .unwrap()
                .set(1, "Alice")
                .unwrap(),
        )
        .update(
            Update::<_, ChangesetFormat>::from(&schema)
                .set(0, 1i64, 1i64)
                .unwrap()
                .set(1, "Alice", "Alicia")
                .unwrap(),
        );
    let our_changeset: Vec<u8> = changeset.into();

    let patchset: PatchSet<&CreateTable> = PatchSet::new()
        .insert(
            Insert::from(&schema)
                .set(0, 1i64)
                .unwrap()
                .set(1, "Alice")
                .unwrap(),
        )
        .update(
            Update::<_, PatchsetFormat>::from(&schema)
                .set(0, 1i64)
                .unwrap()
                .set(1, "Alicia")
                .unwrap(),
        );
    let our_patchset: Vec<u8> = patchset.into();

    assert_bit_parity(
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "UPDATE users SET name = 'Alicia' WHERE id = 1",
        ],
        &our_changeset,
        &our_patchset,
    );
}

#[test]
fn bit_parity_builder_two_tables() {
    let schema_u = parse_schema("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    let schema_p = parse_schema("CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)");

    let changeset: ChangeSet<&CreateTable> = ChangeSet::new()
        .insert(
            Insert::from(&schema_u)
                .set(0, 1i64)
                .unwrap()
                .set(1, "Alice")
                .unwrap(),
        )
        .insert(
            Insert::from(&schema_p)
                .set(0, 1i64)
                .unwrap()
                .set(1, "Hello")
                .unwrap(),
        );
    let our_changeset: Vec<u8> = changeset.into();

    let patchset: PatchSet<&CreateTable> = PatchSet::new()
        .insert(
            Insert::from(&schema_u)
                .set(0, 1i64)
                .unwrap()
                .set(1, "Alice")
                .unwrap(),
        )
        .insert(
            Insert::from(&schema_p)
                .set(0, 1i64)
                .unwrap()
                .set(1, "Hello")
                .unwrap(),
        );
    let our_patchset: Vec<u8> = patchset.into();

    assert_bit_parity(
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)",
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "INSERT INTO posts (id, title) VALUES (1, 'Hello')",
        ],
        &our_changeset,
        &our_patchset,
    );
}

#[test]
fn bit_parity_builder_table_cancel_and_readd() {
    let schema_a = parse_schema("CREATE TABLE table_a (id INTEGER PRIMARY KEY, val TEXT)");
    let schema_b = parse_schema("CREATE TABLE table_b (id INTEGER PRIMARY KEY, val TEXT)");

    // Changeset
    let changeset: ChangeSet<&CreateTable> = ChangeSet::new()
        .insert(
            Insert::from(&schema_a)
                .set(0, 1i64)
                .unwrap()
                .set(1, "a1")
                .unwrap(),
        )
        .insert(
            Insert::from(&schema_b)
                .set(0, 1i64)
                .unwrap()
                .set(1, "b1")
                .unwrap(),
        )
        .delete(
            ChangeDelete::from(&schema_a)
                .set(0, 1i64)
                .unwrap()
                .set(1, "a1")
                .unwrap(),
        )
        .insert(
            Insert::from(&schema_a)
                .set(0, 2i64)
                .unwrap()
                .set(1, "a2")
                .unwrap(),
        );
    let our_changeset: Vec<u8> = changeset.into();

    // Patchset (delete uses PK only)
    let patchset: PatchSet<&CreateTable> = PatchSet::new()
        .insert(
            Insert::from(&schema_a)
                .set(0, 1i64)
                .unwrap()
                .set(1, "a1")
                .unwrap(),
        )
        .insert(
            Insert::from(&schema_b)
                .set(0, 1i64)
                .unwrap()
                .set(1, "b1")
                .unwrap(),
        )
        .delete(&&schema_a, &[sqlite_diff_rs::Value::Integer(1)])
        .insert(
            Insert::from(&schema_a)
                .set(0, 2i64)
                .unwrap()
                .set(1, "a2")
                .unwrap(),
        );
    let our_patchset: Vec<u8> = patchset.into();

    assert_bit_parity(
        &[
            "CREATE TABLE table_a (id INTEGER PRIMARY KEY, val TEXT)",
            "CREATE TABLE table_b (id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO table_a (id, val) VALUES (1, 'a1')",
            "INSERT INTO table_b (id, val) VALUES (1, 'b1')",
            "DELETE FROM table_a WHERE id = 1",
            "INSERT INTO table_a (id, val) VALUES (2, 'a2')",
        ],
        &our_changeset,
        &our_patchset,
    );
}

// =============================================================================
// Data type edge cases
// =============================================================================

#[test]
fn bit_parity_integer_boundaries() {
    let sql = "\
        CREATE TABLE numbers (id INTEGER PRIMARY KEY, value INTEGER);\
        INSERT INTO numbers (id, value) VALUES (1, 0);\
        INSERT INTO numbers (id, value) VALUES (2, -1);\
        INSERT INTO numbers (id, value) VALUES (3, 127);\
        INSERT INTO numbers (id, value) VALUES (4, 128);\
        INSERT INTO numbers (id, value) VALUES (5, 9223372036854775807);\
        INSERT INTO numbers (id, value) VALUES (6, -9223372036854775808);";
    assert_fromstr_bit_parity(sql);
}

#[test]
fn bit_parity_float_values() {
    let sql = "\
        CREATE TABLE floats (id INTEGER PRIMARY KEY, value REAL);\
        INSERT INTO floats (id, value) VALUES (1, 0.0);\
        INSERT INTO floats (id, value) VALUES (2, 3.14159265358979);\
        INSERT INTO floats (id, value) VALUES (3, -273.15);";
    assert_fromstr_bit_parity(sql);
}

#[test]
fn bit_parity_unicode_text() {
    let sql = "\
        CREATE TABLE strings (id INTEGER PRIMARY KEY, value TEXT);\
        INSERT INTO strings (id, value) VALUES (1, '日本語');\
        INSERT INTO strings (id, value) VALUES (2, 'Ñoño');";
    assert_fromstr_bit_parity(sql);
}

#[test]
fn bit_parity_blob_value() {
    let sql = "\
        CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB);\
        INSERT INTO blobs (id, data) VALUES (1, X'DEADBEEF');";
    assert_fromstr_bit_parity(sql);
}

#[test]
fn bit_parity_all_nulls() {
    let sql = "\
        CREATE TABLE items (id INTEGER PRIMARY KEY, a TEXT, b REAL, c INTEGER);\
        INSERT INTO items (id, a, b, c) VALUES (1, NULL, NULL, NULL);";
    assert_fromstr_bit_parity(sql);
}
