//! Bit-for-bit parity tests between our library and rusqlite's session extension.
//!
//! These tests verify that our changeset/patchset binary output is **byte-identical**
//! to what `SQLite`'s session extension produces for the same sequence of operations.
//!
//! Previous tests only compared at the semantic level (via `ParsedDiffSet::PartialEq`),
//! which uses `HashMap` comparison and thus ignores:
//! - Table ordering differences
//! - Row ordering differences within a table
//!
//! This file catches both classes of bug by comparing raw `Vec<u8>` output.
#![cfg(feature = "testing")]

use sqlite_diff_rs::testing::{
    assert_bit_parity, assert_patchset_sql_parity, byte_diff_report,
    session_changeset_and_patchset_with_setup,
};
use sqlite_diff_rs::{
    ChangeDelete, ChangeSet, ChangesetFormat, DiffOps, Insert, PatchDelete, PatchSet,
    PatchsetFormat, SimpleTable, Update, Value,
};

// =============================================================================
// Single-table, single-operation tests
// =============================================================================

#[test]
fn bit_parity_single_insert() {
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
fn bit_parity_single_insert_integer_only() {
    let nums = SimpleTable::new("nums", &["id", "val"], &[0]);
    assert_patchset_sql_parity(
        &[nums],
        &[
            "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)",
            "INSERT INTO nums (id, val) VALUES (1, 42)",
        ],
    );
}

#[test]
fn bit_parity_single_insert_with_null() {
    let items = SimpleTable::new("items", &["id", "description", "price"], &[0]);
    assert_patchset_sql_parity(
        &[items],
        &[
            "CREATE TABLE items (id INTEGER PRIMARY KEY, description TEXT, price REAL)",
            "INSERT INTO items (id, description, price) VALUES (1, NULL, 9.99)",
        ],
    );
}

#[test]
fn bit_parity_single_insert_with_real() {
    let measurements = SimpleTable::new("measurements", &["id", "value"], &[0]);
    assert_patchset_sql_parity(
        &[measurements],
        &[
            "CREATE TABLE measurements (id INTEGER PRIMARY KEY, value REAL)",
            "INSERT INTO measurements (id, value) VALUES (1, 3.14)",
        ],
    );
}

#[test]
fn bit_parity_single_insert_empty_string() {
    let strings = SimpleTable::new("strings", &["id", "value"], &[0]);
    assert_patchset_sql_parity(
        &[strings],
        &[
            "CREATE TABLE strings (id INTEGER PRIMARY KEY, value TEXT)",
            "INSERT INTO strings (id, value) VALUES (1, '')",
        ],
    );
}

// =============================================================================
// Multiple rows in a single table (tests row ordering)
// =============================================================================

#[test]
fn bit_parity_two_inserts_same_table() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    assert_patchset_sql_parity(
        &[users],
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "INSERT INTO users (id, name) VALUES (2, 'Bob')",
        ],
    );
}

#[test]
fn bit_parity_three_inserts_same_table() {
    let users = SimpleTable::new("users", &["id", "name", "age"], &[0]);
    assert_patchset_sql_parity(
        &[users],
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)",
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)",
            "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)",
        ],
    );
}

#[test]
fn bit_parity_insert_then_update() {
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
fn bit_parity_insert_then_delete_cancel() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    assert_patchset_sql_parity(
        &[users],
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "DELETE FROM users WHERE id = 1",
        ],
    );
}

#[test]
fn bit_parity_two_inserts_one_deleted() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    assert_patchset_sql_parity(
        &[users],
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "INSERT INTO users (id, name) VALUES (2, 'Bob')",
            "DELETE FROM users WHERE id = 1",
        ],
    );
}

// =============================================================================
// Multi-table tests (tests table ordering)
// =============================================================================

#[test]
fn bit_parity_two_tables() {
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
fn bit_parity_two_tables_reverse_order() {
    // Insert into posts first, then users
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    let posts = SimpleTable::new("posts", &["id", "title"], &[0]);
    assert_patchset_sql_parity(
        &[users, posts],
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)",
            "INSERT INTO posts (id, title) VALUES (1, 'Hello')",
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
        ],
    );
}

#[test]
fn bit_parity_three_tables() {
    let alpha = SimpleTable::new("alpha", &["id", "val"], &[0]);
    let beta = SimpleTable::new("beta", &["id", "val"], &[0]);
    let gamma = SimpleTable::new("gamma", &["id", "val"], &[0]);
    assert_patchset_sql_parity(
        &[alpha, beta, gamma],
        &[
            "CREATE TABLE alpha (id INTEGER PRIMARY KEY, val TEXT)",
            "CREATE TABLE beta (id INTEGER PRIMARY KEY, val TEXT)",
            "CREATE TABLE gamma (id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO alpha (id, val) VALUES (1, 'a')",
            "INSERT INTO beta (id, val) VALUES (1, 'b')",
            "INSERT INTO gamma (id, val) VALUES (1, 'c')",
        ],
    );
}

#[test]
fn bit_parity_table_cancel_and_readd() {
    let table_a = SimpleTable::new("table_a", &["id", "val"], &[0]);
    let table_b = SimpleTable::new("table_b", &["id", "val"], &[0]);
    assert_patchset_sql_parity(
        &[table_a, table_b],
        &[
            "CREATE TABLE table_a (id INTEGER PRIMARY KEY, val TEXT)",
            "CREATE TABLE table_b (id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO table_a (id, val) VALUES (1, 'a1')",
            "INSERT INTO table_b (id, val) VALUES (1, 'b1')",
            "DELETE FROM table_a WHERE id = 1",
            "INSERT INTO table_a (id, val) VALUES (2, 'a2')",
        ],
    );
}

#[test]
fn bit_parity_cancelled_table_excluded() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    let posts = SimpleTable::new("posts", &["id", "title"], &[0]);
    assert_patchset_sql_parity(
        &[users, posts],
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)",
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "DELETE FROM users WHERE id = 1",
            "INSERT INTO posts (id, title) VALUES (1, 'Hello')",
        ],
    );
}

// =============================================================================
// Composite primary keys
// =============================================================================

#[test]
fn bit_parity_composite_pk() {
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

// =============================================================================
// Builder API parity (not going through FromStr)
// =============================================================================

#[test]
fn bit_parity_builder_single_insert() {
    let schema = SimpleTable::new("users", &["id", "name"], &[0]);

    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new().insert(
        Insert::<_, String, Vec<u8>>::from(schema.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "Alice")
            .unwrap(),
    );
    let our_changeset: Vec<u8> = changeset.build();

    let patchset: PatchSet<SimpleTable, String, Vec<u8>> = PatchSet::new().insert(
        Insert::<_, String, Vec<u8>>::from(schema)
            .set(0, 1i64)
            .unwrap()
            .set(1, "Alice")
            .unwrap(),
    );
    let our_patchset: Vec<u8> = patchset.build();

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
    let schema = SimpleTable::new("users", &["id", "name"], &[0]);

    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new()
        .insert(
            Insert::<_, String, Vec<u8>>::from(schema.clone())
                .set(0, 1i64)
                .unwrap()
                .set(1, "Alice")
                .unwrap(),
        )
        .insert(
            Insert::<_, String, Vec<u8>>::from(schema.clone())
                .set(0, 2i64)
                .unwrap()
                .set(1, "Bob")
                .unwrap(),
        );
    let our_changeset: Vec<u8> = changeset.build();

    let patchset: PatchSet<SimpleTable, String, Vec<u8>> = PatchSet::new()
        .insert(
            Insert::<_, String, Vec<u8>>::from(schema.clone())
                .set(0, 1i64)
                .unwrap()
                .set(1, "Alice")
                .unwrap(),
        )
        .insert(
            Insert::<_, String, Vec<u8>>::from(schema)
                .set(0, 2i64)
                .unwrap()
                .set(1, "Bob")
                .unwrap(),
        );
    let our_patchset: Vec<u8> = patchset.build();

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
    let schema = SimpleTable::new("users", &["id", "name"], &[0]);

    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new()
        .insert(
            Insert::<_, String, Vec<u8>>::from(schema.clone())
                .set(0, 1i64)
                .unwrap()
                .set(1, "Alice")
                .unwrap(),
        )
        .update(
            Update::<SimpleTable, ChangesetFormat, String, Vec<u8>>::from(schema.clone())
                .set(0, 1i64, 1i64)
                .unwrap()
                .set(1, "Alice", "Alicia")
                .unwrap(),
        );
    let our_changeset: Vec<u8> = changeset.build();

    let patchset: PatchSet<SimpleTable, String, Vec<u8>> = PatchSet::new()
        .insert(
            Insert::<_, String, Vec<u8>>::from(schema.clone())
                .set(0, 1i64)
                .unwrap()
                .set(1, "Alice")
                .unwrap(),
        )
        .update(
            Update::<SimpleTable, PatchsetFormat, String, Vec<u8>>::from(schema)
                .set(0, 1i64)
                .unwrap()
                .set(1, "Alicia")
                .unwrap(),
        );
    let our_patchset: Vec<u8> = patchset.build();

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
    let schema_u = SimpleTable::new("users", &["id", "name"], &[0]);
    let schema_p = SimpleTable::new("posts", &["id", "title"], &[0]);

    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new()
        .insert(
            Insert::<_, String, Vec<u8>>::from(schema_u.clone())
                .set(0, 1i64)
                .unwrap()
                .set(1, "Alice")
                .unwrap(),
        )
        .insert(
            Insert::<_, String, Vec<u8>>::from(schema_p.clone())
                .set(0, 1i64)
                .unwrap()
                .set(1, "Hello")
                .unwrap(),
        );
    let our_changeset: Vec<u8> = changeset.build();

    let patchset: PatchSet<SimpleTable, String, Vec<u8>> = PatchSet::new()
        .insert(
            Insert::<_, String, Vec<u8>>::from(schema_u)
                .set(0, 1i64)
                .unwrap()
                .set(1, "Alice")
                .unwrap(),
        )
        .insert(
            Insert::<_, String, Vec<u8>>::from(schema_p)
                .set(0, 1i64)
                .unwrap()
                .set(1, "Hello")
                .unwrap(),
        );
    let our_patchset: Vec<u8> = patchset.build();

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
    let schema_a = SimpleTable::new("table_a", &["id", "val"], &[0]);
    let schema_b = SimpleTable::new("table_b", &["id", "val"], &[0]);

    // Changeset
    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new()
        .insert(
            Insert::<_, String, Vec<u8>>::from(schema_a.clone())
                .set(0, 1i64)
                .unwrap()
                .set(1, "a1")
                .unwrap(),
        )
        .insert(
            Insert::<_, String, Vec<u8>>::from(schema_b.clone())
                .set(0, 1i64)
                .unwrap()
                .set(1, "b1")
                .unwrap(),
        )
        .delete(
            ChangeDelete::<_, String, Vec<u8>>::from(schema_a.clone())
                .set(0, 1i64)
                .unwrap()
                .set(1, "a1")
                .unwrap(),
        )
        .insert(
            Insert::<_, String, Vec<u8>>::from(schema_a.clone())
                .set(0, 2i64)
                .unwrap()
                .set(1, "a2")
                .unwrap(),
        );
    let our_changeset: Vec<u8> = changeset.build();

    // Patchset (delete uses PK only)
    let patchset: PatchSet<SimpleTable, String, Vec<u8>> = PatchSet::new()
        .insert(
            Insert::<_, String, Vec<u8>>::from(schema_a.clone())
                .set(0, 1i64)
                .unwrap()
                .set(1, "a1")
                .unwrap(),
        )
        .insert(
            Insert::<_, String, Vec<u8>>::from(schema_b)
                .set(0, 1i64)
                .unwrap()
                .set(1, "b1")
                .unwrap(),
        )
        .delete(PatchDelete::new(schema_a.clone(), vec![Value::Integer(1)]))
        .insert(
            Insert::<_, String, Vec<u8>>::from(schema_a)
                .set(0, 2i64)
                .unwrap()
                .set(1, "a2")
                .unwrap(),
        );
    let our_patchset: Vec<u8> = patchset.build();

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
    let numbers = SimpleTable::new("numbers", &["id", "value"], &[0]);
    assert_patchset_sql_parity(
        &[numbers],
        &[
            "CREATE TABLE numbers (id INTEGER PRIMARY KEY, value INTEGER)",
            "INSERT INTO numbers (id, value) VALUES (1, 0)",
            "INSERT INTO numbers (id, value) VALUES (2, -1)",
            "INSERT INTO numbers (id, value) VALUES (3, 127)",
            "INSERT INTO numbers (id, value) VALUES (4, 128)",
            "INSERT INTO numbers (id, value) VALUES (5, 9223372036854775807)",
            "INSERT INTO numbers (id, value) VALUES (6, -9223372036854775808)",
        ],
    );
}

#[test]
fn bit_parity_float_values() {
    let floats = SimpleTable::new("floats", &["id", "value"], &[0]);
    assert_patchset_sql_parity(
        &[floats],
        &[
            "CREATE TABLE floats (id INTEGER PRIMARY KEY, value REAL)",
            "INSERT INTO floats (id, value) VALUES (1, 0.0)",
            "INSERT INTO floats (id, value) VALUES (2, 3.14159265358979)",
            "INSERT INTO floats (id, value) VALUES (3, -273.15)",
        ],
    );
}

#[test]
fn bit_parity_unicode_text() {
    let strings = SimpleTable::new("strings", &["id", "value"], &[0]);
    assert_patchset_sql_parity(
        &[strings],
        &[
            "CREATE TABLE strings (id INTEGER PRIMARY KEY, value TEXT)",
            "INSERT INTO strings (id, value) VALUES (1, '日本語')",
            "INSERT INTO strings (id, value) VALUES (2, 'Ñoño')",
        ],
    );
}

#[test]
fn bit_parity_blob_value() {
    let blobs = SimpleTable::new("blobs", &["id", "data"], &[0]);
    assert_patchset_sql_parity(
        &[blobs],
        &[
            "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)",
            "INSERT INTO blobs (id, data) VALUES (1, X'DEADBEEF')",
        ],
    );
}

#[test]
fn bit_parity_all_nulls() {
    let items = SimpleTable::new("items", &["id", "a", "b", "c"], &[0]);
    assert_patchset_sql_parity(
        &[items],
        &[
            "CREATE TABLE items (id INTEGER PRIMARY KEY, a TEXT, b REAL, c INTEGER)",
            "INSERT INTO items (id, a, b, c) VALUES (1, NULL, NULL, NULL)",
        ],
    );
}

// =============================================================================
// Standalone UPDATE / DELETE against a pre-existing row.
//
// These are the only scenarios that expose SQLite's real patchset UPDATE wire
// layout. When INSERT and UPDATE are recorded in the same session they
// consolidate to a single INSERT, so the UPDATE branch of the encoder is never
// exercised. The tests below insert the row BEFORE the session attaches, so
// the session records only the UPDATE (or DELETE), matching the flow used by
// downstream tools that stream a session's output.
// =============================================================================

#[test]
fn bit_parity_standalone_update_single_pk() {
    // Regression: `parse_patchset_operation`'s UPDATE branch used to read
    // `column_count` values on both sides. SQLite writes only `pk_count` on the
    // old side and `column_count - pk_count` on the new side. This test drives
    // the reported bug by producing a real standalone patchset UPDATE (the
    // session sees only the UPDATE because the initial INSERT ran before
    // `Session::attach`) and comparing bytes.
    //
    // Note: only patchset parity is asserted here. The changeset UPDATE builder
    // has a separate, adjacent gap on the new side for PK columns that is out
    // of scope for this fix.
    let schema = SimpleTable::new("orders", &["id", "amount", "status"], &[0]);

    let our_patchset: Vec<u8> = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .update(
            Update::<SimpleTable, PatchsetFormat, String, Vec<u8>>::from(schema)
                .set(0, 5i64)
                .unwrap()
                .set(2, "shipped")
                .unwrap(),
        )
        .build();

    let (_sqlite_cs, sqlite_ps) = session_changeset_and_patchset_with_setup(
        &[
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)",
            "INSERT INTO orders VALUES (5, 100, 'pending')",
        ],
        &["UPDATE orders SET status = 'shipped' WHERE id = 5"],
    );

    let ps_report = byte_diff_report("patchset", &sqlite_ps, &our_patchset);
    assert!(
        sqlite_ps == our_patchset,
        "standalone UPDATE patchset bit-parity failure\n{ps_report}",
    );
}

#[test]
fn bit_parity_standalone_update_composite_pk() {
    let schema = SimpleTable::new("items", &["a", "b", "val"], &[0, 1]);

    let our_patchset: Vec<u8> = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .update(
            Update::<SimpleTable, PatchsetFormat, String, Vec<u8>>::from(schema)
                .set(0, 1i64)
                .unwrap()
                .set(1, 2i64)
                .unwrap()
                .set(2, "v2")
                .unwrap(),
        )
        .build();

    let (_sqlite_cs, sqlite_ps) = session_changeset_and_patchset_with_setup(
        &[
            "CREATE TABLE items (a INTEGER NOT NULL, b INTEGER NOT NULL, val TEXT, PRIMARY KEY(a, b))",
            "INSERT INTO items VALUES (1, 2, 'v1')",
        ],
        &["UPDATE items SET val = 'v2' WHERE a = 1 AND b = 2"],
    );

    let ps_report = byte_diff_report("patchset", &sqlite_ps, &our_patchset);
    assert!(
        sqlite_ps == our_patchset,
        "composite PK standalone UPDATE bit-parity failure\n{ps_report}",
    );
}

#[test]
fn bit_parity_standalone_update_all_non_pk_changed() {
    let schema = SimpleTable::new("orders", &["id", "amount", "status"], &[0]);

    let our_patchset: Vec<u8> = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .update(
            Update::<SimpleTable, PatchsetFormat, String, Vec<u8>>::from(schema)
                .set(0, 5i64)
                .unwrap()
                .set(1, 200i64)
                .unwrap()
                .set(2, "shipped")
                .unwrap(),
        )
        .build();

    let (_sqlite_cs, sqlite_ps) = session_changeset_and_patchset_with_setup(
        &[
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)",
            "INSERT INTO orders VALUES (5, 100, 'pending')",
        ],
        &["UPDATE orders SET amount = 200, status = 'shipped' WHERE id = 5"],
    );

    let ps_report = byte_diff_report("patchset", &sqlite_ps, &our_patchset);
    assert!(
        sqlite_ps == our_patchset,
        "all-non-PK UPDATE bit-parity failure\n{ps_report}",
    );
}

#[test]
fn bit_parity_standalone_delete_single_pk() {
    let schema = SimpleTable::new("orders", &["id", "amount", "status"], &[0]);

    let our_patchset: Vec<u8> = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .delete(PatchDelete::new(schema, vec![Value::Integer(5)]))
        .build();

    let (_sqlite_cs, sqlite_ps) = session_changeset_and_patchset_with_setup(
        &[
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)",
            "INSERT INTO orders VALUES (5, 100, 'pending')",
        ],
        &["DELETE FROM orders WHERE id = 5"],
    );

    let ps_report = byte_diff_report("patchset", &sqlite_ps, &our_patchset);
    assert!(
        sqlite_ps == our_patchset,
        "standalone DELETE bit-parity failure\n{ps_report}",
    );
}
