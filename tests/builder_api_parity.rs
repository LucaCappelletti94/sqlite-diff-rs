//! Tests verifying parity between builder API and FromStr parsing.
//!
//! These tests ensure that constructing a ChangeSet/PatchSet using the builder API
//! produces identical binary output to parsing equivalent SQL via FromStr.
//!
//! This helps catch:
//! 1. Bugs in either path (builder vs parser)
//! 2. Inconsistencies in how operations are encoded
//! 3. API ergonomics issues (if the builder is hard to use correctly)

use sqlite_diff_rs::{
    ChangeDelete, ChangeSet, ChangesetFormat, Insert, PatchSet, PatchsetFormat, Update, Value,
};
use sqlparser::ast::{CreateTable, Statement};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

// =============================================================================
// Helper functions
// =============================================================================

/// Parse a CREATE TABLE statement from SQL.
fn parse_create_table(sql: &str) -> CreateTable {
    let dialect = SQLiteDialect {};
    let statements = Parser::parse_sql(&dialect, sql).expect("Failed to parse SQL");
    match &statements[0] {
        Statement::CreateTable(create) => create.clone(),
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// Compare two changesets semantically.
///
/// Note: We compare the in-memory structures rather than byte output because
/// the byte serialization order depends on HashMap iteration order, which is
/// non-deterministic. Two changesets with different byte representations can
/// still be semantically equivalent (same operations, different order).
fn assert_changeset_eq(builder: &ChangeSet<CreateTable>, from_str: &ChangeSet<CreateTable>) {
    // First, compare the structures semantically
    if builder != from_str {
        let builder_bytes: Vec<u8> = builder.clone().into();
        let from_str_bytes: Vec<u8> = from_str.clone().into();
        eprintln!("=== MISMATCH ===");
        eprintln!("Builder bytes:  {builder_bytes:02x?}");
        eprintln!("FromStr bytes:  {from_str_bytes:02x?}");
        eprintln!("Builder:  {builder:?}");
        eprintln!("FromStr:  {from_str:?}");
    }

    assert_eq!(
        builder, from_str,
        "ChangeSet structure mismatch between builder and FromStr"
    );
}

/// Compare two patchsets semantically.
///
/// Note: We compare the in-memory structures rather than byte output because
/// the byte serialization order depends on HashMap iteration order, which is
/// non-deterministic. Two patchsets with different byte representations can
/// still be semantically equivalent (same operations, different order).
fn assert_patchset_eq(builder: &PatchSet<CreateTable>, from_str: &PatchSet<CreateTable>) {
    // First, compare the structures semantically
    if builder != from_str {
        let builder_bytes: Vec<u8> = builder.clone().into();
        let from_str_bytes: Vec<u8> = from_str.clone().into();
        eprintln!("=== MISMATCH ===");
        eprintln!("Builder bytes:  {builder_bytes:02x?}");
        eprintln!("FromStr bytes:  {from_str_bytes:02x?}");
        eprintln!("Builder:  {builder:?}");
        eprintln!("FromStr:  {from_str:?}");
    }

    assert_eq!(
        builder, from_str,
        "PatchSet structure mismatch between builder and FromStr"
    );
}

// =============================================================================
// INSERT tests
// =============================================================================

#[test]
fn test_insert_single_row_parity() {
    // SQL approach
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
    ";
    let from_str_changeset: ChangeSet<CreateTable> = sql.parse().unwrap();
    let from_str_patchset: PatchSet<CreateTable> = sql.parse().unwrap();

    // Builder approach
    let schema = parse_create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

    let insert = Insert::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap();

    let builder_changeset = ChangeSet::new().insert(insert.clone());
    let builder_patchset = PatchSet::new().insert(insert);

    assert_changeset_eq(&builder_changeset, &from_str_changeset);
    assert_patchset_eq(&builder_patchset, &from_str_patchset);
}

#[test]
fn test_insert_with_null_parity() {
    let sql = "
        CREATE TABLE items (id INTEGER PRIMARY KEY, description TEXT, price REAL);
        INSERT INTO items (id, description, price) VALUES (1, NULL, 9.99);
    ";
    let from_str_changeset: ChangeSet<CreateTable> = sql.parse().unwrap();
    let from_str_patchset: PatchSet<CreateTable> = sql.parse().unwrap();

    let schema = parse_create_table(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, description TEXT, price REAL)",
    );

    // Now we can use set_null() for NULL values!
    let insert = Insert::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set_null(1) // description = NULL
        .unwrap()
        .set(2, 9.99f64)
        .unwrap();

    let builder_changeset = ChangeSet::new().insert(insert.clone());
    let builder_patchset = PatchSet::new().insert(insert);

    assert_changeset_eq(&builder_changeset, &from_str_changeset);
    assert_patchset_eq(&builder_patchset, &from_str_patchset);
}

#[test]
fn test_insert_multiple_rows_parity() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
        INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
        INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);
    ";
    let from_str_changeset: ChangeSet<CreateTable> = sql.parse().unwrap();
    let from_str_patchset: PatchSet<CreateTable> = sql.parse().unwrap();

    let schema =
        parse_create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");

    let insert1 = Insert::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap()
        .set(2, 30i64)
        .unwrap();

    let insert2 = Insert::from(schema.clone())
        .set(0, 2i64)
        .unwrap()
        .set(1, "Bob")
        .unwrap()
        .set(2, 25i64)
        .unwrap();

    let builder_changeset = ChangeSet::new()
        .insert(insert1.clone())
        .insert(insert2.clone());
    let builder_patchset = PatchSet::new().insert(insert1).insert(insert2);

    assert_changeset_eq(&builder_changeset, &from_str_changeset);
    assert_patchset_eq(&builder_patchset, &from_str_patchset);
}

#[test]
fn test_insert_blob_parity() {
    let sql = "
        CREATE TABLE files (id INTEGER PRIMARY KEY, data BLOB);
        INSERT INTO files (id, data) VALUES (1, X'DEADBEEF');
    ";
    let from_str_changeset: ChangeSet<CreateTable> = sql.parse().unwrap();
    let from_str_patchset: PatchSet<CreateTable> = sql.parse().unwrap();

    let schema = parse_create_table("CREATE TABLE files (id INTEGER PRIMARY KEY, data BLOB)");

    let insert = Insert::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, vec![0xDE, 0xAD, 0xBE, 0xEF])
        .unwrap();

    let builder_changeset = ChangeSet::new().insert(insert.clone());
    let builder_patchset = PatchSet::new().insert(insert);

    assert_changeset_eq(&builder_changeset, &from_str_changeset);
    assert_patchset_eq(&builder_patchset, &from_str_patchset);
}

// =============================================================================
// DELETE tests
// =============================================================================

#[test]
fn test_delete_single_row_changeset_parity() {
    // For changeset DELETE, we need to specify all old values
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        DELETE FROM users WHERE id = 1 AND name = 'Alice';
    ";
    let from_str_changeset: ChangeSet<CreateTable> = sql.parse().unwrap();

    let schema = parse_create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

    let delete = ChangeDelete::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap();

    let builder_changeset = ChangeSet::new().delete(delete);

    assert_changeset_eq(&builder_changeset, &from_str_changeset);
}

#[test]
fn test_delete_single_row_patchset_parity() {
    // For patchset DELETE, we only need PK values
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        DELETE FROM users WHERE id = 1;
    ";
    let from_str_patchset: PatchSet<CreateTable> = sql.parse().unwrap();

    let schema = parse_create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

    // Pass PK values directly to delete()
    let builder_patchset = PatchSet::new().delete(schema, &[1i64.into()]);

    assert_patchset_eq(&builder_patchset, &from_str_patchset);
}

// =============================================================================
// UPDATE tests
// =============================================================================

#[test]
fn test_update_changeset_parity() {
    // Changeset UPDATE requires old and new values
    // Note: FromStr can't know the original values, so this tests a standalone UPDATE
    // which will have Undefined for non-PK old values
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        UPDATE users SET name = 'Bob' WHERE id = 1;
    ";
    let from_str_changeset: ChangeSet<CreateTable> = sql.parse().unwrap();

    let schema = parse_create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

    // The UPDATE from SQL parsing has:
    // - old_values[0] = 1 (PK)
    // - old_values[1] = Undefined (non-PK, unknown)
    // - new_values[0] = 1 (PK unchanged)
    // - new_values[1] = "Bob" (changed)
    //
    // Now we can use set_new() for columns where only the new value is known!
    let update = Update::<CreateTable, ChangesetFormat>::from(schema.clone())
        .set(0, 1i64, 1i64) // PK: old=1, new=1 (unchanged)
        .unwrap()
        .set_new(1, "Bob") // name: only new value known
        .unwrap();

    let builder_changeset = ChangeSet::new().update(update);

    assert_changeset_eq(&builder_changeset, &from_str_changeset);
}

#[test]
fn test_update_patchset_parity() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        UPDATE users SET name = 'Bob' WHERE id = 1;
    ";
    let from_str_patchset: PatchSet<CreateTable> = sql.parse().unwrap();

    let schema = parse_create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

    // For patchset UPDATE, we only have new values
    let update = Update::<CreateTable, PatchsetFormat>::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Bob")
        .unwrap();

    // PK is now extracted automatically from the Update's new values
    let builder_patchset = PatchSet::new().update(update);

    assert_patchset_eq(&builder_patchset, &from_str_patchset);
}

// =============================================================================
// INSERT + UPDATE consolidation tests
// =============================================================================

#[test]
fn test_insert_then_update_consolidation_parity() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
        INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
        UPDATE users SET age = 31 WHERE id = 1;
    ";
    let from_str_changeset: ChangeSet<CreateTable> = sql.parse().unwrap();
    let from_str_patchset: PatchSet<CreateTable> = sql.parse().unwrap();

    let schema =
        parse_create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");

    // With builder, we have two options:
    // 1. Add INSERT then UPDATE and let consolidation happen
    // 2. Create the final INSERT directly

    // Option 1: Let consolidation happen
    let insert = Insert::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap()
        .set(2, 30i64)
        .unwrap();

    // For changeset, UPDATE needs old and new values
    let update_old_values = vec![Value::Integer(1), Value::Undefined, Value::Undefined];
    let update_new_values = vec![Value::Integer(1), Value::Undefined, Value::Integer(31)];
    let update_changeset = Update::<CreateTable, ChangesetFormat>::from_values(
        schema.clone(),
        update_old_values,
        update_new_values,
    );

    // For patchset, UPDATE only needs new values (PK is extracted automatically)
    let update_patchset = Update::<CreateTable, PatchsetFormat>::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(2, 31i64)
        .unwrap();

    let builder_changeset = ChangeSet::new()
        .insert(insert.clone())
        .update(update_changeset);
    let builder_patchset = PatchSet::new().insert(insert).update(update_patchset);

    assert_changeset_eq(&builder_changeset, &from_str_changeset);
    assert_patchset_eq(&builder_patchset, &from_str_patchset);
}

#[test]
fn test_insert_then_delete_cancellation_parity() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
        DELETE FROM users WHERE id = 1 AND name = 'Alice';
    ";
    let from_str_changeset: ChangeSet<CreateTable> = sql.parse().unwrap();
    let from_str_patchset: PatchSet<CreateTable> = sql.parse().unwrap();

    let schema = parse_create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

    let insert = Insert::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap();

    let delete_changeset = ChangeDelete::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap();

    let builder_changeset = ChangeSet::new()
        .insert(insert.clone())
        .delete(delete_changeset);
    let builder_patchset = PatchSet::new()
        .insert(insert)
        .delete(schema, &[1i64.into()]);

    // Both should be empty (INSERT + DELETE cancels out)
    assert!(builder_changeset.is_empty());
    assert!(from_str_changeset.is_empty());
    assert!(builder_patchset.is_empty());
    assert!(from_str_patchset.is_empty());

    assert_changeset_eq(&builder_changeset, &from_str_changeset);
    assert_patchset_eq(&builder_patchset, &from_str_patchset);
}

// =============================================================================
// Composite primary key tests
// =============================================================================

#[test]
fn test_composite_pk_insert_parity() {
    let sql = "
        CREATE TABLE order_items (order_id INTEGER, item_id INTEGER, quantity INTEGER, PRIMARY KEY (order_id, item_id));
        INSERT INTO order_items (order_id, item_id, quantity) VALUES (1, 100, 5);
    ";
    let from_str_changeset: ChangeSet<CreateTable> = sql.parse().unwrap();
    let from_str_patchset: PatchSet<CreateTable> = sql.parse().unwrap();

    let schema = parse_create_table(
        "CREATE TABLE order_items (order_id INTEGER, item_id INTEGER, quantity INTEGER, PRIMARY KEY (order_id, item_id))",
    );

    let insert = Insert::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, 100i64)
        .unwrap()
        .set(2, 5i64)
        .unwrap();

    let builder_changeset = ChangeSet::new().insert(insert.clone());
    let builder_patchset = PatchSet::new().insert(insert);

    assert_changeset_eq(&builder_changeset, &from_str_changeset);
    assert_patchset_eq(&builder_patchset, &from_str_patchset);
}

// =============================================================================
// Multiple tables tests
// =============================================================================

#[test]
fn test_multiple_tables_parity() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, content TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
        INSERT INTO posts (id, user_id, content) VALUES (1, 1, 'Hello');
    ";
    let from_str_changeset: ChangeSet<CreateTable> = sql.parse().unwrap();
    let from_str_patchset: PatchSet<CreateTable> = sql.parse().unwrap();

    let users_schema = parse_create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    let posts_schema = parse_create_table(
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, content TEXT)",
    );

    let users_insert = Insert::from(users_schema)
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap();

    let posts_insert = Insert::from(posts_schema)
        .set(0, 1i64)
        .unwrap()
        .set(1, 1i64)
        .unwrap()
        .set(2, "Hello")
        .unwrap();

    let builder_changeset = ChangeSet::new()
        .insert(users_insert.clone())
        .insert(posts_insert.clone());
    let builder_patchset = PatchSet::new().insert(users_insert).insert(posts_insert);

    assert_changeset_eq(&builder_changeset, &from_str_changeset);
    assert_patchset_eq(&builder_patchset, &from_str_patchset);
}

// =============================================================================
// Edge cases and various data types
// =============================================================================

#[test]
fn test_empty_string_parity() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, '');
    ";
    let from_str_changeset: ChangeSet<CreateTable> = sql.parse().unwrap();

    let schema = parse_create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

    let insert = Insert::from(schema)
        .set(0, 1i64)
        .unwrap()
        .set(1, "")
        .unwrap();

    let builder_changeset = ChangeSet::new().insert(insert);

    assert_changeset_eq(&builder_changeset, &from_str_changeset);
}

#[test]
fn test_negative_integer_parity() {
    let sql = "
        CREATE TABLE values (id INTEGER PRIMARY KEY, val INTEGER);
        INSERT INTO values (id, val) VALUES (1, -42);
    ";
    let from_str_changeset: ChangeSet<CreateTable> = sql.parse().unwrap();

    let schema = parse_create_table("CREATE TABLE values (id INTEGER PRIMARY KEY, val INTEGER)");

    let insert = Insert::from(schema)
        .set(0, 1i64)
        .unwrap()
        .set(1, -42i64)
        .unwrap();

    let builder_changeset = ChangeSet::new().insert(insert);

    assert_changeset_eq(&builder_changeset, &from_str_changeset);
}

#[test]
fn test_float_parity() {
    let sql = "
        CREATE TABLE measurements (id INTEGER PRIMARY KEY, value REAL);
        INSERT INTO measurements (id, value) VALUES (1, 6.14159);
    ";
    let from_str_changeset: ChangeSet<CreateTable> = sql.parse().unwrap();

    let schema =
        parse_create_table("CREATE TABLE measurements (id INTEGER PRIMARY KEY, value REAL)");

    let insert = Insert::from(schema)
        .set(0, 1i64)
        .unwrap()
        .set(1, 6.14159f64)
        .unwrap();

    let builder_changeset = ChangeSet::new().insert(insert);

    assert_changeset_eq(&builder_changeset, &from_str_changeset);
}

// =============================================================================
// API Ergonomics observations - UPDATED
// =============================================================================

// Issues identified while writing these tests.
// Status: ✅ = Fixed, 🔄 = In Progress, ❌ = Not Yet Implemented
//
// ✅ 1. set_null() METHOD on Insert/Update/Delete
//    - FIXED: Now have insert.set_null(1)?, update.set_null(1)?, etc.
//
// ✅ 2. PatchSet::update() REQUIRES EXTRA VALUES PARAMETER
//    - FIXED: Now extracts PK from Update's new values automatically
//    - API: patchset.update(update) (no extra old_values parameter needed)
//
// ✅ 3. Update for ChangesetFormat HAS NO ERGONOMIC WAY TO SET "NEW ONLY" VALUES
//    - FIXED: Now have update.set_new(idx, new)? that sets old=Undefined
//
// ✅ 4. PatchSet::delete() API SIMPLIFIED
//    - FIXED: PatchSet::delete(table, &pk) takes the table and PK values directly
//    - No need to construct PatchDelete manually
//    - PatchDelete is now internal-only (not exported)
//
// ❌ 5. INCONSISTENT NAMING: set() vs set_pk() vs from_values()
//    - ChangeDelete has set() for all columns
//    - Consider standardizing naming across types
//
// ❌ 6. Update::from() INITIALIZES WITH UNDEFINED/DEFAULT VALUES
//    - This is confusing - you get an Update with all Undefined values
//    - Then you have to call set() to populate what you need
//    - Consider: Update::new(schema) or Update::empty(schema)
//
// ❌ 7. CONSIDER NAMED COLUMN SETTERS
//    - Column indices must be used directly (changeset format has no column names)
//    - Would be more readable and less error-prone
//
// ❌ 8. CONSIDER BUILDER MACRO FOR SIMPLE CASES
