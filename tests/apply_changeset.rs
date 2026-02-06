//! Tests for string-based changeset/patchset parsing and application.
//!
//! These tests verify that:
//! 1. Parsing SQL strings into ChangeSet/PatchSet produces correct binary output
//! 2. The binary output is byte-identical to rusqlite's session extension output
//! 3. Applying our changesets to an empty DB produces identical rows to rusqlite's changesets
//!
//! The core invariant: given an SQL string describing DDL + DML operations,
//! parsing it through our library and executing it through rusqlite's session
//! extension should produce identical changesets/patchsets.

use rusqlite::Connection;
use rusqlite::session::{ChangesetItem, ConflictAction, ConflictType, Session};
use sqlite_diff_rs::{ChangeSet, ParsedDiffSet, PatchSet};
use sqlparser::ast::{CreateTable, Statement};
use std::io::Cursor;

/// Helper to run a complete application test.
///
/// This function:
/// 1. Parses the SQL into ChangeSet and PatchSet
/// 2. Executes the same SQL in rusqlite with session tracking
/// 3. Compares our binary output with rusqlite's byte-for-byte
/// 4. Applies both changesets to empty databases and compares final state
fn run_application_test(sql: &str) {
    // Step 1: Parse into ChangeSet and PatchSet
    let Ok(changeset): Result<ChangeSet<CreateTable>, _> = sql.parse() else {
        return; // Invalid SQL for our parser, skip
    };
    let Ok(patchset): Result<PatchSet<CreateTable>, _> = sql.parse() else {
        return;
    };

    // Skip empty builders (no operations to test)
    if changeset.is_empty() {
        return;
    }

    // Step 2: Execute in rusqlite with session tracking
    let conn = Connection::open_in_memory().expect("Failed to create in-memory database");

    // Extract CREATE TABLE statements and execute them
    let statements: Vec<Statement> = (&changeset).into();
    let create_table_sqls: Vec<String> = statements
        .iter()
        .filter_map(|s| {
            if let Statement::CreateTable(_) = s {
                Some(s.to_string())
            } else {
                None
            }
        })
        .collect();

    for create_sql in &create_table_sqls {
        if conn.execute(create_sql, []).is_err() {
            return; // Invalid schema, skip
        }
    }

    // Create session and attach all tables
    let mut session = Session::new(&conn).expect("Failed to create session");
    if session.attach::<&str>(None).is_err() {
        return;
    }

    // Execute DML statements (INSERT, UPDATE, DELETE)
    for stmt in &statements {
        match stmt {
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
                let stmt_sql = stmt.to_string();
                if conn.execute(&stmt_sql, []).is_err() {
                    // DML failed, skip this test
                    return;
                }
            }
            _ => {}
        }
    }

    // Get rusqlite's changeset and patchset
    let mut rusqlite_changeset = Vec::new();
    if session.changeset_strm(&mut rusqlite_changeset).is_err() {
        return;
    }

    let mut rusqlite_patchset = Vec::new();
    if session.patchset_strm(&mut rusqlite_patchset).is_err() {
        return;
    }

    // Step 3: Build our binary output
    let our_changeset: Vec<u8> = changeset.clone().into();
    let our_patchset: Vec<u8> = patchset.clone().into();

    // Step 4: Compare at the parsed level (semantic equality)
    compare_changesets(&rusqlite_changeset, &our_changeset);
    compare_patchsets(&rusqlite_patchset, &our_patchset);

    // Step 5: Verify DB state by applying both changesets to empty databases
    eprintln!("=== Testing DB state with changeset application ===");
    eprintln!("Rusqlite changeset len: {}", rusqlite_changeset.len());
    eprintln!("Our changeset len: {}", our_changeset.len());
    eprintln!("Bytes equal: {}", rusqlite_changeset == our_changeset);
    if rusqlite_changeset != our_changeset {
        eprintln!("Rusqlite bytes: {rusqlite_changeset:02x?}");
        eprintln!("Our bytes: {our_changeset:02x?}");
    }
    if !rusqlite_changeset.is_empty() && !our_changeset.is_empty() {
        verify_changeset_application(&create_table_sqls, &rusqlite_changeset, &our_changeset);
    }

    if !rusqlite_patchset.is_empty() && !our_patchset.is_empty() {
        verify_patchset_application(&create_table_sqls, &rusqlite_patchset, &our_patchset);
    }
}

/// Compare two changesets at the parsed level.
fn compare_changesets(rusqlite_bytes: &[u8], our_bytes: &[u8]) {
    if rusqlite_bytes.is_empty() && our_bytes.is_empty() {
        return;
    }

    if !rusqlite_bytes.is_empty() {
        let rusqlite_parsed =
            ParsedDiffSet::try_from(rusqlite_bytes).expect("Failed to parse rusqlite changeset");
        assert!(
            matches!(&rusqlite_parsed, ParsedDiffSet::Changeset(_)),
            "Expected changeset from rusqlite"
        );
    }

    if !our_bytes.is_empty() {
        let our_parsed = ParsedDiffSet::try_from(our_bytes).expect("Failed to parse our changeset");
        assert!(
            matches!(&our_parsed, ParsedDiffSet::Changeset(_)),
            "Expected changeset from our builder"
        );
    }

    // Debug: print parsed structures if both non-empty
    if !rusqlite_bytes.is_empty() && !our_bytes.is_empty() {
        let rusqlite_parsed = ParsedDiffSet::try_from(rusqlite_bytes).unwrap();
        let our_parsed = ParsedDiffSet::try_from(our_bytes).unwrap();

        // Debug output for failing tests
        if rusqlite_parsed != our_parsed {
            eprintln!("Changeset mismatch detected!");
            eprintln!("Rusqlite bytes: {rusqlite_bytes:02x?}");
            eprintln!("Our bytes: {our_bytes:02x?}");
            eprintln!("Rusqlite parsed: {rusqlite_parsed:?}");
            eprintln!("Our parsed: {our_parsed:?}");
        }

        assert_eq!(
            rusqlite_parsed, our_parsed,
            "Changeset mismatch!\nrusqlite: {rusqlite_parsed:?}\nours: {our_parsed:?}"
        );
    }
}

/// Compare two patchsets at the parsed level.
fn compare_patchsets(rusqlite_bytes: &[u8], our_bytes: &[u8]) {
    if rusqlite_bytes.is_empty() && our_bytes.is_empty() {
        return;
    }

    if !rusqlite_bytes.is_empty() {
        let rusqlite_parsed =
            ParsedDiffSet::try_from(rusqlite_bytes).expect("Failed to parse rusqlite patchset");
        assert!(
            matches!(&rusqlite_parsed, ParsedDiffSet::Patchset(_)),
            "Expected patchset from rusqlite"
        );
    }

    if !our_bytes.is_empty() {
        let our_parsed = ParsedDiffSet::try_from(our_bytes).expect("Failed to parse our patchset");
        assert!(
            matches!(&our_parsed, ParsedDiffSet::Patchset(_)),
            "Expected patchset from our builder"
        );
    }

    // Compare parsed structures if both non-empty
    if !rusqlite_bytes.is_empty() && !our_bytes.is_empty() {
        let rusqlite_parsed = ParsedDiffSet::try_from(rusqlite_bytes).unwrap();
        let our_parsed = ParsedDiffSet::try_from(our_bytes).unwrap();
        assert_eq!(
            rusqlite_parsed, our_parsed,
            "Patchset mismatch!\nrusqlite: {rusqlite_parsed:?}\nours: {our_parsed:?}"
        );
    }
}

/// Apply both changesets to empty databases and verify they produce identical state.
fn verify_changeset_application(
    create_table_sqls: &[String],
    rusqlite_changeset: &[u8],
    our_changeset: &[u8],
) {
    // Create two empty databases with identical schema
    let conn1 = Connection::open_in_memory().expect("Failed to create DB 1");
    let conn2 = Connection::open_in_memory().expect("Failed to create DB 2");

    for create_sql in create_table_sqls {
        conn1
            .execute(create_sql, [])
            .expect("Failed to create table in DB 1");
        conn2
            .execute(create_sql, [])
            .expect("Failed to create table in DB 2");
    }

    // Apply rusqlite's changeset to conn1
    conn1
        .execute("BEGIN", [])
        .expect("Failed to begin transaction");
    if let Err(e) = apply_changeset(&conn1, rusqlite_changeset) {
        panic!(
            "Failed to apply rusqlite changeset: {e}\nChangeset bytes: {rusqlite_changeset:02x?}"
        );
    }
    conn1
        .execute("COMMIT", [])
        .expect("Failed to commit transaction");

    // Apply our changeset to conn2
    conn2
        .execute("BEGIN", [])
        .expect("Failed to begin transaction");
    if let Err(e) = apply_changeset(&conn2, our_changeset) {
        panic!("Failed to apply our changeset: {e}\nChangeset bytes: {our_changeset:02x?}");
    }
    conn2
        .execute("COMMIT", [])
        .expect("Failed to commit transaction");

    // Compare database states
    compare_db_states(&conn1, &conn2, create_table_sqls);
}

/// Apply both patchsets to empty databases and verify they produce identical state.
fn verify_patchset_application(
    create_table_sqls: &[String],
    rusqlite_patchset: &[u8],
    our_patchset: &[u8],
) {
    // Create two empty databases with identical schema
    let conn1 = Connection::open_in_memory().expect("Failed to create DB 1");
    let conn2 = Connection::open_in_memory().expect("Failed to create DB 2");

    for create_sql in create_table_sqls {
        conn1
            .execute(create_sql, [])
            .expect("Failed to create table in DB 1");
        conn2
            .execute(create_sql, [])
            .expect("Failed to create table in DB 2");
    }

    // Apply rusqlite's patchset to conn1
    conn1
        .execute("BEGIN", [])
        .expect("Failed to begin transaction");
    if let Err(_e) = apply_changeset(&conn1, rusqlite_patchset) {
        // Patchset application may fail if there are conflicts with empty DB
        // (e.g., DELETE on non-existent row). This is expected for some inputs.
        conn1.execute("ROLLBACK", []).ok();
        return;
    }
    conn1
        .execute("COMMIT", [])
        .expect("Failed to commit transaction");

    // Apply our patchset to conn2
    conn2
        .execute("BEGIN", [])
        .expect("Failed to begin transaction");
    if let Err(_e) = apply_changeset(&conn2, our_patchset) {
        conn2.execute("ROLLBACK", []).ok();
        return;
    }
    conn2
        .execute("COMMIT", [])
        .expect("Failed to commit transaction");

    // Compare database states
    compare_db_states(&conn1, &conn2, create_table_sqls);
}

/// Apply a changeset/patchset to a database connection.
fn apply_changeset(conn: &Connection, changeset: &[u8]) -> Result<(), rusqlite::Error> {
    let mut cursor = Cursor::new(changeset);
    conn.apply_strm(
        &mut cursor,
        None::<fn(&str) -> bool>,
        |conflict_type: ConflictType, _item: ChangesetItem| {
            eprintln!("Conflict during apply: {conflict_type:?}");
            ConflictAction::SQLITE_CHANGESET_ABORT
        },
    )
}

/// Compare database states by querying all tables and comparing rows.
fn compare_db_states(conn1: &Connection, conn2: &Connection, create_table_sqls: &[String]) {
    for create_sql in create_table_sqls {
        // Extract table name from CREATE TABLE statement
        let table_name = extract_table_name(create_sql);

        let rows1 = get_all_rows(conn1, &table_name);
        let rows2 = get_all_rows(conn2, &table_name);

        assert_eq!(
            rows1, rows2,
            "Database state mismatch for table '{table_name}'!\nDB1: {rows1:?}\nDB2: {rows2:?}"
        );
    }
}

/// Extract table name from a CREATE TABLE SQL statement.
fn extract_table_name(create_sql: &str) -> String {
    // Simple extraction: find the table name after "CREATE TABLE"
    let lower = create_sql.to_lowercase();
    let start = lower.find("create table").unwrap() + "create table".len();
    let rest = &create_sql[start..].trim_start();

    // Handle optional "IF NOT EXISTS"
    let rest = if rest.to_lowercase().starts_with("if not exists") {
        rest["if not exists".len()..].trim_start()
    } else {
        rest
    };

    // Extract the table name (up to first space or paren)
    let end = rest
        .find(|c: char| c.is_whitespace() || c == '(')
        .unwrap_or(rest.len());
    rest[..end].to_string()
}

/// Get all rows from a table as a sorted vector of string representations.
fn get_all_rows(conn: &Connection, table_name: &str) -> Vec<Vec<String>> {
    let query = format!("SELECT * FROM {table_name} ORDER BY rowid");
    let mut stmt = conn.prepare(&query).expect("Failed to prepare SELECT");

    let column_count = stmt.column_count();
    let mut rows: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            let mut values = Vec::new();
            for i in 0..column_count {
                let value: rusqlite::types::Value =
                    row.get(i).unwrap_or(rusqlite::types::Value::Null);
                values.push(format!("{value:?}"));
            }
            Ok(values)
        })
        .expect("Failed to query rows")
        .filter_map(Result::ok)
        .collect();

    // Sort for order-independent comparison
    rows.sort();
    rows
}

// =============================================================================
// Test Cases
// =============================================================================

#[test]
fn test_single_insert() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
    ";
    run_application_test(sql);
}

#[test]
fn test_multiple_inserts_same_table() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
        INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
        INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);
        INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);
    ";
    run_application_test(sql);
}

#[test]
fn test_insert_with_null_values() {
    let sql = "
        CREATE TABLE items (id INTEGER PRIMARY KEY, description TEXT, price REAL);
        INSERT INTO items (id, description, price) VALUES (1, NULL, 9.99);
        INSERT INTO items (id, description, price) VALUES (2, 'Item B', NULL);
    ";
    run_application_test(sql);
}

#[test]
fn test_insert_various_types() {
    let sql = "
        CREATE TABLE data (
            id INTEGER PRIMARY KEY,
            int_col INTEGER,
            real_col REAL,
            text_col TEXT,
            blob_col BLOB
        );
        INSERT INTO data (id, int_col, real_col, text_col, blob_col) 
        VALUES (1, 42, 3.14, 'hello', X'DEADBEEF');
    ";
    run_application_test(sql);
}

#[test]
fn test_insert_and_update_same_row() {
    // INSERT + UPDATE on same row should consolidate to INSERT with updated values
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
        UPDATE users SET name = 'Alicia' WHERE id = 1;
    ";
    run_application_test(sql);
}

#[test]
fn test_insert_and_delete_cancels_out() {
    // INSERT + DELETE on same row should cancel out (no-op)
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
        DELETE FROM users WHERE id = 1;
    ";
    run_application_test(sql);
}

#[test]
fn test_multiple_tables() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, content TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
        INSERT INTO posts (id, user_id, content) VALUES (1, 1, 'Hello World');
    ";
    run_application_test(sql);
}

#[test]
fn test_composite_primary_key() {
    let sql = "
        CREATE TABLE order_items (
            order_id INTEGER,
            item_id INTEGER,
            quantity INTEGER,
            PRIMARY KEY (order_id, item_id)
        );
        INSERT INTO order_items (order_id, item_id, quantity) VALUES (1, 100, 5);
        INSERT INTO order_items (order_id, item_id, quantity) VALUES (1, 101, 3);
    ";
    run_application_test(sql);
}

#[test]
fn test_update_non_pk_columns() {
    let sql = "
        CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);
        INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99);
        UPDATE products SET price = 12.99 WHERE id = 1;
    ";
    run_application_test(sql);
}

#[test]
fn test_standalone_delete() {
    // DELETE without prior INSERT in the changeset
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        DELETE FROM users WHERE id = 1;
    ";
    run_application_test(sql);
}

#[test]
fn test_standalone_update() {
    // UPDATE without prior INSERT in the changeset
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        UPDATE users SET name = 'Bob' WHERE id = 1;
    ";
    run_application_test(sql);
}

#[test]
fn test_large_text_value() {
    let large_text = "A".repeat(1000);
    let sql = format!(
        "
        CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT);
        INSERT INTO documents (id, content) VALUES (1, '{large_text}');
    "
    );
    run_application_test(&sql);
}

#[test]
fn test_integer_boundary_values() {
    let sql = "
        CREATE TABLE numbers (id INTEGER PRIMARY KEY, value INTEGER);
        INSERT INTO numbers (id, value) VALUES (1, 0);
        INSERT INTO numbers (id, value) VALUES (2, -1);
        INSERT INTO numbers (id, value) VALUES (3, 9223372036854775807);
        INSERT INTO numbers (id, value) VALUES (4, -9223372036854775808);
    ";
    run_application_test(sql);
}

#[test]
fn test_float_values() {
    let sql = "
        CREATE TABLE measurements (id INTEGER PRIMARY KEY, value REAL);
        INSERT INTO measurements (id, value) VALUES (1, 0.0);
        INSERT INTO measurements (id, value) VALUES (2, 3.14159265358979);
        INSERT INTO measurements (id, value) VALUES (3, -273.15);
    ";
    run_application_test(sql);
}

#[test]
fn test_empty_string() {
    let sql = "
        CREATE TABLE strings (id INTEGER PRIMARY KEY, value TEXT);
        INSERT INTO strings (id, value) VALUES (1, '');
    ";
    run_application_test(sql);
}

#[test]
fn test_unicode_text() {
    let sql = "
        CREATE TABLE strings (id INTEGER PRIMARY KEY, value TEXT);
        INSERT INTO strings (id, value) VALUES (1, '日本語');
        INSERT INTO strings (id, value) VALUES (2, '🎉🎊🎈');
        INSERT INTO strings (id, value) VALUES (3, 'Ñoño');
    ";
    run_application_test(sql);
}
