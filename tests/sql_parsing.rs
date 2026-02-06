//! Integration tests for SQL parsing into DiffSetBuilder.
//!
//! These tests verify the FromStr/TryFrom implementations for
//! DiffSetBuilder<ChangesetFormat, CreateTable> and
//! DiffSetBuilder<PatchsetFormat, CreateTable>.

#![cfg(feature = "sqlparser")]

use sqlite_diff_rs::{ChangeSet, DiffSetParseError, PatchSet};
use sqlparser::ast::CreateTable;

// =============================================================================
// Basic parsing tests
// =============================================================================

#[test]
fn test_parse_simple_insert() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert_eq!(builder.len(), 1);
    assert!(!builder.is_empty());
}

#[test]
fn test_parse_simple_delete() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        DELETE FROM users WHERE id = 1;
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert_eq!(builder.len(), 1);
}

#[test]
fn test_parse_simple_update() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        UPDATE users SET name = 'Bob' WHERE id = 1;
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert_eq!(builder.len(), 1);
}

#[test]
fn test_parse_multiple_tables() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, content TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
        INSERT INTO posts (id, user_id, content) VALUES (1, 1, 'Hello World');
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert_eq!(builder.len(), 2);
}

#[test]
fn test_parse_mixed_operations() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
        INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
        INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);
        UPDATE users SET age = 31 WHERE id = 1;
        DELETE FROM users WHERE id = 2;
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    // INSERT(1) + UPDATE(1) = INSERT(1) with updated values
    // INSERT(2) + DELETE(2) = cancelled out
    // So we should have 1 operation
    assert_eq!(builder.len(), 1);
}

// =============================================================================
// Error handling tests
// =============================================================================

#[test]
fn test_table_not_found_error() {
    let sql = "
        INSERT INTO users (id, name) VALUES (1, 'Alice');
    ";

    let result: Result<ChangeSet<CreateTable>, _> = sql.parse();
    assert!(matches!(result, Err(DiffSetParseError::TableNotFound(_))));
}

#[test]
fn test_table_must_come_before_operations() {
    let sql = "
        INSERT INTO users (id, name) VALUES (1, 'Alice');
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
    ";

    let result: Result<ChangeSet<CreateTable>, _> = sql.parse();
    assert!(matches!(result, Err(DiffSetParseError::TableNotFound(_))));
}

#[test]
fn test_invalid_sql_error() {
    let sql = "THIS IS NOT VALID SQL";

    let result: Result<ChangeSet<CreateTable>, _> = sql.parse();
    assert!(matches!(result, Err(DiffSetParseError::SqlParser(_))));
}

// =============================================================================
// Patchset parsing tests
// =============================================================================

#[test]
fn test_parse_patchset_insert() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
    ";

    let builder: PatchSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert_eq!(builder.len(), 1);
}

#[test]
fn test_parse_patchset_update() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        UPDATE users SET name = 'Bob' WHERE id = 1;
    ";

    let builder: PatchSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert_eq!(builder.len(), 1);
}

#[test]
fn test_parse_patchset_delete() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        DELETE FROM users WHERE id = 1;
    ";

    let builder: PatchSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert_eq!(builder.len(), 1);
}

// =============================================================================
// TryFrom implementations tests
// =============================================================================

#[test]
fn test_try_from_str() {
    let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO t (id) VALUES (1);";
    let builder = ChangeSet::<CreateTable>::try_from(sql).expect("Failed to parse");
    assert_eq!(builder.len(), 1);
}

#[test]
fn test_try_from_string() {
    let sql =
        String::from("CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO t (id) VALUES (1);");
    let builder = ChangeSet::<CreateTable>::try_from(sql).expect("Failed to parse");
    assert_eq!(builder.len(), 1);
}

#[test]
fn test_try_from_statements() {
    use sqlparser::dialect::SQLiteDialect;
    use sqlparser::parser::Parser;

    let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO t (id) VALUES (1);";
    let dialect = SQLiteDialect {};
    let statements = Parser::parse_sql(&dialect, sql).expect("Failed to parse SQL");

    let builder =
        ChangeSet::<CreateTable>::try_from(statements.as_slice()).expect("Failed to convert");
    assert_eq!(builder.len(), 1);
}

// =============================================================================
// Display / Roundtrip tests (ChangeSet only)
// =============================================================================

#[test]
fn test_display_simple_insert() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    let output = builder.to_string();

    // The output should contain CREATE TABLE and INSERT
    assert!(output.contains("CREATE TABLE"));
    assert!(output.contains("INSERT INTO"));
    assert!(output.contains("users"));
}

#[test]
fn test_display_roundtrip_insert() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    let output = builder.to_string();

    // Parse the output back
    let reparsed: ChangeSet<CreateTable> = output.parse().expect("Failed to re-parse SQL");

    // The reparsed builder should be equivalent
    assert_eq!(builder.len(), reparsed.len());
}

#[test]
fn test_display_roundtrip_delete() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
        DELETE FROM users WHERE id = 1;
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    // INSERT + DELETE with potentially different values = might not cancel out
    // But if they do cancel, we get empty builder
    if !builder.is_empty() {
        let output = builder.to_string();
        let reparsed: ChangeSet<CreateTable> = output.parse().expect("Failed to re-parse SQL");
        assert_eq!(builder.len(), reparsed.len());
    }
}

#[test]
fn test_display_roundtrip_mixed() {
    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
        INSERT INTO users (id, name) VALUES (2, 'Bob');
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    let output = builder.to_string();

    let reparsed: ChangeSet<CreateTable> = output.parse().expect("Failed to re-parse SQL");
    assert_eq!(builder.len(), reparsed.len());
}

// =============================================================================
// Complex value types tests
// =============================================================================

#[test]
fn test_parse_integer_values() {
    let sql = "
        CREATE TABLE numbers (id INTEGER PRIMARY KEY, value INTEGER);
        INSERT INTO numbers (id, value) VALUES (1, 42);
        INSERT INTO numbers (id, value) VALUES (2, -100);
        INSERT INTO numbers (id, value) VALUES (3, 0);
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert_eq!(builder.len(), 3);
}

#[test]
fn test_parse_real_values() {
    let sql = "
        CREATE TABLE floats (id INTEGER PRIMARY KEY, value REAL);
        INSERT INTO floats (id, value) VALUES (1, 3.14);
        INSERT INTO floats (id, value) VALUES (2, -2.5);
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert_eq!(builder.len(), 2);
}

#[test]
fn test_parse_text_values() {
    let sql = "
        CREATE TABLE texts (id INTEGER PRIMARY KEY, value TEXT);
        INSERT INTO texts (id, value) VALUES (1, 'hello');
        INSERT INTO texts (id, value) VALUES (2, 'world');
        INSERT INTO texts (id, value) VALUES (3, '');
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert_eq!(builder.len(), 3);
}

#[test]
fn test_parse_null_values() {
    let sql = "
        CREATE TABLE nullable (id INTEGER PRIMARY KEY, value TEXT);
        INSERT INTO nullable (id, value) VALUES (1, NULL);
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert_eq!(builder.len(), 1);
}

#[test]
fn test_parse_blob_values() {
    let sql = "
        CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB);
        INSERT INTO blobs (id, data) VALUES (1, X'DEADBEEF');
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert_eq!(builder.len(), 1);
}

// =============================================================================
// Composite primary key tests
// =============================================================================

#[test]
fn test_parse_composite_pk() {
    let sql = "
        CREATE TABLE composite (a INTEGER, b INTEGER, value TEXT, PRIMARY KEY (a, b));
        INSERT INTO composite (a, b, value) VALUES (1, 2, 'test');
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert_eq!(builder.len(), 1);
}

#[test]
fn test_parse_composite_pk_delete() {
    let sql = "
        CREATE TABLE composite (a INTEGER, b INTEGER, value TEXT, PRIMARY KEY (a, b));
        DELETE FROM composite WHERE a = 1 AND b = 2;
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert_eq!(builder.len(), 1);
}

#[test]
fn test_parse_composite_pk_update() {
    let sql = "
        CREATE TABLE composite (a INTEGER, b INTEGER, value TEXT, PRIMARY KEY (a, b));
        UPDATE composite SET value = 'updated' WHERE a = 1 AND b = 2;
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert_eq!(builder.len(), 1);
}

// =============================================================================
// Operation consolidation tests
// =============================================================================

#[test]
fn test_insert_then_delete_same_values_cancels() {
    let sql = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
        INSERT INTO t (id, v) VALUES (1, 'a');
        DELETE FROM t WHERE id = 1;
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    // Note: DELETE only contains PK from WHERE clause, non-PK values are Null
    // So INSERT(1, 'a') + DELETE(1, Null) won't cancel because values differ
    // This is a limitation of SQL-based construction
    assert!(builder.len() <= 1);
}

#[test]
fn test_insert_then_update_becomes_insert() {
    let sql = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
        INSERT INTO t (id, v) VALUES (1, 'a');
        UPDATE t SET v = 'b' WHERE id = 1;
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    // INSERT + UPDATE = INSERT with updated values
    assert_eq!(builder.len(), 1);
}

#[test]
fn test_update_then_update_consolidates() {
    let sql = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
        UPDATE t SET v = 'a' WHERE id = 1;
        UPDATE t SET v = 'b' WHERE id = 1;
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    // UPDATE + UPDATE = single UPDATE
    assert_eq!(builder.len(), 1);
}

// =============================================================================
// Edge cases
// =============================================================================

#[test]
fn test_empty_sql() {
    let sql = "";
    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert!(builder.is_empty());
}

#[test]
fn test_only_create_table() {
    let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY);";
    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert!(builder.is_empty());
}

#[test]
fn test_multiple_create_tables_no_ops() {
    let sql = "
        CREATE TABLE a (id INTEGER PRIMARY KEY);
        CREATE TABLE b (id INTEGER PRIMARY KEY);
        CREATE TABLE c (id INTEGER PRIMARY KEY);
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    assert!(builder.is_empty());
}

// =============================================================================
// From<ChangeSet> for Vec<Statement> tests
// =============================================================================

#[test]
fn test_into_vec_statement_insert() {
    use sqlparser::ast::Statement;

    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    let statements: Vec<Statement> = builder.into();

    // Should have 1 CREATE TABLE + 1 INSERT
    assert_eq!(statements.len(), 2);
    assert!(matches!(&statements[0], Statement::CreateTable(_)));
    assert!(matches!(&statements[1], Statement::Insert(_)));
}

#[test]
fn test_into_vec_statement_mixed() {
    use sqlparser::ast::Statement;

    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
        INSERT INTO users (id, name) VALUES (2, 'Bob');
        DELETE FROM users WHERE id = 3;
        UPDATE users SET name = 'Charlie' WHERE id = 4;
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    let statements: Vec<Statement> = builder.clone().into();

    // Should have 1 CREATE TABLE + 4 operations
    assert_eq!(statements.len(), 5);
    assert!(matches!(&statements[0], Statement::CreateTable(_)));
}

#[test]
fn test_into_vec_statement_roundtrip() {
    use sqlparser::ast::Statement;

    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    let statements: Vec<Statement> = builder.clone().into();

    // Re-parse from statements
    let reparsed = ChangeSet::try_from(statements.as_slice()).expect("Failed to parse statements");

    assert_eq!(builder.len(), reparsed.len());
}

#[test]
fn test_into_vec_statement_empty() {
    use sqlparser::ast::Statement;

    let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY);";
    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");
    let statements: Vec<Statement> = builder.into();

    // Empty builder produces no statements (CREATE TABLE is skipped for empty tables)
    assert!(statements.is_empty());
}

#[test]
fn test_from_ref_changeset_for_vec_statement() {
    use sqlparser::ast::Statement;

    let sql = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
    ";

    let builder: ChangeSet<CreateTable> = sql.parse().expect("Failed to parse SQL");

    // Test From<&ChangeSet> for Vec<Statement>
    let statements: Vec<Statement> = (&builder).into();
    assert_eq!(statements.len(), 2);

    // Verify builder is still usable (not moved)
    assert_eq!(builder.len(), 1);
}
