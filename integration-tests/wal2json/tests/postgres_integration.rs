//! Integration tests for wal2json parsing against real PostgreSQL instances.
//!
//! These tests use testcontainers to spawn PostgreSQL containers with wal2json
//! and verify our parsing and conversion logic against real WAL output.

use sqlite_diff_rs::wal2json::{Action, parse_v1, parse_v2};
use sqlite_diff_rs::{ChangeDelete, Insert, SimpleTable};
use wal2json_integration_tests::{
    connect, create_replication_slot, drop_replication_slot, get_changes_v1, get_changes_v2,
    start_postgres,
};

/// Run an INSERT test for a specific PostgreSQL version.
async fn run_insert_test(version: &str) {
    let (_container, port) = start_postgres(version).await;
    let client = connect(port).await;

    // Create table
    client
        .execute(
            "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, email TEXT)",
            &[],
        )
        .await
        .expect("Failed to create table");

    // Set REPLICA IDENTITY to FULL for better change tracking
    client
        .execute("ALTER TABLE users REPLICA IDENTITY FULL", &[])
        .await
        .expect("Failed to set replica identity");

    // Create replication slot
    create_replication_slot(&client, "test_slot").await;

    // Insert data
    client
        .execute(
            "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
            &[],
        )
        .await
        .expect("Failed to insert");

    // Get changes in v2 format
    let changes = get_changes_v2(&client, "test_slot").await;

    // Find the insert message
    let insert_msg = changes
        .iter()
        .filter_map(|json| parse_v2(json).ok())
        .find(|msg| msg.action == Action::I);

    assert!(
        insert_msg.is_some(),
        "Expected INSERT message in changes for PG {version}"
    );

    let msg = insert_msg.unwrap();
    assert_eq!(msg.table.as_deref(), Some("users"));

    let columns = msg.columns.as_ref().expect("Expected columns");
    assert_eq!(columns.len(), 3);

    // Verify column values
    let id_col = columns
        .iter()
        .find(|c| c.name == "id")
        .expect("Expected id column");
    assert!(id_col.value.as_i64().is_some());

    let name_col = columns
        .iter()
        .find(|c| c.name == "name")
        .expect("Expected name column");
    assert_eq!(name_col.value.as_str(), Some("Alice"));

    let email_col = columns
        .iter()
        .find(|c| c.name == "email")
        .expect("Expected email column");
    assert_eq!(email_col.value.as_str(), Some("alice@example.com"));

    // Test conversion to Insert builder - verify it doesn't error
    let table = SimpleTable::new("users", &["id", "name", "email"], &[0]);
    let _insert: Insert<_, String, Vec<u8>> = (&msg, &table)
        .try_into()
        .expect("Failed to convert to Insert");

    // Cleanup
    drop_replication_slot(&client, "test_slot").await;
}

/// Run an UPDATE test for a specific PostgreSQL version.
async fn run_update_test(version: &str) {
    let (_container, port) = start_postgres(version).await;
    let client = connect(port).await;

    // Create table and insert initial data
    client
        .execute(
            "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, email TEXT)",
            &[],
        )
        .await
        .expect("Failed to create table");

    client
        .execute("ALTER TABLE users REPLICA IDENTITY FULL", &[])
        .await
        .expect("Failed to set replica identity");

    client
        .execute(
            "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
            &[],
        )
        .await
        .expect("Failed to insert");

    // Create replication slot AFTER initial data
    create_replication_slot(&client, "test_slot").await;

    // Update data
    client
        .execute(
            "UPDATE users SET name = 'Bob', email = 'bob@example.com' WHERE id = 1",
            &[],
        )
        .await
        .expect("Failed to update");

    // Get changes
    let changes = get_changes_v2(&client, "test_slot").await;

    let update_msg = changes
        .iter()
        .filter_map(|json| parse_v2(json).ok())
        .find(|msg| msg.action == Action::U);

    assert!(
        update_msg.is_some(),
        "Expected UPDATE message in changes for PG {version}"
    );

    let msg = update_msg.unwrap();
    assert_eq!(msg.table.as_deref(), Some("users"));

    // Verify new values
    let columns = msg.columns.as_ref().expect("Expected columns");
    let name_col = columns
        .iter()
        .find(|c| c.name == "name")
        .expect("Expected name column");
    assert_eq!(name_col.value.as_str(), Some("Bob"));

    // Verify identity (old PK)
    let identity = msg.identity.as_ref().expect("Expected identity");
    assert!(!identity.is_empty());

    // Cleanup
    drop_replication_slot(&client, "test_slot").await;
}

/// Run a DELETE test for a specific PostgreSQL version.
async fn run_delete_test(version: &str) {
    let (_container, port) = start_postgres(version).await;
    let client = connect(port).await;

    // Create table and insert initial data
    client
        .execute("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)", &[])
        .await
        .expect("Failed to create table");

    client
        .execute("ALTER TABLE users REPLICA IDENTITY FULL", &[])
        .await
        .expect("Failed to set replica identity");

    client
        .execute("INSERT INTO users (name) VALUES ('Alice')", &[])
        .await
        .expect("Failed to insert");

    // Create replication slot AFTER initial data
    create_replication_slot(&client, "test_slot").await;

    // Delete data
    client
        .execute("DELETE FROM users WHERE id = 1", &[])
        .await
        .expect("Failed to delete");

    // Get changes
    let changes = get_changes_v2(&client, "test_slot").await;

    let delete_msg = changes
        .iter()
        .filter_map(|json| parse_v2(json).ok())
        .find(|msg| msg.action == Action::D);

    assert!(
        delete_msg.is_some(),
        "Expected DELETE message in changes for PG {version}"
    );

    let msg = delete_msg.unwrap();
    assert_eq!(msg.table.as_deref(), Some("users"));

    // DELETE should have identity columns
    let identity = msg.identity.as_ref().expect("Expected identity");
    let id_col = identity
        .iter()
        .find(|c| c.name == "id")
        .expect("Expected id column");
    assert_eq!(id_col.value.as_i64(), Some(1));

    // Test conversion to ChangeDelete builder
    let table = SimpleTable::new("users", &["id", "name"], &[0]);
    let _delete: ChangeDelete<_, String, Vec<u8>> = (&msg, &table)
        .try_into()
        .expect("Failed to convert to ChangeDelete");

    // Cleanup
    drop_replication_slot(&client, "test_slot").await;
}

/// Run a test with multiple operations in a single transaction.
async fn run_multiple_operations_test(version: &str) {
    let (_container, port) = start_postgres(version).await;
    let client = connect(port).await;

    client
        .execute(
            "CREATE TABLE items (id SERIAL PRIMARY KEY, name TEXT, quantity INTEGER)",
            &[],
        )
        .await
        .expect("Failed to create table");

    client
        .execute("ALTER TABLE items REPLICA IDENTITY FULL", &[])
        .await
        .expect("Failed to set replica identity");

    create_replication_slot(&client, "test_slot").await;

    // Perform multiple operations in one transaction
    client
        .batch_execute(
            "BEGIN;
             INSERT INTO items (name, quantity) VALUES ('Apple', 10);
             INSERT INTO items (name, quantity) VALUES ('Banana', 20);
             UPDATE items SET quantity = 15 WHERE name = 'Apple';
             DELETE FROM items WHERE name = 'Banana';
             COMMIT;",
        )
        .await
        .expect("Failed to execute batch");

    let changes = get_changes_v2(&client, "test_slot").await;

    let messages: Vec<_> = changes
        .iter()
        .filter_map(|json| parse_v2(json).ok())
        .collect();

    // Count action types
    let inserts = messages.iter().filter(|m| m.action == Action::I).count();
    let updates = messages.iter().filter(|m| m.action == Action::U).count();
    let deletes = messages.iter().filter(|m| m.action == Action::D).count();

    assert_eq!(inserts, 2, "Expected 2 inserts");
    assert_eq!(updates, 1, "Expected 1 update");
    assert_eq!(deletes, 1, "Expected 1 delete");

    drop_replication_slot(&client, "test_slot").await;
}

/// Test NULL value handling.
async fn run_null_values_test(version: &str) {
    let (_container, port) = start_postgres(version).await;
    let client = connect(port).await;

    client
        .execute(
            "CREATE TABLE nullable (id SERIAL PRIMARY KEY, optional_text TEXT, optional_int INTEGER)",
            &[],
        )
        .await
        .expect("Failed to create table");

    client
        .execute("ALTER TABLE nullable REPLICA IDENTITY FULL", &[])
        .await
        .expect("Failed to set replica identity");

    create_replication_slot(&client, "test_slot").await;

    // Insert with NULL values
    client
        .execute(
            "INSERT INTO nullable (optional_text, optional_int) VALUES (NULL, NULL)",
            &[],
        )
        .await
        .expect("Failed to insert");

    let changes = get_changes_v2(&client, "test_slot").await;

    let insert_msg = changes
        .iter()
        .filter_map(|json| parse_v2(json).ok())
        .find(|msg| msg.action == Action::I);

    assert!(insert_msg.is_some());
    let msg = insert_msg.unwrap();

    let columns = msg.columns.as_ref().expect("Expected columns");
    let text_col = columns
        .iter()
        .find(|c| c.name == "optional_text")
        .expect("Expected optional_text column");
    assert!(text_col.value.is_null());

    let int_col = columns
        .iter()
        .find(|c| c.name == "optional_int")
        .expect("Expected optional_int column");
    assert!(int_col.value.is_null());

    // Test conversion succeeds
    let table = SimpleTable::new("nullable", &["id", "optional_text", "optional_int"], &[0]);
    let _insert: Insert<_, String, Vec<u8>> = (&msg, &table).try_into().expect("Failed to convert");

    drop_replication_slot(&client, "test_slot").await;
}

/// Test various PostgreSQL types.
async fn run_various_types_test(version: &str) {
    let (_container, port) = start_postgres(version).await;
    let client = connect(port).await;

    client
        .execute(
            "CREATE TABLE typed (
                id SERIAL PRIMARY KEY,
                int_col INTEGER,
                bigint_col BIGINT,
                real_col REAL,
                double_col DOUBLE PRECISION,
                bool_col BOOLEAN,
                text_col TEXT,
                varchar_col VARCHAR(100)
            )",
            &[],
        )
        .await
        .expect("Failed to create table");

    client
        .execute("ALTER TABLE typed REPLICA IDENTITY FULL", &[])
        .await
        .expect("Failed to set replica identity");

    create_replication_slot(&client, "test_slot").await;

    client
        .execute(
            "INSERT INTO typed (int_col, bigint_col, real_col, double_col, bool_col, text_col, varchar_col)
             VALUES (42, 9223372036854775807, 3.14, 2.718281828, true, 'hello', 'world')",
            &[],
        )
        .await
        .expect("Failed to insert");

    let changes = get_changes_v2(&client, "test_slot").await;

    let insert_msg = changes
        .iter()
        .filter_map(|json| parse_v2(json).ok())
        .find(|msg| msg.action == Action::I);

    assert!(insert_msg.is_some());
    let msg = insert_msg.unwrap();
    let columns = msg.columns.as_ref().expect("Expected columns");

    // Verify integer
    let int_col = columns.iter().find(|c| c.name == "int_col").unwrap();
    assert_eq!(int_col.value.as_i64(), Some(42));

    // Verify bigint
    let bigint_col = columns.iter().find(|c| c.name == "bigint_col").unwrap();
    assert_eq!(bigint_col.value.as_i64(), Some(9223372036854775807i64));

    // Verify real/float
    let real_col = columns.iter().find(|c| c.name == "real_col").unwrap();
    assert!(real_col.value.as_f64().is_some());

    // Verify boolean
    let bool_col = columns.iter().find(|c| c.name == "bool_col").unwrap();
    assert_eq!(bool_col.value.as_bool(), Some(true));

    // Verify text
    let text_col = columns.iter().find(|c| c.name == "text_col").unwrap();
    assert_eq!(text_col.value.as_str(), Some("hello"));

    drop_replication_slot(&client, "test_slot").await;
}

/// Test v1 format parsing.
async fn run_v1_format_test(version: &str) {
    let (_container, port) = start_postgres(version).await;
    let client = connect(port).await;

    client
        .execute(
            "CREATE TABLE v1test (id SERIAL PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .expect("Failed to create table");

    client
        .execute("ALTER TABLE v1test REPLICA IDENTITY FULL", &[])
        .await
        .expect("Failed to set replica identity");

    create_replication_slot(&client, "test_slot").await;

    client
        .execute("INSERT INTO v1test (name) VALUES ('v1 test')", &[])
        .await
        .expect("Failed to insert");

    let changes = get_changes_v1(&client, "test_slot").await;

    // v1 format returns one JSON object per transaction
    assert!(!changes.is_empty());

    // Find a transaction with changes
    let tx = changes
        .iter()
        .filter_map(|json| parse_v1(json).ok())
        .find(|tx| !tx.change.is_empty());

    assert!(tx.is_some(), "Expected transaction with changes");
    let tx = tx.unwrap();

    let insert_change = tx.change.iter().find(|c| c.kind == "insert");
    assert!(insert_change.is_some());

    let change = insert_change.unwrap();
    assert_eq!(change.table, "v1test");
    assert!(change.columnnames.contains(&"name".to_string()));
    assert!(change.columnvalues.iter().any(|v| v == "v1 test"));

    drop_replication_slot(&client, "test_slot").await;
}

/// Test v2 format specifically.
async fn run_v2_format_test(version: &str) {
    let (_container, port) = start_postgres(version).await;
    let client = connect(port).await;

    client
        .execute(
            "CREATE TABLE v2test (id SERIAL PRIMARY KEY, data TEXT)",
            &[],
        )
        .await
        .expect("Failed to create table");

    client
        .execute("ALTER TABLE v2test REPLICA IDENTITY FULL", &[])
        .await
        .expect("Failed to set replica identity");

    create_replication_slot(&client, "test_slot").await;

    client
        .execute("INSERT INTO v2test (data) VALUES ('v2 format test')", &[])
        .await
        .expect("Failed to insert");

    let changes = get_changes_v2(&client, "test_slot").await;

    // v2 format has BEGIN, action, COMMIT messages
    let has_begin = changes.iter().any(|json| {
        parse_v2(json)
            .map(|msg| msg.action == Action::B)
            .unwrap_or(false)
    });
    let has_commit = changes.iter().any(|json| {
        parse_v2(json)
            .map(|msg| msg.action == Action::C)
            .unwrap_or(false)
    });
    let has_insert = changes.iter().any(|json| {
        parse_v2(json)
            .map(|msg| msg.action == Action::I)
            .unwrap_or(false)
    });

    assert!(has_begin, "Expected BEGIN message");
    assert!(has_commit, "Expected COMMIT message");
    assert!(has_insert, "Expected INSERT message");

    drop_replication_slot(&client, "test_slot").await;
}

// PostgreSQL 15 tests (only version with wal2json Docker image available)
// For newer PostgreSQL versions (16+), use pg_walstream with pgoutput instead.
#[tokio::test]
async fn test_insert_pg15() {
    run_insert_test("15").await;
}

#[tokio::test]
async fn test_update_pg15() {
    run_update_test("15").await;
}

#[tokio::test]
async fn test_delete_pg15() {
    run_delete_test("15").await;
}

#[tokio::test]
async fn test_multiple_operations_pg15() {
    run_multiple_operations_test("15").await;
}

#[tokio::test]
async fn test_null_values_pg15() {
    run_null_values_test("15").await;
}

#[tokio::test]
async fn test_various_types_pg15() {
    run_various_types_test("15").await;
}

#[tokio::test]
async fn test_v1_format_pg15() {
    run_v1_format_test("15").await;
}

#[tokio::test]
async fn test_v2_format_pg15() {
    run_v2_format_test("15").await;
}
