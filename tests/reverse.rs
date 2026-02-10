//! Tests for the Reverse trait and reversing changesets.
//!
//! This module tests:
//! - Reversing individual operations (INSERT, DELETE, UPDATE)
//! - Reversing complete changesets
//! - Applying a changeset and its reverse yields the original state
//! - Double reversal is idempotent (reverse(reverse(x)) == x)

#![cfg(feature = "testing")]

use rusqlite::Connection;
use sqlite_diff_rs::testing::{apply_changeset, get_all_rows};
use sqlite_diff_rs::{
    ChangeDelete, ChangeSet, ChangesetFormat, DiffOps, Insert, Reverse, SimpleTable, Update,
};

// =============================================================================
// Basic reversal tests
// =============================================================================

#[test]
fn test_reverse_single_insert() {
    let schema = SimpleTable::new("users", &["id", "name"], &[0]);

    let insert = Insert::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap();

    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new().insert(insert);
    let reversed = changeset.clone().reverse();

    // Reversed should have one operation
    assert_eq!(reversed.len(), 1);

    // Original: INSERT, Reversed: DELETE
    let changeset_bytes = changeset.build();
    let reversed_bytes = reversed.build();

    // They should be different
    assert_ne!(changeset_bytes, reversed_bytes);
}

#[test]
fn test_reverse_single_delete() {
    let schema = SimpleTable::new("users", &["id", "name"], &[0]);

    let delete = ChangeDelete::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap();

    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new().delete(delete);
    let reversed = changeset.clone().reverse();

    assert_eq!(reversed.len(), 1);
}

#[test]
fn test_reverse_single_update() {
    let schema = SimpleTable::new("users", &["id", "name"], &[0]);

    let update = Update::<SimpleTable, ChangesetFormat, String, Vec<u8>>::from(schema.clone())
        .set(0, 1i64, 1i64)
        .unwrap() // PK unchanged
        .set(1, "Alice", "Alicia")
        .unwrap();

    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new().update(update);
    let reversed = changeset.clone().reverse();

    assert_eq!(reversed.len(), 1);
}

#[test]
fn test_reverse_multiple_operations() {
    let schema = SimpleTable::new("users", &["id", "name"], &[0]);

    let insert1 = Insert::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap();

    let insert2 = Insert::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 2i64)
        .unwrap()
        .set(1, "Bob")
        .unwrap();

    let delete = ChangeDelete::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 3i64)
        .unwrap()
        .set(1, "Charlie")
        .unwrap();

    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new()
        .insert(insert1)
        .insert(insert2)
        .delete(delete);

    let reversed = changeset.clone().reverse();
    assert_eq!(reversed.len(), 3);
}

// =============================================================================
// Idempotency tests: reverse(reverse(x)) == x
// =============================================================================

#[test]
fn test_double_reverse_is_identity() {
    let schema = SimpleTable::new("users", &["id", "name"], &[0]);

    let insert = Insert::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap();

    let original: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new().insert(insert);
    let double_reversed = original.clone().reverse().reverse();

    // Double reverse should equal original
    assert_eq!(original, double_reversed);
}

#[test]
fn test_double_reverse_complex() {
    let schema = SimpleTable::new("users", &["id", "name", "age"], &[0]);

    let insert = Insert::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap()
        .set(2, 30i64)
        .unwrap();

    let update = Update::<SimpleTable, ChangesetFormat, String, Vec<u8>>::from(schema.clone())
        .set(0, 2i64, 2i64)
        .unwrap()
        .set(1, "Bob", "Robert")
        .unwrap()
        .set(2, 25i64, 26i64)
        .unwrap();

    let delete = ChangeDelete::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 3i64)
        .unwrap()
        .set(1, "Charlie")
        .unwrap()
        .set(2, 35i64)
        .unwrap();

    let original: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new()
        .insert(insert)
        .update(update)
        .delete(delete);

    let double_reversed = original.clone().reverse().reverse();
    assert_eq!(original, double_reversed);
}

// =============================================================================
// Database state tests: applying changeset then reverse yields original state
// =============================================================================

#[test]
fn test_apply_and_reverse_insert() {
    // Create two identical databases
    let conn1 = Connection::open_in_memory().unwrap();
    let conn2 = Connection::open_in_memory().unwrap();

    let schema_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
    conn1.execute(schema_sql, []).unwrap();
    conn2.execute(schema_sql, []).unwrap();

    // Apply an insert to conn1
    let schema = SimpleTable::new("users", &["id", "name"], &[0]);
    let insert = Insert::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap();

    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new().insert(insert);
    let changeset_bytes = changeset.build();
    apply_changeset(&conn1, &changeset_bytes).unwrap();

    // conn1 now has the row, conn2 doesn't
    assert_eq!(get_all_rows(&conn1, "users").len(), 1);
    assert_eq!(get_all_rows(&conn2, "users").len(), 0);

    // Apply the reverse to conn1
    let reversed = changeset.reverse();
    let reversed_bytes = reversed.build();
    apply_changeset(&conn1, &reversed_bytes).unwrap();

    // Now both should be empty (back to original state)
    assert_eq!(get_all_rows(&conn1, "users").len(), 0);
    assert_eq!(get_all_rows(&conn2, "users").len(), 0);
}

#[test]
fn test_apply_and_reverse_delete() {
    let conn1 = Connection::open_in_memory().unwrap();
    let conn2 = Connection::open_in_memory().unwrap();

    let schema_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
    conn1.execute(schema_sql, []).unwrap();
    conn2.execute(schema_sql, []).unwrap();

    // Insert initial data into both
    conn1
        .execute("INSERT INTO users (id, name) VALUES (1, 'Alice')", [])
        .unwrap();
    conn2
        .execute("INSERT INTO users (id, name) VALUES (1, 'Alice')", [])
        .unwrap();

    // Delete from conn1
    let schema = SimpleTable::new("users", &["id", "name"], &[0]);
    let delete = ChangeDelete::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap();

    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new().delete(delete);
    let changeset_bytes = changeset.build();
    apply_changeset(&conn1, &changeset_bytes).unwrap();

    // conn1 is empty, conn2 still has the row
    assert_eq!(get_all_rows(&conn1, "users").len(), 0);
    assert_eq!(get_all_rows(&conn2, "users").len(), 1);

    // Apply reverse to conn1 (should re-insert)
    let reversed = changeset.reverse();
    let reversed_bytes = reversed.build();
    apply_changeset(&conn1, &reversed_bytes).unwrap();

    // Both should have 1 row again
    assert_eq!(get_all_rows(&conn1, "users").len(), 1);
    assert_eq!(get_all_rows(&conn2, "users").len(), 1);

    // Check content matches
    let rows1 = get_all_rows(&conn1, "users");
    let rows2 = get_all_rows(&conn2, "users");
    assert_eq!(rows1, rows2);
}

#[test]
fn test_apply_and_reverse_update() {
    let conn1 = Connection::open_in_memory().unwrap();
    let conn2 = Connection::open_in_memory().unwrap();

    let schema_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
    conn1.execute(schema_sql, []).unwrap();
    conn2.execute(schema_sql, []).unwrap();

    // Insert initial data
    conn1
        .execute("INSERT INTO users (id, name) VALUES (1, 'Alice')", [])
        .unwrap();
    conn2
        .execute("INSERT INTO users (id, name) VALUES (1, 'Alice')", [])
        .unwrap();

    // Update in conn1
    let schema = SimpleTable::new("users", &["id", "name"], &[0]);
    let update = Update::<SimpleTable, ChangesetFormat, String, Vec<u8>>::from(schema.clone())
        .set(0, 1i64, 1i64)
        .unwrap()
        .set(1, "Alice", "Alicia")
        .unwrap();

    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new().update(update);
    let changeset_bytes = changeset.build();
    apply_changeset(&conn1, &changeset_bytes).unwrap();

    // conn1 has "Alicia", conn2 has "Alice"
    let rows1 = get_all_rows(&conn1, "users");
    let rows2 = get_all_rows(&conn2, "users");
    assert_ne!(rows1, rows2);
    assert!(rows1[0][1].contains("Alicia"));
    assert!(rows2[0][1].contains("Alice"));

    // Apply reverse to conn1
    let reversed = changeset.reverse();
    let reversed_bytes = reversed.build();
    apply_changeset(&conn1, &reversed_bytes).unwrap();

    // Both should have "Alice" again
    let rows1 = get_all_rows(&conn1, "users");
    let rows2 = get_all_rows(&conn2, "users");
    assert_eq!(rows1, rows2);
    assert!(rows1[0][1].contains("Alice"));
}

// =============================================================================
// Complex multi-operation tests
// =============================================================================

#[test]
fn test_apply_and_reverse_multiple_operations() {
    let conn1 = Connection::open_in_memory().unwrap();
    let conn2 = Connection::open_in_memory().unwrap();

    let schema_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
    conn1.execute(schema_sql, []).unwrap();
    conn2.execute(schema_sql, []).unwrap();

    // Start with some data
    conn1
        .execute("INSERT INTO users (id, name) VALUES (1, 'Alice')", [])
        .unwrap();
    conn1
        .execute("INSERT INTO users (id, name) VALUES (2, 'Bob')", [])
        .unwrap();
    conn2
        .execute("INSERT INTO users (id, name) VALUES (1, 'Alice')", [])
        .unwrap();
    conn2
        .execute("INSERT INTO users (id, name) VALUES (2, 'Bob')", [])
        .unwrap();

    let schema = SimpleTable::new("users", &["id", "name"], &[0]);

    // Create a changeset with multiple operations
    let insert = Insert::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 3i64)
        .unwrap()
        .set(1, "Charlie")
        .unwrap();

    let update = Update::<SimpleTable, ChangesetFormat, String, Vec<u8>>::from(schema.clone())
        .set(0, 1i64, 1i64)
        .unwrap()
        .set(1, "Alice", "Alicia")
        .unwrap();

    let delete = ChangeDelete::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 2i64)
        .unwrap()
        .set(1, "Bob")
        .unwrap();

    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new()
        .insert(insert)
        .update(update)
        .delete(delete);

    let changeset_bytes = changeset.build();

    // Save original state of conn1
    let original_rows = get_all_rows(&conn1, "users");

    // Apply changeset
    apply_changeset(&conn1, &changeset_bytes).unwrap();

    // Verify it changed
    let modified_rows = get_all_rows(&conn1, "users");
    assert_ne!(original_rows, modified_rows);

    // Apply reverse
    let reversed = changeset.reverse();
    let reversed_bytes = reversed.build();
    apply_changeset(&conn1, &reversed_bytes).unwrap();

    // Should be back to original state
    let final_rows = get_all_rows(&conn1, "users");
    assert_eq!(original_rows, final_rows);

    // Both databases should match
    let conn2_rows = get_all_rows(&conn2, "users");
    assert_eq!(final_rows, conn2_rows);
}

#[test]
fn test_reverse_with_composite_primary_key() {
    let schema = SimpleTable::new("orders", &["user_id", "order_id", "status"], &[0, 1]);

    let insert = Insert::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, 100i64)
        .unwrap()
        .set(2, "pending")
        .unwrap();

    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new().insert(insert);
    let reversed = changeset.clone().reverse();

    // Double reverse should equal original
    let double_reversed = reversed.reverse();
    assert_eq!(changeset, double_reversed);
}

#[test]
fn test_reverse_with_null_values() {
    let schema = SimpleTable::new("items", &["id", "description", "price"], &[0]);

    let insert = Insert::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set_null(1)
        .unwrap() // description = NULL
        .set(2, 9.99f64)
        .unwrap();

    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new().insert(insert);
    let reversed = changeset.clone().reverse();
    let double_reversed = reversed.reverse();

    assert_eq!(changeset, double_reversed);
}

// =============================================================================
// Edge cases
// =============================================================================

#[test]
fn test_reverse_empty_changeset() {
    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = ChangeSet::new();
    let reversed = changeset.clone().reverse();

    assert_eq!(reversed.len(), 0);
    assert!(reversed.is_empty());
}

#[test]
fn test_reverse_consolidated_operations() {
    // Test that consolidated operations reverse correctly
    let schema = SimpleTable::new("users", &["id", "name"], &[0]);

    // INSERT + UPDATE consolidates to INSERT with updated values
    let insert = Insert::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap();

    let update = Update::<SimpleTable, ChangesetFormat, String, Vec<u8>>::from(schema.clone())
        .set(0, 1i64, 1i64)
        .unwrap()
        .set(1, "Alice", "Alicia")
        .unwrap();

    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> =
        ChangeSet::new().insert(insert).update(update); // This consolidates to INSERT with "Alicia"

    // Should have 1 operation (consolidated)
    assert_eq!(changeset.len(), 1);

    let reversed = changeset.clone().reverse();

    // Reversed should also have 1 operation (DELETE)
    assert_eq!(reversed.len(), 1);

    // Double reverse should equal original
    let double_reversed = reversed.reverse();
    assert_eq!(changeset, double_reversed);
}

#[test]
fn test_reverse_cancelled_operations() {
    // INSERT + DELETE of same row cancels out
    let schema = SimpleTable::new("users", &["id", "name"], &[0]);

    let insert = Insert::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap();

    let delete = ChangeDelete::<_, String, Vec<u8>>::from(schema.clone())
        .set(0, 1i64)
        .unwrap()
        .set(1, "Alice")
        .unwrap();

    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> =
        ChangeSet::new().insert(insert).delete(delete);

    // Should cancel out to empty
    assert_eq!(changeset.len(), 0);

    let reversed = changeset.clone().reverse();
    assert_eq!(reversed.len(), 0);
}
