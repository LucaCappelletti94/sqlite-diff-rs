//! End-to-end byte-parity check against rusqlite's session extension.
//!
//! Pins the `differential_testing::run_differential_test` helper without
//! requiring a fuzz run. The crash inputs and structured fuzzers exercise
//! this helper through wrappers, but this file exercises it directly.

#![cfg(feature = "testing")]

use sqlite_diff_rs::SimpleTable;
use sqlite_diff_rs::differential_testing::run_differential_test;

#[test]
fn differential_insert_update_delete_byte_parity() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    let create = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
    let dml = [
        "INSERT INTO users (id, name) VALUES (1, 'Alice')",
        "INSERT INTO users (id, name) VALUES (2, 'Bob')",
        "UPDATE users SET name = 'Alicia' WHERE id = 1",
        "DELETE FROM users WHERE id = 2",
    ];
    run_differential_test(&[users], &[create], &dml);
}

#[test]
fn differential_multi_table_byte_parity() {
    let users = SimpleTable::new("users", &["id", "name"], &[0]);
    let posts = SimpleTable::new("posts", &["id", "user_id", "body"], &[0]);
    let create_users = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
    let create_posts = "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT)";
    let dml = [
        "INSERT INTO users (id, name) VALUES (1, 'Alice')",
        "INSERT INTO posts (id, user_id, body) VALUES (10, 1, 'hello')",
        "UPDATE posts SET body = 'world' WHERE id = 10",
    ];
    run_differential_test(&[users, posts], &[create_users, create_posts], &dml);
}
