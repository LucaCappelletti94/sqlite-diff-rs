//! DDL statements and sqlite-diff-rs `TableSchema` definitions.

use alloc::string::String;
use alloc::vec;
use sqlite_diff_rs::TableSchema;

/// SQL to create the `users` table.
///
/// Schema: `users (id BLOB PK, name TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL)`
pub const USERS_DDL: &str = "\
CREATE TABLE IF NOT EXISTS users (
    id BLOB PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);";

/// SQL to create the `messages` table.
///
/// Schema: `messages (id BLOB PK, sender_id BLOB NOT NULL, receiver_id BLOB NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL)`
pub const MESSAGES_DDL: &str = "\
CREATE TABLE IF NOT EXISTS messages (
    id BLOB PRIMARY KEY NOT NULL,
    sender_id BLOB NOT NULL REFERENCES users(id),
    receiver_id BLOB NOT NULL REFERENCES users(id),
    body TEXT NOT NULL,
    created_at TEXT NOT NULL
);";

/// Combined DDL for initializing a fresh database.
pub const INIT_DDL: &str = "\
CREATE TABLE IF NOT EXISTS users (
    id BLOB PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
    id BLOB PRIMARY KEY NOT NULL,
    sender_id BLOB NOT NULL REFERENCES users(id),
    receiver_id BLOB NOT NULL REFERENCES users(id),
    body TEXT NOT NULL,
    created_at TEXT NOT NULL
);";

/// sqlite-diff-rs `TableSchema` for the `users` table.
///
/// Columns: `[id (PK), name, created_at]`
/// PK flags: `[1, 0, 0]` — `id` is the sole primary key column.
#[must_use]
pub fn users_table_schema() -> TableSchema {
    TableSchema::new(String::from("users"), 3, vec![1, 0, 0])
}

/// sqlite-diff-rs `TableSchema` for the `messages` table.
///
/// Columns: `[id (PK), sender_id, receiver_id, body, created_at]`
/// PK flags: `[1, 0, 0, 0, 0]` — `id` is the sole primary key column.
#[must_use]
pub fn messages_table_schema() -> TableSchema {
    TableSchema::new(String::from("messages"), 5, vec![1, 0, 0, 0, 0])
}
