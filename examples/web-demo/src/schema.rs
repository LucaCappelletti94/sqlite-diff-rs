//! Database schema. Three tables, all session-attached so writes flow
//! through the gossip mesh:
//!
//! - `messages`: chat row per user write.
//! - `peers`: one row per peer who has ever announced themselves in
//!   this session. Functions as the room membership list.
//! - `typing`: one row per peer who is currently typing. The
//!   `updated_at` column is a millisecond timestamp that the sender
//!   refreshes while keys are being pressed. Consumers render a row
//!   as "typing" only when it is recent enough.

use sqlite_diff_rs::SimpleTable;

diesel::table! {
    messages (id) {
        id -> Binary,
        author -> Text,
        body -> Text,
        created_at -> BigInt,
        edited_at -> Nullable<BigInt>,
    }
}

diesel::table! {
    peers (self_id) {
        self_id -> Binary,
        display_name -> Text,
        last_seen -> BigInt,
    }
}

diesel::table! {
    typing (self_id) {
        self_id -> Binary,
        updated_at -> BigInt,
    }
}

/// DDL executed when the in-memory database is opened. The order
/// matters only insofar as the session must attach to *existing*
/// tables, so all three are created before `Db::open` calls
/// `attach_by_name`.
pub const INIT_DDL: &str = "\
CREATE TABLE messages (\
    id BLOB PRIMARY KEY NOT NULL, \
    author TEXT NOT NULL, \
    body TEXT NOT NULL, \
    created_at INTEGER NOT NULL, \
    edited_at INTEGER\
);\
CREATE TABLE peers (\
    self_id BLOB PRIMARY KEY NOT NULL, \
    display_name TEXT NOT NULL, \
    last_seen INTEGER NOT NULL\
);\
CREATE TABLE typing (\
    self_id BLOB PRIMARY KEY NOT NULL, \
    updated_at INTEGER NOT NULL\
);";

/// `SimpleTable` view of the `messages` schema used by sqlite-diff-rs
/// builders and parsers. Column order must stay aligned with the diesel
/// `table!` macro and the DDL.
#[must_use]
pub fn messages_table() -> SimpleTable {
    SimpleTable::new(
        "messages",
        &["id", "author", "body", "created_at", "edited_at"],
        &[0],
    )
}

/// `SimpleTable` view of the `peers` schema.
#[must_use]
pub fn peers_table() -> SimpleTable {
    SimpleTable::new("peers", &["self_id", "display_name", "last_seen"], &[0])
}

/// `SimpleTable` view of the `typing` schema.
#[must_use]
pub fn typing_table() -> SimpleTable {
    SimpleTable::new("typing", &["self_id", "updated_at"], &[0])
}
