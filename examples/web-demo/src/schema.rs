//! Database schema shared between diesel and sqlite-diff-rs.
//!
//! The DDL is the single source of truth. The diesel `table!` macro
//! describes the same shape for query-building, and the [`messages_table`]
//! helper returns a `SimpleTable` so sqlite-diff-rs can refer to the same
//! columns by index when parsing or constructing changesets.

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

/// Schema definition executed when the in-memory database is opened.
pub const INIT_DDL: &str = "CREATE TABLE messages (\
    id BLOB PRIMARY KEY NOT NULL, \
    author TEXT NOT NULL, \
    body TEXT NOT NULL, \
    created_at INTEGER NOT NULL, \
    edited_at INTEGER\
)";

/// Returns the `SimpleTable` view of the `messages` schema used by
/// sqlite-diff-rs builders and parsers. The column order must stay aligned
/// with [`INIT_DDL`] and the diesel `table!` block above.
#[allow(dead_code)] // wired up in M5 (diff inspector pane)
#[must_use]
pub fn messages_table() -> SimpleTable {
    SimpleTable::new(
        "messages",
        &["id", "author", "body", "created_at", "edited_at"],
        &[0],
    )
}
