//! Table column index constants.
//!
//! These are used by both the backend (for typed parsing of patchset
//! column values) and the frontend (for typed reads from local SQLite).

/// Column index constants for the `users` table, matching the DDL order.
pub mod users_columns {
    /// `id` column index.
    pub const ID: usize = 0;
    /// `name` column index.
    pub const NAME: usize = 1;
    /// `created_at` column index.
    pub const CREATED_AT: usize = 2;
}

/// Column index constants for the `messages` table, matching the DDL order.
pub mod messages_columns {
    /// `id` column index.
    pub const ID: usize = 0;
    /// `sender_id` column index.
    pub const SENDER_ID: usize = 1;
    /// `receiver_id` column index.
    pub const RECEIVER_ID: usize = 2;
    /// `body` column index.
    pub const BODY: usize = 3;
    /// `created_at` column index.
    pub const CREATED_AT: usize = 4;
}
