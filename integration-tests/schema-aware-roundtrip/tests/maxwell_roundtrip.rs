//! End-to-end schema-aware roundtrip via Maxwell (MySQL binlog CDC).
//!
//! MySQL source with a `users` table (`id BIGINT PK`, `name TEXT`, `score INT`).
//! Drive an INSERT through Maxwell, which reads the MySQL binlog and emits JSON
//! messages, capture the JSON event, digest via the unified
//! [`DiffSetBuilder::digest`] entry point with `sqlite_diff_rs::TypeMap::defaults()`,
//! apply the patchset to a fresh SQLite via `diesel-sqlite-session`, and verify
//! the SQLite row state matches the MySQL source.
// Test files do not require documentation; diesel::table! generates undocumented
// items and the missing_docs lint cannot be suppressed per-macro.
#![allow(missing_docs)]

use diesel::mysql::MysqlConnection;
use diesel::prelude::*;
use diesel::sql_query;
use diesel_sqlite_session::{ConflictAction, SqliteSessionExt};
use schema_aware_roundtrip::{MaxwellAppSchema, MaxwellUsersTable, start_maxwell, start_mysql};
use sqlite_diff_rs::maxwell::parse;
use sqlite_diff_rs::{PatchSet, TypeMap};
use std::time::Duration;
use tokio::io::AsyncBufReadExt;

// ---------------------------------------------------------------------------
// Shared schema: one table! covers both the MySQL source and the SQLite replica.
// ---------------------------------------------------------------------------

diesel::table! {
    users (id) {
        id -> BigInt,
        name -> Text,
        score -> Integer,
    }
}

/// Values inserted into MySQL via the typed Diesel DSL.
#[derive(Insertable)]
#[diesel(table_name = users)]
struct NewUser<'a> {
    id: i64,
    name: &'a str,
    score: i32,
}

/// Row read back from SQLite after the patchset is applied.
#[derive(Queryable, Selectable, Debug, PartialEq)]
#[diesel(table_name = users)]
struct UserRow {
    id: i64,
    name: String,
    score: i32,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// SQLite DDL matching the MySQL source shape.
const SQLITE_DDL: &str = "\
CREATE TABLE users (\
 id INTEGER PRIMARY KEY,\
 name TEXT NOT NULL,\
 score INTEGER NOT NULL\
)";

fn spin_sqlite() -> SqliteConnection {
    let mut conn =
        SqliteConnection::establish(":memory:").expect("Failed to open in-memory SQLite");
    // CREATE TABLE is migration DDL; the typed DSL cannot express it.
    sql_query(SQLITE_DDL)
        .execute(&mut conn)
        .expect("Failed to apply SQLite DDL");
    conn
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

/// End-to-end Maxwell INSERT roundtrip.
///
/// MySQL INSERT flows through Maxwell binlog CDC, schema-aware digest, and
/// SQLite session apply. The SQLite row should match the MySQL source values.
#[tokio::test(flavor = "multi_thread")]
async fn maxwell_insert_roundtrip_e2e() {
    // 1. Start MySQL with row-based binlog enabled.
    let (_mysql_container, mysql_host_port) = start_mysql().await;

    // 2. Start Maxwell before writing to MySQL. Maxwell reads from the current
    //    binlog position, so events written after this point will be captured.
    let maxwell_container = start_maxwell(mysql_host_port).await;

    // 3. Create the users table and insert a test row. All Diesel MySQL I/O is
    //    synchronous; block_in_place avoids stalling the tokio runtime.
    let mysql_url = format!("mysql://root:test@127.0.0.1:{mysql_host_port}/testdb");
    tokio::task::block_in_place(|| {
        let mut conn = MysqlConnection::establish(&mysql_url).expect("Failed to connect to MySQL");

        // CREATE TABLE is migration DDL; the typed DSL cannot express it.
        sql_query(
            "CREATE TABLE users (\
             id BIGINT NOT NULL PRIMARY KEY,\
             name VARCHAR(255) NOT NULL,\
             score INT NOT NULL\
             )",
        )
        .execute(&mut conn)
        .expect("Failed to create users table in MySQL");

        diesel::insert_into(users::table)
            .values(&NewUser {
                id: 42,
                name: "Alice",
                score: 100,
            })
            .execute(&mut conn)
            .expect("Failed to insert test row into MySQL");
    });

    // 4. Read Maxwell stdout until we see the INSERT event for the users table.
    let mut stdout = maxwell_container.stdout(true);
    let insert_json = tokio::time::timeout(Duration::from_secs(60), async {
        let mut line = String::new();
        loop {
            line.clear();
            stdout
                .read_line(&mut line)
                .await
                .expect("Failed to read from Maxwell stdout");
            let trimmed = line.trim();
            if trimmed.starts_with('{')
                && trimmed.contains("\"type\":\"insert\"")
                && trimmed.contains("\"table\":\"users\"")
            {
                break trimmed.to_string();
            }
        }
    })
    .await
    .expect("Timed out waiting for Maxwell INSERT event");

    // 5. Parse the Maxwell CDC message.
    let msg = parse(&insert_json).expect("Failed to parse Maxwell JSON message");

    // 6. Digest to patchset via the unified schema-aware API.
    let schema = MaxwellAppSchema::default();
    let types = TypeMap::defaults();
    let patchset_bytes: Vec<u8> = PatchSet::<MaxwellUsersTable, String, Vec<u8>>::new()
        .digest(&msg, &schema, &types)
        .expect("Failed to digest Maxwell INSERT message")
        .build();

    // 7. Apply the patchset to a fresh SQLite database.
    let mut sqlite = spin_sqlite();
    sqlite
        .apply_patchset(&patchset_bytes, |_| ConflictAction::Abort)
        .expect("Failed to apply patchset to SQLite");

    // 8. Verify the SQLite row state matches the MySQL source.
    let rows: Vec<UserRow> = users::table
        .select(UserRow::as_select())
        .load(&mut sqlite)
        .expect("Failed to query SQLite users table");

    assert_eq!(rows.len(), 1, "expected exactly one row in SQLite");
    let row = &rows[0];
    assert_eq!(row.id, 42);
    assert_eq!(row.name, "Alice");
    assert_eq!(row.score, 100);
}
