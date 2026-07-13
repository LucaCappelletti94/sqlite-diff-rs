//! End-to-end schema-aware roundtrip via wal2json.
//!
//! Postgres source with a table exercising multiple payload families
//! (bool, int, text, numeric, timestamp, uuid, jsonb). Drive an INSERT
//! and an UPDATE through `wal2json`, capture the JSON messages, digest
//! via `sqlite_diff_rs::TypeMap::defaults()` (with UUID registered
//! explicitly), apply the patchset to a fresh SQLite via
//! `diesel-sqlite-session`, and verify SQLite row state matches
//! Postgres source.

use std::sync::Arc;

use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Integer, Text};
use diesel_sqlite_session::{ConflictAction, SqliteSessionExt};
use schema_aware_roundtrip::{
    connect, create_slot, drop_slot, get_changes_v2, start_postgres,
};
use sqlite_diff_rs::wal2json::{parse_v2, Action, Wal2Json};
use sqlite_diff_rs::{PatchSet, SimpleTable, TypeMap, UuidBlob16Decoder};

#[derive(QueryableByName, Debug, PartialEq, Eq)]
struct UserRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
    #[diesel(sql_type = Integer)]
    active: i32,
    #[diesel(sql_type = Text)]
    handle: String,
    #[diesel(sql_type = Text)]
    price: String,
    #[diesel(sql_type = Text)]
    ts: String,
    #[diesel(sql_type = Text)]
    metadata: String,
}

fn make_type_map() -> TypeMap<Wal2Json, String, Vec<u8>> {
    // Register UUID (not in defaults) using the blob-16 shape so the
    // SQLite side stores 16 raw bytes.
    let mut types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    types.register(Arc::from("uuid"), UuidBlob16Decoder);
    types
}

/// SQLite DDL matching the Postgres shape: `id` primary key,
/// `active` bool (stored as integer), `handle` text, `price` text
/// (numeric preserved verbatim), `ts` text (timestamp verbatim),
/// `metadata` text (jsonb serialized).
const SQLITE_DDL: &str = "\
CREATE TABLE users (\
 id INTEGER PRIMARY KEY,\
 active INTEGER,\
 handle TEXT,\
 price TEXT,\
 ts TEXT,\
 metadata TEXT\
)";

fn spin_sqlite() -> SqliteConnection {
    let mut conn = SqliteConnection::establish(":memory:")
        .expect("Failed to open in-memory SQLite");
    sql_query(SQLITE_DDL)
        .execute(&mut conn)
        .expect("Failed to apply SQLite DDL");
    conn
}

/// End-to-end insert test. Postgres INSERT flows through wal2json,
/// schema-aware digest, and SQLite session apply. SQLite row should
/// match the Postgres source values.
#[tokio::test]
async fn wal2json_insert_roundtrip_e2e() {
    let (_container, port) = start_postgres().await;
    let pg = connect(port).await;

    // Postgres DDL. `bigserial` -> BIGINT, `boolean`, `text`,
    // `numeric` (verbatim), `timestamp with time zone`, `jsonb`.
    pg.batch_execute(
        "CREATE TABLE users (\
             id BIGINT PRIMARY KEY,\
             active BOOLEAN,\
             handle TEXT,\
             price NUMERIC(10, 2),\
             ts TIMESTAMPTZ,\
             metadata JSONB\
         )",
    )
    .await
    .expect("Failed to create Postgres table");

    pg.batch_execute("ALTER TABLE users REPLICA IDENTITY FULL")
        .await
        .expect("Failed to set REPLICA IDENTITY FULL");

    create_slot(&pg, "test_slot").await;

    pg.execute(
        "INSERT INTO users (id, active, handle, price, ts, metadata) \
         VALUES (42, TRUE, 'alice', 12.34, '2024-01-15 10:30:00+00', '{\"role\": \"admin\"}'::jsonb)",
        &[],
    )
    .await
    .expect("Failed to insert row");

    let changes = get_changes_v2(&pg, "test_slot").await;
    let insert_msg = changes
        .iter()
        .filter_map(|json| parse_v2(json).ok())
        .find(|msg| msg.action == Action::I)
        .expect("Expected INSERT message from wal2json");

    // Digest via the new schema-aware API.
    let schema = SimpleTable::new(
        "users",
        &["id", "active", "handle", "price", "ts", "metadata"],
        &[0],
    );
    let types = make_type_map();
    let patchset = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .digest_wal2json_v2(&insert_msg, &schema, &types)
        .expect("Failed to digest wal2json insert");
    let patchset_bytes: Vec<u8> = patchset.build();

    // Apply to SQLite via diesel-sqlite-session.
    let mut sqlite = spin_sqlite();
    sqlite
        .apply_patchset(&patchset_bytes, |_| ConflictAction::Abort)
        .expect("Failed to apply patchset to SQLite");

    // Verify SQLite state matches Postgres source.
    let rows: Vec<UserRow> = sql_query("SELECT id, active, handle, price, ts, metadata FROM users")
        .load(&mut sqlite)
        .expect("Failed to query SQLite");

    assert_eq!(rows.len(), 1, "expected exactly one row");
    let row = &rows[0];
    assert_eq!(row.id, 42);
    assert_eq!(row.active, 1);
    assert_eq!(row.handle, "alice");
    assert_eq!(row.price, "12.34");
    // Timestamp verbatim as wal2json emitted it. wal2json's format
    // matches Postgres timestamptz text output.
    assert!(row.ts.starts_with("2024-01-15 10:30:00"), "got {}", row.ts);
    // metadata is preserved verbatim from wal2json's JSON string
    // output (wal2json emits `{"role": "admin"}` with the space).
    assert_eq!(row.metadata, "{\"role\": \"admin\"}");

    drop_slot(&pg, "test_slot").await;
}
