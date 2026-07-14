//! End-to-end schema-aware roundtrip via `pg_walstream` (pgoutput).
//!
//! Postgres source with a table exercising bool, int, text, numeric,
//! timestamptz, and jsonb. Drives an INSERT through a real pgoutput
//! replication stream via `pg_walstream::LogicalReplicationStream`,
//! extracts the resulting `EventType`, digests via the unified
//! [`DiffSetBuilder::digest`] entry point with
//! `sqlite_diff_rs::TypeMap::defaults()`, applies the patchset to a
//! fresh SQLite via `diesel-sqlite-session`, and verifies the SQLite
//! row state matches the Postgres source.

use std::time::Duration;

use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Integer, Text};
use diesel_sqlite_session::{ConflictAction, SqliteSessionExt};
use pg_walstream::EventType;
use schema_aware_roundtrip::{AppSchema, UsersTable, connect, start_postgres};
use sqlite_diff_rs::pg_walstream::PgWalstream;
use sqlite_diff_rs::{PatchSet, TypeMap};
use tokio_util::sync::CancellationToken;

#[derive(QueryableByName, Debug)]
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

const SQLITE_DDL: &str = "\
CREATE TABLE users (\
 id INTEGER PRIMARY KEY,\
 active INTEGER,\
 handle TEXT,\
 price TEXT,\
 ts TEXT,\
 metadata TEXT\
)";

fn make_type_map() -> TypeMap<PgWalstream, String, Vec<u8>> {
    TypeMap::defaults()
}

fn spin_sqlite() -> SqliteConnection {
    let mut conn =
        SqliteConnection::establish(":memory:").expect("Failed to open in-memory SQLite");
    sql_query(SQLITE_DDL)
        .execute(&mut conn)
        .expect("Failed to apply SQLite DDL");
    conn
}

#[tokio::test]
async fn pg_walstream_insert_roundtrip_e2e() {
    let (_container, port) = start_postgres().await;
    let pg = connect(port).await;

    pg.batch_execute(
        "CREATE TABLE users (\
             id BIGINT PRIMARY KEY,\
             active BOOLEAN,\
             handle TEXT,\
             price NUMERIC(10, 2),\
             ts TIMESTAMPTZ,\
             metadata JSONB\
         );\
         ALTER TABLE users REPLICA IDENTITY FULL;\
         CREATE PUBLICATION test_pub FOR TABLE users;",
    )
    .await
    .expect("Failed to bootstrap Postgres schema");

    // Start the pg_walstream client. This creates the replication
    // slot at the current WAL LSN, so subsequent inserts land in it.
    let conn_str = format!(
        "host=127.0.0.1 port={port} user=test password=test dbname=testdb replication=database"
    );
    let stream_config = pg_walstream::ReplicationStreamConfig::new(
        "sqlite_diff_rs_slot".to_string(),
        "test_pub".to_string(),
        1, // pgoutput protocol v1
        pg_walstream::StreamingMode::Off,
        Duration::from_secs(1),
        Duration::from_secs(10),
        Duration::from_secs(5),
        pg_walstream::RetryConfig::default(),
    );
    let mut stream = pg_walstream::LogicalReplicationStream::new(&conn_str, stream_config)
        .await
        .expect("Failed to build LogicalReplicationStream");
    stream
        .start(None)
        .await
        .expect("Failed to start replication");

    // Insert AFTER the slot is live.
    pg.execute(
        "INSERT INTO users (id, active, handle, price, ts, metadata) \
         VALUES (42, TRUE, 'alice', 12.34, '2024-01-15 10:30:00+00', '{\"role\": \"admin\"}'::jsonb)",
        &[],
    )
    .await
    .expect("Failed to insert row");

    // Consume events until we hit the INSERT. pgoutput emits Begin ->
    // Relation (relation cache entry, not yielded) -> Insert -> Commit.
    let cancel = CancellationToken::new();
    let insert_event = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let ev = stream
                .next_event(&cancel)
                .await
                .expect("Failed to read event");
            if let EventType::Insert { .. } = ev.event_type {
                break ev;
            }
        }
    })
    .await
    .expect("Timed out waiting for INSERT event");

    let schema = AppSchema::default();
    let types = make_type_map();
    let patchset = PatchSet::<UsersTable, String, Vec<u8>>::new()
        .digest(&insert_event.event_type, &schema, &types)
        .expect("Failed to digest pg_walstream insert");
    let bytes: Vec<u8> = patchset.build();

    let mut sqlite = spin_sqlite();
    sqlite
        .apply_patchset(&bytes, |_| ConflictAction::Abort)
        .expect("Failed to apply patchset to SQLite");

    let rows: Vec<UserRow> = sql_query("SELECT id, active, handle, price, ts, metadata FROM users")
        .load(&mut sqlite)
        .expect("Failed to query SQLite");

    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.id, 42);
    assert_eq!(row.active, 1);
    assert_eq!(row.handle, "alice");
    assert_eq!(row.price, "12.34");
    assert!(row.ts.starts_with("2024-01-15 10:30:00"), "got {}", row.ts);
    // jsonb text arrives from pgoutput without extra whitespace (differs
    // from wal2json's verbatim JSON-with-space output).
    assert!(row.metadata.contains("\"role\""), "got {}", row.metadata);
    assert!(row.metadata.contains("admin"), "got {}", row.metadata);

    stream.stop().await.expect("Failed to stop stream");
}
