//! Shared harness for the schema-aware roundtrip integration tests.
//!
//! Spins Postgres with `wal2json` via testcontainers, drives DDL and
//! DML through `tokio_postgres`, captures the CDC events, digests them
//! via `sqlite_diff_rs::TypeMap::defaults()`, applies the resulting
//! patchset to a `SqliteConnection` through `diesel-sqlite-session`,
//! and hands the connection back to the test for verification.

use std::time::Duration;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio_postgres::{Client, NoTls};

/// PostgreSQL port inside the container.
pub const POSTGRES_PORT: u16 = 5432;

/// Boot a Postgres container preloaded with the `wal2json` output
/// plugin. Uses `bfontaine/postgres-wal2json` (PG 15).
pub async fn start_postgres() -> (ContainerAsync<GenericImage>, u16) {
    let image = GenericImage::new("bfontaine/postgres-wal2json", "15-bookworm")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_wait_for(WaitFor::seconds(2))
        .with_env_var("POSTGRES_USER", "test")
        .with_env_var("POSTGRES_PASSWORD", "test")
        .with_env_var("POSTGRES_DB", "testdb")
        .with_cmd(vec![
            "-c".to_string(),
            "wal_level=logical".to_string(),
            "-c".to_string(),
            "max_replication_slots=4".to_string(),
            "-c".to_string(),
            "max_wal_senders=4".to_string(),
        ]);

    let container = image
        .start()
        .await
        .expect("Failed to start PostgreSQL container");

    let host_port = container
        .get_host_port_ipv4(POSTGRES_PORT.tcp())
        .await
        .expect("Failed to get host port");

    tokio::time::sleep(Duration::from_secs(1)).await;

    (container, host_port)
}

/// Connect a `tokio_postgres` client to a running container.
pub async fn connect(host_port: u16) -> Client {
    let conn_str =
        format!("host=127.0.0.1 port={host_port} user=test password=test dbname=testdb");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("Failed to connect to PostgreSQL");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Postgres connection error: {e}");
        }
    });
    client
}

/// Create a `wal2json` replication slot.
pub async fn create_slot(client: &Client, slot: &str) {
    client
        .execute(
            &format!("SELECT pg_create_logical_replication_slot('{slot}', 'wal2json')"),
            &[],
        )
        .await
        .expect("Failed to create replication slot");
}

/// Drop a replication slot.
pub async fn drop_slot(client: &Client, slot: &str) {
    let _ = client
        .execute(&format!("SELECT pg_drop_replication_slot('{slot}')"), &[])
        .await;
}

/// Pull v2 changes as JSON strings from the slot.
pub async fn get_changes_v2(client: &Client, slot: &str) -> Vec<String> {
    let rows = client
        .query(
            &format!(
                "SELECT data FROM pg_logical_slot_get_changes('{slot}', NULL, NULL, 'format-version', '2')"
            ),
            &[],
        )
        .await
        .expect("Failed to read wal2json changes");
    rows.iter().map(|r| r.get::<_, String>("data")).collect()
}
