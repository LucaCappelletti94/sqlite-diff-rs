//! Test utilities and helpers for wal2json integration tests.
//!
//! This crate provides utilities for testing wal2json parsing and conversion
//! against real PostgreSQL instances using testcontainers.

use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio_postgres::{Client, NoTls};

/// Default PostgreSQL port inside the container.
pub const POSTGRES_PORT: u16 = 5432;

/// Start a PostgreSQL container with wal2json support.
///
/// Uses `bfontaine/postgres-wal2json` for PG 15+ which includes wal2json pre-installed.
///
/// # Arguments
///
/// * `version` - PostgreSQL version tag (e.g., "15")
///
/// # Returns
///
/// A running container and the host port mapped to PostgreSQL.
///
/// # Supported Versions
///
/// Currently only PostgreSQL 15 is supported via `bfontaine/postgres-wal2json:15-bookworm`.
/// For older versions or pgoutput-based CDC, use the `pg_walstream` feature instead.
pub async fn start_postgres(version: &str) -> (ContainerAsync<GenericImage>, u16) {
    // Use bfontaine/postgres-wal2json which includes wal2json extension
    // Currently only PG 15 is available: "15-bookworm"
    let tag = format!("{version}-bookworm");
    let image = GenericImage::new("bfontaine/postgres-wal2json", &tag)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
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

    (container, host_port)
}

/// Connect to PostgreSQL.
///
/// # Arguments
///
/// * `host_port` - The host port mapped to PostgreSQL
///
/// # Returns
///
/// A connected PostgreSQL client.
pub async fn connect(host_port: u16) -> Client {
    let connection_string =
        format!("host=127.0.0.1 port={host_port} user=test password=test dbname=testdb");

    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("Failed to connect to PostgreSQL");

    // Spawn the connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    client
}

/// Create a logical replication slot with wal2json.
///
/// # Arguments
///
/// * `client` - PostgreSQL client
/// * `slot_name` - Name for the replication slot
pub async fn create_replication_slot(client: &Client, slot_name: &str) {
    client
        .execute(
            &format!("SELECT pg_create_logical_replication_slot('{slot_name}', 'wal2json')"),
            &[],
        )
        .await
        .expect("Failed to create replication slot");
}

/// Drop a logical replication slot.
///
/// # Arguments
///
/// * `client` - PostgreSQL client
/// * `slot_name` - Name of the replication slot to drop
pub async fn drop_replication_slot(client: &Client, slot_name: &str) {
    let _ = client
        .execute(
            &format!("SELECT pg_drop_replication_slot('{slot_name}')"),
            &[],
        )
        .await;
}

/// Get changes from a logical replication slot in v2 format.
///
/// # Arguments
///
/// * `client` - PostgreSQL client
/// * `slot_name` - Name of the replication slot
///
/// # Returns
///
/// A vector of JSON strings, one per change.
pub async fn get_changes_v2(client: &Client, slot_name: &str) -> Vec<String> {
    let rows = client
        .query(
            &format!(
                "SELECT data FROM pg_logical_slot_get_changes('{slot_name}', NULL, NULL, 'format-version', '2')"
            ),
            &[],
        )
        .await
        .expect("Failed to get changes");

    rows.iter().map(|row| row.get::<_, String>(0)).collect()
}

/// Get changes from a logical replication slot in v1 format.
///
/// # Arguments
///
/// * `client` - PostgreSQL client
/// * `slot_name` - Name of the replication slot
///
/// # Returns
///
/// A vector of JSON strings (typically one transaction per entry).
pub async fn get_changes_v1(client: &Client, slot_name: &str) -> Vec<String> {
    let rows = client
        .query(
            &format!(
                "SELECT data FROM pg_logical_slot_get_changes('{slot_name}', NULL, NULL, 'format-version', '1')"
            ),
            &[],
        )
        .await
        .expect("Failed to get changes");

    rows.iter().map(|row| row.get::<_, String>(0)).collect()
}

/// PostgreSQL versions to test against.
pub const PG_VERSIONS: &[&str] = &["14", "15", "16", "17"];
