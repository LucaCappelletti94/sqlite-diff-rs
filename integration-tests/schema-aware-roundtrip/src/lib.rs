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
    let conn_str = format!("host=127.0.0.1 port={host_port} user=test password=test dbname=testdb");
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

// ============================================================================
// Shared schema types for the roundtrip tests.
// ============================================================================

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use sqlite_diff_rs::pg_walstream::PgWalstream;
use sqlite_diff_rs::wal2json::Wal2Json;
use sqlite_diff_rs::{
    DynTable, IndexableValues, NamedColumns, SchemaWithPK, SimpleTable, Value, WireColumnTypes,
    WireSchema,
};

/// The `users` table both roundtrip tests exercise. Columns:
/// `id BIGINT PK`, `active BOOL`, `handle TEXT`, `price NUMERIC(10,2)`,
/// `ts TIMESTAMPTZ`, `metadata JSONB`.
#[derive(Debug, Clone)]
pub struct UsersTable {
    inner: SimpleTable,
    pg_oids: Vec<u32>,
    pg_type_names: Vec<Arc<str>>,
}

impl UsersTable {
    /// Build the fixed users-schema instance.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: SimpleTable::new(
                "users",
                &["id", "active", "handle", "price", "ts", "metadata"],
                &[0],
            ),
            // int8 = 20, bool = 16, text = 25, numeric = 1700,
            // timestamptz = 1184, jsonb = 3802.
            pg_oids: vec![20, 16, 25, 1700, 1184, 3802],
            pg_type_names: vec![
                Arc::from("bigint"),
                Arc::from("boolean"),
                Arc::from("text"),
                Arc::from("numeric"),
                Arc::from("timestamp with time zone"),
                Arc::from("jsonb"),
            ],
        }
    }

    /// Underlying [`SimpleTable`], for callers that need it verbatim.
    #[must_use]
    pub fn simple_table(&self) -> &SimpleTable {
        &self.inner
    }
}

impl Default for UsersTable {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for UsersTable {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl Eq for UsersTable {}

impl Hash for UsersTable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl DynTable for UsersTable {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn number_of_columns(&self) -> usize {
        self.inner.number_of_columns()
    }
    fn write_pk_flags(&self, buf: &mut [u8]) {
        self.inner.write_pk_flags(buf);
    }
}

impl SchemaWithPK for UsersTable {
    fn number_of_primary_keys(&self) -> usize {
        self.inner.number_of_primary_keys()
    }
    fn primary_key_index(&self, col_idx: usize) -> Option<usize> {
        self.inner.primary_key_index(col_idx)
    }
    fn extract_pk<S, B>(
        &self,
        values: &impl IndexableValues<Text = S, Binary = B>,
    ) -> Vec<Value<S, B>>
    where
        S: Clone,
        B: Clone,
    {
        self.inner.extract_pk(values)
    }
}

impl NamedColumns for UsersTable {
    fn column_index(&self, column_name: &str) -> Option<usize> {
        NamedColumns::column_index(&self.inner, column_name)
    }
}

impl WireColumnTypes<PgWalstream> for UsersTable {
    fn column_type_key(&self, column_index: usize) -> u32 {
        self.pg_oids[column_index]
    }
}

impl WireColumnTypes<Wal2Json> for UsersTable {
    fn column_type_key(&self, column_index: usize) -> Arc<str> {
        Arc::clone(&self.pg_type_names[column_index])
    }
}

/// Static schema container. Both roundtrip tests only touch one
/// table.
#[derive(Debug, Clone, Default)]
pub struct AppSchema {
    /// The users table.
    pub users: UsersTable,
}

impl WireSchema<PgWalstream> for AppSchema {
    type Table = UsersTable;
    fn get(&self, table_name: &str) -> Option<&Self::Table> {
        (table_name == self.users.name()).then_some(&self.users)
    }
}

impl WireSchema<Wal2Json> for AppSchema {
    type Table = UsersTable;
    fn get(&self, table_name: &str) -> Option<&Self::Table> {
        (table_name == self.users.name()).then_some(&self.users)
    }
}
