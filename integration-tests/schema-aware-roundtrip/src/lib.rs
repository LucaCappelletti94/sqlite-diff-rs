//! Shared harness for the schema-aware roundtrip integration tests.
//!
//! Spins Postgres with `wal2json` via testcontainers, drives DDL and
//! DML through `tokio_postgres`, captures the CDC events, digests them
//! via `sqlite_diff_rs::TypeMap::defaults()`, applies the resulting
//! patchset to a `SqliteConnection` through `diesel-sqlite-session`,
//! and hands the connection back to the test for verification. Also
//! provides helpers for the Maxwell MySQL roundtrip test.
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

use sqlite_diff_rs::{
    DynTable, IndexableValues, NamedColumns, SchemaWithPK, SimpleTable, Value, WireColumnTypes,
    WireSchema, WireType,
};

/// The `users` table both roundtrip tests exercise. Columns:
/// `id BIGINT PK`, `active BOOL`, `handle TEXT`, `price NUMERIC(10,2)`,
/// `ts TIMESTAMPTZ`, `metadata JSONB`.
#[derive(Debug, Clone)]
pub struct UsersTable {
    inner: SimpleTable,
    wire_types: Vec<WireType>,
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
            wire_types: vec![
                WireType::Int,
                WireType::Bool,
                WireType::Text,
                WireType::Decimal,
                WireType::TimestampTz,
                WireType::Jsonb,
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

impl WireColumnTypes for UsersTable {
    fn column_type(&self, column_index: usize) -> WireType {
        self.wire_types[column_index]
    }
}

/// Static schema container. Both roundtrip tests only touch one
/// table.
#[derive(Debug, Clone, Default)]
pub struct AppSchema {
    /// The users table.
    pub users: UsersTable,
}

impl WireSchema for AppSchema {
    type Table = UsersTable;
    fn get(&self, _source_schema: Option<&str>, table_name: &str) -> Option<&Self::Table> {
        (table_name == self.users.name()).then_some(&self.users)
    }
}

// ============================================================================
// Maxwell roundtrip helpers.
// ============================================================================

/// MySQL port inside the container.
pub const MYSQL_PORT: u16 = 3306;

/// Boot a MySQL 8.0 container with row-based binlog enabled.
///
/// Returns the container and the host-mapped port for the MySQL service.
/// Binlog settings required by Maxwell are passed as command arguments.
pub async fn start_mysql() -> (ContainerAsync<GenericImage>, u16) {
    let image = GenericImage::new("mysql", "8.0")
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_wait_for(WaitFor::seconds(2))
        .with_env_var("MYSQL_ROOT_PASSWORD", "test")
        .with_env_var("MYSQL_DATABASE", "testdb")
        .with_cmd(vec![
            "--server-id=1".to_string(),
            "--log-bin=mysql-bin".to_string(),
            "--binlog-format=ROW".to_string(),
            "--binlog-row-image=FULL".to_string(),
        ]);

    let container = image
        .start()
        .await
        .expect("Failed to start MySQL container");

    let host_port = container
        .get_host_port_ipv4(MYSQL_PORT.tcp())
        .await
        .expect("Failed to get MySQL host port");

    tokio::time::sleep(Duration::from_secs(2)).await;

    (container, host_port)
}

/// Boot a Maxwell container that connects to MySQL at the given host port.
///
/// Maxwell runs with `MAXWELL_PRODUCER=stdout` so CDC events arrive on the
/// container's stdout stream. The container uses host networking so Maxwell
/// can reach MySQL at `127.0.0.1:<mysql_host_port>`. Environment variables
/// follow the convention of the bundled `bin/maxwell-docker` startup script.
pub async fn start_maxwell(mysql_host_port: u16) -> ContainerAsync<GenericImage> {
    let image = GenericImage::new("zendesk/maxwell", "v1.44.0")
        .with_wait_for(WaitFor::seconds(10))
        .with_env_var("MYSQL_HOST", "127.0.0.1")
        .with_env_var("MYSQL_USERNAME", "root")
        .with_env_var("MYSQL_PASSWORD", "test")
        .with_env_var("MAXWELL_PRODUCER", "stdout")
        .with_env_var(
            "MAXWELL_OPTIONS",
            format!("--port={mysql_host_port} --log_level=WARN"),
        )
        .with_network("host");

    image
        .start()
        .await
        .expect("Failed to start Maxwell container")
}

/// The `users` table for the Maxwell roundtrip test.
///
/// MySQL schema: `id BIGINT PRIMARY KEY, name VARCHAR(255), score INT`.
/// SQLite schema: `id INTEGER PRIMARY KEY, name TEXT, score INTEGER`.
/// Wire types: id=Int, name=Text, score=Int.
#[derive(Debug, Clone)]
pub struct MaxwellUsersTable {
    inner: SimpleTable,
}

impl MaxwellUsersTable {
    /// Build the Maxwell test table schema.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: SimpleTable::new("users", &["id", "name", "score"], &[0]),
        }
    }
}

impl Default for MaxwellUsersTable {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for MaxwellUsersTable {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl Eq for MaxwellUsersTable {}

impl Hash for MaxwellUsersTable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl DynTable for MaxwellUsersTable {
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

impl SchemaWithPK for MaxwellUsersTable {
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

impl NamedColumns for MaxwellUsersTable {
    fn column_index(&self, column_name: &str) -> Option<usize> {
        NamedColumns::column_index(&self.inner, column_name)
    }
}

impl WireColumnTypes for MaxwellUsersTable {
    fn column_type(&self, column_index: usize) -> WireType {
        match column_index {
            0 | 2 => WireType::Int,
            1 => WireType::Text,
            _ => panic!("column index {column_index} out of range for MaxwellUsersTable"),
        }
    }
}

/// Schema container for the Maxwell roundtrip test.
#[derive(Debug, Clone, Default)]
pub struct MaxwellAppSchema {
    /// The users table.
    pub users: MaxwellUsersTable,
}

impl WireSchema for MaxwellAppSchema {
    type Table = MaxwellUsersTable;
    fn get(&self, _source_schema: Option<&str>, table_name: &str) -> Option<&Self::Table> {
        (table_name == self.users.name()).then_some(&self.users)
    }
}
