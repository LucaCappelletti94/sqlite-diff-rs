//! MySQL E2E: spin up the official MySQL image via testcontainers,
//! apply the shared patchsets, verify state.
//!
//! Requires a running Docker daemon. Without it, `Mysql::default().start()`
//! errors and the tests fail loudly.

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Binary, Double, Nullable, Text};
use testcontainers_modules::mysql;
use testcontainers_modules::testcontainers::runners::SyncRunner;

use diesel_e2e::{
    blobs_schema, insert_blob_row, insert_three_users, kv_full_cycle, kv_schema,
    update_alice_delete_bob, users_schema,
};

// MySQL identifiers: `LONGBLOB` for large binary, `DOUBLE` for floats, `TEXT`
// for variable text. `name` uses `VARCHAR(255)` because MySQL requires an
// explicit length on indexed text; the schema layout otherwise mirrors the
// SQLite and Postgres cases.
const USERS_DDL: &str = "\
CREATE TABLE users (
    id      BIGINT PRIMARY KEY,
    name    VARCHAR(255) NOT NULL,
    email   VARCHAR(255),
    score   DOUBLE
)";

const BLOBS_DDL: &str = "\
CREATE TABLE blobs (
    id      BIGINT PRIMARY KEY,
    payload LONGBLOB
)";

const KV_DDL: &str = "\
CREATE TABLE kv (
    tenant_id BIGINT NOT NULL,
    user_id   BIGINT NOT NULL,
    value     TEXT,
    PRIMARY KEY (tenant_id, user_id)
)";

#[derive(QueryableByName, Debug, PartialEq)]
struct UserRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Nullable<Text>)]
    email: Option<String>,
    #[diesel(sql_type = Nullable<Double>)]
    score: Option<f64>,
}

#[derive(QueryableByName, Debug, PartialEq)]
struct BlobRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
    #[diesel(sql_type = Nullable<Binary>)]
    payload: Option<Vec<u8>>,
}

#[derive(QueryableByName, Debug, PartialEq)]
struct KvRow {
    #[diesel(sql_type = BigInt)]
    tenant_id: i64,
    #[diesel(sql_type = BigInt)]
    user_id: i64,
    #[diesel(sql_type = Nullable<Text>)]
    value: Option<String>,
}

fn boot() -> (
    testcontainers_modules::testcontainers::Container<mysql::Mysql>,
    MysqlConnection,
) {
    let container = mysql::Mysql::default()
        .start()
        .expect("start MySQL container (Docker must be running)");
    // `libmysqlclient` treats the host string `localhost` as a hint to use
    // the Unix socket. Normalize any loopback name to `127.0.0.1` so we
    // always connect over TCP to the container.
    let host = container.get_host().expect("container host").to_string();
    let host = if host == "localhost" {
        "127.0.0.1".into()
    } else {
        host
    };
    let port = container
        .get_host_port_ipv4(3306)
        .expect("container MySQL port");
    let url = format!("mysql://root@{host}:{port}/test");
    let conn = MysqlConnection::establish(&url).expect("connect to MySQL");
    (container, conn)
}

fn apply(
    conn: &mut MysqlConnection,
    patchset: &sqlite_diff_rs::PatchSet<sqlite_diff_rs::SimpleTable, String, Vec<u8>>,
) {
    for op in patchset.iter() {
        op.execute(conn).expect("execute patchset op against MySQL");
    }
}

#[test]
fn users_insert_update_delete_full_cycle() {
    let (_container, mut conn) = boot();
    conn.batch_execute(USERS_DDL).unwrap();

    let schema = users_schema();
    apply(&mut conn, &insert_three_users(&schema));

    let rows: Vec<UserRow> = sql_query("SELECT id, name, email, score FROM users ORDER BY id")
        .load(&mut conn)
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].name, "Alice");
    assert_eq!(rows[0].email.as_deref(), Some("alice@example.com"));
    assert_eq!(rows[0].score, Some(95.5));
    assert_eq!(rows[1].id, 2);
    assert_eq!(rows[1].email, None);
    assert_eq!(rows[2].name, "Carol'); DROP TABLE users; --");
    assert_eq!(rows[2].score, None);

    apply(&mut conn, &update_alice_delete_bob(&schema));

    let after: Vec<UserRow> = sql_query("SELECT id, name, email, score FROM users ORDER BY id")
        .load(&mut conn)
        .unwrap();
    assert_eq!(after.len(), 2);
    assert_eq!(after[0].id, 1);
    assert_eq!(after[0].email.as_deref(), Some("alice+new@example.com"));
    assert_eq!(after[0].score, Some(99.0));
    assert_eq!(after[1].id, 3);
}

#[test]
fn blobs_null_and_raw_bytes_roundtrip() {
    let (_container, mut conn) = boot();
    conn.batch_execute(BLOBS_DDL).unwrap();

    let schema = blobs_schema();
    apply(&mut conn, &insert_blob_row(&schema));

    let rows: Vec<BlobRow> = sql_query("SELECT id, payload FROM blobs ORDER BY id")
        .load(&mut conn)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].payload.as_deref(),
        Some(&[0x00, b'\'', 0x7F, 0x80, 0xFE, 0xFF][..])
    );
}

#[test]
fn kv_composite_pk_lifecycle() {
    let (_container, mut conn) = boot();
    conn.batch_execute(KV_DDL).unwrap();

    let schema = kv_schema();
    apply(&mut conn, &kv_full_cycle(&schema));

    let rows: Vec<KvRow> =
        sql_query("SELECT tenant_id, user_id, value FROM kv ORDER BY tenant_id, user_id")
            .load(&mut conn)
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].tenant_id, 1);
    assert_eq!(rows[0].user_id, 20);
    assert_eq!(rows[0].value.as_deref(), Some("two-updated"));
}
