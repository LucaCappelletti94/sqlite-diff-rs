//! MySQL async E2E: spin up the official MySQL image via testcontainers, then
//! drive the shared patchsets through `diesel-async` over a native
//! `AsyncMysqlConnection` and verify state.
//!
//! Requires a running Docker daemon. Without it, `Mysql::default().start()`
//! errors and the tests fail loudly.
//!
//! Only the async `diesel_async::RunQueryDsl` is imported (no
//! `diesel::prelude::*` glob), so `.load`/`.execute` resolve unambiguously to
//! the async connection.

use diesel::prelude::QueryableByName;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Double, Nullable, Text};
use diesel_async::{AsyncConnection, AsyncMysqlConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers_modules::mysql;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use diesel_e2e::{
    insert_three_users, kv_full_cycle, kv_schema, update_alice_delete_bob, users_schema,
};
use sqlite_diff_rs::ApplyOpsAsync;

// MySQL needs an explicit length on indexed text, so `name`/`email` use
// `VARCHAR(255)`; otherwise the layout mirrors the SQLite and Postgres cases.
const USERS_DDL: &str = "\
CREATE TABLE users (
    id      BIGINT PRIMARY KEY,
    name    VARCHAR(255) NOT NULL,
    email   VARCHAR(255),
    score   DOUBLE
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
struct KvRow {
    #[diesel(sql_type = BigInt)]
    tenant_id: i64,
    #[diesel(sql_type = BigInt)]
    user_id: i64,
    #[diesel(sql_type = Nullable<Text>)]
    value: Option<String>,
}

async fn boot() -> (ContainerAsync<mysql::Mysql>, AsyncMysqlConnection) {
    let container = mysql::Mysql::default()
        .start()
        .await
        .expect("start MySQL container (Docker must be running)");
    // `localhost` hints libmysqlclient toward a Unix socket; normalize any
    // loopback name to `127.0.0.1` so we always connect over TCP.
    let host = container
        .get_host()
        .await
        .expect("container host")
        .to_string();
    let host = if host == "localhost" {
        "127.0.0.1".to_string()
    } else {
        host
    };
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("container MySQL port");
    let url = format!("mysql://root@{host}:{port}/test");
    let conn = AsyncMysqlConnection::establish(&url)
        .await
        .expect("connect to MySQL");
    (container, conn)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn users_insert_update_delete_full_cycle_async() {
    let (_container, mut conn) = boot().await;
    conn.batch_execute(USERS_DDL).await.unwrap();

    let schema = users_schema();
    insert_three_users(&schema)
        .iter()
        .apply_async(&mut conn)
        .await
        .unwrap();

    let rows: Vec<UserRow> = sql_query("SELECT id, name, email, score FROM users ORDER BY id")
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].email.as_deref(), Some("alice@example.com"));
    assert_eq!(rows[0].score, Some(95.5));
    assert_eq!(rows[1].email, None);
    assert_eq!(rows[2].name, "Carol'); DROP TABLE users; --");

    update_alice_delete_bob(&schema)
        .iter()
        .apply_transactional_async(&mut conn)
        .await
        .unwrap();

    let after: Vec<UserRow> = sql_query("SELECT id, name, email, score FROM users ORDER BY id")
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(after.len(), 2);
    assert_eq!(after[0].email.as_deref(), Some("alice+new@example.com"));
    assert_eq!(after[1].id, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kv_composite_pk_lifecycle_async() {
    let (_container, mut conn) = boot().await;
    conn.batch_execute(KV_DDL).await.unwrap();

    let schema = kv_schema();
    kv_full_cycle(&schema)
        .iter()
        .apply_transactional_async(&mut conn)
        .await
        .unwrap();

    let rows: Vec<KvRow> =
        sql_query("SELECT tenant_id, user_id, value FROM kv ORDER BY tenant_id, user_id")
            .load(&mut conn)
            .await
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].tenant_id, 1);
    assert_eq!(rows[0].user_id, 20);
    assert_eq!(rows[0].value.as_deref(), Some("two-updated"));
}
