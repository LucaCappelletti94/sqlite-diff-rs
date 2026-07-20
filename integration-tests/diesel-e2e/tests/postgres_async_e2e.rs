//! Postgres async E2E: spin up the official Postgres image via testcontainers,
//! then drive the shared patchsets through `diesel-async` over a native
//! `AsyncPgConnection` and verify state.
//!
//! Requires a running Docker daemon. Without it, `Postgres::default().start()`
//! errors and the tests fail loudly.
//!
//! Only the async `diesel_async::RunQueryDsl` is imported (no
//! `diesel::prelude::*` glob), so `.load`/`.execute` resolve unambiguously to
//! the async connection.

use diesel::pg::Pg;
use diesel::prelude::QueryableByName;
use diesel::query_builder::AstPass;
use diesel::result::QueryResult;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Bool, Double, Nullable, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use diesel_e2e::{
    insert_three_users, kv_full_cycle, kv_schema, update_alice_delete_bob, users_schema,
};
use sqlite_diff_rs::{
    Adapter, ApplyOpsAsync, Binder, DefaultBinder, DiffOps, Insert, PatchSet, SimpleTable, Value,
};

const USERS_DDL: &str = "\
CREATE TABLE users (
    id      BIGINT PRIMARY KEY,
    name    TEXT NOT NULL,
    email   TEXT,
    score   DOUBLE PRECISION
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

async fn boot() -> (ContainerAsync<postgres::Postgres>, AsyncPgConnection) {
    let container = postgres::Postgres::default()
        .start()
        .await
        .expect("start Postgres container (Docker must be running)");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container Postgres port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to Postgres");
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

// Native BOOLEAN bind through the adapter over the async connection: the
// source stores `Value::Integer`, but the adapter binds a real `bool`, so no
// `CAST` appears and Postgres accepts the value into a `BOOLEAN` column.

const USERS_TYPED_DDL: &str = "\
CREATE TABLE users_typed (
    id     BIGINT PRIMARY KEY,
    active BOOLEAN NOT NULL
)";

#[derive(QueryableByName, Debug, PartialEq)]
struct UsersTypedRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
    #[diesel(sql_type = Bool)]
    active: bool,
}

struct BoolBinder(bool);

impl Binder<Pg> for BoolBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Bool, bool>(&self.0)
    }
}

struct UsersTypedAdapter;

impl<S: AsRef<str> + Sync, B: AsRef<[u8]> + Sync> Adapter<Pg, S, B> for UsersTypedAdapter {
    fn column_name(&self, _table: &str, column_index: usize) -> &str {
        ["id", "active"][column_index]
    }

    fn bind<'a>(
        &self,
        table: &str,
        column_index: usize,
        value: &'a Value<S, B>,
    ) -> QueryResult<Box<dyn Binder<Pg> + Send + 'a>> {
        match (table, column_index, value) {
            ("users_typed", 1, Value::Integer(i)) => Ok(Box::new(BoolBinder(*i != 0))),
            _ => Ok(Box::new(DefaultBinder::from(value))),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adapter_binds_bool_natively_against_pg_boolean_column_async() {
    let (_container, mut conn) = boot().await;
    conn.batch_execute(USERS_TYPED_DDL).await.unwrap();

    let schema = SimpleTable::new("users_typed", &["id", "active"], &[0]);
    let patchset = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .insert(
            Insert::from(schema.clone())
                .set(0, 1_i64)
                .unwrap()
                .set(1, 1_i64) // truthy
                .unwrap(),
        )
        .insert(
            Insert::from(schema.clone())
                .set(0, 2_i64)
                .unwrap()
                .set(1, 0_i64) // falsy
                .unwrap(),
        );

    let n = patchset
        .iter()
        .map(|op| op.with_adapter::<Pg, _>(&UsersTypedAdapter))
        .apply_async(&mut conn)
        .await
        .expect("apply adapter-bound ops against Pg BOOLEAN column");
    assert_eq!(n, 2);

    let rows: Vec<UsersTypedRow> = sql_query("SELECT id, active FROM users_typed ORDER BY id")
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows[0].active);
    assert!(!rows[1].active);
}
