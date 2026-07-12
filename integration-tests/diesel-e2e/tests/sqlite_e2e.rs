//! SQLite E2E: build a patchset, iterate over `PatchsetOp`, execute each via
//! Diesel, then read the state back and assert.

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Binary, Double, Nullable, Text};
use sqlite_diff_rs::DiffOps;

use diesel_e2e::{
    blobs_schema, insert_blob_row, insert_three_users, kv_full_cycle, kv_schema,
    update_alice_delete_bob, users_schema,
};

const USERS_DDL: &str = "\
CREATE TABLE users (
    id      INTEGER PRIMARY KEY,
    name    TEXT NOT NULL,
    email   TEXT,
    score   REAL
)";

const BLOBS_DDL: &str = "\
CREATE TABLE blobs (
    id      INTEGER PRIMARY KEY,
    payload BLOB
)";

const KV_DDL: &str = "\
CREATE TABLE kv (
    tenant_id INTEGER NOT NULL,
    user_id   INTEGER NOT NULL,
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

fn conn() -> SqliteConnection {
    SqliteConnection::establish(":memory:").expect("open in-memory SQLite")
}

fn apply(
    conn: &mut SqliteConnection,
    patchset: &sqlite_diff_rs::PatchSet<sqlite_diff_rs::SimpleTable, String, Vec<u8>>,
) {
    for op in patchset.iter() {
        op.execute(conn).expect("execute patchset op");
    }
}

// ---------------------------------------------------------------------------

#[test]
fn users_insert_update_delete_full_cycle() {
    let mut conn = conn();
    conn.batch_execute(USERS_DDL).unwrap();

    let schema = users_schema();
    apply(&mut conn, &insert_three_users(&schema));

    let after_insert: Vec<UserRow> =
        sql_query("SELECT id, name, email, score FROM users ORDER BY id")
            .load(&mut conn)
            .unwrap();
    assert_eq!(after_insert.len(), 3);
    assert_eq!(after_insert[0].id, 1);
    assert_eq!(after_insert[0].name, "Alice");
    assert_eq!(after_insert[0].email.as_deref(), Some("alice@example.com"));
    assert_eq!(after_insert[0].score, Some(95.5));
    assert_eq!(after_insert[1].id, 2);
    assert_eq!(after_insert[1].email, None); // set_null landed
    assert_eq!(after_insert[2].name, "Carol'); DROP TABLE users; --"); // injection payload stored verbatim
    assert_eq!(after_insert[2].score, None);

    apply(&mut conn, &update_alice_delete_bob(&schema));

    let after_update: Vec<UserRow> =
        sql_query("SELECT id, name, email, score FROM users ORDER BY id")
            .load(&mut conn)
            .unwrap();
    assert_eq!(after_update.len(), 2);
    assert_eq!(after_update[0].id, 1);
    assert_eq!(
        after_update[0].email.as_deref(),
        Some("alice+new@example.com")
    );
    assert_eq!(after_update[0].score, Some(99.0));
    assert_eq!(after_update[0].name, "Alice"); // untouched
    assert_eq!(after_update[1].id, 3); // Bob is gone
}

#[test]
fn blobs_null_and_raw_bytes_roundtrip() {
    let mut conn = conn();
    conn.batch_execute(BLOBS_DDL).unwrap();

    let schema = blobs_schema();
    apply(&mut conn, &insert_blob_row(&schema));

    let rows: Vec<BlobRow> = sql_query("SELECT id, payload FROM blobs ORDER BY id")
        .load(&mut conn)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);
    assert_eq!(
        rows[0].payload.as_deref(),
        Some(&[0x00, b'\'', 0x7F, 0x80, 0xFE, 0xFF][..]),
        "raw bytes should survive round trip through a bind parameter"
    );
}

#[test]
fn kv_composite_pk_lifecycle() {
    let mut conn = conn();
    conn.batch_execute(KV_DDL).unwrap();

    let schema = kv_schema();
    apply(&mut conn, &kv_full_cycle(&schema));

    let rows: Vec<KvRow> =
        sql_query("SELECT tenant_id, user_id, value FROM kv ORDER BY tenant_id, user_id")
            .load(&mut conn)
            .unwrap();
    // (1, 10) inserted then deleted; (1, 20) inserted then updated.
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].tenant_id, 1);
    assert_eq!(rows[0].user_id, 20);
    assert_eq!(rows[0].value.as_deref(), Some("two-updated"));
}

#[test]
fn identifier_with_embedded_quotes_survives_execution() {
    // SQLite tolerates any identifier when double-quoted. Diesel's identifier
    // quoter doubles embedded quotes; the resulting DDL and DML must match.
    #[derive(QueryableByName)]
    struct One {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }

    let mut conn = conn();
    conn.batch_execute(r#"CREATE TABLE "we""ird" ("i""d" INTEGER PRIMARY KEY, "va""l" TEXT)"#)
        .unwrap();

    let schema = sqlite_diff_rs::SimpleTable::new("we\"ird", &["i\"d", "va\"l"], &[0]);
    let patchset = sqlite_diff_rs::PatchSet::<_, String, Vec<u8>>::new().insert(
        sqlite_diff_rs::Insert::from(schema.clone())
            .set(0, 1_i64)
            .unwrap()
            .set(1, "hello")
            .unwrap(),
    );
    apply(&mut conn, &patchset);

    let count: One = sql_query(r#"SELECT COUNT(*) AS n FROM "we""ird""#)
        .get_result(&mut conn)
        .unwrap();
    assert_eq!(count.n, 1);
}

// ApplyOps trait: apply()/apply_transactional() atomicity ------------------

use sqlite_diff_rs::ApplyOps;

/// DDL with a UNIQUE constraint we can trigger without also tripping the
/// patchset PK consolidation (which would drop the duplicate at build time).
const UNIQ_USERS_DDL: &str = "\
CREATE TABLE users (
    id      INTEGER PRIMARY KEY,
    email   TEXT NOT NULL UNIQUE
)";

#[derive(QueryableByName, Debug, PartialEq)]
struct UniqUserRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
    #[diesel(sql_type = Text)]
    email: String,
}

/// Two rows with distinct PKs but colliding UNIQUE email.
fn duplicate_email_patchset()
-> sqlite_diff_rs::PatchSet<sqlite_diff_rs::SimpleTable, String, Vec<u8>> {
    let schema = sqlite_diff_rs::SimpleTable::new("users", &["id", "email"], &[0]);
    sqlite_diff_rs::PatchSet::<sqlite_diff_rs::SimpleTable, String, Vec<u8>>::new()
        .insert(
            sqlite_diff_rs::Insert::from(schema.clone())
                .set(0, 1_i64)
                .unwrap()
                .set(1, "alice@example.com")
                .unwrap(),
        )
        .insert(
            sqlite_diff_rs::Insert::from(schema.clone())
                .set(0, 2_i64)
                .unwrap()
                .set(1, "alice@example.com") // UNIQUE violation on second insert
                .unwrap(),
        )
}

/// `apply` (no transaction) commits everything up to a failing op, so state
/// after a mid-op failure is partial.
#[test]
fn apply_without_transaction_leaves_partial_state_on_failure() {
    let mut conn = conn();
    conn.batch_execute(UNIQ_USERS_DDL).unwrap();

    let patchset = duplicate_email_patchset();
    let result = patchset.iter().apply(&mut conn);
    assert!(
        result.is_err(),
        "expected UNIQUE violation on second insert"
    );

    let rows: Vec<UniqUserRow> = sql_query("SELECT id, email FROM users ORDER BY id")
        .load(&mut conn)
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "first insert should have committed before the failing second one"
    );
    assert_eq!(rows[0].id, 1);
}

/// `apply_transactional` rolls the whole batch back atomically on any mid-op
/// failure, so after a failure the DB is unchanged.
#[test]
fn apply_transactional_rolls_back_all_ops_on_mid_batch_failure() {
    let mut conn = conn();
    conn.batch_execute(UNIQ_USERS_DDL).unwrap();

    let patchset = duplicate_email_patchset();
    let result = patchset.iter().apply_transactional(&mut conn);
    assert!(result.is_err(), "expected UNIQUE violation, batch aborted");

    let rows: Vec<UniqUserRow> = sql_query("SELECT id, email FROM users")
        .load(&mut conn)
        .unwrap();
    assert!(
        rows.is_empty(),
        "transactional apply must roll back the first insert too, found: {rows:?}"
    );
}

/// Happy path: `apply_transactional` returns the summed affected-row count.
#[test]
fn apply_transactional_returns_summed_row_count() {
    let mut conn = conn();
    conn.batch_execute(USERS_DDL).unwrap();

    let schema = users_schema();
    let patchset = insert_three_users(&schema);

    let n = patchset.iter().apply_transactional(&mut conn).unwrap();
    assert_eq!(n, 3, "three inserts should report three affected rows");

    let rows: Vec<UserRow> = sql_query("SELECT id, name, email, score FROM users ORDER BY id")
        .load(&mut conn)
        .unwrap();
    assert_eq!(rows.len(), 3);
}
