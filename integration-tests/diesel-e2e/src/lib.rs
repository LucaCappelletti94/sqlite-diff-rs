//! Backend-agnostic helpers for the E2E test suite.
//!
//! Every function here returns a fully constructed [`PatchSet`] or
//! [`SimpleTable`] schema. The per-backend test files take care of DDL,
//! container startup, connection, patchset application, and verification.

use sqlite_diff_rs::{DiffOps, Insert, PatchDelete, PatchSet, PatchUpdate, SimpleTable};

/// `users(id INT PK, name TEXT, email TEXT NULL, score REAL NULL)`.
#[must_use]
pub fn users_schema() -> SimpleTable {
    SimpleTable::new("users", &["id", "name", "email", "score"], &[0])
}

/// `blobs(id INT PK, payload BLOB NULL)`.
#[must_use]
pub fn blobs_schema() -> SimpleTable {
    SimpleTable::new("blobs", &["id", "payload"], &[0])
}

/// `kv(tenant_id INT, user_id INT, value TEXT NULL, PRIMARY KEY(tenant_id, user_id))`.
#[must_use]
pub fn kv_schema() -> SimpleTable {
    SimpleTable::new("kv", &["tenant_id", "user_id", "value"], &[0, 1])
}

/// Insert three users covering every scalar `Value` variant:
///
/// * `id=1` Alice, real score, non-null email.
/// * `id=2` Bob, null email, real score.
/// * `id=3` "Carol'); DROP TABLE users; --" (injection payload), null score.
#[must_use]
pub fn insert_three_users(schema: &SimpleTable) -> PatchSet<SimpleTable, String, Vec<u8>> {
    PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .insert(
            Insert::from(schema.clone())
                .set(0, 1_i64)
                .unwrap()
                .set(1, "Alice")
                .unwrap()
                .set(2, "alice@example.com")
                .unwrap()
                .set(3, 95.5_f64)
                .unwrap(),
        )
        .insert(
            Insert::from(schema.clone())
                .set(0, 2_i64)
                .unwrap()
                .set(1, "Bob")
                .unwrap()
                .set_null(2)
                .unwrap()
                .set(3, 87.0_f64)
                .unwrap(),
        )
        .insert(
            Insert::from(schema.clone())
                .set(0, 3_i64)
                .unwrap()
                .set(1, "Carol'); DROP TABLE users; --")
                .unwrap()
                .set(2, "carol@example.com")
                .unwrap()
                .set_null(3)
                .unwrap(),
        )
}

/// Update Alice's email and score, delete Bob, leave Carol untouched.
#[must_use]
pub fn update_alice_delete_bob(schema: &SimpleTable) -> PatchSet<SimpleTable, String, Vec<u8>> {
    PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .update(
            PatchUpdate::<_, String, Vec<u8>>::from(schema.clone())
                .set(0, 1_i64)
                .unwrap()
                .set(2, "alice+new@example.com")
                .unwrap()
                .set(3, 99.0_f64)
                .unwrap(),
        )
        .delete(PatchDelete::new(schema.clone(), vec![2_i64.into()]))
}

/// Insert a single blob row with a raw byte payload including `NUL`, `0xFF`,
/// and single-quote characters.
#[must_use]
pub fn insert_blob_row(schema: &SimpleTable) -> PatchSet<SimpleTable, String, Vec<u8>> {
    let payload = vec![0x00_u8, b'\'', 0x7F, 0x80, 0xFE, 0xFF];
    PatchSet::<SimpleTable, String, Vec<u8>>::new().insert(
        Insert::from(schema.clone())
            .set(0, 1_i64)
            .unwrap()
            .set(1, payload)
            .unwrap(),
    )
}

/// Insert two `kv` rows with distinct composite PKs, update the second, and
/// delete the first.
#[must_use]
pub fn kv_full_cycle(schema: &SimpleTable) -> PatchSet<SimpleTable, String, Vec<u8>> {
    PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .insert(
            Insert::from(schema.clone())
                .set(0, 1_i64)
                .unwrap()
                .set(1, 10_i64)
                .unwrap()
                .set(2, "one")
                .unwrap(),
        )
        .insert(
            Insert::from(schema.clone())
                .set(0, 1_i64)
                .unwrap()
                .set(1, 20_i64)
                .unwrap()
                .set(2, "two")
                .unwrap(),
        )
        .update(
            PatchUpdate::<_, String, Vec<u8>>::from(schema.clone())
                .set(0, 1_i64)
                .unwrap()
                .set(1, 20_i64)
                .unwrap()
                .set(2, "two-updated")
                .unwrap(),
        )
        .delete(PatchDelete::new(
            schema.clone(),
            vec![1_i64.into(), 10_i64.into()],
        ))
}
