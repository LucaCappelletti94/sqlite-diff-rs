//! Smoke test for the new `test_wire_*` fuzz helpers.
//!
//! Runs the schema-aware fuzz entry points on a small hand-picked
//! corpus (empty, malformed UTF-8, well-formed inputs across every
//! payload family) to confirm none of the decoders panic.
//!
//! This is intentionally cheap so it runs in CI. The actual honggfuzz
//! runs live under `fuzz/`.

#![cfg(all(
    feature = "testing",
    feature = "pg-walstream",
    feature = "wal2json",
    feature = "maxwell"
))]

use sqlite_diff_rs::testing::{test_wire_maxwell, test_wire_pg_walstream, test_wire_wal2json};

const CORPUS: &[&[u8]] = &[
    b"",
    b"\xFF\xFE\xFD",
    b"t",
    b"f",
    b"42",
    b"3.14",
    b"true",
    b"null",
    b"hello",
    b"\\xdeadbeef",
    b"550e8400-e29b-41d4-a716-446655440000",
    b"{\"k\": 1}",
    b"[1,2,3]",
    b"2024-01-15 10:30:00",
    b"1234567890.12345678",
    b"9223372036854775808",
    b"3q2+7w==",
];

#[test]
fn wire_fuzz_helpers_survive_hand_corpus() {
    for entry in CORPUS {
        test_wire_pg_walstream(entry);
        test_wire_wal2json(entry);
        test_wire_maxwell(entry);
    }
}
