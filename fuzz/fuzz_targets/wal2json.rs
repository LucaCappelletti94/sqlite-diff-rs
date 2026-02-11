//! wal2json parsing and conversion fuzzer.
//!
//! Tests that wal2json parsing and conversion to changeset operations
//! doesn't panic on arbitrary input.

use honggfuzz::fuzz;
use sqlite_diff_rs::testing::test_wal2json;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            test_wal2json(data);
        });
    }
}
