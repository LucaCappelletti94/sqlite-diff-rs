//! pg_walstream parsing and conversion fuzzer.
//!
//! Tests that pg_walstream event parsing and conversion to changeset operations
//! doesn't panic on arbitrary input.

use honggfuzz::fuzz;
use sqlite_diff_rs::testing::test_pg_walstream;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            test_pg_walstream(data);
        });
    }
}
