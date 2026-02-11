//! Maxwell parsing and conversion fuzzer.
//!
//! Tests that Maxwell parsing and conversion to changeset operations
//! doesn't panic on arbitrary input.

use honggfuzz::fuzz;
use sqlite_diff_rs::testing::test_maxwell;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            test_maxwell(data);
        });
    }
}
