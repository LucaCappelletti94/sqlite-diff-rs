//! Debezium parsing and conversion fuzzer.
//!
//! Tests that Debezium parsing and conversion to changeset operations
//! doesn't panic on arbitrary input.

use honggfuzz::fuzz;
use sqlite_diff_rs::testing::test_debezium;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            test_debezium(data);
        });
    }
}
