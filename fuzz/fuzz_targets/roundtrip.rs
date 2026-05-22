//! Binary round-trip fuzzer for changeset/patchset generation.
//!
//! Tests that parse, serialize, and re-parse produce equal structures.

use honggfuzz::fuzz;
use sqlite_diff_rs::testing::test_roundtrip;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            test_roundtrip(data);
        });
    }
}
