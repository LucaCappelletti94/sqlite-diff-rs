//! maxwell wire-decoder fuzzer.
//!
//! Feeds arbitrary bytes into every built-in decoder registered by
//! `TypeMap::defaults()` for the `maxwell` source.

use honggfuzz::fuzz;
use sqlite_diff_rs::testing::test_wire_maxwell;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            test_wire_maxwell(data);
        });
    }
}
