//! wal2json wire-decoder fuzzer.
//!
//! Feeds arbitrary bytes into every built-in decoder registered by
//! `TypeMap::defaults()` for the `wal2json` source, using both string
//! and parsed-JSON payload flavors.

use honggfuzz::fuzz;
use sqlite_diff_rs::testing::test_wire_wal2json;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            test_wire_wal2json(data);
        });
    }
}
