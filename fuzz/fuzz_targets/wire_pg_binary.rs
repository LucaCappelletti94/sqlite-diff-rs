//! `PgBinary` wire-decoder fuzzer.
//!
//! Feeds arbitrary bytes into every built-in decoder registered by
//! `TypeMap::defaults()` for the `PgBinary` source, as both a present
//! binary field and a SQL NULL. Asserts nothing panics.

use honggfuzz::fuzz;
use sqlite_diff_rs::testing::test_wire_pg_binary;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            test_wire_pg_binary(data);
        });
    }
}
