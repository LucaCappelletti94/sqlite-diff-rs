//! pg_walstream wire-decoder fuzzer.
//!
//! Feeds arbitrary bytes into every built-in decoder registered by
//! `TypeMap::defaults()` for the `pg_walstream` source. Exercises the
//! vendored code paths for hex-escape, integer/float parse, UUID
//! parse, JSON canonicalize.

use honggfuzz::fuzz;
use sqlite_diff_rs::testing::test_wire_pg_walstream;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            test_wire_pg_walstream(data);
        });
    }
}
