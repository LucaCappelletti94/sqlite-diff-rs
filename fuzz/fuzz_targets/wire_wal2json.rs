//! wal2json wire-decoder fuzzer.
//!
//! Feeds arbitrary bytes into every built-in decoder registered by
//! `TypeMap::defaults()` for the `wal2json` source, using both string
//! and parsed-JSON payload flavors.

#![no_main]

use libfuzzer_sys::fuzz_target;
use sqlite_diff_rs::testing::test_wire_wal2json;

fuzz_target!(|data: &[u8]| {
    test_wire_wal2json(data);
});
