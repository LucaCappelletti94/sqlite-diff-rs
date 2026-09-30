//! maxwell wire-decoder fuzzer.
//!
//! Feeds arbitrary bytes into every built-in decoder registered by
//! `TypeMap::defaults()` for the `maxwell` source.

#![no_main]

use libfuzzer_sys::fuzz_target;
use sqlite_diff_rs::testing::test_wire_maxwell;

fuzz_target!(|data: &[u8]| {
    test_wire_maxwell(data);
});
