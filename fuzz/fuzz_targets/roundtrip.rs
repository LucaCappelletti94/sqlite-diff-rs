//! Binary round-trip fuzzer for changeset/patchset generation.
//!
//! Tests that parse, serialize, and re-parse produce equal structures.

#![no_main]

use libfuzzer_sys::fuzz_target;
use sqlite_diff_rs::testing::test_roundtrip;

fuzz_target!(|data: &[u8]| {
    test_roundtrip(data);
});
