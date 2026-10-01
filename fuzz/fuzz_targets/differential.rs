//! Differential (bit-parity) fuzzer: compare our patchset output against rusqlite.
//!
//! This fuzzer tests that for a given table schema and SQL DML, our patchset
//! builder produces **byte-identical** output to rusqlite's session extension.

#![no_main]

use arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use sqlite_diff_rs::testing::{FuzzSchemas, test_differential};

// `arbitrary`, not `arbitrary_take_rest`, so crash files replay in tests
fuzz_target!(|bytes: &[u8]| {
    let Ok((schemas, sql)) = Unstructured::new(bytes).arbitrary::<(FuzzSchemas, String)>() else {
        return;
    };
    test_differential(&schemas, &sql);
});
