//! SQL digest fuzzer for DiffSetBuilder.
//!
//! Generates an arbitrary table schema and feeds arbitrary strings through
//! `digest_sql`. If digestion succeeds, verifies the resulting patchset can
//! be serialized and re-parsed as a valid binary patchset.

#![no_main]

use arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use sqlite_diff_rs::testing::{FuzzSchemas, test_sql_roundtrip};

// `arbitrary`, not `arbitrary_take_rest`, so crash files replay in tests
fuzz_target!(|bytes: &[u8]| {
    let Ok((schemas, sql)) = Unstructured::new(bytes).arbitrary::<(FuzzSchemas, String)>() else {
        return;
    };
    test_sql_roundtrip(&schemas, &sql);
});
