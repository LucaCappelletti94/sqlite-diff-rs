//! Apply-roundtrip fuzzer: verify parsed binary changesets can be applied to rusqlite.
//!
//! For each input the fuzzer parses arbitrary bytes as a binary changeset or
//! patchset, serializes back and asserts byte equality, then applies the
//! re-serialized changeset to an in-memory rusqlite database. Input size is
//! capped to keep per-iteration cost bounded, since SQLite I/O is expensive
//! compared to pure-computation harnesses.

#![no_main]

use arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use sqlite_diff_rs::testing::{FuzzSchemas, test_apply_roundtrip};

/// Maximum byte length for the changeset payload. Larger inputs amplify
/// parse, build, and apply time without meaningfully increasing coverage.
const MAX_CHANGESET_LEN: usize = 4096;

// `arbitrary`, not `arbitrary_take_rest`, so crash files replay in tests
fuzz_target!(|bytes: &[u8]| {
    let Ok((schemas, data)) = Unstructured::new(bytes).arbitrary::<(FuzzSchemas, Vec<u8>)>() else {
        return;
    };
    if data.len() > MAX_CHANGESET_LEN {
        return;
    }
    test_apply_roundtrip(&schemas, &data);
});
