//! Apply-roundtrip fuzzer: verify parsed binary changesets can be applied to rusqlite.
//!
//! This fuzzer tests:
//! 1. Parses arbitrary bytes as a binary changeset/patchset
//! 2. Serializes back and verifies byte equality (roundtrip)
//! 3. Applies the re-serialized changeset to an in-memory database via rusqlite
//!
//! Input size is capped to keep per-iteration cost bounded (SQLite I/O is
//! expensive compared to pure-computation harnesses).

use honggfuzz::fuzz;
use sqlite_diff_rs::testing::{FuzzSchemas, test_apply_roundtrip};

/// Maximum byte length for the changeset payload.  Larger inputs amplify
/// parse → build → apply time without meaningfully increasing coverage.
const MAX_CHANGESET_LEN: usize = 4096;

fn main() {
    loop {
        fuzz!(|input: (FuzzSchemas, Vec<u8>)| {
            let (schemas, data) = input;
            if data.len() > MAX_CHANGESET_LEN {
                return;
            }
            test_apply_roundtrip(&schemas, &data);
        });
    }
}
