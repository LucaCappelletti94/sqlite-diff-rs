//! Apply-roundtrip fuzzer: verify parsed binary changesets can be applied to rusqlite.
//!
//! For each input the fuzzer parses arbitrary bytes as a binary changeset or
//! patchset, serializes back and asserts byte equality, then applies the
//! re-serialized changeset to an in-memory rusqlite database. Input size is
//! capped to keep per-iteration cost bounded, since SQLite I/O is expensive
//! compared to pure-computation harnesses.

use honggfuzz::fuzz;
use sqlite_diff_rs::testing::{FuzzSchemas, test_apply_roundtrip};

/// Maximum byte length for the changeset payload. Larger inputs amplify
/// parse, build, and apply time without meaningfully increasing coverage.
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
