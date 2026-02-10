//! Apply-roundtrip fuzzer: verify parsed binary changesets can be applied to rusqlite.
//!
//! This fuzzer tests:
//! 1. Parses arbitrary bytes as a binary changeset/patchset
//! 2. Serializes back and verifies byte equality (roundtrip)
//! 3. Applies the changeset to an in-memory database via rusqlite

use honggfuzz::fuzz;
use sqlite_diff_rs::testing::{TypedSimpleTable, test_apply_roundtrip};

fn main() {
    loop {
        fuzz!(|input: (TypedSimpleTable, Vec<u8>)| {
            let (schema, data) = input;
            test_apply_roundtrip(&schema, &data);
        });
    }
}
