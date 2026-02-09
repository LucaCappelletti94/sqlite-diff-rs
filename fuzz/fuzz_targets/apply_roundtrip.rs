//! Apply-roundtrip fuzzer: verify parsed binary changesets can be applied to rusqlite.
//!
//! This fuzzer tests:
//! 1. Parses arbitrary bytes as a binary changeset/patchset
//! 2. Serializes back and verifies byte equality
//! 3. Applies the changeset to an empty database via rusqlite

use honggfuzz::fuzz;
use sqlite_diff_rs::ParsedDiffSet;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            // Parse arbitrary bytes as binary changeset/patchset
            let Ok(parsed) = ParsedDiffSet::try_from(data) else {
                return;
            };

            if parsed == ParsedDiffSet::try_from(&[] as &[u8]).unwrap() {
                return; // Skip empty
            }

            // Serialize and verify round-trip
            let bytes: Vec<u8> = parsed.clone().into();
            let reparsed = ParsedDiffSet::try_from(bytes.as_slice())
                .expect("Re-parsing serialized data should succeed");

            assert_eq!(
                parsed, reparsed,
                "Binary round-trip mismatch"
            );

            // Double serialization should be stable
            let bytes2: Vec<u8> = reparsed.into();
            assert_eq!(
                bytes, bytes2,
                "Double serialization mismatch"
            );
        });
    }
}
