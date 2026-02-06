//! Binary round-trip fuzzer for changeset/patchset generation.
//!
//! Tests that parse → serialize → re-parse produces equal structures.

use honggfuzz::fuzz;
use sqlite_diff_rs::ParsedDiffSet;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            let Ok(parsed): Result<ParsedDiffSet, _> = ParsedDiffSet::try_from(data) else {
                return; // Skip invalid inputs
            };

            let serialized: Vec<u8> = parsed.clone().into();
            let reparsed =
                ParsedDiffSet::try_from(serialized.as_slice()).expect("Re-parsing failed");
            assert_eq!(
                parsed, reparsed,
                "Semantic roundtrip mismatch for input: {:?}",
                data
            );
        });
    }
}
