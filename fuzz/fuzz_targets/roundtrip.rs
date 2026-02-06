//! Serde-based round-trip fuzzer for changeset/patchset generation.

use honggfuzz::fuzz;
use sqlite_diff_rs::ParsedDiffSet;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            let Ok(parsed): Result<ParsedDiffSet, _> = ParsedDiffSet::try_from(data) else {
                return; // Skip invalid inputs
            };
            // We rasterize back to bytes and parse again, verifying we get the same result.
            let rasterized: Vec<u8> = parsed.clone().into();
            let reparsed =
                ParsedDiffSet::try_from(rasterized.as_slice()).expect("Re-parsing failed");
            assert_eq!(
                parsed, reparsed,
                "Round-trip mismatch for input: {:?}",
                data
            );
        });
    }
}
