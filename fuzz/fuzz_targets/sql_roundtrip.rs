//! SQL digest fuzzer for DiffSetBuilder.
//!
//! Pre-registers a fixed set of table schemas, then feeds arbitrary strings
//! through `digest_sql`. If digestion succeeds, verifies the resulting patchset
//! can be serialized and re-parsed as a valid binary patchset.

use honggfuzz::fuzz;
use sqlite_diff_rs::{ParsedDiffSet, PatchSet, SimpleTable};

/// Fixed schemas the fuzzer knows about.
fn schemas() -> Vec<SimpleTable> {
    vec![
        SimpleTable::new("users", &["id", "name", "email"], &[0]),
        SimpleTable::new("posts", &["id", "user_id", "title", "content"], &[0]),
        SimpleTable::new("tags", &["id", "name"], &[0]),
        SimpleTable::new("post_tags", &["post_id", "tag_id"], &[0, 1]),
    ]
}

fn main() {
    loop {
        fuzz!(|sql: String| {
            let tables = schemas();
            let mut builder: PatchSet<SimpleTable, String, Vec<u8>> = PatchSet::new();
            for t in &tables {
                builder.add_table(t);
            }

            // Try to digest — most fuzz inputs will fail, that's fine
            if builder.digest_sql(&sql).is_err() {
                return;
            }

            if builder.is_empty() {
                return;
            }

            // Serialize to binary
            let bytes = builder.build();

            // Re-parse from binary
            let reparsed = ParsedDiffSet::try_from(bytes.as_slice())
                .expect("Serialized patchset should be re-parseable");

            // Verify round-trip
            let reparsed_bytes: Vec<u8> = reparsed.into();
            assert_eq!(
                bytes, reparsed_bytes,
                "Binary round-trip mismatch after SQL digest"
            );
        });
    }
}
