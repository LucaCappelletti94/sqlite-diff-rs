//! Fuzz test for reverse idempotency: reverse(reverse(x)) == x
//!
//! This fuzzer parses arbitrary bytes as a binary changeset and verifies that:
//! 1. Reversing twice yields the original changeset
//! 2. Binary representations match after double reverse
//! 3. No panics occur during reversal

use honggfuzz::fuzz;
use sqlite_diff_rs::{ParsedDiffSet, Reverse};

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            // Parse arbitrary bytes as a binary changeset/patchset
            let Ok(parsed) = ParsedDiffSet::try_from(data) else {
                return;
            };

            // Only changesets support Reverse
            let ParsedDiffSet::Changeset(changeset) = parsed else {
                return;
            };

            // Property 1: reverse(reverse(x)) == x
            let reversed = changeset.clone().reverse();
            let double_reversed = reversed.clone().reverse();

            assert_eq!(
                changeset, double_reversed,
                "Double reverse should equal original"
            );

            // Property 2: Binary representations should roundtrip
            let original_bytes = changeset.build();
            let double_reversed_bytes = double_reversed.build();

            assert_eq!(
                original_bytes, double_reversed_bytes,
                "Binary representation should be identical after double reverse"
            );

            // Property 3: Reversed changeset should have same number of operations
            assert_eq!(
                changeset.len(),
                reversed.len(),
                "Reversed changeset should have same number of operations"
            );

            // Property 4: Empty changeset reverses to empty
            if changeset.is_empty() {
                assert!(
                    reversed.is_empty(),
                    "Empty changeset should reverse to empty"
                );
            }
        });
    }
}
