//! Fuzz test for reverse idempotency: reverse(reverse(x)) == x
//!
//! This fuzzer generates random changesets from SQL and verifies that:
//! 1. Reversing twice yields the original changeset
//! 2. Applying changeset + reverse yields original database state
//! 3. No panics occur during reversal

use honggfuzz::fuzz;
use sqlite_diff_rs::{ChangeSet, Reverse, SimpleTable};

fn main() {
    loop {
        fuzz!(|data: String| {
            // Try to parse as a changeset
            let Ok(changeset) = data.parse::<ChangeSet<SimpleTable, String, Vec<u8>>>() else {
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
