//! Fuzz test for reverse idempotency: reverse(reverse(x)) == x
//!
//! This fuzzer parses arbitrary bytes as a binary changeset and verifies that:
//! 1. Reversing twice yields the original changeset
//! 2. Binary representations match after double reverse
//! 3. No panics occur during reversal

use honggfuzz::fuzz;
use sqlite_diff_rs::testing::test_reverse_idempotent;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            test_reverse_idempotent(data);
        });
    }
}
