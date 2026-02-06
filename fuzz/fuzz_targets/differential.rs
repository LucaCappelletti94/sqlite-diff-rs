//! Differential fuzzing: compare our changeset/patchset with rusqlite's session extension.

use honggfuzz::fuzz;
use sqlite_diff_rs::differential_testing::run_differential_test;

fn main() {
    loop {
        fuzz!(|sql: String| {
            run_differential_test(&sql);
        });
    }
}
