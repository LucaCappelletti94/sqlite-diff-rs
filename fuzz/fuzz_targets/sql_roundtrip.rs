//! SQL round-trip fuzzer for DiffSetBuilder parsing.
//!
//! This fuzzer tests:
//! 1. ChangeSet: Parse arbitrary SQL, convert to Vec<Statement>, convert back to SQL, re-parse
//! 2. PatchSet: Parse arbitrary SQL without crashing (no roundtrip since PatchSet has no Display)

use honggfuzz::fuzz;
use sqlite_diff_rs::{ChangeSet, DiffSetParseError, PatchSet, SimpleTable};
use sqlite_diff_rs::sql::Statement;

fn main() {
    loop {
        fuzz!(|sql: String| {
            // Test PatchSet parsing - just ensure it doesn't crash
            // PatchSet has no Display, so no roundtrip check
            let _patchset: Result<PatchSet<SimpleTable, String, Vec<u8>>, DiffSetParseError> = sql.parse();

            // Test ChangeSet parsing with full roundtrip
            let Ok(builder): Result<ChangeSet<SimpleTable, String, Vec<u8>>, DiffSetParseError> = sql.parse() else {
                return; // Skip unparseable inputs for roundtrip test
            };

            // Skip empty builders (nothing to roundtrip)
            if builder.is_empty() {
                return;
            }

            // Convert to Vec<Statement>
            let statements: Vec<Statement> = builder.clone().into();

            // Verify we have statements
            assert!(
                !statements.is_empty(),
                "Non-empty builder should produce non-empty statements"
            );

            // Convert back to SQL string via Display
            let output = builder.to_string();

            // Re-parse the generated SQL
            let reparsed: ChangeSet<SimpleTable, String, Vec<u8>> = output
                .parse()
                .expect("Re-parsing generated SQL should succeed");

            // Verify equivalence
            // Note: We compare lengths since the internal state may differ
            // (e.g., due to operation consolidation during parsing)
            assert_eq!(
                builder.len(),
                reparsed.len(),
                "Round-trip operation count mismatch.\nOriginal SQL:\n{}\nGenerated SQL:\n{}",
                sql,
                output
            );

            // Verify Vec<Statement> conversion is consistent
            let reparsed_statements: Vec<Statement> = reparsed.clone().into();
            assert_eq!(
                statements.len(),
                reparsed_statements.len(),
                "Statement count mismatch after roundtrip"
            );

            // Also verify the reparsed builder can be serialized again
            let output2 = reparsed.to_string();
            let reparsed2: ChangeSet<SimpleTable, String, Vec<u8>> =
                output2.parse().expect("Second re-parse should succeed");
            assert_eq!(
                reparsed.len(),
                reparsed2.len(),
                "Second round-trip mismatch"
            );
        });
    }
}
