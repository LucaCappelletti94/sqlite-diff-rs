//! SQL digest fuzzer for DiffSetBuilder.
//!
//! Generates an arbitrary table schema and feeds arbitrary strings through
//! `digest_sql`. If digestion succeeds, verifies the resulting patchset can
//! be serialized and re-parsed as a valid binary patchset.

use honggfuzz::fuzz;
use sqlite_diff_rs::testing::{FuzzSchemas, test_sql_roundtrip};

fn main() {
    loop {
        fuzz!(|input: (FuzzSchemas, String)| {
            let (schemas, sql) = input;
            test_sql_roundtrip(&schemas, &sql);
        });
    }
}
