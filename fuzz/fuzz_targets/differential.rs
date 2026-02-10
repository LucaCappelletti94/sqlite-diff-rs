//! Differential (bit-parity) fuzzer: compare our patchset output against rusqlite.
//!
//! This fuzzer tests that for a given table schema and SQL DML, our patchset
//! builder produces **byte-identical** output to rusqlite's session extension.

use honggfuzz::fuzz;
use sqlite_diff_rs::testing::{FuzzSchemas, test_differential};

fn main() {
    loop {
        fuzz!(|input: (FuzzSchemas, String)| {
            let (schemas, sql) = input;
            test_differential(&schemas, &sql);
        });
    }
}
