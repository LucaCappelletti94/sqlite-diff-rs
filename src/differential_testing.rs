//! Differential testing: compare our patchset builder against rusqlite's session extension.
//!
//! [`run_differential_test`] takes pre-built schemas and SQL DML statements,
//! digests the DML into a patchset via [`DiffSetBuilder::digest_sql`], runs
//! the same SQL through rusqlite with session tracking, and compares the two
//! patchsets byte for byte. Only patchset output is tested because the SQL
//! parser is patchset-only (changesets need old-value tracking).
//!
//! Feature-gated behind `testing`.

extern crate std;

use crate::schema::SimpleTable;
use crate::testing::{byte_diff_report, session_changeset_and_patchset};
use crate::{DiffSetBuilder, PatchsetFormat};
use alloc::string::String;
use alloc::vec::Vec;

/// Run a differential test comparing our patchset builder output against
/// rusqlite's session extension.
///
/// `schemas` are pre-built [`SimpleTable`] definitions (one per table),
/// `create_sqls` are the matching `CREATE TABLE` strings for rusqlite, and
/// `dml_sqls` are the `INSERT`/`UPDATE`/`DELETE` statements to execute.
///
/// Schemas are registered in our builder and DML is digested via
/// [`DiffSetBuilder::digest_sql`]. The same statements run in rusqlite with
/// session tracking, and the patchset bytes are compared byte for byte.
///
/// # Panics
///
/// Panics if the patchset bytes differ (this is a test helper).
pub fn run_differential_test(schemas: &[SimpleTable], create_sqls: &[&str], dml_sqls: &[&str]) {
    // Build our patchset via digest_sql
    let mut our_patchset_builder: DiffSetBuilder<PatchsetFormat, SimpleTable, String, Vec<u8>> =
        DiffSetBuilder::default();

    // Register all schemas
    for schema in schemas {
        our_patchset_builder.add_table(schema);
    }

    // Digest each DML statement
    for &dml in dml_sqls {
        if our_patchset_builder.digest_sql(dml).is_err() {
            // Skip statements that fail to parse
        }
    }

    let our_patchset = our_patchset_builder.build();

    // Build the full statement list for rusqlite
    let mut all_sqls: Vec<&str> = Vec::new();
    all_sqls.extend_from_slice(create_sqls);
    all_sqls.extend_from_slice(dml_sqls);

    let (_rusqlite_changeset, rusqlite_patchset) = session_changeset_and_patchset(&all_sqls);

    // Byte-for-byte comparison (patchset only)
    let ps_report = byte_diff_report("patchset", &rusqlite_patchset, &our_patchset);

    assert!(
        rusqlite_patchset == our_patchset,
        "Patchset bit parity failure in differential test!\n\n{ps_report}\n\nSQL:\n{}",
        all_sqls.join("\n")
    );
}
