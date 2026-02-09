//! Differential testing: compare our patchset builder against rusqlite's session extension.
//!
//! This module provides [`run_differential_test`], which:
//! 1. Receives pre-built schemas and SQL DML statements
//! 2. Digests the DML into a patchset via [`DiffSetBuilder::digest_sql`]
//! 3. Executes the same SQL in rusqlite with session tracking
//! 4. Compares our patchset output with rusqlite's **byte-for-byte**
//!
//! Note: only patchset output is tested because the SQL parser only supports
//! patchset format (no old-value tracking needed for changesets).
//!
//! This module is feature-gated behind `testing`.

extern crate std;

use crate::schema::SimpleTable;
use crate::testing::{byte_diff_report, session_changeset_and_patchset};
use crate::{DiffSetBuilder, PatchsetFormat};
use alloc::string::String;
use alloc::vec::Vec;

/// Run a differential test comparing our patchset builder output against
/// rusqlite's session extension.
///
/// `schemas` are pre-built [`SimpleTable`] definitions (one per table).
/// `create_sqls` are the corresponding `CREATE TABLE` SQL strings for rusqlite.
/// `dml_sqls` are the DML statements (`INSERT`, `UPDATE`, `DELETE`) to execute.
///
/// The function will:
/// - Register schemas in our builder, digest DML via [`DiffSetBuilder::digest_sql`]
/// - Execute CREATE TABLE + DML in rusqlite with session tracking
/// - Compare our patchset bytes with rusqlite's byte-for-byte
///
/// # Panics
///
/// Panics if the patchset bytes differ (this is a test helper).
pub fn run_differential_test(
    schemas: &[SimpleTable],
    create_sqls: &[&str],
    dml_sqls: &[&str],
) {
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
            continue; // Skip statements that fail to parse
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
