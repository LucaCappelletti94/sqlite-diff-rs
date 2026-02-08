//! Differential testing: compare our changeset/patchset builder against rusqlite's session extension.
//!
//! This module provides [`run_differential_test`], which:
//! 1. Parses a string as SQL using our custom parser
//! 2. Filters for CREATE TABLE, INSERT, UPDATE, DELETE
//! 3. Executes in rusqlite with session tracking
//! 4. Builds the same changeset/patchset with our builders
//! 5. Compares our output with rusqlite's **byte-for-byte**
//!
//! This module is feature-gated behind `testing`.

extern crate std;

use crate::schema::SimpleTable;
use crate::sql::{FormatSql, Parser, Statement};
use crate::testing::{byte_diff_report, session_changeset_and_patchset};
use crate::{
    ChangeDelete, ChangesetFormat, DiffSetBuilder, Insert, PatchsetFormat, SchemaWithPK, Update,
};
use alloc::string::String;
use alloc::string::ToString;
use alloc::vec::Vec;
use rusqlite::Connection;
use rusqlite::session::Session;
use std::collections::HashMap;

/// Run a differential test comparing our builder output against rusqlite's session extension.
///
/// This function is designed to be called from both the honggfuzz harness and
/// from regression tests. It will:
/// - Return silently for invalid/uninteresting SQL (expected failures)
/// - Panic on real bugs (byte-level comparison mismatches)
#[allow(clippy::too_many_lines)]
pub fn run_differential_test(sql: &str) {
    // Parse SQL with our custom parser
    let mut parser = Parser::new(sql);
    let Ok(statements) = parser.parse_all() else {
        return; // Invalid SQL is expected for random input
    };

    if statements.is_empty() {
        return;
    }

    // Separate CREATE TABLE from DML statements, collect schemas
    let mut schemas: HashMap<std::string::String, SimpleTable> = HashMap::new();
    let mut dml_statements = std::vec::Vec::new();

    for stmt in statements {
        match &stmt {
            Statement::CreateTable(create) => {
                let name = create.name.clone();
                schemas.insert(name, SimpleTable::from(create.clone()));
            }
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
                dml_statements.push(stmt);
            }
        }
    }

    // Need at least one table and one DML statement
    if schemas.is_empty() || dml_statements.is_empty() {
        return;
    }

    // Create in-memory database to validate SQL executes successfully
    let Ok(conn) = Connection::open_in_memory() else {
        return;
    };

    // Execute CREATE TABLE statements
    for (name, schema) in &schemas {
        let sql_str = schema.to_create_table_sql();
        if conn.execute(&sql_str, []).is_err() {
            return; // Invalid CREATE TABLE, skip
        }
    }

    // Create session and attach all tables
    let Ok(mut session) = Session::new(&conn) else {
        return;
    };
    if session.attach::<&str>(None).is_err() {
        return;
    }

    // Build our changeset and patchset incrementally
    let mut our_changeset_builder: DiffSetBuilder<ChangesetFormat, SimpleTable, String, Vec<u8>> =
        DiffSetBuilder::default();
    let mut our_patchset_builder: DiffSetBuilder<PatchsetFormat, SimpleTable, String, Vec<u8>> =
        DiffSetBuilder::default();

    // Collect SQL strings for rusqlite comparison
    let mut sql_strings = std::vec::Vec::new();
    for schema in schemas.values() {
        sql_strings.push(schema.to_create_table_sql());
    }

    // Execute DML statements
    for stmt in &dml_statements {
        let stmt_sql = stmt.format_sql();

        // Execute in rusqlite
        if conn.execute(&stmt_sql, []).is_err() {
            continue; // Invalid DML, skip
        }

        sql_strings.push(stmt_sql);

        // Try to build the same operation with our builders
        match stmt {
            Statement::Insert(insert) => {
                let Some(schema) = schemas.get(&insert.table) else {
                    continue;
                };
                let Ok(our_insert) = Insert::try_from_sql(insert, schema) else {
                    continue;
                };
                our_changeset_builder = our_changeset_builder.insert(our_insert.clone());
                our_patchset_builder = our_patchset_builder.insert(our_insert);
            }
            Statement::Update(update) => {
                let Some(schema) = schemas.get(&update.table) else {
                    continue;
                };
                let Ok(cs_update) =
                    Update::<SimpleTable, ChangesetFormat, String, Vec<u8>>::try_from_sql(update, schema)
                else {
                    continue;
                };
                let Ok(ps_update) =
                    Update::<SimpleTable, PatchsetFormat, String, Vec<u8>>::try_from_sql(update, schema)
                else {
                    continue;
                };
                our_changeset_builder = our_changeset_builder.update(cs_update);
                our_patchset_builder = our_patchset_builder.update(ps_update);
            }
            Statement::Delete(delete) => {
                let Some(schema) = schemas.get(&delete.table) else {
                    continue;
                };
                let Ok(our_delete) = ChangeDelete::try_from_sql(delete, schema) else {
                    continue;
                };
                // Extract PK values for patchset delete
                let pk: alloc::vec::Vec<_> =
                    schema.extract_pk(our_delete.values()).into_iter().collect();
                our_changeset_builder = our_changeset_builder.delete(our_delete);
                our_patchset_builder = our_patchset_builder.delete(schema, &pk);
            }
            _ => {}
        }
    }

    // Build our changeset and patchset bytes
    let our_changeset: std::vec::Vec<u8> = our_changeset_builder.build();
    let our_patchset: std::vec::Vec<u8> = our_patchset_builder.build();

    // Get rusqlite's changeset and patchset via session_changeset_and_patchset
    let sql_refs: std::vec::Vec<&str> = sql_strings.iter().map(String::as_str).collect();
    let (rusqlite_changeset, rusqlite_patchset) = session_changeset_and_patchset(&sql_refs);

    // --- Byte-for-byte comparison ---
    let cs_report = byte_diff_report("changeset", &rusqlite_changeset, &our_changeset);
    let ps_report = byte_diff_report("patchset", &rusqlite_patchset, &our_patchset);

    assert!(
        rusqlite_changeset == our_changeset && rusqlite_patchset == our_patchset,
        "Bit parity failure in differential test!\n\n{cs_report}\n{ps_report}\n\nSQL:\n{}",
        sql_strings.join("\n")
    );
}
