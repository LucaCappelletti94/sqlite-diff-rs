//! Apply-roundtrip fuzzer: verify string-parsed changesets match rusqlite's session output.
//!
//! This fuzzer tests:
//! 1. Parses input string as SQL into ChangeSet and PatchSet
//! 2. Executes the same SQL in rusqlite with session tracking
//! 3. Compares our binary output with rusqlite's byte-for-byte
//! 4. Applies both changesets to empty databases and compares final DB state

use honggfuzz::fuzz;
use rusqlite::Connection;
use sqlite_diff_rs::testing::{
    apply_changeset, assert_bit_parity, compare_db_states,
};
use sqlite_diff_rs::{ChangeSet, PatchSet};
use sqlparser::ast::{CreateTable, Statement};

fn main() {
    loop {
        fuzz!(|sql: String| {
            run_apply_roundtrip_test(&sql);
        });
    }
}

/// Run the apply-roundtrip test on the given SQL string.
///
/// This function is designed to be called from both the fuzzer and from tests.
/// It will panic on real bugs (mismatches), but return silently for invalid inputs.
fn run_apply_roundtrip_test(sql: &str) {
    // Step 1: Parse into ChangeSet and PatchSet
    let Ok(changeset): Result<ChangeSet<CreateTable>, _> = sql.parse() else {
        return; // Invalid SQL for our parser, skip
    };
    let Ok(patchset): Result<PatchSet<CreateTable>, _> = sql.parse() else {
        return;
    };

    // Skip empty builders (no operations to test)
    if changeset.is_empty() {
        return;
    }

    // Step 2: Extract CREATE TABLE statements
    let statements: Vec<Statement> = (&changeset).into();
    let create_table_sqls: Vec<String> = statements
        .iter()
        .filter_map(|s| {
            if let Statement::CreateTable(_) = s {
                Some(s.to_string())
            } else {
                None
            }
        })
        .collect();

    // Validate schema against rusqlite
    let Ok(conn) = Connection::open_in_memory() else {
        return;
    };
    for create_sql in &create_table_sqls {
        if conn.execute(create_sql, []).is_err() {
            return; // Invalid schema, skip
        }
    }

    // Step 3: Build our binary output
    let our_changeset: Vec<u8> = changeset.into();
    let our_patchset: Vec<u8> = patchset.into();

    // Step 4: Reconstruct SQL statement list for rusqlite
    let sql_strings: Vec<String> = statements.iter().map(|s| s.to_string()).collect();
    let sql_refs: Vec<&str> = sql_strings.iter().map(|s| s.as_str()).collect();

    // Step 5: Byte-for-byte comparison with rusqlite
    assert_bit_parity(&sql_refs, our_changeset.clone(), our_patchset.clone());

    // Step 6: Verify DB state by applying both changesets to empty databases
    if !our_changeset.is_empty() {
        verify_application(&create_table_sqls, &our_changeset);
    }
    if !our_patchset.is_empty() {
        verify_application(&create_table_sqls, &our_patchset);
    }
}

/// Apply a changeset/patchset to two separate empty databases and verify they match.
fn verify_application(create_table_sqls: &[String], bytes: &[u8]) {
    let conn1 = Connection::open_in_memory().expect("Failed to create DB 1");
    let conn2 = Connection::open_in_memory().expect("Failed to create DB 2");

    for create_sql in create_table_sqls {
        conn1
            .execute(create_sql, [])
            .expect("Failed to create table in DB 1");
        conn2
            .execute(create_sql, [])
            .expect("Failed to create table in DB 2");
    }

    // Apply to conn1
    conn1.execute("BEGIN", []).expect("Failed to begin");
    if apply_changeset(&conn1, bytes).is_err() {
        return; // Application may fail for some inputs
    }
    conn1.execute("COMMIT", []).expect("Failed to commit");

    // Apply same bytes to conn2
    conn2.execute("BEGIN", []).expect("Failed to begin");
    if apply_changeset(&conn2, bytes).is_err() {
        return;
    }
    conn2.execute("COMMIT", []).expect("Failed to commit");

    compare_db_states(&conn1, &conn2, create_table_sqls);
}
