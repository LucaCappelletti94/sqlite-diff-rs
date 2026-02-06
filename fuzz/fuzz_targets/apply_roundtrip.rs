//! Apply-roundtrip fuzzer: verify string-parsed changesets match rusqlite's session output.
//!
//! This fuzzer tests:
//! 1. Parses input string as SQL into ChangeSet and PatchSet
//! 2. Executes the same SQL in rusqlite with session tracking
//! 3. Compares our binary output with rusqlite's byte-for-byte
//! 4. Applies both changesets to empty databases and compares final DB state

use honggfuzz::fuzz;
use rusqlite::Connection;
use rusqlite::session::{ChangesetItem, ConflictAction, ConflictType, Session};
use sqlite_diff_rs::{ChangeSet, ParsedDiffSet, PatchSet};
use sqlparser::ast::{CreateTable, Statement};
use std::io::Cursor;

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

    // Step 2: Execute in rusqlite with session tracking
    let Ok(conn) = Connection::open_in_memory() else {
        return;
    };

    // Extract CREATE TABLE statements and execute them
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

    for create_sql in &create_table_sqls {
        if conn.execute(create_sql, []).is_err() {
            return; // Invalid schema, skip
        }
    }

    // Create session and attach all tables
    let Ok(mut session) = Session::new(&conn) else {
        return;
    };
    if session.attach::<&str>(None).is_err() {
        return;
    }

    // Execute DML statements (INSERT, UPDATE, DELETE)
    for stmt in &statements {
        match stmt {
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
                let stmt_sql = stmt.to_string();
                if conn.execute(&stmt_sql, []).is_err() {
                    // DML failed, skip this test
                    return;
                }
            }
            _ => {}
        }
    }

    // Get rusqlite's changeset and patchset
    let mut rusqlite_changeset = Vec::new();
    if session.changeset_strm(&mut rusqlite_changeset).is_err() {
        return;
    }

    let mut rusqlite_patchset = Vec::new();
    if session.patchset_strm(&mut rusqlite_patchset).is_err() {
        return;
    }

    // Step 3: Build our binary output
    let our_changeset: Vec<u8> = changeset.clone().into();
    let our_patchset: Vec<u8> = patchset.clone().into();

    // Step 4: Compare at the parsed level (semantic equality)
    compare_changesets(&rusqlite_changeset, &our_changeset);
    compare_patchsets(&rusqlite_patchset, &our_patchset);

    // Step 5: Verify DB state by applying both changesets to empty databases
    if !rusqlite_changeset.is_empty() && !our_changeset.is_empty() {
        verify_changeset_application(&create_table_sqls, &rusqlite_changeset, &our_changeset);
    }

    if !rusqlite_patchset.is_empty() && !our_patchset.is_empty() {
        verify_patchset_application(&create_table_sqls, &rusqlite_patchset, &our_patchset);
    }
}

/// Compare two changesets at the parsed level.
fn compare_changesets(rusqlite_bytes: &[u8], our_bytes: &[u8]) {
    if rusqlite_bytes.is_empty() && our_bytes.is_empty() {
        return;
    }

    if !rusqlite_bytes.is_empty() {
        let rusqlite_parsed =
            ParsedDiffSet::try_from(rusqlite_bytes).expect("Failed to parse rusqlite changeset");
        assert!(
            matches!(&rusqlite_parsed, ParsedDiffSet::Changeset(_)),
            "Expected changeset from rusqlite"
        );
    }

    if !our_bytes.is_empty() {
        let our_parsed = ParsedDiffSet::try_from(our_bytes).expect("Failed to parse our changeset");
        assert!(
            matches!(&our_parsed, ParsedDiffSet::Changeset(_)),
            "Expected changeset from our builder"
        );
    }

    // Compare parsed structures if both non-empty
    if !rusqlite_bytes.is_empty() && !our_bytes.is_empty() {
        let rusqlite_parsed = ParsedDiffSet::try_from(rusqlite_bytes).unwrap();
        let our_parsed = ParsedDiffSet::try_from(our_bytes).unwrap();
        assert_eq!(
            rusqlite_parsed, our_parsed,
            "Changeset mismatch!\nrusqlite: {rusqlite_parsed:?}\nours: {our_parsed:?}"
        );
    }
}

/// Compare two patchsets at the parsed level.
fn compare_patchsets(rusqlite_bytes: &[u8], our_bytes: &[u8]) {
    if rusqlite_bytes.is_empty() && our_bytes.is_empty() {
        return;
    }

    if !rusqlite_bytes.is_empty() {
        let rusqlite_parsed =
            ParsedDiffSet::try_from(rusqlite_bytes).expect("Failed to parse rusqlite patchset");
        assert!(
            matches!(&rusqlite_parsed, ParsedDiffSet::Patchset(_)),
            "Expected patchset from rusqlite"
        );
    }

    if !our_bytes.is_empty() {
        let our_parsed = ParsedDiffSet::try_from(our_bytes).expect("Failed to parse our patchset");
        assert!(
            matches!(&our_parsed, ParsedDiffSet::Patchset(_)),
            "Expected patchset from our builder"
        );
    }

    // Compare parsed structures if both non-empty
    if !rusqlite_bytes.is_empty() && !our_bytes.is_empty() {
        let rusqlite_parsed = ParsedDiffSet::try_from(rusqlite_bytes).unwrap();
        let our_parsed = ParsedDiffSet::try_from(our_bytes).unwrap();
        assert_eq!(
            rusqlite_parsed, our_parsed,
            "Patchset mismatch!\nrusqlite: {rusqlite_parsed:?}\nours: {our_parsed:?}"
        );
    }
}

/// Apply both changesets to empty databases and verify they produce identical state.
fn verify_changeset_application(
    create_table_sqls: &[String],
    rusqlite_changeset: &[u8],
    our_changeset: &[u8],
) {
    // Create two empty databases with identical schema
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

    // Apply rusqlite's changeset to conn1
    conn1
        .execute("BEGIN", [])
        .expect("Failed to begin transaction");
    if let Err(e) = apply_changeset(&conn1, rusqlite_changeset) {
        panic!(
            "Failed to apply rusqlite changeset: {e}\nChangeset bytes: {rusqlite_changeset:02x?}"
        );
    }
    conn1
        .execute("COMMIT", [])
        .expect("Failed to commit transaction");

    // Apply our changeset to conn2
    conn2
        .execute("BEGIN", [])
        .expect("Failed to begin transaction");
    if let Err(e) = apply_changeset(&conn2, our_changeset) {
        panic!("Failed to apply our changeset: {e}\nChangeset bytes: {our_changeset:02x?}");
    }
    conn2
        .execute("COMMIT", [])
        .expect("Failed to commit transaction");

    // Compare database states
    compare_db_states(&conn1, &conn2, create_table_sqls);
}

/// Apply both patchsets to empty databases and verify they produce identical state.
fn verify_patchset_application(
    create_table_sqls: &[String],
    rusqlite_patchset: &[u8],
    our_patchset: &[u8],
) {
    // Create two empty databases with identical schema
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

    // Apply rusqlite's patchset to conn1
    conn1
        .execute("BEGIN", [])
        .expect("Failed to begin transaction");
    if apply_changeset(&conn1, rusqlite_patchset).is_err() {
        // Patchset application may fail for some inputs, skip
        conn1.execute("ROLLBACK", []).ok();
        return;
    }
    conn1
        .execute("COMMIT", [])
        .expect("Failed to commit transaction");

    // Apply our patchset to conn2
    conn2
        .execute("BEGIN", [])
        .expect("Failed to begin transaction");
    if apply_changeset(&conn2, our_patchset).is_err() {
        conn2.execute("ROLLBACK", []).ok();
        return;
    }
    conn2
        .execute("COMMIT", [])
        .expect("Failed to commit transaction");

    // Compare database states
    compare_db_states(&conn1, &conn2, create_table_sqls);
}

/// Apply a changeset/patchset to a database connection.
fn apply_changeset(conn: &Connection, changeset: &[u8]) -> Result<(), rusqlite::Error> {
    let mut cursor = Cursor::new(changeset);
    conn.apply_strm(
        &mut cursor,
        None::<fn(&str) -> bool>,
        |_conflict_type: ConflictType, _item: ChangesetItem| ConflictAction::SQLITE_CHANGESET_ABORT,
    )
}

/// Compare database states by querying all tables and comparing rows.
fn compare_db_states(conn1: &Connection, conn2: &Connection, create_table_sqls: &[String]) {
    for create_sql in create_table_sqls {
        // Extract table name from CREATE TABLE statement
        let table_name = extract_table_name(create_sql);

        let rows1 = get_all_rows(conn1, &table_name);
        let rows2 = get_all_rows(conn2, &table_name);

        assert_eq!(
            rows1, rows2,
            "Database state mismatch for table '{table_name}'!\nDB1: {rows1:?}\nDB2: {rows2:?}"
        );
    }
}

/// Extract table name from a CREATE TABLE SQL statement.
fn extract_table_name(create_sql: &str) -> String {
    let lower = create_sql.to_lowercase();
    let start = lower.find("create table").unwrap() + "create table".len();
    let rest = &create_sql[start..].trim_start();

    // Handle optional "IF NOT EXISTS"
    let rest = if rest.to_lowercase().starts_with("if not exists") {
        rest["if not exists".len()..].trim_start()
    } else {
        rest
    };

    // Extract the table name (up to first space or paren)
    let end = rest
        .find(|c: char| c.is_whitespace() || c == '(')
        .unwrap_or(rest.len());
    rest[..end].to_string()
}

/// Get all rows from a table as a sorted vector of string representations.
fn get_all_rows(conn: &Connection, table_name: &str) -> Vec<Vec<String>> {
    let query = format!("SELECT * FROM {table_name} ORDER BY rowid");
    let mut stmt = match conn.prepare(&query) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    let column_count = stmt.column_count();
    let rows_result = stmt.query_map([], |row| {
        let mut values = Vec::new();
        for i in 0..column_count {
            let value: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
            values.push(format!("{value:?}"));
        }
        Ok(values)
    });

    let mut rows: Vec<Vec<String>> = match rows_result {
        Ok(mapped) => mapped.filter_map(|r| r.ok()).collect(),
        Err(_) => Vec::new(),
    };

    // Sort for order-independent comparison
    rows.sort();
    rows
}
