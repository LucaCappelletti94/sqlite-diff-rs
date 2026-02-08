//! Testing utilities for bit-parity verification against rusqlite's session extension.
//!
//! This module is gated behind the `testing` feature.
//!
//! # Provided helpers
//!
//! - [`session_changeset_and_patchset`]: execute SQL in rusqlite and capture raw changeset/patchset bytes
//! - [`byte_diff_report`]: pretty-print a byte-level diff between two buffers
//! - [`assert_bit_parity`]: assert byte-for-byte equality for both changeset and patchset
//! - [`parse_schema`]: parse a `CREATE TABLE` statement into a `SimpleTable`

use core::fmt::Write;

extern crate std;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use rusqlite::Connection;
use rusqlite::session::Session;
use std::io::Cursor;

use crate::schema::SimpleTable;
use crate::sql::{FormatSql, Parser, Statement};
use crate::{ChangeSet, PatchSet};

/// Parse a `CREATE TABLE` statement and return a `SimpleTable`.
///
/// # Panics
///
/// Panics if the SQL is not a valid `CREATE TABLE` statement.
#[must_use]
pub fn parse_schema(sql: &str) -> SimpleTable {
    let mut parser = Parser::new(sql);
    let stmts = parser.parse_all().expect("Failed to parse SQL");
    match &stmts[0] {
        Statement::CreateTable(ct) => SimpleTable::from(ct.clone()),
        _ => panic!("Expected CREATE TABLE"),
    }
}

/// Execute a sequence of SQL statements against rusqlite with session tracking
/// and return the raw changeset and patchset bytes.
///
/// DDL (`CREATE TABLE`) is executed before the session starts.
/// DML (`INSERT`/`UPDATE`/`DELETE`) is executed inside the session.
#[must_use]
pub fn session_changeset_and_patchset(statements: &[&str]) -> (Vec<u8>, Vec<u8>) {
    fn run_session(statements: &[&str], extract: impl Fn(&mut Session<'_>) -> Vec<u8>) -> Vec<u8> {
        let conn = Connection::open_in_memory().unwrap();
        for &sql in statements {
            if sql.trim().to_uppercase().starts_with("CREATE TABLE") {
                conn.execute(sql, []).unwrap();
            }
        }
        let mut session = Session::new(&conn).unwrap();
        session.attach::<&str>(None).unwrap();
        for &sql in statements {
            if !sql.trim().to_uppercase().starts_with("CREATE TABLE") {
                conn.execute(sql, []).unwrap();
            }
        }
        extract(&mut session)
    }

    let changeset = run_session(statements, |session| {
        let mut buf = Vec::new();
        session.changeset_strm(&mut buf).unwrap();
        buf
    });
    let patchset = run_session(statements, |session| {
        let mut buf = Vec::new();
        session.patchset_strm(&mut buf).unwrap();
        buf
    });

    (changeset, patchset)
}

/// Pretty-print a byte-level diff between two changeset/patchset buffers.
///
/// Returns a human-readable string describing where they differ.
#[must_use]
pub fn byte_diff_report(label: &str, expected: &[u8], actual: &[u8]) -> String {
    if expected == actual {
        return format!("{label}: MATCH ({} bytes)", expected.len());
    }

    let mut report = format!(
        "{label}: MISMATCH\n  expected len: {}\n  actual len:   {}\n",
        expected.len(),
        actual.len()
    );

    // Find first divergence point
    let min_len = expected.len().min(actual.len());
    let first_diff = (0..min_len).find(|&i| expected[i] != actual[i]);

    if let Some(pos) = first_diff {
        let _ = writeln!(
            report,
            "  first diff at byte {pos}: expected 0x{:02x}, actual 0x{:02x}",
            expected[pos], actual[pos]
        );
        // Show context around the diff
        let start = pos.saturating_sub(4);
        let end = (pos + 8).min(min_len);
        let _ = writeln!(
            report,
            "  expected[{start}..{end}]: {:02x?}",
            &expected[start..end]
        );
        let _ = writeln!(
            report,
            "  actual  [{start}..{end}]: {:02x?}",
            &actual[start..end]
        );
    } else {
        report.push_str("  common prefix matches, difference is in length only\n");
    }

    let _ = writeln!(report, "  expected: {expected:02x?}");
    let _ = writeln!(report, "  actual:   {actual:02x?}");

    report
}

/// Assert byte-for-byte equality between our output and rusqlite's output,
/// for both changeset and patchset.
///
/// # Panics
///
/// Panics with a detailed diff report if the bytes don't match.
pub fn assert_bit_parity(sql_statements: &[&str], our_changeset: &[u8], our_patchset: &[u8]) {
    let (sqlite_changeset, sqlite_patchset) = session_changeset_and_patchset(sql_statements);

    let cs_report = byte_diff_report("changeset", &sqlite_changeset, our_changeset);
    let ps_report = byte_diff_report("patchset", &sqlite_patchset, our_patchset);

    assert!(
        sqlite_changeset == our_changeset && sqlite_patchset == our_patchset,
        "Bit parity failure!\n\n{cs_report}\n{ps_report}\n\nSQL:\n{}",
        sql_statements.join("\n")
    );
}

/// Run bit-parity test using the `FromStr` parsing path.
///
/// Parses the SQL into [`ChangeSet`]/[`PatchSet`] via `str::parse()`, serializes
/// to bytes, and compares with rusqlite's output.
///
/// # Panics
///
/// Panics if parsing fails or if the bytes don't match.
pub fn assert_fromstr_bit_parity(sql: &str) {
    let changeset: ChangeSet<SimpleTable, String, Vec<u8>> = sql.parse().unwrap();
    let patchset: PatchSet<SimpleTable, String, Vec<u8>> = sql.parse().unwrap();

    let our_changeset: Vec<u8> = changeset.into();
    let our_patchset: Vec<u8> = patchset.into();

    // Reconstruct the statement list for rusqlite
    let mut parser = Parser::new(sql);
    let stmts = parser.parse_all().unwrap();
    let sql_strings: Vec<String> = stmts.iter().map(|s| s.format_sql()).collect();
    let sql_refs: Vec<&str> = sql_strings.iter().map(String::as_str).collect();

    assert_bit_parity(&sql_refs, &our_changeset, &our_patchset);
}

/// Apply a changeset or patchset to a database connection.
///
/// Uses `SQLITE_CHANGESET_ABORT` on conflict.
///
/// # Errors
///
/// Returns an error if the changeset application fails.
pub fn apply_changeset(conn: &Connection, changeset: &[u8]) -> Result<(), rusqlite::Error> {
    use rusqlite::session::{ChangesetItem, ConflictAction, ConflictType};
    let mut cursor = Cursor::new(changeset);
    conn.apply_strm(
        &mut cursor,
        None::<fn(&str) -> bool>,
        |_conflict_type: ConflictType, _item: ChangesetItem| ConflictAction::SQLITE_CHANGESET_ABORT,
    )
}

/// Query all rows from a table as a sorted vector of string-formatted values.
///
/// Rows are sorted for order-independent comparison.
pub fn get_all_rows(conn: &Connection, table_name: &str) -> Vec<Vec<String>> {
    let query = format!("SELECT * FROM {table_name} ORDER BY rowid");
    let Ok(mut stmt) = conn.prepare(&query) else {
        return Vec::new();
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
        Ok(mapped) => mapped.filter_map(Result::ok).collect(),
        Err(_) => Vec::new(),
    };

    // Sort for order-independent comparison
    rows.sort();
    rows
}

/// Assert that two database connections have identical contents across all given tables.
///
/// # Panics
///
/// Panics if any table has different rows in the two connections.
pub fn compare_db_states(conn1: &Connection, conn2: &Connection, create_table_sqls: &[String]) {
    for create_sql in create_table_sqls {
        let table_name = extract_table_name(create_sql);

        let rows1 = get_all_rows(conn1, &table_name);
        let rows2 = get_all_rows(conn2, &table_name);

        assert_eq!(
            rows1, rows2,
            "Database state mismatch for table '{table_name}'!\nDB1: {rows1:?}\nDB2: {rows2:?}"
        );
    }
}

/// Extract the table name from a `CREATE TABLE` SQL string.
#[must_use]
pub fn extract_table_name(create_sql: &str) -> String {
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
