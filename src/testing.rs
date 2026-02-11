//! Testing utilities for bit-parity verification against rusqlite's session extension.
//!
//! This module is gated behind the `testing` feature.
//!
//! # Provided helpers
//!
//! - [`session_changeset_and_patchset`]: execute SQL in rusqlite and capture raw changeset/patchset bytes
//! - [`byte_diff_report`]: pretty-print a byte-level diff between two buffers
//! - [`assert_bit_parity`]: assert byte-for-byte equality for both changeset and patchset
//! - [`TypedSimpleTable`]: a [`SimpleTable`] with column type information and `Display` for DDL
//! - [`SqlType`]: `SQLite` column type affinities
//! - [`test_roundtrip`]: parse → serialize → reparse → assert equality
//! - [`test_apply_roundtrip`]: parse, roundtrip, then apply changeset to an in-memory database
//! - [`test_reverse_idempotent`]: verify `reverse(reverse(x)) == x` for changesets
//! - [`test_sql_roundtrip`]: digest SQL into a patchset, then binary roundtrip
//! - [`test_differential`]: compare our patchset output against rusqlite byte-for-byte

use core::fmt::{self, Write};
use core::ops::Deref;

extern crate std;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use rusqlite::Connection;
use rusqlite::session::Session;
use std::io::Cursor;

use crate::DynTable;
use crate::PatchSet;
use crate::Reverse;
use crate::differential_testing::run_differential_test;
use crate::parser::ParsedDiffSet;
use crate::schema::SimpleTable;

// ---------------------------------------------------------------------------
// SqlType – SQLite column type affinities
// ---------------------------------------------------------------------------

/// `SQLite` column type affinities for use in `CREATE TABLE` DDL generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlType {
    /// `INTEGER` affinity.
    Integer,
    /// `TEXT` affinity.
    Text,
    /// `REAL` affinity.
    Real,
    /// `BLOB` affinity (accepts any value).
    Blob,
}

impl fmt::Display for SqlType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer => f.write_str("INTEGER"),
            Self::Text => f.write_str("TEXT"),
            Self::Real => f.write_str("REAL"),
            Self::Blob => f.write_str("BLOB"),
        }
    }
}

impl<'a> arbitrary::Arbitrary<'a> for SqlType {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(*u.choose(&[Self::Integer, Self::Text, Self::Real, Self::Blob])?)
    }
}

// ---------------------------------------------------------------------------
// TypedSimpleTable – SimpleTable + column types with Display for DDL
// ---------------------------------------------------------------------------

/// A [`SimpleTable`] augmented with column type information.
///
/// Implements [`Display`](fmt::Display) to emit a `CREATE TABLE` SQL statement,
/// enabling database setup from schema metadata alone (e.g. in fuzz harnesses).
///
/// Dereferences to [`SimpleTable`], so it can be used anywhere a `SimpleTable`
/// is expected.
///
/// # Example
///
/// ```rust
/// use sqlite_diff_rs::testing::{TypedSimpleTable, SqlType};
///
/// let table = TypedSimpleTable::new(
///     "users",
///     &[("id", SqlType::Integer), ("name", SqlType::Text)],
///     &[0],
/// );
/// assert_eq!(
///     table.to_string(),
///     "CREATE TABLE \"users\" (\"id\" INTEGER PRIMARY KEY, \"name\" TEXT)"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TypedSimpleTable {
    table: SimpleTable,
    column_types: Vec<SqlType>,
}

impl TypedSimpleTable {
    /// Create a new typed table schema.
    ///
    /// # Arguments
    ///
    /// * `name` – The table name.
    /// * `columns` – Pairs of `(column_name, column_type)` in order.
    /// * `pk_indices` – Indices of primary key columns (in PK order).
    ///
    /// # Panics
    ///
    /// Panics if any `pk_indices` value is out of bounds.
    #[must_use]
    pub fn new(name: &str, columns: &[(&str, SqlType)], pk_indices: &[usize]) -> Self {
        let col_names: Vec<&str> = columns.iter().map(|(n, _)| *n).collect();
        let col_types: Vec<SqlType> = columns.iter().map(|(_, t)| *t).collect();
        Self {
            table: SimpleTable::new(name, &col_names, pk_indices),
            column_types: col_types,
        }
    }

    /// Create a `TypedSimpleTable` from a [`crate::parser::TableSchema`].
    ///
    /// Synthesizes generic column names (`c0`, `c1`, …) and uses
    /// [`SqlType::Blob`] for every column (the most permissive `SQLite` type).
    /// This is primarily useful in fuzz harnesses that parse arbitrary binary
    /// changesets and need to create matching database tables.
    #[must_use]
    pub fn from_table_schema(schema: &crate::parser::TableSchema<String>) -> Self {
        let ncols = schema.number_of_columns();
        let mut pk_flags_buf = vec![0u8; ncols];
        schema.write_pk_flags(&mut pk_flags_buf);

        // Derive pk_indices from pk_flags (sorted by ordinal)
        let mut pk_cols: Vec<(usize, u8)> = pk_flags_buf
            .iter()
            .enumerate()
            .filter_map(|(i, &ord)| if ord > 0 { Some((i, ord)) } else { None })
            .collect();
        pk_cols.sort_by_key(|&(_, ord)| ord);
        let pk_indices: Vec<usize> = pk_cols.into_iter().map(|(i, _)| i).collect();

        let columns: Vec<(String, SqlType)> = (0..ncols)
            .map(|i| (format!("c{i}"), SqlType::Blob))
            .collect();
        let col_refs: Vec<(&str, SqlType)> =
            columns.iter().map(|(n, t)| (n.as_str(), *t)).collect();

        Self::new(schema.name(), &col_refs, &pk_indices)
    }

    /// The column types in order.
    #[must_use]
    pub fn column_types(&self) -> &[SqlType] {
        &self.column_types
    }
}

impl Deref for TypedSimpleTable {
    type Target = SimpleTable;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

impl fmt::Display for TypedSimpleTable {
    /// Emit a `CREATE TABLE` DDL statement.
    ///
    /// For a single-column PK the `PRIMARY KEY` clause is inlined on the column.
    /// For composite PKs a trailing `PRIMARY KEY(…)` constraint is appended.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pk_indices = self.table.pk_indices();
        let columns = self.table.column_names();
        let single_pk = pk_indices.len() == 1;

        write!(f, "CREATE TABLE \"{}\" (", self.table.name())?;

        for (i, (col_name, col_type)) in columns.iter().zip(&self.column_types).enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "\"{col_name}\" {col_type}")?;
            if single_pk && pk_indices[0] == i {
                f.write_str(" PRIMARY KEY")?;
            }
        }

        if !single_pk && !pk_indices.is_empty() {
            f.write_str(", PRIMARY KEY(")?;
            for (j, &pk_idx) in pk_indices.iter().enumerate() {
                if j > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "\"{}\"", columns[pk_idx])?;
            }
            f.write_char(')')?;
        }

        f.write_char(')')
    }
}

impl<'a> arbitrary::Arbitrary<'a> for TypedSimpleTable {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Table name: 1–8 lowercase alpha chars
        let name_len = u.int_in_range(1..=8)?;
        let name: String = (0..name_len)
            .map(|_| u.int_in_range(b'a'..=b'z').map(char::from))
            .collect::<arbitrary::Result<_>>()?;

        Self::arbitrary_with_name(u, &name)
    }
}

impl TypedSimpleTable {
    /// Generate an arbitrary table with a given name.
    ///
    /// This is shared between the single-table and multi-table `Arbitrary`
    /// implementations so that column count, types, and PK layout are still
    /// fuzz-driven while the caller controls naming.
    fn arbitrary_with_name(
        u: &mut arbitrary::Unstructured<'_>,
        name: &str,
    ) -> arbitrary::Result<Self> {
        use arbitrary::Arbitrary;

        // Column count: 1–8
        let ncols: usize = u.int_in_range(1..=8)?;
        let columns: Vec<(&str, SqlType)> = Vec::new(); // placeholder
        let mut col_data: Vec<(String, SqlType)> = Vec::with_capacity(ncols);
        for i in 0..ncols {
            let ty = SqlType::arbitrary(u)?;
            col_data.push((format!("c{i}"), ty));
        }
        let col_refs: Vec<(&str, SqlType)> =
            col_data.iter().map(|(n, t)| (n.as_str(), *t)).collect();
        drop(columns);

        // PK: at least 1 column, up to ncols
        let npk: usize = u.int_in_range(1..=ncols)?;
        // Choose npk distinct indices from 0..ncols
        let mut available: Vec<usize> = (0..ncols).collect();
        let mut pk_indices = Vec::with_capacity(npk);
        for _ in 0..npk {
            // available.len() is guaranteed > 0 here because npk <= ncols and we remove one per iteration
            #[allow(clippy::range_minus_one)]
            let idx = u.int_in_range(0..=available.len() - 1)?;
            pk_indices.push(available.remove(idx));
        }

        Ok(Self::new(name, &col_refs, &pk_indices))
    }
}

// ---------------------------------------------------------------------------
// FuzzSchemas – Vec<TypedSimpleTable> with guaranteed unique table names
// ---------------------------------------------------------------------------

/// A collection of 1–5 [`TypedSimpleTable`] schemas with unique table names.
///
/// Used as fuzz input for multi-table harnesses. Table names are deterministic
/// (`t0`, `t1`, …) to avoid collisions; column count, types, and PK layout
/// remain fuzz-driven.
///
/// Dereferences to `[TypedSimpleTable]` for ergonomic slice access.
#[derive(Debug, Clone)]
pub struct FuzzSchemas(pub Vec<TypedSimpleTable>);

impl Deref for FuzzSchemas {
    type Target = [TypedSimpleTable];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> arbitrary::Arbitrary<'a> for FuzzSchemas {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let ntables: usize = u.int_in_range(1..=5)?;
        let mut tables = Vec::with_capacity(ntables);
        for i in 0..ntables {
            let name = format!("t{i}");
            tables.push(TypedSimpleTable::arbitrary_with_name(u, &name)?);
        }
        Ok(Self(tables))
    }
}

// ---------------------------------------------------------------------------
// Shared fuzzer / regression-test helpers
// ---------------------------------------------------------------------------

/// Test binary roundtrip: parse → serialize → reparse → assert equality.
///
/// Returns early (no panic) if the input cannot be parsed.
///
/// # Panics
///
/// Panics if re-parsing our own output fails or if the reparsed data doesn't
/// match the original.
pub fn test_roundtrip(input: &[u8]) {
    let Ok(parsed) = ParsedDiffSet::try_from(input) else {
        return; // Invalid input is fine, we just shouldn't crash
    };

    let serialized: Vec<u8> = parsed.clone().into();
    let reparsed = ParsedDiffSet::try_from(serialized.as_slice())
        .expect("Re-parsing our own output should never fail");
    assert_eq!(parsed, reparsed, "Roundtrip mismatch");
}

/// Test that a changeset can be applied to an in-memory database without
/// crashing, in addition to binary roundtrip verification.
///
/// Each schema in `schemas` provides a `CREATE TABLE` DDL (via its
/// [`Display`](fmt::Display) impl) so the changeset has matching tables to
/// apply against.
///
/// Returns early if the input does not parse as a valid changeset/patchset,
/// skipping the (expensive) `SQLite` setup. When parsing succeeds the
/// **re-serialized** bytes — not the original fuzz input — are applied,
/// so we test that *our* output is accepted by `SQLite`.
///
/// Application errors are **not** treated as failures — the changeset may
/// be semantically invalid for the given schemas. Panics or crashes are bugs.
///
/// # Panics
///
/// Panics if re-parsing our serialized output fails or if the reparsed data
/// doesn't match the original.
pub fn test_apply_roundtrip(schemas: &[TypedSimpleTable], changeset_bytes: &[u8]) {
    // Parse the input; bail early if it is not a valid changeset/patchset.
    // This avoids paying the SQLite-setup cost for the vast majority of
    // fuzzed inputs (random bytes almost never form valid changesets).
    let Ok(parsed) = ParsedDiffSet::try_from(changeset_bytes) else {
        return;
    };

    // Binary roundtrip check: serialize → reparse → assert equality.
    let serialized: Vec<u8> = parsed.clone().into();
    let reparsed = ParsedDiffSet::try_from(serialized.as_slice())
        .expect("Re-parsing our own output should never fail");
    assert_eq!(parsed, reparsed, "Roundtrip mismatch");

    // Create an in-memory database with all tables
    let Ok(conn) = Connection::open_in_memory() else {
        return;
    };
    for schema in schemas {
        let ddl = schema.to_string();
        if conn.execute(&ddl, []).is_err() {
            return; // Schema might be invalid (e.g. no PK)
        }
    }

    // Apply the *re-serialized* bytes — errors are acceptable, panics are not
    let _ = apply_changeset(&conn, &serialized);
}

/// Test reverse idempotency: `reverse(reverse(x)) == x`.
///
/// Parses the input as a binary changeset (skips patchsets, since they don't
/// support [`Reverse`]). Verifies four properties:
///
/// 1. Double-reversing yields a structurally equal changeset.
/// 2. Binary representations match after double reverse.
/// 3. Operation count is preserved by reversal.
/// 4. Empty changesets reverse to empty.
///
/// Returns early (no panic) if the input cannot be parsed or is a patchset.
///
/// # Panics
///
/// Panics if any of the reversal invariants are violated (double-reverse
/// not equal to original, operation count changes, etc.).
pub fn test_reverse_idempotent(input: &[u8]) {
    let Ok(parsed) = ParsedDiffSet::try_from(input) else {
        return;
    };

    // Only changesets support Reverse
    let ParsedDiffSet::Changeset(changeset) = parsed else {
        return;
    };

    let reversed = changeset.clone().reverse();
    let double_reversed = reversed.clone().reverse();

    assert_eq!(
        changeset, double_reversed,
        "Double reverse should equal original"
    );

    let original_bytes = changeset.build();
    let double_reversed_bytes = double_reversed.build();
    assert_eq!(
        original_bytes, double_reversed_bytes,
        "Binary representation should be identical after double reverse"
    );

    assert_eq!(
        changeset.len(),
        reversed.len(),
        "Reversed changeset should have same number of operations"
    );

    if changeset.is_empty() {
        assert!(
            reversed.is_empty(),
            "Empty changeset should reverse to empty"
        );
    }
}

/// Test SQL-digest roundtrip: digest SQL into a patchset, serialize, reparse.
///
/// Given one or more table schemas and a SQL string, this:
/// 1. Builds a [`PatchSet`] with all schemas, digests the SQL.
/// 2. If digestion fails or the result is empty, returns early.
/// 3. Serializes to binary and re-parses, asserting byte equality.
///
/// Returns early (no panic) if SQL digestion fails or produces no operations.
///
/// # Panics
///
/// Panics if the serialized patchset cannot be re-parsed or if the binary
/// representation changes after roundtrip.
pub fn test_sql_roundtrip(schemas: &[TypedSimpleTable], sql: &str) {
    let mut builder: PatchSet<SimpleTable, String, Vec<u8>> = PatchSet::new();
    for schema in schemas {
        builder.add_table(&**schema);
    }

    if builder.digest_sql(sql).is_err() {
        return;
    }
    if builder.is_empty() {
        return;
    }

    let bytes = builder.build();
    let reparsed = ParsedDiffSet::try_from(bytes.as_slice())
        .expect("Serialized patchset should be re-parseable");
    let reparsed_bytes: Vec<u8> = reparsed.into();
    assert_eq!(
        bytes, reparsed_bytes,
        "Binary round-trip mismatch after SQL digest"
    );
}

/// Test differential (bit-parity) between our patchset output and rusqlite's.
///
/// Given one or more table schemas and a SQL DML string, this:
/// 1. Builds a [`PatchSet`] with all schemas, digests the SQL.
/// 2. If digestion fails or the result is empty, returns early.
/// 3. Delegates to `run_differential_test` from the `differential_testing` module
///    to compare our bytes against rusqlite's session extension output.
///
/// Returns early (no panic) if SQL digestion fails or produces no operations.
pub fn test_differential(schemas: &[TypedSimpleTable], sql: &str) {
    let mut builder: PatchSet<SimpleTable, String, Vec<u8>> = PatchSet::new();
    for schema in schemas {
        builder.add_table(&**schema);
    }

    if builder.digest_sql(sql).is_err() || builder.is_empty() {
        return;
    }

    let create_sqls: Vec<String> = schemas.iter().map(ToString::to_string).collect();
    let create_sql_refs: Vec<&str> = create_sqls.iter().map(String::as_str).collect();
    let simples: Vec<SimpleTable> = schemas.iter().map(|s| (**s).clone()).collect();
    run_differential_test(&simples, &create_sql_refs, &[sql]);
}

/// Create an in-memory `SQLite` database, execute statements with a session,
/// and return the raw changeset and patchset bytes.
///
/// DDL (`CREATE TABLE`) is executed before the session starts.
/// DML (`INSERT`/`UPDATE`/`DELETE`) is executed inside the session.
///
/// # Panics
///
/// Panics if database creation, statement execution, or session operations fail.
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

/// Run bit-parity test by digesting SQL into a `PatchSet` via `digest_sql`,
/// serializing to bytes, and comparing the patchset with rusqlite's output.
///
/// Note: this only tests patchset parity since SQL digestion is patchset-only.
///
/// The `schemas` must be pre-built [`SimpleTable`]s matching the CREATE TABLE
/// statements in `sql_statements`. The builder is seeded with these schemas
/// before digesting.
///
/// # Panics
///
/// Panics if parsing fails or if the bytes don't match.
pub fn assert_patchset_sql_parity(schemas: &[SimpleTable], sql_statements: &[&str]) {
    let mut patchset = PatchSet::<SimpleTable, String, Vec<u8>>::new();

    // Build a lookup map so we can register tables on first DML reference
    let schema_map: std::collections::HashMap<&str, &SimpleTable> =
        schemas.iter().map(|s| (s.name(), s)).collect();

    // Digest DML statements, registering each table on first touch
    for dml in sql_statements
        .iter()
        .filter(|s| !s.trim().to_uppercase().starts_with("CREATE"))
    {
        // Extract the table name from the DML to ensure proper registration order
        let upper = dml.trim().to_uppercase();
        let table_name = if upper.starts_with("INSERT INTO") {
            dml.trim()["INSERT INTO".len()..].split_whitespace().next()
        } else if upper.starts_with("UPDATE") {
            dml.trim()["UPDATE".len()..].split_whitespace().next()
        } else if upper.starts_with("DELETE FROM") {
            dml.trim()["DELETE FROM".len()..].split_whitespace().next()
        } else {
            None
        };

        if let Some(name) = table_name
            && let Some(schema) = schema_map.get(name)
        {
            patchset.add_table(schema);
        }
        patchset.digest_sql(dml).unwrap();
    }

    let our_patchset: Vec<u8> = patchset.build();
    let (_, sqlite_patchset) = session_changeset_and_patchset(sql_statements);

    let ps_report = byte_diff_report("patchset", &sqlite_patchset, &our_patchset);
    assert!(
        sqlite_patchset == our_patchset,
        "Patchset bit parity failure!\n\n{ps_report}\n\nSQL:\n{}",
        sql_statements.join("\n")
    );
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
///
/// # Panics
///
/// Panics if the input does not contain a valid `CREATE TABLE` statement.
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

/// Run all crash files in a directory through a test function, with timing
/// and auto-copy from the fuzz workspace.
///
/// This is the shared implementation behind the per-target regression tests
/// (e.g. `roundtrip`, `apply_roundtrip`). It:
///
/// 1. Ensures `crash_dir` exists.
/// 2. Copies any `.fuzz` files from `fuzz_source_dir` that aren't already in `crash_dir`.
/// 3. Runs `test_fn` on every file in `crash_dir`, enforcing `time_limit` per input.
/// 4. Panics with a summary if any input fails or exceeds the time limit.
///
/// Returns the number of files tested.
///
/// # Panics
///
/// Panics if any test input causes `test_fn` to panic, or if any input exceeds
/// the time limit.
pub fn run_crash_dir_regression(
    crash_dir: &str,
    fuzz_source_dir: &str,
    time_limit: std::time::Duration,
    test_fn: impl Fn(&[u8]),
) -> usize {
    use std::fs;
    use std::time::Instant;

    // Ensure crash_inputs directory exists
    let _ = fs::create_dir_all(crash_dir);

    // Copy any new crash files from fuzz workspace
    if let Ok(fuzz_entries) = fs::read_dir(fuzz_source_dir) {
        for entry in fuzz_entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "fuzz") {
                let dest = format!(
                    "{}/{}",
                    crash_dir,
                    path.file_name().unwrap().to_string_lossy()
                );
                if !std::path::Path::new(&dest).exists() {
                    let _ = fs::copy(&path, &dest);
                }
            }
        }
    }

    let Ok(entries) = fs::read_dir(crash_dir) else {
        return 0;
    };

    let mut tested = 0;
    let mut failures: Vec<String> = Vec::new();
    let mut slow_inputs: Vec<String> = Vec::new();

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(e) => {
                failures.push(format!("{}: read error: {e}", path.display()));
                continue;
            }
        };

        let filename = path.file_name().unwrap().to_string_lossy().to_string();
        let start = Instant::now();

        // Use catch_unwind to collect panics without aborting the loop
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            test_fn(&data);
        }));

        if let Err(panic) = result {
            let msg = if let Some(s) = panic.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = panic.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic".to_string()
            };
            failures.push(format!("{filename}: {msg}"));
        }

        let elapsed = start.elapsed();
        if elapsed > time_limit {
            slow_inputs.push(format!(
                "{filename}: {:.3}s (limit: {:.1}s) [{} bytes]",
                elapsed.as_secs_f64(),
                time_limit.as_secs_f64(),
                data.len(),
            ));
        }

        tested += 1;
    }

    assert!(
        failures.is_empty(),
        "Failures in {}/{tested} crash files:\n{}",
        failures.len(),
        failures.join("\n")
    );

    assert!(
        slow_inputs.is_empty(),
        "Timeout-class bugs: {}/{tested} inputs exceeded {:.1}s limit:\n{}",
        slow_inputs.len(),
        time_limit.as_secs_f64(),
        slow_inputs.join("\n")
    );

    tested
}

// ---------------------------------------------------------------------------
// wal2json fuzzing helpers (requires `wal2json` feature)
// ---------------------------------------------------------------------------

/// Test wal2json parsing and conversion: parse JSON, convert to changeset operations.
///
/// This fuzzer tests that:
/// 1. Parsing arbitrary bytes as JSON doesn't panic
/// 2. Parsing valid JSON as wal2json messages doesn't panic
/// 3. Converting parsed messages to changeset operations doesn't panic
/// 4. Round-trip through serialize/deserialize produces equal structures
///
/// Returns early (no panic) if the input cannot be parsed as valid JSON.
///
/// # Panics
///
/// Panics if serde round-trip produces data that doesn't match the original
/// parsed structure.
#[cfg(feature = "wal2json")]
pub fn test_wal2json(input: &[u8]) {
    use crate::SimpleTable;
    use crate::wal2json::{Action, MessageV2, TransactionV1, parse_v1, parse_v2};
    use crate::{ChangeDelete, Insert};

    // Try to interpret as UTF-8 JSON
    let Ok(json_str) = core::str::from_utf8(input) else {
        return;
    };

    // Try parsing as v2 format (per-tuple JSON)
    if let Ok(msg) = parse_v2(json_str) {
        // Round-trip through serde
        let serialized = serde_json::to_string(&msg);
        if let Ok(json) = serialized {
            let reparsed: Result<MessageV2, _> = serde_json::from_str(&json);
            if let Ok(reparsed) = reparsed {
                assert_eq!(msg.action, reparsed.action);
                assert_eq!(msg.table, reparsed.table);
                assert_eq!(msg.schema, reparsed.schema);
            }
        }

        // Try conversion to changeset operations (if it has table info)
        if let Some(ref table_name) = msg.table {
            // Build a generic schema for testing conversion
            let col_names: Vec<&str> = msg.columns.as_ref().map_or_else(
                || {
                    msg.identity.as_ref().map_or_else(Vec::new, |cols| {
                        cols.iter().map(|c| c.name.as_str()).collect()
                    })
                },
                |cols| cols.iter().map(|c| c.name.as_str()).collect(),
            );

            if !col_names.is_empty() {
                let table = SimpleTable::new(table_name, &col_names, &[0]);

                match msg.action {
                    Action::I => {
                        let _: Result<Insert<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
                    }
                    Action::D => {
                        let _: Result<ChangeDelete<_, String, Vec<u8>>, _> =
                            (&msg, &table).try_into();
                    }
                    _ => {}
                }
            }
        }
    }

    // Try parsing as v1 format (transaction-level JSON)
    if let Ok(tx) = parse_v1(json_str) {
        // Round-trip through serde
        let serialized = serde_json::to_string(&tx);
        if let Ok(json) = serialized {
            let reparsed: Result<TransactionV1, _> = serde_json::from_str(&json);
            if let Ok(reparsed) = reparsed {
                assert_eq!(tx.change.len(), reparsed.change.len());
            }
        }

        // Try conversion for each change
        for change in &tx.change {
            if !change.columnnames.is_empty() {
                let col_refs: Vec<&str> = change.columnnames.iter().map(String::as_str).collect();
                let table = SimpleTable::new(&change.table, &col_refs, &[0]);

                match change.kind.as_str() {
                    "insert" => {
                        let _: Result<Insert<_, String, Vec<u8>>, _> = (change, &table).try_into();
                    }
                    "delete" => {
                        let _: Result<ChangeDelete<_, String, Vec<u8>>, _> =
                            (change, &table).try_into();
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Test wal2json with structured arbitrary input.
///
/// This tests the wal2json types with their `Arbitrary` implementations,
/// ensuring serialization and conversion don't panic on any valid structure.
#[cfg(feature = "wal2json")]
pub fn test_wal2json_arbitrary(msg: &crate::wal2json::MessageV2) {
    use crate::SimpleTable;
    use crate::wal2json::Action;
    use crate::{ChangeDelete, Insert};

    // Serialize to JSON and back
    if let Ok(json) = serde_json::to_string(msg) {
        let _: Result<crate::wal2json::MessageV2, _> = serde_json::from_str(&json);
    }

    // Try conversion to changeset operations
    if let Some(ref table_name) = msg.table {
        let col_names: Vec<&str> = msg.columns.as_ref().map_or_else(
            || {
                msg.identity.as_ref().map_or_else(Vec::new, |cols| {
                    cols.iter().map(|c| c.name.as_str()).collect()
                })
            },
            |cols| cols.iter().map(|c| c.name.as_str()).collect(),
        );

        if !col_names.is_empty() {
            let table = SimpleTable::new(table_name, &col_names, &[0]);

            match msg.action {
                Action::I => {
                    let _: Result<Insert<_, String, Vec<u8>>, _> = (msg, &table).try_into();
                }
                Action::D => {
                    let _: Result<ChangeDelete<_, String, Vec<u8>>, _> = (msg, &table).try_into();
                }
                _ => {}
            }
        }
    }
}

// ---------------------------------------------------------------------------
// pg_walstream fuzz testing
// ---------------------------------------------------------------------------

/// Test `pg_walstream` event parsing and conversion from arbitrary bytes.
///
/// This function attempts to:
/// 1. Parse bytes as UTF-8 JSON
/// 2. Deserialize as `EventType`
/// 3. Serialize and verify round-trip
/// 4. Attempt conversion to changeset operations
///
/// The function will not panic on invalid input - it simply returns early.
#[cfg(feature = "pg-walstream")]
pub fn test_pg_walstream(input: &[u8]) {
    use crate::SimpleTable;
    use crate::pg_walstream::EventType;
    use crate::{ChangeDelete, Insert};

    // Try to interpret as UTF-8 JSON
    let Ok(json_str) = core::str::from_utf8(input) else {
        return;
    };

    // Try parsing as EventType
    let Ok(event) = serde_json::from_str::<EventType>(json_str) else {
        return;
    };

    // Round-trip through serde
    if let Ok(json) = serde_json::to_string(&event) {
        let _: Result<EventType, _> = serde_json::from_str(&json);
    }

    // Try conversion to changeset operations based on event type
    match event {
        EventType::Insert {
            ref table,
            ref data,
            ..
        } => {
            if !data.is_empty() {
                let col_names: Vec<&str> = data.keys().map(String::as_str).collect();
                let schema = SimpleTable::new(table, &col_names, &[0]);
                // Clone event since we need ownership
                let event_clone = event.clone();
                let _: Result<Insert<_, String, Vec<u8>>, _> = (event_clone, schema).try_into();
            }
        }
        EventType::Delete {
            ref table,
            ref old_data,
            ..
        } => {
            if !old_data.is_empty() {
                let col_names: Vec<&str> = old_data.keys().map(String::as_str).collect();
                let schema = SimpleTable::new(table, &col_names, &[0]);
                let event_clone = event.clone();
                let _: Result<ChangeDelete<_, String, Vec<u8>>, _> =
                    (event_clone, schema).try_into();
            }
        }
        _ => {}
    }
}

/// Test `pg_walstream` `ChangeEvent` parsing and conversion from arbitrary bytes.
#[cfg(feature = "pg-walstream")]
pub fn test_pg_walstream_change_event(input: &[u8]) {
    use crate::SimpleTable;
    use crate::pg_walstream::{ChangeEvent, EventType};
    use crate::{ChangeDelete, Insert};

    // Try to interpret as UTF-8 JSON
    let Ok(json_str) = core::str::from_utf8(input) else {
        return;
    };

    // Try parsing as ChangeEvent
    let Ok(event) = serde_json::from_str::<ChangeEvent>(json_str) else {
        return;
    };

    // Round-trip through serde
    if let Ok(json) = serde_json::to_string(&event) {
        let _: Result<ChangeEvent, _> = serde_json::from_str(&json);
    }

    // Try conversion to changeset operations based on event type
    match &event.event_type {
        EventType::Insert { table, data, .. } => {
            if !data.is_empty() {
                let col_names: Vec<&str> = data.keys().map(String::as_str).collect();
                let schema = SimpleTable::new(table, &col_names, &[0]);
                let _: Result<Insert<_, String, Vec<u8>>, _> = (event, schema).try_into();
            }
        }
        EventType::Delete {
            table, old_data, ..
        } => {
            if !old_data.is_empty() {
                let col_names: Vec<&str> = old_data.keys().map(String::as_str).collect();
                let schema = SimpleTable::new(table, &col_names, &[0]);
                let _: Result<ChangeDelete<_, String, Vec<u8>>, _> = (event, schema).try_into();
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Debezium fuzz testing
// ---------------------------------------------------------------------------

/// Test Debezium envelope parsing and conversion from arbitrary bytes.
///
/// This function attempts to:
/// 1. Parse bytes as UTF-8 JSON
/// 2. Deserialize as Debezium `Envelope<serde_json::Value>`
/// 3. Serialize and verify round-trip
/// 4. Attempt conversion to changeset operations based on operation type
///
/// The function will not panic on invalid input - it simply returns early.
#[cfg(feature = "debezium")]
pub fn test_debezium(input: &[u8]) {
    use crate::SimpleTable;
    use crate::debezium::{Envelope, Op, parse};
    use crate::{ChangeDelete, ChangeUpdate, Insert};

    // Try to interpret as UTF-8 JSON
    let Ok(json_str) = core::str::from_utf8(input) else {
        return;
    };

    // Try parsing as Debezium Envelope
    let Ok(envelope) = parse::<serde_json::Value>(json_str) else {
        return;
    };

    // Round-trip through serde
    if let Ok(json) = serde_json::to_string(&envelope) {
        let _: Result<Envelope<serde_json::Value>, _> = serde_json::from_str(&json);
    }

    // Extract column names from the after or before data for schema building
    let col_names: Vec<String> = envelope
        .after
        .as_ref()
        .or(envelope.before.as_ref())
        .and_then(|v| v.as_object())
        .map(|obj| obj.keys().cloned().collect())
        .unwrap_or_default();

    if col_names.is_empty() {
        return;
    }

    // Get table name from source metadata
    let table_name = envelope.source.table.as_deref().unwrap_or("test_table");

    let col_refs: Vec<&str> = col_names.iter().map(String::as_str).collect();
    let table = SimpleTable::new(table_name, &col_refs, &[0]);

    // Try conversion based on operation type
    match envelope.op {
        Op::Create | Op::Read => {
            let _: Result<Insert<_, String, Vec<u8>>, _> = (&envelope, &table).try_into();
        }
        Op::Update => {
            let _: Result<ChangeUpdate<_, String, Vec<u8>>, _> = (&envelope, &table).try_into();
        }
        Op::Delete => {
            let _: Result<ChangeDelete<_, String, Vec<u8>>, _> = (&envelope, &table).try_into();
        }
        Op::Truncate | Op::Message => {
            // These operations don't have direct changeset equivalents
        }
    }
}

// ---------------------------------------------------------------------------
// Maxwell fuzz testing
// ---------------------------------------------------------------------------

/// Test Maxwell message parsing and conversion from arbitrary bytes.
///
/// This function attempts to:
/// 1. Parse bytes as UTF-8 JSON
/// 2. Deserialize as Maxwell `Message`
/// 3. Serialize and verify round-trip
/// 4. Attempt conversion to changeset operations based on operation type
///
/// The function will not panic on invalid input - it simply returns early.
#[cfg(feature = "maxwell")]
pub fn test_maxwell(input: &[u8]) {
    use crate::SimpleTable;
    use crate::maxwell::{Message, OpType, parse};
    use crate::{ChangeDelete, ChangeUpdate, Insert};

    // Try to interpret as UTF-8 JSON
    let Ok(json_str) = core::str::from_utf8(input) else {
        return;
    };

    // Try parsing as Maxwell Message
    let Ok(message) = parse(json_str) else {
        return;
    };

    // Round-trip through serde
    if let Ok(json) = serde_json::to_string(&message) {
        let _: Result<Message, _> = serde_json::from_str(&json);
    }

    // Extract column names from the data
    let col_names: Vec<String> = message.data.keys().cloned().collect();

    if col_names.is_empty() {
        return;
    }

    let col_refs: Vec<&str> = col_names.iter().map(String::as_str).collect();
    let table = SimpleTable::new(&message.table, &col_refs, &[0]);

    // Try conversion based on operation type
    match message.op_type {
        OpType::Insert => {
            let _: Result<Insert<_, String, Vec<u8>>, _> = (&message, &table).try_into();
        }
        OpType::Update => {
            let _: Result<ChangeUpdate<_, String, Vec<u8>>, _> = (&message, &table).try_into();
        }
        OpType::Delete => {
            let _: Result<ChangeDelete<_, String, Vec<u8>>, _> = (&message, &table).try_into();
        }
    }
}
