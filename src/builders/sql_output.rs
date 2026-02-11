//! SQL statement generation from changesets and patchsets.
//!
//! This module provides methods to convert changeset/patchset operations
//! back into SQL statements, useful for debugging, logging, or applying
//! changes to non-SQLite databases.
//!
//! # Example
//!
//! ```rust
//! use sqlite_diff_rs::{SimpleTable, PatchSet, DiffOps, Insert};
//!
//! let table = SimpleTable::new("users", &["id", "name"], &[0]);
//! let insert = Insert::from(table.clone())
//!     .set(0, 1i64).unwrap()
//!     .set(1, "Alice").unwrap();
//!
//! let patchset = PatchSet::<SimpleTable, String, Vec<u8>>::new().insert(insert);
//!
//! for sql in patchset.sql_statements() {
//!     println!("{}", sql);
//!     // Prints: INSERT INTO "users" ("id", "name") VALUES (1, 'Alice')
//! }
//! ```

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt::{Debug, Write};
use core::hash::Hash;

use crate::builders::operation::Operation;
use crate::builders::{ChangesetFormat, DiffSetBuilder, PatchsetFormat};
use crate::encoding::{MaybeValue, Value};
use crate::schema::NamedColumns;

/// Type alias for changeset update pairs: (old_value, new_value) for each column.
type ChangesetUpdatePairs<S, B> = [(MaybeValue<S, B>, MaybeValue<S, B>)];

/// Quote a SQL identifier (table or column name) with double quotes.
///
/// Escapes any embedded double quotes by doubling them.
fn quote_identifier(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    out.push('"');
    for c in name.chars() {
        if c == '"' {
            out.push_str("\"\"");
        } else {
            out.push(c);
        }
    }
    out.push('"');
    out
}

/// Trait for tables that can provide column names by index.
///
/// This extends [`NamedColumns`] to support SQL generation.
pub trait ColumnNames: NamedColumns {
    /// Get the column name for a given index.
    ///
    /// Returns `None` if the index is out of bounds.
    fn column_name(&self, index: usize) -> Option<&str>;

    /// Get the column indices that are part of the primary key, in PK order.
    fn pk_indices(&self) -> Vec<usize> {
        // Build a list of (pk_ordinal, col_idx) pairs, then sort by pk_ordinal
        let mut pk_cols: Vec<(usize, usize)> = Vec::new();
        for col_idx in 0..self.number_of_columns() {
            if let Some(pk_ordinal) = self.primary_key_index(col_idx) {
                pk_cols.push((pk_ordinal, col_idx));
            }
        }
        pk_cols.sort_by_key(|(ordinal, _)| *ordinal);
        pk_cols.into_iter().map(|(_, col_idx)| col_idx).collect()
    }
}

impl ColumnNames for crate::SimpleTable {
    fn column_name(&self, index: usize) -> Option<&str> {
        self.column_name(index)
    }

    fn pk_indices(&self) -> Vec<usize> {
        // SimpleTable already has this method
        crate::SimpleTable::pk_indices(self)
    }
}

/// Format an INSERT statement with column names.
fn format_insert<T: ColumnNames, S: AsRef<str>, B: AsRef<[u8]>>(
    table: &T,
    values: &[Value<S, B>],
) -> String {
    let mut sql = String::new();
    write!(sql, "INSERT INTO {}", quote_identifier(table.name())).unwrap();

    // Column names
    sql.push_str(" (");
    for i in 0..table.number_of_columns() {
        if i > 0 {
            sql.push_str(", ");
        }
        if let Some(name) = table.column_name(i) {
            sql.push_str(&quote_identifier(name));
        } else {
            write!(sql, "\"col{i}\"").unwrap();
        }
    }
    sql.push_str(") VALUES (");

    // Values
    for (i, val) in values.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        write!(sql, "{val}").unwrap();
    }
    sql.push(')');
    sql
}

/// Format a DELETE statement (changeset format - has all old values).
fn format_delete_changeset<T: ColumnNames, S: AsRef<str>, B: AsRef<[u8]>>(
    table: &T,
    values: &[Value<S, B>],
) -> String {
    let mut sql = String::new();
    write!(sql, "DELETE FROM {}", quote_identifier(table.name())).unwrap();
    sql.push_str(" WHERE ");

    // Use PK columns for the WHERE clause
    let mut first = true;
    for (col_idx, value) in values.iter().enumerate() {
        if table.primary_key_index(col_idx).is_some() {
            if !first {
                sql.push_str(" AND ");
            }
            first = false;
            if let Some(name) = table.column_name(col_idx) {
                sql.push_str(&quote_identifier(name));
            } else {
                write!(sql, "\"col{col_idx}\"").unwrap();
            }
            sql.push_str(" = ");
            write!(sql, "{value}").unwrap();
        }
    }
    sql
}

/// Format a DELETE statement (patchset format - PK only).
fn format_delete_patchset<T: ColumnNames, S: AsRef<str>, B: AsRef<[u8]>>(
    table: &T,
    pk: &[Value<S, B>],
) -> String {
    let mut sql = String::new();
    write!(sql, "DELETE FROM {}", quote_identifier(table.name())).unwrap();
    sql.push_str(" WHERE ");

    // Get PK column indices in order
    let pk_indices = table.pk_indices();

    let mut first = true;
    for (pk_ordinal, &col_idx) in pk_indices.iter().enumerate() {
        if !first {
            sql.push_str(" AND ");
        }
        first = false;
        if let Some(name) = table.column_name(col_idx) {
            sql.push_str(&quote_identifier(name));
        } else {
            write!(sql, "\"col{col_idx}\"").unwrap();
        }
        sql.push_str(" = ");
        write!(sql, "{}", &pk[pk_ordinal]).unwrap();
    }
    sql
}

/// Format an UPDATE statement (changeset format).
fn format_update_changeset<T: ColumnNames, S: AsRef<str>, B: AsRef<[u8]>>(
    table: &T,
    pairs: &ChangesetUpdatePairs<S, B>,
) -> String {
    let mut sql = String::new();
    write!(sql, "UPDATE {}", quote_identifier(table.name())).unwrap();
    sql.push_str(" SET ");

    // SET clause: columns that changed (have new values)
    let mut first_set = true;
    for (col_idx, (_old, new)) in pairs.iter().enumerate() {
        if let Some(new_val) = new {
            // Skip PK columns in SET (they go in WHERE)
            if table.primary_key_index(col_idx).is_some() {
                continue;
            }
            if !first_set {
                sql.push_str(", ");
            }
            first_set = false;
            if let Some(name) = table.column_name(col_idx) {
                sql.push_str(&quote_identifier(name));
            } else {
                write!(sql, "\"col{col_idx}\"").unwrap();
            }
            sql.push_str(" = ");
            write!(sql, "{new_val}").unwrap();
        }
    }

    // WHERE clause: use old PK values
    sql.push_str(" WHERE ");
    let mut first_where = true;
    for (col_idx, (old, _new)) in pairs.iter().enumerate() {
        if table.primary_key_index(col_idx).is_some() {
            if let Some(old_val) = old {
                if !first_where {
                    sql.push_str(" AND ");
                }
                first_where = false;
                if let Some(name) = table.column_name(col_idx) {
                    sql.push_str(&quote_identifier(name));
                } else {
                    write!(sql, "\"col{col_idx}\"").unwrap();
                }
                sql.push_str(" = ");
                write!(sql, "{old_val}").unwrap();
            }
        }
    }
    sql
}

/// Format an UPDATE statement (patchset format).
fn format_update_patchset<T: ColumnNames, S: AsRef<str>, B: AsRef<[u8]>>(
    table: &T,
    pk: &[Value<S, B>],
    pairs: &[((), MaybeValue<S, B>)],
) -> String {
    let mut sql = String::new();
    write!(sql, "UPDATE {}", quote_identifier(table.name())).unwrap();
    sql.push_str(" SET ");

    // SET clause: columns that changed (have new values), excluding PK
    let mut first_set = true;
    for (col_idx, ((), new)) in pairs.iter().enumerate() {
        if let Some(new_val) = new {
            // Skip PK columns in SET
            if table.primary_key_index(col_idx).is_some() {
                continue;
            }
            if !first_set {
                sql.push_str(", ");
            }
            first_set = false;
            if let Some(name) = table.column_name(col_idx) {
                sql.push_str(&quote_identifier(name));
            } else {
                write!(sql, "\"col{col_idx}\"").unwrap();
            }
            sql.push_str(" = ");
            write!(sql, "{new_val}").unwrap();
        }
    }

    // WHERE clause: use PK values
    sql.push_str(" WHERE ");
    let pk_indices = table.pk_indices();

    let mut first_where = true;
    for (pk_ordinal, &col_idx) in pk_indices.iter().enumerate() {
        if !first_where {
            sql.push_str(" AND ");
        }
        first_where = false;
        if let Some(name) = table.column_name(col_idx) {
            sql.push_str(&quote_identifier(name));
        } else {
            write!(sql, "\"col{col_idx}\"").unwrap();
        }
        sql.push_str(" = ");
        write!(sql, "{}", &pk[pk_ordinal]).unwrap();
    }
    sql
}

// ============================================================================
// DiffSetBuilder::sql_statements() implementations
// ============================================================================

impl<
    T: ColumnNames,
    S: AsRef<str> + Clone + Debug + Hash + Eq,
    B: AsRef<[u8]> + Clone + Debug + Hash + Eq,
> DiffSetBuilder<ChangesetFormat, T, S, B>
{
    /// Iterate over operations as SQL statements.
    ///
    /// Each operation is converted to an INSERT, UPDATE, or DELETE statement.
    /// Identifiers (table and column names) are quoted with double quotes.
    /// Statements do not include trailing semicolons.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlite_diff_rs::{SimpleTable, ChangeSet, DiffOps, Insert};
    ///
    /// let table = SimpleTable::new("users", &["id", "name"], &[0]);
    /// let insert = Insert::from(table.clone())
    ///     .set(0, 1i64).unwrap()
    ///     .set(1, "Alice").unwrap();
    ///
    /// let changeset = ChangeSet::<SimpleTable, String, Vec<u8>>::new().insert(insert);
    ///
    /// for sql in changeset.sql_statements() {
    ///     assert_eq!(sql, r#"INSERT INTO "users" ("id", "name") VALUES (1, 'Alice')"#);
    /// }
    /// ```
    pub fn sql_statements(&self) -> impl Iterator<Item = String> + '_ {
        self.tables.iter().flat_map(|(table, rows)| {
            rows.values().map(move |op| match op {
                Operation::Insert(values) => format_insert(table, values),
                Operation::Delete(values) => format_delete_changeset(table, values),
                Operation::Update(pairs) => format_update_changeset(table, pairs),
            })
        })
    }
}

impl<T: ColumnNames, S: AsRef<str> + Clone + Hash + Eq, B: AsRef<[u8]> + Clone + Hash + Eq>
    DiffSetBuilder<PatchsetFormat, T, S, B>
{
    /// Iterate over operations as SQL statements.
    ///
    /// Each operation is converted to an INSERT, UPDATE, or DELETE statement.
    /// Identifiers (table and column names) are quoted with double quotes.
    /// Statements do not include trailing semicolons.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlite_diff_rs::{SimpleTable, PatchSet, DiffOps, Insert};
    ///
    /// let table = SimpleTable::new("users", &["id", "name"], &[0]);
    /// let insert = Insert::from(table.clone())
    ///     .set(0, 1i64).unwrap()
    ///     .set(1, "Alice").unwrap();
    ///
    /// let patchset = PatchSet::<SimpleTable, String, Vec<u8>>::new().insert(insert);
    ///
    /// for sql in patchset.sql_statements() {
    ///     assert_eq!(sql, r#"INSERT INTO "users" ("id", "name") VALUES (1, 'Alice')"#);
    /// }
    /// ```
    pub fn sql_statements(&self) -> impl Iterator<Item = String> + '_ {
        self.tables.iter().flat_map(|(table, rows)| {
            rows.iter().map(move |(pk, op)| match op {
                Operation::Insert(values) => format_insert(table, values),
                Operation::Delete(()) => format_delete_patchset(table, pk),
                Operation::Update(pairs) => format_update_patchset(table, pk, pairs),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ChangeDelete, ChangeSet, DiffOps, Insert, PatchDelete, PatchSet, SimpleTable, Update,
    };

    #[test]
    fn test_quote_identifier_simple() {
        assert_eq!(quote_identifier("users"), r#""users""#);
    }

    #[test]
    fn test_quote_identifier_with_quotes() {
        assert_eq!(quote_identifier(r#"user"name"#), r#""user""name""#);
    }

    #[test]
    fn test_changeset_insert_sql() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "Alice")
            .unwrap();

        let cs = ChangeSet::<SimpleTable, String, Vec<u8>>::new().insert(insert);
        let stmts: Vec<_> = cs.sql_statements().collect();

        assert_eq!(stmts.len(), 1);
        assert_eq!(
            stmts[0],
            r#"INSERT INTO "users" ("id", "name") VALUES (1, 'Alice')"#
        );
    }

    #[test]
    fn test_patchset_insert_sql() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "Alice")
            .unwrap();

        let ps = PatchSet::<SimpleTable, String, Vec<u8>>::new().insert(insert);
        let stmts: Vec<_> = ps.sql_statements().collect();

        assert_eq!(stmts.len(), 1);
        assert_eq!(
            stmts[0],
            r#"INSERT INTO "users" ("id", "name") VALUES (1, 'Alice')"#
        );
    }

    #[test]
    fn test_changeset_delete_sql() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let delete = ChangeDelete::from(table.clone())
            .set(0, 42i64)
            .unwrap()
            .set(1, "Bob")
            .unwrap();

        let cs = ChangeSet::<SimpleTable, String, Vec<u8>>::new().delete(delete);
        let stmts: Vec<_> = cs.sql_statements().collect();

        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], r#"DELETE FROM "users" WHERE "id" = 42"#);
    }

    #[test]
    fn test_patchset_delete_sql() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let delete = PatchDelete::new(table.clone(), alloc::vec![Value::Integer(42)]);

        let ps = PatchSet::<SimpleTable, String, Vec<u8>>::new().delete(delete);
        let stmts: Vec<_> = ps.sql_statements().collect();

        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], r#"DELETE FROM "users" WHERE "id" = 42"#);
    }

    #[test]
    fn test_changeset_update_sql() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let update = Update::<SimpleTable, ChangesetFormat, String, Vec<u8>>::from(table.clone())
            .set(0, 1i64, 1i64)
            .unwrap()
            .set(1, "Alice", "Alicia")
            .unwrap();

        let cs = ChangeSet::<SimpleTable, String, Vec<u8>>::new().update(update);
        let stmts: Vec<_> = cs.sql_statements().collect();

        assert_eq!(stmts.len(), 1);
        assert_eq!(
            stmts[0],
            r#"UPDATE "users" SET "name" = 'Alicia' WHERE "id" = 1"#
        );
    }

    #[test]
    fn test_patchset_update_sql() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let update = Update::<SimpleTable, PatchsetFormat, String, Vec<u8>>::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "Alicia")
            .unwrap();

        let ps = PatchSet::<SimpleTable, String, Vec<u8>>::new().update(update);
        let stmts: Vec<_> = ps.sql_statements().collect();

        assert_eq!(stmts.len(), 1);
        assert_eq!(
            stmts[0],
            r#"UPDATE "users" SET "name" = 'Alicia' WHERE "id" = 1"#
        );
    }

    #[test]
    fn test_sql_escapes_quotes_in_strings() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "O'Brien")
            .unwrap();

        let cs = ChangeSet::<SimpleTable, String, Vec<u8>>::new().insert(insert);
        let stmts: Vec<_> = cs.sql_statements().collect();

        assert_eq!(
            stmts[0],
            r#"INSERT INTO "users" ("id", "name") VALUES (1, 'O''Brien')"#
        );
    }

    #[test]
    fn test_sql_escapes_quotes_in_identifiers() {
        let table = SimpleTable::new(r#"user"table"#, &["id", r#"user"name"#], &[0]);
        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "Alice")
            .unwrap();

        let cs = ChangeSet::<SimpleTable, String, Vec<u8>>::new().insert(insert);
        let stmts: Vec<_> = cs.sql_statements().collect();

        assert_eq!(
            stmts[0],
            r#"INSERT INTO "user""table" ("id", "user""name") VALUES (1, 'Alice')"#
        );
    }

    #[test]
    fn test_multiple_operations() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let insert1 = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "Alice")
            .unwrap();

        let insert2 = Insert::from(table.clone())
            .set(0, 2i64)
            .unwrap()
            .set(1, "Bob")
            .unwrap();

        let cs = ChangeSet::<SimpleTable, String, Vec<u8>>::new()
            .insert(insert1)
            .insert(insert2);

        let stmts: Vec<_> = cs.sql_statements().collect();
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_composite_pk_delete() {
        let table = SimpleTable::new("order_items", &["order_id", "item_id", "qty"], &[0, 1]);
        let delete = ChangeDelete::from(table.clone())
            .set(0, 100i64)
            .unwrap()
            .set(1, 5i64)
            .unwrap()
            .set(2, 10i64)
            .unwrap();

        let cs = ChangeSet::<SimpleTable, String, Vec<u8>>::new().delete(delete);
        let stmts: Vec<_> = cs.sql_statements().collect();

        assert_eq!(
            stmts[0],
            r#"DELETE FROM "order_items" WHERE "order_id" = 100 AND "item_id" = 5"#
        );
    }
}
