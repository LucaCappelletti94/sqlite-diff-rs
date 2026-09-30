//! Simple table schema for SQL-based operations.
//!
//! [`SimpleTable`] is a schema type that can be created from SQL
//! `CREATE TABLE` statements and used to generate SQL INSERT, UPDATE, and
//! DELETE statements.

use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use core::hash::{Hash, Hasher};

use crate::parser::TableSchema;
use crate::{Affinity, encoding::Value, schema::dyn_table::IndexableValues};

use super::{DynTable, SchemaWithPK};

/// A simple table schema with column names for SQL generation.
///
/// This type wraps [`TableSchema`] and adds column names, allowing it to be
/// used for both binary encoding/decoding and SQL statement digestion.
///
/// # Example
///
/// ```rust
/// use sqlite_diff_rs::SimpleTable;
/// use sqlite_diff_rs::{PatchSet, DiffSetBuilder};
///
/// let table = SimpleTable::with_rowid_alias("users", &[("id", "INTEGER"), ("name", "TEXT")], 0);
/// let mut patchset = PatchSet::<SimpleTable, String, Vec<u8>>::new();
/// patchset.add_table(&table);
/// patchset.digest_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
/// ```
#[derive(Debug, Clone, Eq)]
pub struct SimpleTable {
    /// The underlying table schema (for binary encoding).
    schema: TableSchema<String>,
    /// Column names in order.
    columns: Vec<String>,
    /// Column affinities in order, from the declared types.
    affinities: Vec<Affinity>,
    /// The key column that aliases the rowid, if any.
    rowid_alias: Option<usize>,
}

impl SimpleTable {
    /// Create a table schema without a rowid alias.
    ///
    /// This models every table whose primary key is not a lone
    /// `INTEGER PRIMARY KEY` column of a rowid table, including a
    /// `WITHOUT ROWID` table. Use [`SimpleTable::with_rowid_alias`] for that case.
    ///
    /// # Arguments
    ///
    /// * `name` - the table name.
    /// * `columns` - `(name, declared type)` pairs in order, with `""` for an untyped column.
    /// * `pk_indices` - indices of primary key columns (in PK order).
    ///
    /// # Panics
    ///
    /// Panics if any `pk_indices` value is out of bounds.
    #[must_use]
    pub fn new(name: impl Into<String>, columns: &[(&str, &str)], pk_indices: &[usize]) -> Self {
        let name = name.into();
        let column_count = columns.len();

        // Convert pk_indices to pk_flags
        let mut pk_flags = vec![0u8; column_count];
        for (pk_ordinal, &col_idx) in pk_indices.iter().enumerate() {
            assert!(col_idx < column_count, "PK index out of bounds");
            pk_flags[col_idx] = u8::try_from(pk_ordinal + 1).expect("Too many PK columns");
        }

        Self {
            schema: TableSchema::new(name, column_count, pk_flags),
            columns: columns.iter().map(|&(c, _)| String::from(c)).collect(),
            affinities: columns
                .iter()
                .map(|&(_, declared)| Affinity::from_declared_type(declared))
                .collect(),
            rowid_alias: None,
        }
    }

    /// Create a rowid table whose primary key is the single column
    /// `key_column`, declared `INTEGER PRIMARY KEY`, which aliases the rowid.
    ///
    /// SQLite stores only integers in such a column and refuses other values.
    ///
    /// # Panics
    ///
    /// Panics if `key_column` is out of bounds or its declared type is not `INTEGER`.
    #[must_use]
    pub fn with_rowid_alias(
        name: impl Into<String>,
        columns: &[(&str, &str)],
        key_column: usize,
    ) -> Self {
        assert!(
            columns
                .get(key_column)
                .is_some_and(|&(_, declared)| declared.eq_ignore_ascii_case("INTEGER")),
            "a rowid alias column must be declared INTEGER"
        );
        let mut table = Self::new(name, columns, &[key_column]);
        table.rowid_alias = Some(key_column);
        table
    }

    /// Get the column names.
    #[must_use]
    pub fn column_names(&self) -> &[String] {
        &self.columns
    }

    /// Get a column name by index.
    #[must_use]
    pub fn column_name(&self, index: usize) -> Option<&str> {
        self.columns.get(index).map(String::as_str)
    }

    /// Get the column index by name.
    #[must_use]
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c == name)
    }

    /// The affinity of the column at `index`, or `None` past the last column.
    #[must_use]
    pub fn column_affinity(&self, index: usize) -> Option<Affinity> {
        self.affinities.get(index).copied()
    }

    /// The key column that aliases the rowid, if the table has one.
    #[must_use]
    pub fn rowid_alias(&self) -> Option<usize> {
        self.rowid_alias
    }

    /// Get the inner `TableSchema`.
    #[must_use]
    pub fn inner(&self) -> &TableSchema<String> {
        &self.schema
    }
}

impl PartialEq for SimpleTable {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema && self.columns == other.columns
    }
}

impl Hash for SimpleTable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.columns.hash(state);
    }
}

impl DynTable for SimpleTable {
    #[inline]
    fn name(&self) -> &str {
        self.schema.name()
    }

    #[inline]
    fn number_of_columns(&self) -> usize {
        self.schema.number_of_columns()
    }

    #[inline]
    fn write_pk_flags(&self, buf: &mut [u8]) {
        self.schema.write_pk_flags(buf);
    }
}

impl SchemaWithPK for SimpleTable {
    fn number_of_primary_keys(&self) -> usize {
        self.schema.number_of_primary_keys()
    }

    fn primary_key_index(&self, col_idx: usize) -> Option<usize> {
        self.schema.primary_key_index(col_idx)
    }

    fn extract_pk<S, B>(
        &self,
        values: &impl IndexableValues<Text = S, Binary = B>,
    ) -> alloc::vec::Vec<Value<S, B>>
    where
        S: Clone,
        B: Clone,
    {
        self.schema.extract_pk(values)
    }
}

/// Defines a schema in which the mapping of column names to
/// column positions is known at runtime.
pub trait NamedColumns: SchemaWithPK {
    /// Get the column index for a given column name.
    ///
    /// Returns `Some(index)` if the column exists, or `None` if it doesn't.
    fn column_index(&self, column_name: &str) -> Option<usize>;

    /// The affinity SQLite derives from the declared type of the column at
    /// `column_index`, or `None` past the last column.
    fn column_affinity(&self, column_index: usize) -> Option<Affinity>;

    /// The key column declared `INTEGER PRIMARY KEY` in a rowid table, which
    /// aliases the rowid and stores only integers, if the table has one.
    fn rowid_alias(&self) -> Option<usize>;
}

impl NamedColumns for SimpleTable {
    #[inline]
    fn column_index(&self, column_name: &str) -> Option<usize> {
        self.column_index(column_name)
    }

    #[inline]
    fn column_affinity(&self, column_index: usize) -> Option<Affinity> {
        self.column_affinity(column_index)
    }

    #[inline]
    fn rowid_alias(&self) -> Option<usize> {
        self.rowid_alias
    }
}

impl<T: NamedColumns> NamedColumns for &T {
    #[inline]
    fn column_index(&self, column_name: &str) -> Option<usize> {
        T::column_index(self, column_name)
    }

    #[inline]
    fn column_affinity(&self, column_index: usize) -> Option<Affinity> {
        T::column_affinity(self, column_index)
    }

    #[inline]
    fn rowid_alias(&self) -> Option<usize> {
        T::rowid_alias(self)
    }
}

#[cfg(test)]
mod tests {
    use super::{NamedColumns, SimpleTable};
    use crate::Affinity;
    use core::hash::BuildHasher;

    #[test]
    fn column_affinity_follows_declared_types() {
        let table = SimpleTable::new(
            "t",
            &[
                ("a", "VARCHAR(10)"),
                ("b", ""),
                ("c", "BIGINT"),
                ("d", "DOUBLE"),
                ("e", "DATE"),
            ],
            &[0],
        );
        let affinities: [Option<Affinity>; 6] =
            core::array::from_fn(|i| NamedColumns::column_affinity(&table, i));
        assert_eq!(
            affinities,
            [
                Some(Affinity::Text),
                Some(Affinity::Blob),
                Some(Affinity::Integer),
                Some(Affinity::Real),
                Some(Affinity::Numeric),
                None,
            ]
        );
    }

    #[test]
    fn rowid_alias_only_when_stated() {
        let columns = [("id", "INTEGER"), ("v", "")];
        assert_eq!(
            SimpleTable::with_rowid_alias("t", &columns, 0).rowid_alias(),
            Some(0)
        );
        assert_eq!(SimpleTable::new("t", &columns, &[0]).rowid_alias(), None);
    }

    #[test]
    #[should_panic(expected = "a rowid alias column must be declared INTEGER")]
    fn rowid_alias_requires_integer_declaration() {
        let _ = SimpleTable::with_rowid_alias("t", &[("id", "BIGINT")], 0);
    }

    #[test]
    fn identity_ignores_declared_types() {
        let typed = SimpleTable::with_rowid_alias("t", &[("id", "INTEGER"), ("v", "TEXT")], 0);
        let untyped = SimpleTable::new("t", &[("id", ""), ("v", "")], &[0]);
        let hasher = hashbrown::DefaultHashBuilder::default();
        assert_eq!(typed, untyped);
        assert_eq!(hasher.hash_one(&typed), hasher.hash_one(&untyped));
    }
}
