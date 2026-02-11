//! Simple table schema for SQL-based operations.
//!
//! This module provides [`SimpleTable`], a schema type that can be created from
//! SQL `CREATE TABLE` statements and used for generating SQL INSERT, UPDATE,
//! and DELETE statements.

use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use core::hash::{Hash, Hasher};

use crate::parser::TableSchema;
use crate::{encoding::Value, schema::dyn_table::IndexableValues};

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
/// let table = SimpleTable::new("users", &["id", "name"], &[0]);
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
}

impl SimpleTable {
    /// Create a new simple table schema.
    ///
    /// # Arguments
    ///
    /// * `name` - The table name.
    /// * `columns` - The column names in order.
    /// * `pk_indices` - Indices of primary key columns (in PK order).
    ///
    /// # Panics
    ///
    /// Panics if any `pk_indices` value is out of bounds.
    #[must_use]
    pub fn new(name: impl Into<String>, columns: &[&str], pk_indices: &[usize]) -> Self {
        let name = name.into();
        let columns: Vec<String> = columns.iter().map(|&c| String::from(c)).collect();
        let column_count = columns.len();

        // Convert pk_indices to pk_flags
        let mut pk_flags = vec![0u8; column_count];
        for (pk_ordinal, &col_idx) in pk_indices.iter().enumerate() {
            assert!(col_idx < column_count, "PK index out of bounds");
            pk_flags[col_idx] = u8::try_from(pk_ordinal + 1).expect("Too many PK columns");
        }

        Self {
            schema: TableSchema::new(name, column_count, pk_flags),
            columns,
        }
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

    /// Get the indices of primary key columns, in PK order.
    #[must_use]
    pub fn pk_indices(&self) -> Vec<usize> {
        self.schema.pk_indices()
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
}

impl NamedColumns for SimpleTable {
    #[inline]
    fn column_index(&self, column_name: &str) -> Option<usize> {
        self.column_index(column_name)
    }
}

impl<T: NamedColumns> NamedColumns for &T {
    #[inline]
    fn column_index(&self, column_name: &str) -> Option<usize> {
        T::column_index(self, column_name)
    }
}
