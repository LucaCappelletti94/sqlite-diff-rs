//! Submodule defining a builder for an insert operation.

use alloc::vec;
use alloc::vec::Vec;
use core::fmt::Debug;

use crate::{DynTable, encoding::Value};

#[derive(Debug, Clone)]
/// Builder for an insert operation.
pub struct Insert<T: DynTable, S: AsRef<str>, B: AsRef<[u8]>> {
    /// The table being inserted into.
    pub(crate) table: T,
    /// Values for the inserted row.
    pub(crate) values: Vec<Value<S, B>>,
}

impl<T: DynTable + PartialEq, S: PartialEq + AsRef<str>, B: PartialEq + AsRef<[u8]>> PartialEq for Insert<T, S, B> {
    fn eq(&self, other: &Self) -> bool {
        self.table == other.table && self.values == other.values
    }
}

impl<T: DynTable + Eq, S: Eq + AsRef<str>, B: Eq + AsRef<[u8]>> Eq for Insert<T, S, B> {}

impl<T: DynTable, S: Default + Clone + AsRef<str>, B: Default + Clone + AsRef<[u8]>> From<T> for Insert<T, S, B> {
    #[inline]
    fn from(table: T) -> Self {
        let num_cols = table.number_of_columns();
        Self {
            table,
            values: vec![Value::Null; num_cols],
        }
    }
}

impl<T: DynTable, S: AsRef<str>, B: AsRef<[u8]>> AsRef<T> for Insert<T, S, B> {
    #[inline]
    fn as_ref(&self) -> &T {
        &self.table
    }
}

impl<T: DynTable, S: AsRef<str>, B: AsRef<[u8]>> Insert<T, S, B> {
    /// Returns a reference to the values.
    #[inline]
    pub(crate) fn values(&self) -> &[Value<S, B>] {
        &self.values
    }

    /// Consumes self and returns the values.
    #[inline]
    pub(crate) fn into_values(self) -> Vec<Value<S, B>> {
        self.values
    }

    /// Sets the value for a specific column by index.
    ///
    /// # Arguments
    ///
    /// * `col_idx` - The index of the column to set.
    /// * `value` - The value to set for the column.
    ///
    /// # Errors
    ///
    /// * `ColumnIndexOutOfBounds` - If the provided column index is out of bounds for the table schema.
    ///
    pub fn set(
        mut self,
        col_idx: usize,
        value: impl Into<Value<S, B>>,
    ) -> Result<Self, crate::errors::Error> {
        if col_idx >= self.values.len() {
            return Err(crate::errors::Error::ColumnIndexOutOfBounds(
                col_idx,
                self.values.len(),
            ));
        }

        self.values[col_idx] = value.into();
        Ok(self)
    }

    /// Sets a column to NULL.
    ///
    /// This is a convenience method equivalent to `.set(col_idx, Value::Null)`.
    ///
    /// # Errors
    ///
    /// * `ColumnIndexOutOfBounds` - If the provided column index is out of bounds for the table schema.
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_diff_rs::{Insert, TableSchema};
    ///
    /// // CREATE TABLE items (id INTEGER PRIMARY KEY, description TEXT, price REAL)
    /// let schema: TableSchema<String> = TableSchema::new("items".into(), 3, vec![1, 0, 0]);
    ///
    /// // INSERT INTO items (id, description, price) VALUES (1, NULL, 9.99)
    /// let insert = Insert::<_, String, Vec<u8>>::from(schema)
    ///     .set(0, 1i64).unwrap()
    ///     .set_null(1).unwrap()    // description = NULL
    ///     .set(2, 9.99f64).unwrap();
    /// ```
    pub fn set_null(self, col_idx: usize) -> Result<Self, crate::errors::Error>
    where
        S: Default,
        B: Default,
    {
        self.set(col_idx, Value::Null)
    }
}
