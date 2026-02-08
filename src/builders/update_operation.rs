//! Submodule defining a builder for an update operation.

use alloc::vec;
use alloc::vec::Vec;
use core::fmt::Debug;

use crate::{
    DynTable,
    builders::{ChangesetFormat, PatchsetFormat, format::Format},
    encoding::{MaybeValue, Value},
};

#[derive(Debug, Clone)]
/// Builder for an update operation, parameterized by the format type `F` and value types `S`, `B`.
pub struct Update<T: DynTable, F: Format<S, B>, S: AsRef<str>, B: AsRef<[u8]>> {
    /// The table being updated.
    pub(crate) table: T,
    /// Values for the updated row, stored as pairs of (old, new) values.
    /// New values use `MaybeValue<S, B>` (Option<Value<S, B>>) where `None` = undefined/unchanged.
    pub(crate) values: Vec<(F::Old, MaybeValue<S, B>)>,
}

impl<T: DynTable + PartialEq, F: Format<S, B>, S: PartialEq + AsRef<str>, B: PartialEq + AsRef<[u8]>> PartialEq for Update<T, F, S, B>
where
    F::Old: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.table == other.table && self.values == other.values
    }
}

impl<T: DynTable + Eq, F: Format<S, B>, S: Eq + AsRef<str>, B: Eq + AsRef<[u8]>> Eq for Update<T, F, S, B>
where
    F::Old: Eq,
{}

impl<T: DynTable, F: Format<S, B>, S: AsRef<str>, B: AsRef<[u8]>> From<Update<T, F, S, B>> for Vec<(F::Old, MaybeValue<S, B>)> {
    #[inline]
    fn from(update: Update<T, F, S, B>) -> Self {
        update.values
    }
}

impl<T: DynTable, F: Format<S, B>, S: AsRef<str>, B: AsRef<[u8]>> AsRef<T> for Update<T, F, S, B> {
    #[inline]
    fn as_ref(&self) -> &T {
        &self.table
    }
}

impl<T: DynTable, F: Format<S, B>, S: AsRef<str>, B: AsRef<[u8]>> Update<T, F, S, B> {
    /// Returns a reference to the (old, new) value pairs.
    #[inline]
    pub fn values(&self) -> &[(F::Old, MaybeValue<S, B>)] {
        &self.values
    }
}

impl<T: DynTable, F: Format<S, B>, S: Clone + AsRef<str>, B: Clone + AsRef<[u8]>> From<T> for Update<T, F, S, B>
where
    F::Old: Clone,
{
    #[inline]
    fn from(table: T) -> Self {
        let num_cols = table.number_of_columns();
        Self {
            table,
            values: vec![(F::Old::default(), None); num_cols],
        }
    }
}

impl<T: DynTable, S: Clone + Debug + AsRef<str>, B: Clone + Debug + AsRef<[u8]>> Update<T, ChangesetFormat, S, B> {
    /// Sets the value for a specific column by index.
    ///
    /// # Arguments
    ///
    /// * `col_idx` - The index of the column to set.
    /// * `old` - The old value for the column.
    /// * `new` - The new value for the column.
    ///
    /// # Errors
    ///
    /// * `ColumnIndexOutOfBounds` - If the provided column index is out of bounds for the table schema.
    ///
    pub fn set(
        mut self,
        col_idx: usize,
        old: impl Into<Value<S, B>>,
        new: impl Into<Value<S, B>>,
    ) -> Result<Self, crate::errors::Error> {
        if col_idx >= self.values.len() {
            return Err(crate::errors::Error::ColumnIndexOutOfBounds(
                col_idx,
                self.values.len(),
            ));
        }

        self.values[col_idx] = (Some(old.into()), Some(new.into()));
        Ok(self)
    }

    /// Sets only the new value for a column, leaving old as undefined.
    ///
    /// This is useful when the old value is not known (e.g., when parsing SQL
    /// UPDATE statements where only the new value is specified).
    ///
    /// # Arguments
    ///
    /// * `col_idx` - The index of the column to set.
    /// * `new` - The new value for the column.
    ///
    /// # Errors
    ///
    /// * `ColumnIndexOutOfBounds` - If the column index is out of bounds.
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_diff_rs::{Update, ChangesetFormat, TableSchema};
    ///
    /// // CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)
    /// let schema: TableSchema<String> = TableSchema::new("users".into(), 3, vec![1, 0, 0]);
    ///
    /// // UPDATE users SET name = 'Bob' WHERE id = 1
    /// // We know id=1 (PK, unchanged) and name='Bob' (new), but not the old name
    /// let update = Update::<_, ChangesetFormat, String, Vec<u8>>::from(schema)
    ///     .set(0, 1i64, 1i64).unwrap()      // PK: old=1, new=1 (unchanged)
    ///     .set_new(1, "Bob").unwrap();      // name: old=undefined, new="Bob"
    /// ```
    pub fn set_new(
        mut self,
        col_idx: usize,
        new: impl Into<Value<S, B>>,
    ) -> Result<Self, crate::errors::Error> {
        if col_idx >= self.values.len() {
            return Err(crate::errors::Error::ColumnIndexOutOfBounds(
                col_idx,
                self.values.len(),
            ));
        }

        self.values[col_idx] = (None, Some(new.into()));
        Ok(self)
    }

    /// Sets a column to NULL for both old and new values.
    ///
    /// # Errors
    ///
    /// * `ColumnIndexOutOfBounds` - If the column index is out of bounds for the table schema.
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_diff_rs::{Update, ChangesetFormat, TableSchema};
    ///
    /// // CREATE TABLE items (id INTEGER PRIMARY KEY, description TEXT)
    /// let schema: TableSchema<String> = TableSchema::new("items".into(), 2, vec![1, 0]);
    ///
    /// // UPDATE items SET description = NULL WHERE id = 1 AND description = NULL
    /// let update = Update::<_, ChangesetFormat, String, Vec<u8>>::from(schema)
    ///     .set(0, 1i64, 1i64).unwrap()
    ///     .set_null(1).unwrap();
    /// ```
    #[inline]
    pub fn set_null(mut self, col_idx: usize) -> Result<Self, crate::errors::Error>
    where
        S: Default,
        B: Default,
    {
        if col_idx >= self.values.len() {
            return Err(crate::errors::Error::ColumnIndexOutOfBounds(
                col_idx,
                self.values.len(),
            ));
        }
        self.values[col_idx] = (Some(Value::Null), Some(Value::Null));
        Ok(self)
    }
}

impl<T: DynTable, S: AsRef<str>, B: AsRef<[u8]>> Update<T, PatchsetFormat, S, B> {
    /// Returns the new values.
    ///
    /// This is useful for extracting the primary key values for patchset operations,
    /// where the PK values are stored in the new values.
    #[inline]
    pub(crate) fn new_values(&self) -> Vec<MaybeValue<S, B>>
    where
        S: Clone,
        B: Clone,
    {
        self.values.iter().map(|((), new)| new.clone()).collect()
    }

    /// Sets the value for a specific column by index.
    ///
    /// # Implementation Note
    ///
    /// In the patchset format, the old value is not stored for updates,
    /// so we set it to the default value of `()`. Only the new value is stored.
    ///
    /// # Arguments
    ///
    /// * `col_idx` - The index of the column to set.
    /// * `new` - The new value for the column.
    ///
    /// # Errors
    ///
    /// * `ColumnIndexOutOfBounds` - If the provided column index is out of bounds for the table schema.
    ///
    pub fn set(
        mut self,
        col_idx: usize,
        new: impl Into<Value<S, B>>,
    ) -> Result<Self, crate::errors::Error> {
        if col_idx >= self.values.len() {
            return Err(crate::errors::Error::ColumnIndexOutOfBounds(
                col_idx,
                self.values.len(),
            ));
        }

        self.values[col_idx] = ((), Some(new.into()));
        Ok(self)
    }

    /// Sets a column to NULL.
    ///
    /// # Errors
    ///
    /// * `ColumnIndexOutOfBounds` - If the column index is out of bounds for the table schema.
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_diff_rs::{Update, PatchsetFormat, TableSchema};
    ///
    /// // CREATE TABLE items (id INTEGER PRIMARY KEY, description TEXT)
    /// let schema: TableSchema<String> = TableSchema::new("items".into(), 2, vec![1, 0]);
    ///
    /// // UPDATE items SET description = NULL WHERE id = 1
    /// let update = Update::<_, PatchsetFormat, String, Vec<u8>>::from(schema)
    ///     .set(0, 1i64).unwrap()
    ///     .set_null(1).unwrap();
    /// ```
    pub fn set_null(self, col_idx: usize) -> Result<Self, crate::errors::Error>
    where
        S: Default,
        B: Default,
    {
        self.set(col_idx, Value::Null)
    }
}

