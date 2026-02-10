//! Dynamic (runtime) table schema traits.
use core::{fmt::Debug, hash::Hash};

use alloc::vec::Vec;

use crate::encoding::Value;

/// A table schema known at runtime (object-safe).
///
/// While extremely generic, this trait does not provide much type safety.
pub trait DynTable: Debug + Eq + Clone + PartialEq {
    /// The table name.
    fn name(&self) -> &str;

    /// The number of columns in the table.
    fn number_of_columns(&self) -> usize;

    /// Write primary key flags to the buffer.
    ///
    /// The buffer must have length equal to `number_of_columns()`.
    /// Each byte represents the 1-based ordinal position of the column
    /// in the composite primary key, or 0 if the column is not part of
    /// the primary key.
    ///
    /// For example, for a table with columns (A, B, C) where (B, A) is the PK
    /// (B is the first PK column, A is the second), the buffer should be:
    /// `[2, 1, 0]` - A is 2nd in PK order, B is 1st in PK order, C is not PK.
    ///
    /// # Panics
    ///
    /// Panics if `buf.len() != self.number_of_columns()`.
    fn write_pk_flags(&self, buf: &mut [u8]);
}

impl<T: DynTable> DynTable for &T {
    #[inline]
    fn name(&self) -> &str {
        T::name(self)
    }

    #[inline]
    fn number_of_columns(&self) -> usize {
        T::number_of_columns(self)
    }

    #[inline]
    fn write_pk_flags(&self, buf: &mut [u8]) {
        T::write_pk_flags(self, buf);
    }
}

/// Collection of indexable values.
pub(crate) trait IndexableValues {
    /// The string variant.
    type Text: Clone;
    /// The binary variant.
    type Binary: Clone;

    /// Get the value at the specified column index.
    ///
    /// # Arguments
    ///
    /// * `col_idx` - The index of the column to retrieve.
    ///
    /// # Returns
    ///
    /// The value at the specified column index, or `None` if the index is out of bounds.
    fn get(&self, col_idx: usize) -> Option<Value<Self::Text, Self::Binary>>;
}

impl<S: Clone, B: Clone> IndexableValues for Vec<Value<S, B>> {
    type Text = S;
    type Binary = B;

    #[inline]
    fn get(&self, col_idx: usize) -> Option<Value<Self::Text, Self::Binary>> {
        <&[Value<S, B>]>::get(&self.as_slice(), col_idx)
    }
}

impl<S: Clone, B: Clone> IndexableValues for &[Value<S, B>] {
    type Text = S;
    type Binary = B;

    #[inline]
    fn get(&self, col_idx: usize) -> Option<Value<Self::Text, Self::Binary>> {
        <[Value<S, B>]>::get(self, col_idx).cloned()
    }
}

impl<S: Clone, B: Clone> IndexableValues for Vec<Option<Value<S, B>>> {
    type Text = S;
    type Binary = B;

    #[inline]
    fn get(&self, col_idx: usize) -> Option<Value<Self::Text, Self::Binary>> {
        <&[Option<Value<S, B>>]>::get(&self.as_slice(), col_idx)
    }
}

impl<S: Clone, B: Clone> IndexableValues for &[Option<Value<S, B>>] {
    type Text = S;
    type Binary = B;

    #[inline]
    fn get(&self, col_idx: usize) -> Option<Value<Self::Text, Self::Binary>> {
        <[Option<Value<S, B>>]>::get(self, col_idx).map(|v| {
            if let Some(value) = v {
                value.clone()
            } else {
                Value::Null
            }
        })
    }
}

impl<O, S: Clone, B: Clone> IndexableValues for Vec<(O, Option<Value<S, B>>)> {
    type Text = S;
    type Binary = B;

    #[inline]
    fn get(&self, col_idx: usize) -> Option<Value<Self::Text, Self::Binary>> {
        <&[(O, Option<Value<S, B>>)]>::get(&self.as_slice(), col_idx)
    }
}

impl<O, S: Clone, B: Clone> IndexableValues for &[(O, Option<Value<S, B>>)] {
    type Text = S;
    type Binary = B;

    #[inline]
    fn get(&self, col_idx: usize) -> Option<Value<Self::Text, Self::Binary>> {
        <[(O, Option<Value<S, B>>)]>::get(self, col_idx).map(|(_old, new)| {
            if let Some(value) = new {
                value.clone()
            } else {
                Value::Null
            }
        })
    }
}

/// Extension trait for schemas with typed primary key extraction.
///
/// This trait is NOT object-safe due to the associated type.
/// Use [`DynTable`] with the free function [`extract_pk`] for dynamic dispatch.
///
/// # Type Parameter
///
/// The `PrimaryKeyValue` type varies by schema:
/// - For `TableSchema` implementors: derived from `<PrimaryKey as NestedColumns>::NestedValues`,
///   e.g., `(i64,)` or `(i64, String)`
/// - For `Box<dyn DynTable>`: `Vec<Value>` (runtime, unknown structure)
pub trait SchemaWithPK: DynTable + Clone + Hash {
    /// Returns the number of primary key columns in the schema.
    fn number_of_primary_keys(&self) -> usize;

    /// Returns the primary key index of the primary key by the column index.
    fn primary_key_index(&self, col_idx: usize) -> Option<usize>;

    /// Extract primary key values from a full row.
    ///
    /// The values slice must have length equal to `number_of_columns()`.
    /// Returns the PK values in column order, typed appropriately.
    ///
    /// # Panics
    ///
    /// Panics if the values collection is shorter than the schema's column count.
    fn extract_pk<S: Clone, B: Clone>(
        &self,
        values: &impl IndexableValues<Text = S, Binary = B>,
    ) -> alloc::vec::Vec<Value<S, B>>;
}

impl<T: SchemaWithPK> SchemaWithPK for &T {
    fn number_of_primary_keys(&self) -> usize {
        T::number_of_primary_keys(self)
    }

    fn primary_key_index(&self, col_idx: usize) -> Option<usize> {
        T::primary_key_index(self, col_idx)
    }

    fn extract_pk<S: Clone, B: Clone>(
        &self,
        values: &impl IndexableValues<Text = S, Binary = B>,
    ) -> alloc::vec::Vec<Value<S, B>> {
        T::extract_pk(self, values)
    }
}
