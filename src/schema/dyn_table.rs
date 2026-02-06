//! Dynamic (runtime) table schema traits.
use core::{fmt::Debug, hash::Hash};

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
    fn name(&self) -> &str {
        T::name(self)
    }

    fn number_of_columns(&self) -> usize {
        T::number_of_columns(self)
    }

    fn write_pk_flags(&self, buf: &mut [u8]) {
        T::write_pk_flags(self, buf);
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
    /// Extract primary key values from a full row.
    ///
    /// The values slice must have length equal to `number_of_columns()`.
    /// Returns the PK values in column order, typed appropriately.
    fn extract_pk(&self, values: &[Value]) -> alloc::vec::Vec<Value>;
}

impl<T: SchemaWithPK> SchemaWithPK for &T {
    fn extract_pk(&self, values: &[Value]) -> alloc::vec::Vec<Value> {
        T::extract_pk(self, values)
    }
}
