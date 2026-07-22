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
pub trait IndexableValues {
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
/// Use [`DynTable`] with the `extract_pk` method for dynamic dispatch.
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

    /// Returns the column indices of the primary key, ordered by their
    /// position within the composite key (key order).
    ///
    /// This is the forward companion to [`primary_key_index`](Self::primary_key_index):
    /// that maps a column index to its key position, while this lists the key
    /// column indices in key order. The ordering matches
    /// [`extract_pk`](Self::extract_pk), so the two agree on which cell is which
    /// key component.
    fn primary_key_columns(&self) -> Vec<usize> {
        let mut pairs: Vec<(usize, usize)> = (0..self.number_of_columns())
            .filter_map(|col| self.primary_key_index(col).map(|pos| (pos, col)))
            .collect();
        pairs.sort_by_key(|&(pos, _)| pos);
        pairs.into_iter().map(|(_, col)| col).collect()
    }
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

#[cfg(test)]
mod tests {
    use super::{DynTable, IndexableValues, SchemaWithPK};
    use crate::encoding::Value;
    use crate::schema::SimpleTable;
    use alloc::string::String;
    use alloc::vec;
    use alloc::vec::Vec;

    fn users() -> SimpleTable {
        SimpleTable::new("users", &["id", "name", "email"], &[0, 2])
    }

    #[test]
    fn test_dyntable_ref_forwards() {
        let t = users();
        let r: &SimpleTable = &t;
        assert_eq!(<&SimpleTable as DynTable>::name(&r), t.name());
        assert_eq!(
            <&SimpleTable as DynTable>::number_of_columns(&r),
            t.number_of_columns()
        );
        let mut buf_ref = [0u8; 3];
        let mut buf_direct = [0u8; 3];
        <&SimpleTable as DynTable>::write_pk_flags(&r, &mut buf_ref);
        t.write_pk_flags(&mut buf_direct);
        assert_eq!(buf_ref, buf_direct);
    }

    #[test]
    fn test_schema_with_pk_ref_forwards() {
        let t = users();
        let r: &SimpleTable = &t;
        assert_eq!(
            <&SimpleTable as SchemaWithPK>::number_of_primary_keys(&r),
            t.number_of_primary_keys()
        );
        for idx in 0..t.number_of_columns() {
            assert_eq!(
                <&SimpleTable as SchemaWithPK>::primary_key_index(&r, idx),
                t.primary_key_index(idx)
            );
        }
        assert_eq!(
            <&SimpleTable as SchemaWithPK>::primary_key_columns(&r),
            t.primary_key_columns()
        );
        let values: Vec<Value<String, Vec<u8>>> = vec![
            Value::Integer(1),
            Value::Text("alice".into()),
            Value::Text("a@x".into()),
        ];
        let pk_ref = <&SimpleTable as SchemaWithPK>::extract_pk(&r, &values);
        let pk_direct = t.extract_pk(&values);
        assert_eq!(pk_ref, pk_direct);
    }

    #[test]
    fn test_indexable_values_vec_option() {
        // Vec<Option<Value>>: None entries map to Value::Null.
        let v: Vec<Option<Value<String, Vec<u8>>>> =
            vec![Some(Value::Integer(7)), None, Some(Value::Text("x".into()))];
        assert_eq!(
            <Vec<Option<Value<String, Vec<u8>>>> as IndexableValues>::get(&v, 0),
            Some(Value::Integer(7))
        );
        assert_eq!(
            <Vec<Option<Value<String, Vec<u8>>>> as IndexableValues>::get(&v, 1),
            Some(Value::Null)
        );
        assert_eq!(
            <Vec<Option<Value<String, Vec<u8>>>> as IndexableValues>::get(&v, 2),
            Some(Value::Text("x".into()))
        );
        assert_eq!(
            <Vec<Option<Value<String, Vec<u8>>>> as IndexableValues>::get(&v, 99),
            None
        );
    }

    #[test]
    fn test_indexable_values_slice_option() {
        let owned: Vec<Option<Value<String, Vec<u8>>>> = vec![Some(Value::Integer(1)), None];
        let slice: &[Option<Value<String, Vec<u8>>>] = &owned;
        assert_eq!(
            <&[Option<Value<String, Vec<u8>>>] as IndexableValues>::get(&slice, 0),
            Some(Value::Integer(1))
        );
        assert_eq!(
            <&[Option<Value<String, Vec<u8>>>] as IndexableValues>::get(&slice, 1),
            Some(Value::Null)
        );
        assert_eq!(
            <&[Option<Value<String, Vec<u8>>>] as IndexableValues>::get(&slice, 5),
            None
        );
    }

    type PairVec = Vec<(u8, Option<Value<String, Vec<u8>>>)>;

    #[test]
    fn test_indexable_values_vec_pair() {
        // Vec<(O, Option<Value>)>: the impl reads only the second element.
        let v: PairVec = vec![(0, Some(Value::Integer(2))), (1, None)];
        assert_eq!(
            <PairVec as IndexableValues>::get(&v, 0),
            Some(Value::Integer(2))
        );
        assert_eq!(<PairVec as IndexableValues>::get(&v, 1), Some(Value::Null));
        assert_eq!(<PairVec as IndexableValues>::get(&v, 2), None);
    }

    type PairSlice<'a> = &'a [(u8, Option<Value<String, Vec<u8>>>)];

    #[test]
    fn test_indexable_values_slice_pair() {
        let owned: PairVec = vec![(0, Some(Value::Text("y".into()))), (1, None)];
        let slice: PairSlice<'_> = &owned;
        assert_eq!(
            <PairSlice<'_> as IndexableValues>::get(&slice, 0),
            Some(Value::Text("y".into()))
        );
        assert_eq!(
            <PairSlice<'_> as IndexableValues>::get(&slice, 1),
            Some(Value::Null)
        );
        assert_eq!(<PairSlice<'_> as IndexableValues>::get(&slice, 3), None);
    }
}
