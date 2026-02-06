//! DiffSet builder for constructing changeset/patchset binary data.
//!
//! This module provides [`DiffSetBuilder`] for building SQLite session extension
//! compatible changesets and patchsets. The builder tracks row state and consolidates
//! operations according to SQLite's changegroup semantics.
//!
//! # Terminology
//!
//! - **Changeset**: Full row data for all operations (invertible)
//! - **Patchset**: Minimal data (PK only for deletes, changed columns for updates)
//! - **DiffSet**: Generic term for either changeset or patchset
//!
//! # Consolidation Rules
//!
//! When multiple operations affect the same row (by primary key), they are
//! consolidated according to SQLite's `sqlite3changegroup_add()` semantics:
//!
//! | Existing | New | Result |
//! |----------|--------|--------|
//! | INSERT | INSERT | Ignore new |
//! | INSERT | UPDATE | INSERT with updated values |
//! | INSERT | DELETE | Remove both (no-op) |
//! | UPDATE | INSERT | Ignore new |
//! | UPDATE | UPDATE | Single UPDATE original→final |
//! | UPDATE | DELETE | DELETE of original |
//! | DELETE | INSERT | UPDATE if different, no-op if same |
//! | DELETE | UPDATE | Ignore new |
//! | DELETE | DELETE | Ignore new |

use indexmap::IndexMap as IndexMapRaw;

use crate::{
    SchemaWithPK,
    builders::{
        ChangeDelete, ChangesetFormat, Insert, Operation, PatchsetFormat, Update, format::Format,
    },
    encoding::{Value, encode_value},
};
use alloc::vec;
use alloc::vec::Vec;

/// `IndexMap` alias using hashbrown's default hasher for `no_std` compatibility.
type IndexMap<K, V> = IndexMapRaw<K, V, hashbrown::DefaultHashBuilder>;

// ============================================================================
// SQLite session extension hash simulation
// ============================================================================

/// The core hash-combine step used throughout SQLite's session extension.
///
/// Matches the C macro: `#define HASH_APPEND(hash, add) ((hash) << 3) ^ (hash) ^ (unsigned int)(add)`
const fn hash_append(h: u32, add: u32) -> u32 {
    (h << 3) ^ h ^ add
}

/// Hash a 64-bit integer using SQLite's `sessionHashAppendI64`.
///
/// Hashes the lower 32 bits first, then the upper 32 bits.
#[allow(clippy::cast_sign_loss)]
fn session_hash_append_i64(h: u32, i: i64) -> u32 {
    let lo = (i as u64 & 0xFFFF_FFFF) as u32;
    let hi = ((i as u64 >> 32) & 0xFFFF_FFFF) as u32;
    let h = hash_append(h, lo);
    hash_append(h, hi)
}

/// Hash a blob using SQLite's `sessionHashAppendBlob`.
///
/// Applies `HASH_APPEND` to each byte.
fn session_hash_append_blob(mut h: u32, data: &[u8]) -> u32 {
    for &byte in data {
        h = hash_append(h, u32::from(byte));
    }
    h
}

/// Hash a primary key using SQLite's `sessionPreupdateHash` algorithm.
///
/// For each PK value: `h = HASH_APPEND(h, type_code)`, then hash the value.
/// Type codes match SQLite: INTEGER=1, FLOAT=2, TEXT=3, BLOB=4.
fn session_hash_pk(pk: &[Value]) -> u32 {
    let mut h: u32 = 0;
    for value in pk {
        match value {
            Value::Integer(i) => {
                h = hash_append(h, 1); // SQLITE_INTEGER
                h = session_hash_append_i64(h, *i);
            }
            Value::Real(f) => {
                h = hash_append(h, 2); // SQLITE_FLOAT
                // SQLite does memcpy(&iVal, &rVal, 8) then hashes as i64
                let i = i64::from_ne_bytes(f.to_ne_bytes());
                h = session_hash_append_i64(h, i);
            }
            Value::Text(s) => {
                h = hash_append(h, 3); // SQLITE_TEXT
                h = session_hash_append_blob(h, s.as_bytes());
            }
            Value::Blob(b) => {
                h = hash_append(h, 4); // SQLITE_BLOB
                h = session_hash_append_blob(h, b);
            }
            Value::Null | Value::Undefined => {
                // NULL/Undefined PKs: SQLite skips hashing for these.
                // In practice, PKs should never be NULL.
            }
        }
    }
    h
}

/// Simulate SQLite's session extension hash table to determine row output order.
///
/// SQLite's session extension tracks changes in a hash table where:
/// - New entries are prepended to their bucket (most recent at list head)
/// - The table starts at 256 buckets and doubles when entries ≥ buckets/2
/// - Changeset iteration walks buckets 0..n-1, following each linked list
///
/// This function returns indices into `rows` in the order that SQLite's
/// changeset/patchset output would contain them.
fn session_row_order<V>(rows: &IndexMap<Vec<Value>, V>) -> Vec<usize> {
    let n = rows.len();
    if n == 0 {
        return Vec::new();
    }

    let pks: Vec<&Vec<Value>> = rows.keys().collect();

    // Simulate the hash table. We store each bucket as a Vec of entry indices
    // in the REVERSE of SQLite's linked-list order (we push; SQLite prepends).
    // We reverse each bucket during final iteration to recover SQLite's order.
    let mut n_change: usize = 0;
    let mut buckets: Vec<Vec<usize>> = Vec::new();

    for idx in 0..n {
        // Growth check (before each insert), matching SQLite's sessionGrowHash.
        // SQLite: grows when nChange==0 or nEntry >= nChange/2.
        // Here idx == current nEntry (entries 0..idx-1 already inserted).
        if n_change == 0 || idx >= n_change / 2 {
            let new_size = if n_change == 0 { 256 } else { n_change * 2 };
            let mut new_buckets: Vec<Vec<usize>> = vec![Vec::new(); new_size];

            // Rehash existing entries. In SQLite, old buckets are walked
            // 0..old_nChange-1, and within each bucket entries are walked
            // from head to tail (reverse of our Vec order), prepending to
            // new buckets. We simulate by walking our Vecs in reverse
            // (= SQLite's head-to-tail) and pushing (= SQLite's prepend
            // into our reversed representation).
            for old_bucket in &buckets {
                for &entry_idx in old_bucket.iter().rev() {
                    let h = session_hash_pk(pks[entry_idx]) as usize % new_size;
                    new_buckets[h].push(entry_idx);
                }
            }

            buckets = new_buckets;
            n_change = new_size;
        }

        // Insert entry (push = prepend in our reversed representation)
        let h = session_hash_pk(pks[idx]) as usize % n_change;
        buckets[h].push(idx);
    }

    // Walk buckets in order. Reverse each bucket to recover SQLite's
    // linked-list iteration order (head to tail).
    let mut order = Vec::with_capacity(n);
    for bucket in &buckets {
        for &idx in bucket.iter().rev() {
            order.push(idx);
        }
    }

    order
}

/// Builder for constructing changeset or patchset binary data.
///
/// Generic over the format `F` (Changeset or Patchset) and table schema `T`.
#[derive(Debug, Clone, Eq)]
pub struct DiffSetBuilder<F: Format, T: SchemaWithPK> {
    tables: IndexMap<T, IndexMap<Vec<Value>, Operation<F>>>,
}

/// Custom PartialEq that ignores tables with empty operations.
///
/// Tables with no operations are not serialized (skipped in build()), so after
/// roundtrip they won't exist. This makes empty tables semantically equivalent
/// to non-existent tables for comparison purposes.
///
/// Verified: SQLite's session extension does NOT include empty table entries in
/// changesets/patchsets when all operations cancel out. Our builder keeps them
/// in memory to preserve table ordering, but they are correctly excluded here
/// and in `build()`.
impl<F: Format, T: SchemaWithPK> PartialEq for DiffSetBuilder<F, T> {
    fn eq(&self, other: &Self) -> bool {
        // Filter out tables with empty operations, then compare element by element.
        // IndexMap preserves insertion order, so this also checks table ordering.
        self.tables
            .iter()
            .filter(|(_, ops)| !ops.is_empty())
            .eq(other.tables.iter().filter(|(_, ops)| !ops.is_empty()))
    }
}

/// Type alias for building changesets.
pub type ChangeSet<T> = DiffSetBuilder<ChangesetFormat, T>;
/// Type alias for building patchsets.
pub type PatchSet<T> = DiffSetBuilder<PatchsetFormat, T>;

impl<F: Format, T: SchemaWithPK> Default for DiffSetBuilder<F, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: SchemaWithPK> From<&DiffSetBuilder<ChangesetFormat, T>> for Vec<u8> {
    fn from(builder: &DiffSetBuilder<ChangesetFormat, T>) -> Self {
        builder.build()
    }
}

impl<T: SchemaWithPK> From<DiffSetBuilder<ChangesetFormat, T>> for Vec<u8> {
    fn from(builder: DiffSetBuilder<ChangesetFormat, T>) -> Self {
        builder.build()
    }
}

impl<T: SchemaWithPK> From<&DiffSetBuilder<PatchsetFormat, T>> for Vec<u8> {
    fn from(builder: &DiffSetBuilder<PatchsetFormat, T>) -> Self {
        builder.build()
    }
}

impl<T: SchemaWithPK> From<DiffSetBuilder<PatchsetFormat, T>> for Vec<u8> {
    fn from(builder: DiffSetBuilder<PatchsetFormat, T>) -> Self {
        builder.build()
    }
}

use crate::encoding::op_codes;

impl<F: Format, T: SchemaWithPK> DiffSetBuilder<F, T> {
    /// Create a new builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: IndexMap::default(),
        }
    }

    /// Ensure a table exists in the builder, returning its row map.
    ///
    /// If the table doesn't exist yet, it's inserted at the end of the
    /// `IndexMap`, preserving first-touch ordering.
    fn ensure_table(&mut self, table: &T) -> &mut IndexMap<Vec<Value>, Operation<F>> {
        self.tables.entry(table.clone()).or_default()
    }

    /// Returns true if the builder has no operations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.values().all(IndexMap::is_empty)
    }

    /// Returns the number of operations across all tables.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tables.values().map(IndexMap::len).sum()
    }

    /// Write the table header to the output buffer.
    ///
    /// Format:
    /// - Table marker: 'T' (changeset) or 'P' (patchset)
    /// - Column count (1 byte)
    /// - PK flags (1 byte per column: 0x01 = PK, 0x00 = not)
    /// - Table name (null-terminated UTF-8)
    fn write_table_header(out: &mut Vec<u8>, table: &T) {
        // Table marker
        out.push(F::TABLE_MARKER);

        // Column count (1 byte)
        let num_cols = table.number_of_columns();
        out.push(u8::try_from(num_cols).unwrap());

        // PK flags (1 byte per column)
        let pk_start = out.len();
        out.resize(pk_start + num_cols, 0);
        table.write_pk_flags(&mut out[pk_start..]);

        // Table name (null-terminated)
        out.extend(table.name().as_bytes());
        out.push(0);
    }

    /// Add any operation, consolidating with existing operations on the same row.
    ///
    /// The table schema is passed separately — operations are schema-less.
    fn add_operation(mut self, table: &T, pk: Vec<Value>, new_op: Operation<F>) -> Self
    where
        Operation<F>: core::ops::Add<Output = Option<Operation<F>>>,
    {
        let rows = self.ensure_table(table);

        match rows.shift_remove_full(&pk) {
            None => {
                rows.insert(pk, new_op);
            }
            Some((original_index, _removed_key, existing)) => {
                // Special case: INSERT + UPDATE may change the PK
                match (&existing, &new_op) {
                    (Operation::Insert(_), Operation::Update(_)) => {
                        // Apply update to insert values, then re-extract PK
                        if let Some(combined) = existing + new_op
                            && let Operation::Insert(ref values) = combined
                        {
                            let new_pk = table.extract_pk(values);
                            // The new PK may collide with a different existing row
                            rows.shift_remove(&new_pk);
                            let index = original_index.min(rows.len());
                            rows.shift_insert(index, new_pk, combined);
                        }
                    }
                    _ => {
                        // Standard consolidation
                        if let Some(combined) = existing + new_op {
                            // Re-insert at original position to preserve row ordering
                            rows.shift_insert(original_index, pk, combined);
                        }
                    }
                }
            }
        }
        self
    }
}

// ============================================================================
// Changeset-specific builder methods
// ============================================================================

impl<T: SchemaWithPK> DiffSetBuilder<ChangesetFormat, T> {
    /// Add an INSERT operation.
    #[must_use]
    pub fn insert(self, insert: Insert<T>) -> Self {
        let pk = insert.as_ref().extract_pk(insert.values());
        let table = insert.as_ref().clone();
        self.add_operation(&table, pk, Operation::Insert(insert.into_values()))
    }

    /// Add an INSERT operation from raw values (internal use).
    #[must_use]
    pub(crate) fn insert_raw(self, table: &T, values: Vec<Value>) -> Self {
        let pk = table.extract_pk(&values);
        self.add_operation(table, pk, Operation::Insert(values))
    }

    /// Add a DELETE operation.
    #[must_use]
    pub fn delete(self, delete: ChangeDelete<T>) -> Self {
        let pk = delete.as_ref().extract_pk(delete.values());
        let table = delete.as_ref().clone();
        self.add_operation(&table, pk, Operation::Delete(delete.into_values()))
    }

    /// Add a DELETE operation from raw values (internal use).
    #[must_use]
    pub(crate) fn delete_raw(self, table: &T, values: Vec<Value>) -> Self {
        let pk = table.extract_pk(&values);
        self.add_operation(table, pk, Operation::Delete(values))
    }

    /// Add an UPDATE operation.
    #[must_use]
    pub fn update(self, update: Update<T, ChangesetFormat>) -> Self {
        // Extract PK from old values
        let old_values: Vec<_> = update
            .values()
            .iter()
            .map(|(old, _): &(_, _)| old.clone())
            .collect();
        let pk = update.as_ref().extract_pk(&old_values);
        let table = update.as_ref().clone();
        let values: Vec<(Value, Value)> = update.into();
        self.add_operation(&table, pk, Operation::Update(values))
    }

    /// Add an UPDATE operation from raw values (internal use).
    #[must_use]
    pub(crate) fn update_raw(
        self,
        table: &T,
        old_values: Vec<Value>,
        new_values: Vec<Value>,
    ) -> Self {
        let pk = table.extract_pk(&old_values);
        let values: Vec<(Value, Value)> = old_values.into_iter().zip(new_values).collect();
        self.add_operation(table, pk, Operation::Update(values))
    }
}

// ============================================================================
// Patchset-specific builder methods
// ============================================================================

impl<T: SchemaWithPK> DiffSetBuilder<PatchsetFormat, T> {
    /// Add an INSERT operation.
    #[must_use]
    pub fn insert(self, insert: Insert<T>) -> Self {
        let pk = insert.as_ref().extract_pk(insert.values());
        let table = insert.as_ref().clone();
        self.add_operation(&table, pk, Operation::Insert(insert.into_values()))
    }

    /// Add an INSERT operation from raw values (internal use).
    #[must_use]
    pub(crate) fn insert_raw(self, table: &T, values: Vec<Value>) -> Self {
        let pk = table.extract_pk(&values);
        self.add_operation(table, pk, Operation::Insert(values))
    }

    /// Add a DELETE operation by specifying the table and primary key values.
    ///
    /// The `pk` slice should contain only the primary key column values, in the order
    /// they appear in the table schema. This is the same format returned by
    /// [`SchemaWithPK::extract_pk`].
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_diff_rs::{PatchSet, SchemaWithPK};
    /// use sqlparser::ast::CreateTable;
    /// use sqlparser::dialect::SQLiteDialect;
    /// use sqlparser::parser::Parser;
    ///
    /// let dialect = SQLiteDialect {};
    /// let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
    /// let statements = Parser::parse_sql(&dialect, sql).unwrap();
    /// let schema = match &statements[0] {
    ///     sqlparser::ast::Statement::CreateTable(ct) => ct.clone(),
    ///     _ => panic!(),
    /// };
    ///
    /// // Delete row where id = 1
    /// let patchset = PatchSet::new().delete(&schema, &[1i64.into()]);
    /// ```
    #[must_use]
    pub fn delete(self, table: &T, pk: &[Value]) -> Self {
        self.add_operation(table, pk.to_vec(), Operation::Delete(()))
    }

    /// Add an UPDATE operation.
    ///
    /// The primary key values are extracted from the Update's new values automatically.
    /// For patchset updates, ensure the PK columns are set in the Update using `.set()`.
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_diff_rs::{PatchSet, PatchsetFormat, Update};
    /// use sqlparser::ast::CreateTable;
    /// use sqlparser::dialect::SQLiteDialect;
    /// use sqlparser::parser::Parser;
    ///
    /// let dialect = SQLiteDialect {};
    /// let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
    /// let statements = Parser::parse_sql(&dialect, sql).unwrap();
    /// let schema = match &statements[0] {
    ///     sqlparser::ast::Statement::CreateTable(ct) => ct.clone(),
    ///     _ => panic!(),
    /// };
    ///
    /// // UPDATE users SET name = 'Bob' WHERE id = 1
    /// let update = Update::<_, PatchsetFormat>::from(schema)
    ///     .set(0, 1i64).unwrap()  // PK value
    ///     .set(1, "Bob").unwrap();
    ///
    /// let patchset = PatchSet::new().update(update);
    /// ```
    #[must_use]
    pub fn update(self, update: Update<T, PatchsetFormat>) -> Self {
        let pk = update.as_ref().extract_pk(&update.new_values());
        let table = update.as_ref().clone();
        let values: Vec<((), Value)> = update.into();
        self.add_operation(&table, pk, Operation::Update(values))
    }

    /// Add an UPDATE operation from raw new values (internal use).
    #[must_use]
    pub(crate) fn update_raw(self, table: &T, new_values: Vec<Value>) -> Self {
        let pk = table.extract_pk(&new_values);
        let values: Vec<((), Value)> = new_values.into_iter().map(|v| ((), v)).collect();
        self.add_operation(table, pk, Operation::Update(values))
    }
}

// ============================================================================
// Unified build implementation
// ============================================================================

impl<T: SchemaWithPK> DiffSetBuilder<ChangesetFormat, T> {
    /// Build the changeset binary data.
    ///
    /// Returns the binary representation compatible with SQLite's session extension.
    #[must_use]
    pub fn build(&self) -> Vec<u8> {
        let mut out = Vec::new();

        for (table, rows) in &self.tables {
            if rows.is_empty() {
                continue;
            }

            Self::write_table_header(&mut out, table);

            for idx in session_row_order(rows) {
                let (_pk, op) = rows.get_index(idx).unwrap();
                match op {
                    Operation::Insert(values) => {
                        out.push(op_codes::INSERT);
                        out.push(0);
                        for value in values {
                            encode_value(&mut out, value);
                        }
                    }
                    Operation::Delete(values) => {
                        out.push(op_codes::DELETE);
                        out.push(0);
                        for value in values {
                            encode_value(&mut out, value);
                        }
                    }
                    Operation::Update(values) => {
                        out.push(op_codes::UPDATE);
                        out.push(0);
                        // Write old values, then new values
                        for (old, _new) in values {
                            encode_value(&mut out, old);
                        }
                        for (_old, new) in values {
                            encode_value(&mut out, new);
                        }
                    }
                }
            }
        }

        out
    }
}

impl<T: SchemaWithPK> DiffSetBuilder<PatchsetFormat, T> {
    /// Build the patchset binary data.
    ///
    /// Returns the binary representation compatible with SQLite's session extension.
    #[must_use]
    pub fn build(&self) -> Vec<u8> {
        let mut out = Vec::new();

        for (table, rows) in &self.tables {
            if rows.is_empty() {
                continue;
            }

            Self::write_table_header(&mut out, table);

            // Get PK flags for this table
            let num_cols = table.number_of_columns();
            let mut pk_flags = alloc::vec![0u8; num_cols];
            table.write_pk_flags(&mut pk_flags);

            // Build a mapping from column index to position in pk vector.
            let mut pk_col_to_pk_pos: alloc::vec::Vec<Option<usize>> = alloc::vec![None; num_cols];
            {
                let mut pk_cols: alloc::vec::Vec<(usize, u8)> = pk_flags
                    .iter()
                    .enumerate()
                    .filter_map(|(i, &ord)| if ord > 0 { Some((i, ord)) } else { None })
                    .collect();
                pk_cols.sort_by_key(|(_, ord)| *ord);
                for (pos, (col_idx, _)) in pk_cols.into_iter().enumerate() {
                    pk_col_to_pk_pos[col_idx] = Some(pos);
                }
            }

            for idx in session_row_order(rows) {
                let (pk, op) = rows.get_index(idx).unwrap();
                match op {
                    Operation::Insert(values) => {
                        out.push(op_codes::INSERT);
                        out.push(0);
                        for value in values {
                            encode_value(&mut out, value);
                        }
                    }
                    Operation::Delete(()) => {
                        out.push(op_codes::DELETE);
                        out.push(0);
                        // Patchset DELETE: write PK values in column order
                        for (col_idx, &pk_flag) in pk_flags.iter().enumerate() {
                            if pk_flag > 0 {
                                if let Some(pk_pos) = pk_col_to_pk_pos[col_idx] {
                                    encode_value(&mut out, &pk[pk_pos]);
                                } else {
                                    encode_value(&mut out, &crate::encoding::Value::Undefined);
                                }
                            }
                        }
                    }
                    Operation::Update(values) => {
                        out.push(op_codes::UPDATE);
                        out.push(0);
                        // Patchset UPDATE old values: PK columns get their values from pk,
                        // non-PK columns are undefined.
                        for (col_idx, &pk_flag) in pk_flags.iter().enumerate() {
                            if pk_flag > 0 {
                                if let Some(pk_pos) = pk_col_to_pk_pos[col_idx] {
                                    encode_value(&mut out, &pk[pk_pos]);
                                } else {
                                    encode_value(&mut out, &crate::encoding::Value::Undefined);
                                }
                            } else {
                                encode_value(&mut out, &crate::encoding::Value::Undefined);
                            }
                        }
                        // Write new values
                        for ((), new) in values {
                            encode_value(&mut out, new);
                        }
                    }
                }
            }
        }

        out
    }
}

// ============================================================================
// Reverse implementation for DiffSetBuilder
// ============================================================================

use crate::builders::operation::Reverse;

impl<T: SchemaWithPK> Reverse for DiffSetBuilder<ChangesetFormat, T> {
    type Output = DiffSetBuilder<ChangesetFormat, T>;

    fn reverse(self) -> Self::Output {
        let mut reversed: DiffSetBuilder<ChangesetFormat, T> = DiffSetBuilder::new();

        for (table, rows) in self.tables {
            for (_pk, op) in rows {
                let rev_op = op.reverse();
                // Recompute PK for reversed operation (INSERT↔DELETE swap values)
                let rev_pk = match &rev_op {
                    Operation::Insert(values) | Operation::Delete(values) => {
                        table.extract_pk(values)
                    }
                    Operation::Update(pairs) => {
                        let old_vals: Vec<_> = pairs.iter().map(|(old, _)| old.clone()).collect();
                        table.extract_pk(&old_vals)
                    }
                };
                reversed = reversed.add_operation(&table, rev_pk, rev_op);
            }
        }

        reversed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::Value;
    use alloc::{string::String, vec};

    /// Simple test table implementation
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestTable {
        name: String,
        num_columns: usize,
        pk_column: usize,
    }

    impl TestTable {
        fn new(name: &str, num_columns: usize, pk_column: usize) -> Self {
            Self {
                name: name.into(),
                num_columns,
                pk_column,
            }
        }
    }

    impl crate::DynTable for TestTable {
        fn name(&self) -> &str {
            &self.name
        }

        fn number_of_columns(&self) -> usize {
            self.num_columns
        }

        fn write_pk_flags(&self, buf: &mut [u8]) {
            assert_eq!(buf.len(), self.num_columns);
            buf.fill(0);
            buf[self.pk_column] = 1;
        }
    }

    impl crate::SchemaWithPK for TestTable {
        fn extract_pk(&self, values: &[Value]) -> alloc::vec::Vec<Value> {
            vec![values[self.pk_column].clone()]
        }
    }

    // Type alias for cleaner test code
    type ChangesetBuilder = DiffSetBuilder<ChangesetFormat, TestTable>;

    #[test]
    fn test_insert_single_row() {
        let table = TestTable::new("users", 2, 0);
        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();

        let builder = ChangesetBuilder::new().insert(insert);

        assert_eq!(builder.len(), 1);
        assert!(!builder.is_empty());
    }

    #[test]
    fn test_insert_then_delete_cancels_out() {
        let table = TestTable::new("users", 2, 0);

        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();

        let delete = ChangeDelete::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();

        let builder = ChangesetBuilder::new().insert(insert).delete(delete);

        assert_eq!(builder.len(), 0);
        assert!(builder.is_empty());
    }

    #[test]
    fn test_insert_then_update_becomes_insert() {
        let table = TestTable::new("users", 2, 0);

        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();

        let update = Update::<TestTable, ChangesetFormat>::from(table.clone())
            .set(0, 1i64, 1i64) // PK unchanged
            .unwrap()
            .set(1, "alice", "bob")
            .unwrap();

        let builder = ChangesetBuilder::new().insert(insert).update(update);

        assert_eq!(builder.len(), 1);
        // Should still be an INSERT with "bob" as the name
    }

    #[test]
    fn test_delete_then_insert_same_values_cancels_out() {
        let table = TestTable::new("users", 2, 0);

        let delete = ChangeDelete::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();

        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();

        let builder = ChangesetBuilder::new().delete(delete).insert(insert);

        assert_eq!(builder.len(), 0);
        assert!(builder.is_empty());
    }

    #[test]
    fn test_delete_then_insert_different_values_becomes_update() {
        let table = TestTable::new("users", 2, 0);

        let delete = ChangeDelete::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();

        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "bob")
            .unwrap();

        let builder = ChangesetBuilder::new().delete(delete).insert(insert);

        assert_eq!(builder.len(), 1);
        // Should be an UPDATE from alice to bob
    }

    #[test]
    fn test_multiple_rows() {
        let table = TestTable::new("users", 2, 0);

        let insert1 = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();

        let insert2 = Insert::from(table.clone())
            .set(0, 2i64)
            .unwrap()
            .set(1, "bob")
            .unwrap();

        let builder = ChangesetBuilder::new().insert(insert1).insert(insert2);

        assert_eq!(builder.len(), 2);
    }

    #[test]
    fn test_update_then_update_consolidates() {
        let table = TestTable::new("users", 2, 0);

        let update1 = Update::<TestTable, ChangesetFormat>::from(table.clone())
            .set(0, 1i64, 1i64)
            .unwrap()
            .set(1, "alice", "bob")
            .unwrap();

        let update2 = Update::<TestTable, ChangesetFormat>::from(table.clone())
            .set(0, 1i64, 1i64)
            .unwrap()
            .set(1, "bob", "charlie")
            .unwrap();

        let builder = ChangesetBuilder::new().update(update1).update(update2);

        assert_eq!(builder.len(), 1);
        // Should be a single UPDATE from alice to charlie
    }

    // ========================================================================
    // Reverse trait tests
    // ========================================================================

    #[test]
    fn test_reverse_operation_insert_becomes_delete() {
        let op: Operation<ChangesetFormat> =
            Operation::Insert(vec![Value::Integer(1), Value::Text("alice".into())]);
        let reversed = op.reverse();
        assert!(matches!(reversed, Operation::Delete(_)));
        if let Operation::Delete(values) = reversed {
            assert_eq!(values, vec![Value::Integer(1), Value::Text("alice".into())]);
        }
    }

    #[test]
    fn test_reverse_operation_delete_becomes_insert() {
        let op: Operation<ChangesetFormat> =
            Operation::Delete(vec![Value::Integer(1), Value::Text("alice".into())]);
        let reversed = op.reverse();
        assert!(matches!(reversed, Operation::Insert(_)));
        if let Operation::Insert(values) = reversed {
            assert_eq!(values, vec![Value::Integer(1), Value::Text("alice".into())]);
        }
    }

    #[test]
    fn test_reverse_operation_update_swaps_old_new() {
        let op: Operation<ChangesetFormat> = Operation::Update(vec![
            (Value::Integer(1), Value::Integer(1)),
            (Value::Text("alice".into()), Value::Text("bob".into())),
        ]);
        let reversed = op.reverse();
        if let Operation::Update(values) = reversed {
            assert_eq!(values[0], (Value::Integer(1), Value::Integer(1)));
            assert_eq!(
                values[1],
                (Value::Text("bob".into()), Value::Text("alice".into()))
            );
        } else {
            panic!("Expected Update operation");
        }
    }

    #[test]
    fn test_reverse_builder_insert_becomes_delete() {
        let table = TestTable::new("users", 2, 0);
        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();

        let builder = ChangesetBuilder::new().insert(insert);
        let reversed = builder.reverse();

        assert_eq!(reversed.len(), 1);
        // The reversed builder should have a delete operation
        let rows = reversed.tables.get(&table).unwrap();
        assert!(matches!(
            rows.values().next().unwrap(),
            Operation::Delete(_)
        ));
    }

    #[test]
    fn test_reverse_builder_delete_becomes_insert() {
        let table = TestTable::new("users", 2, 0);
        let delete = ChangeDelete::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();

        let builder = ChangesetBuilder::new().delete(delete);
        let reversed = builder.reverse();

        assert_eq!(reversed.len(), 1);
        // The reversed builder should have an insert operation
        let rows = reversed.tables.get(&table).unwrap();
        assert!(matches!(
            rows.values().next().unwrap(),
            Operation::Insert(_)
        ));
    }

    #[test]
    fn test_reverse_builder_update_swaps() {
        let table = TestTable::new("users", 2, 0);
        let update = Update::<TestTable, ChangesetFormat>::from(table.clone())
            .set(0, 1i64, 1i64)
            .unwrap()
            .set(1, "alice", "bob")
            .unwrap();

        let builder = ChangesetBuilder::new().update(update);
        let reversed = builder.reverse();

        assert_eq!(reversed.len(), 1);
        // The reversed builder should have an update operation with swapped values
        let rows = reversed.tables.get(&table).unwrap();
        if let Operation::Update(values) = rows.values().next().unwrap() {
            assert_eq!(
                values[1],
                (Value::Text("bob".into()), Value::Text("alice".into()))
            );
        } else {
            panic!("Expected Update operation");
        }
    }

    #[test]
    fn test_reverse_is_involutory() {
        // reverse(reverse(x)) == x
        let table = TestTable::new("users", 2, 0);
        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();

        let original_values = insert.values().to_vec();
        let builder = ChangesetBuilder::new().insert(insert);
        let double_reversed = builder.reverse().reverse();

        assert_eq!(double_reversed.len(), 1);
        let rows = double_reversed.tables.get(&table).unwrap();
        if let Operation::Insert(values) = rows.values().next().unwrap() {
            assert_eq!(values, &original_values);
        } else {
            panic!("Expected Insert operation");
        }
    }

    // ========================================================================
    // Build (serialization) tests
    // ========================================================================

    #[test]
    fn test_build_empty_builder() {
        let builder = ChangesetBuilder::new();
        let bytes = builder.build();
        assert!(bytes.is_empty());
    }

    #[test]
    fn test_build_insert_format() {
        let table = TestTable::new("t", 2, 0);
        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap();

        let bytes = ChangesetBuilder::new().insert(insert).build();

        // Verify the structure:
        // Table header: 'T', col_count(2), pk_flags(1,0), name("t\0")
        // Operation: INSERT(0x12), indirect(0), values...
        assert!(!bytes.is_empty());

        // Check table marker
        assert_eq!(bytes[0], b'T');
        // Column count
        assert_eq!(bytes[1], 2);
        // PK flags: first column is PK
        assert_eq!(bytes[2], 1);
        assert_eq!(bytes[3], 0);
        // Table name "t" + null terminator
        assert_eq!(bytes[4], b't');
        assert_eq!(bytes[5], 0);
        // Operation code: INSERT = 0x12
        assert_eq!(bytes[6], 0x12);
        // Indirect flag
        assert_eq!(bytes[7], 0);
    }

    #[test]
    fn test_build_delete_format() {
        let table = TestTable::new("t", 2, 0);
        let delete = ChangeDelete::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap();

        let bytes = ChangesetBuilder::new().delete(delete).build();

        assert!(!bytes.is_empty());
        assert_eq!(bytes[0], b'T');
        // Operation code: DELETE = 0x09
        assert_eq!(bytes[6], 0x09);
    }

    #[test]
    fn test_build_update_format() {
        let table = TestTable::new("t", 2, 0);
        let update = Update::<TestTable, ChangesetFormat>::from(table.clone())
            .set(0, 1i64, 1i64)
            .unwrap()
            .set(1, "a", "b")
            .unwrap();

        let bytes = ChangesetBuilder::new().update(update).build();

        assert!(!bytes.is_empty());
        assert_eq!(bytes[0], b'T');
        // Operation code: UPDATE = 0x17
        assert_eq!(bytes[6], 0x17);
    }

    #[test]
    fn test_build_multiple_operations() {
        let table = TestTable::new("t", 2, 0);

        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap();

        let insert2 = Insert::from(table.clone())
            .set(0, 2i64)
            .unwrap()
            .set(1, "b")
            .unwrap();

        let bytes = ChangesetBuilder::new()
            .insert(insert)
            .insert(insert2)
            .build();

        assert!(!bytes.is_empty());
        // Should have one table header and two insert operations
        assert_eq!(bytes[0], b'T');
    }

    #[test]
    fn test_build_cancelled_operations_produce_empty() {
        let table = TestTable::new("t", 2, 0);

        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap();

        let delete = ChangeDelete::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap();

        let bytes = ChangesetBuilder::new()
            .insert(insert)
            .delete(delete)
            .build();

        // INSERT + DELETE with same values cancels out
        assert!(bytes.is_empty());
    }
}

// =============================================================================
// sqlparser integration (feature-gated)
// =============================================================================

#[cfg(feature = "sqlparser")]
mod sqlparser_impl {
    use alloc::string::{String, ToString};
    use alloc::vec::Vec;
    use core::fmt::{self, Display};
    use core::str::FromStr;

    use hashbrown::HashMap;
    use sqlparser::ast::{CreateTable, Statement};
    use sqlparser::dialect::SQLiteDialect;
    use sqlparser::parser::Parser;

    use super::{DiffSetBuilder, Operation};
    use crate::builders::{ChangeDelete, ChangesetFormat, Insert, PatchsetFormat, Update};
    use crate::errors::DiffSetParseError;
    use crate::schema::{DynTable, SchemaWithPK};

    impl DiffSetBuilder<ChangesetFormat, CreateTable> {
        /// Try to create a DiffSetBuilder from a slice of SQL statements.
        ///
        /// The statements must include CREATE TABLE statements before any DML
        /// (INSERT/UPDATE/DELETE) statements that reference those tables.
        ///
        /// # Errors
        ///
        /// Returns `DiffSetParseError` if:
        /// - A DML statement references a table that hasn't been created yet
        /// - A statement conversion fails
        /// - An unsupported statement type is encountered
        pub fn try_from_statements(statements: &[Statement]) -> Result<Self, DiffSetParseError> {
            let mut builder = Self::new();
            let mut schemas: HashMap<String, CreateTable> = HashMap::new();

            for stmt in statements {
                match stmt {
                    Statement::CreateTable(create) => {
                        let table_name = create.name().to_string();
                        schemas.insert(table_name, create.clone());
                    }
                    Statement::Insert(insert) => {
                        let table_name =
                            crate::builders::ast_helpers::extract_table_name(match &insert.table {
                                sqlparser::ast::TableObject::TableName(name) => name,
                                sqlparser::ast::TableObject::TableFunction(_) => {
                                    return Err(DiffSetParseError::UnsupportedStatement(
                                        "Table function in INSERT".into(),
                                    ));
                                }
                            });
                        let schema = schemas.get(table_name).ok_or_else(|| {
                            DiffSetParseError::TableNotFound(table_name.to_string())
                        })?;
                        let insert_op = Insert::try_from_ast(insert, schema)?;
                        // Convert to owned schema
                        let owned_insert =
                            Insert::from_values(schema.clone(), insert_op.into_values());
                        builder = builder.insert(owned_insert);
                    }
                    Statement::Update(update) => {
                        let table_name = match &update.table.relation {
                            sqlparser::ast::TableFactor::Table { name, .. } => {
                                crate::builders::ast_helpers::extract_table_name(name)
                            }
                            _ => {
                                return Err(DiffSetParseError::UnsupportedStatement(
                                    "Non-table relation in UPDATE".into(),
                                ));
                            }
                        };
                        let schema = schemas.get(table_name).ok_or_else(|| {
                            DiffSetParseError::TableNotFound(table_name.to_string())
                        })?;
                        let update_op =
                            Update::<&CreateTable, ChangesetFormat>::try_from_ast(update, schema)?;
                        // Convert to owned schema - extract old/new values
                        let values = update_op.values();
                        let old_values: Vec<_> =
                            values.iter().map(|(old, _)| old.clone()).collect();
                        let new_values: Vec<_> =
                            values.iter().map(|(_, new)| new.clone()).collect();
                        let owned_update =
                            Update::from_values(schema.clone(), old_values, new_values);
                        builder = builder.update(owned_update);
                    }
                    Statement::Delete(delete) => {
                        let table_name = match &delete.from {
                            sqlparser::ast::FromTable::WithFromKeyword(tables)
                            | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables
                                .first()
                                .and_then(|t| match &t.relation {
                                    sqlparser::ast::TableFactor::Table { name, .. } => {
                                        Some(crate::builders::ast_helpers::extract_table_name(name))
                                    }
                                    _ => None,
                                })
                                .unwrap_or(""),
                        };
                        let schema = schemas.get(table_name).ok_or_else(|| {
                            DiffSetParseError::TableNotFound(table_name.to_string())
                        })?;
                        let delete_op = ChangeDelete::try_from_ast(delete, schema)?;
                        // Convert to owned schema
                        let owned_delete =
                            ChangeDelete::from_values(schema.clone(), delete_op.values().to_vec());
                        builder = builder.delete(owned_delete);
                    }
                    other => {
                        return Err(DiffSetParseError::UnsupportedStatement(alloc::format!(
                            "{other:?}"
                        )));
                    }
                }
            }

            Ok(builder)
        }
    }

    impl TryFrom<&[Statement]> for DiffSetBuilder<ChangesetFormat, CreateTable> {
        type Error = DiffSetParseError;

        fn try_from(statements: &[Statement]) -> Result<Self, Self::Error> {
            Self::try_from_statements(statements)
        }
    }

    impl TryFrom<Vec<Statement>> for DiffSetBuilder<ChangesetFormat, CreateTable> {
        type Error = DiffSetParseError;

        fn try_from(statements: Vec<Statement>) -> Result<Self, Self::Error> {
            Self::try_from_statements(&statements)
        }
    }

    impl FromStr for DiffSetBuilder<ChangesetFormat, CreateTable> {
        type Err = DiffSetParseError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let dialect = SQLiteDialect {};
            let statements = Parser::parse_sql(&dialect, s)?;
            Self::try_from_statements(&statements)
        }
    }

    impl TryFrom<&str> for DiffSetBuilder<ChangesetFormat, CreateTable> {
        type Error = DiffSetParseError;

        fn try_from(s: &str) -> Result<Self, Self::Error> {
            s.parse()
        }
    }

    impl TryFrom<String> for DiffSetBuilder<ChangesetFormat, CreateTable> {
        type Error = DiffSetParseError;

        fn try_from(s: String) -> Result<Self, Self::Error> {
            s.parse()
        }
    }

    // =========================================================================
    // Patchset parsing (monodirectional - no Display)
    // =========================================================================

    impl DiffSetBuilder<PatchsetFormat, CreateTable> {
        /// Try to create a PatchSet DiffSetBuilder from a slice of SQL statements.
        ///
        /// The statements must include CREATE TABLE statements before any DML
        /// (INSERT/UPDATE/DELETE) statements that reference those tables.
        ///
        /// Note: Patchset format has limited information (no old values for updates,
        /// only PK for deletes), so this conversion may lose information compared
        /// to the original SQL.
        ///
        /// # Errors
        ///
        /// Returns `DiffSetParseError` if:
        /// - A DML statement references a table that hasn't been created yet
        /// - A statement conversion fails
        /// - An unsupported statement type is encountered
        pub fn try_from_statements(statements: &[Statement]) -> Result<Self, DiffSetParseError> {
            let mut builder = Self::new();
            let mut schemas: HashMap<String, CreateTable> = HashMap::new();

            for stmt in statements {
                match stmt {
                    Statement::CreateTable(create) => {
                        let table_name = create.name().to_string();
                        schemas.insert(table_name, create.clone());
                    }
                    Statement::Insert(insert) => {
                        let table_name =
                            crate::builders::ast_helpers::extract_table_name(match &insert.table {
                                sqlparser::ast::TableObject::TableName(name) => name,
                                sqlparser::ast::TableObject::TableFunction(_) => {
                                    return Err(DiffSetParseError::UnsupportedStatement(
                                        "Table function in INSERT".into(),
                                    ));
                                }
                            });
                        let schema = schemas.get(table_name).ok_or_else(|| {
                            DiffSetParseError::TableNotFound(table_name.to_string())
                        })?;
                        let insert_op = Insert::try_from_ast(insert, schema)?;
                        // Convert to owned schema
                        let owned_insert =
                            Insert::from_values(schema.clone(), insert_op.into_values());
                        builder = builder.insert(owned_insert);
                    }
                    Statement::Update(update) => {
                        let table_name = match &update.table.relation {
                            sqlparser::ast::TableFactor::Table { name, .. } => {
                                crate::builders::ast_helpers::extract_table_name(name)
                            }
                            _ => {
                                return Err(DiffSetParseError::UnsupportedStatement(
                                    "Non-table relation in UPDATE".into(),
                                ));
                            }
                        };
                        let schema = schemas.get(table_name).ok_or_else(|| {
                            DiffSetParseError::TableNotFound(table_name.to_string())
                        })?;
                        let update_op =
                            Update::<&CreateTable, PatchsetFormat>::try_from_ast(update, schema)?;
                        let new_values: Vec<_> = update_op
                            .values()
                            .iter()
                            .map(|((), new)| new.clone())
                            .collect();
                        let owned_update = Update::from_new_values(schema.clone(), new_values);
                        builder = builder.update(owned_update);
                    }
                    Statement::Delete(delete) => {
                        let table_name = match &delete.from {
                            sqlparser::ast::FromTable::WithFromKeyword(tables)
                            | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables
                                .first()
                                .and_then(|t| match &t.relation {
                                    sqlparser::ast::TableFactor::Table { name, .. } => {
                                        Some(crate::builders::ast_helpers::extract_table_name(name))
                                    }
                                    _ => None,
                                })
                                .unwrap_or(""),
                        };
                        let schema = schemas.get(table_name).ok_or_else(|| {
                            DiffSetParseError::TableNotFound(table_name.to_string())
                        })?;
                        // For patchset delete, we extract PK values from the WHERE clause
                        let delete_op = ChangeDelete::try_from_ast(delete, schema)?;
                        // Extract just the PK values
                        let pk = schema.extract_pk(delete_op.values());
                        builder = builder.delete(schema, &pk);
                    }
                    other => {
                        return Err(DiffSetParseError::UnsupportedStatement(alloc::format!(
                            "{other:?}"
                        )));
                    }
                }
            }

            Ok(builder)
        }
    }

    impl TryFrom<&[Statement]> for DiffSetBuilder<PatchsetFormat, CreateTable> {
        type Error = DiffSetParseError;

        fn try_from(statements: &[Statement]) -> Result<Self, Self::Error> {
            Self::try_from_statements(statements)
        }
    }

    impl TryFrom<Vec<Statement>> for DiffSetBuilder<PatchsetFormat, CreateTable> {
        type Error = DiffSetParseError;

        fn try_from(statements: Vec<Statement>) -> Result<Self, Self::Error> {
            Self::try_from_statements(&statements)
        }
    }

    impl FromStr for DiffSetBuilder<PatchsetFormat, CreateTable> {
        type Err = DiffSetParseError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let dialect = SQLiteDialect {};
            let statements = Parser::parse_sql(&dialect, s)?;
            Self::try_from_statements(&statements)
        }
    }

    impl TryFrom<&str> for DiffSetBuilder<PatchsetFormat, CreateTable> {
        type Error = DiffSetParseError;

        fn try_from(s: &str) -> Result<Self, Self::Error> {
            s.parse()
        }
    }

    impl TryFrom<String> for DiffSetBuilder<PatchsetFormat, CreateTable> {
        type Error = DiffSetParseError;

        fn try_from(s: String) -> Result<Self, Self::Error> {
            s.parse()
        }
    }

    // =========================================================================
    // Display for ChangeSet only (bidirectional conversion)
    // =========================================================================

    impl Display for DiffSetBuilder<ChangesetFormat, CreateTable> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let statements: Vec<Statement> = self.into();
            for stmt in statements {
                writeln!(f, "{stmt};")?;
            }
            Ok(())
        }
    }

    impl From<&DiffSetBuilder<ChangesetFormat, CreateTable>> for Vec<Statement> {
        fn from(builder: &DiffSetBuilder<ChangesetFormat, CreateTable>) -> Self {
            use sqlparser::ast;

            let mut statements = Vec::new();

            // First, emit CREATE TABLE statements for all tables in order
            for (table, rows) in &builder.tables {
                if !rows.is_empty() {
                    statements.push(Statement::CreateTable(table.clone()));
                }
            }

            // Then, emit operations for each table
            for (table, rows) in &builder.tables {
                if rows.is_empty() {
                    continue;
                }

                for (_pk, op) in rows {
                    match op {
                        Operation::Insert(values) => {
                            let owned_insert = Insert::from_values(table.clone(), values.clone());
                            let ast_insert: ast::Insert = (&owned_insert).into();
                            statements.push(Statement::Insert(ast_insert));
                        }
                        Operation::Delete(values) => {
                            let owned_delete =
                                ChangeDelete::from_values(table.clone(), values.clone());
                            let ast_delete: ast::Delete = (&owned_delete).into();
                            statements.push(Statement::Delete(ast_delete));
                        }
                        Operation::Update(pairs) => {
                            let old_values: Vec<_> =
                                pairs.iter().map(|(old, _)| old.clone()).collect();
                            let new_values: Vec<_> =
                                pairs.iter().map(|(_, new)| new.clone()).collect();
                            let owned_update =
                                Update::from_values(table.clone(), old_values, new_values);
                            let ast_update: ast::Update = (&owned_update).into();
                            statements.push(Statement::Update(ast_update));
                        }
                    }
                }
            }

            statements
        }
    }

    impl From<DiffSetBuilder<ChangesetFormat, CreateTable>> for Vec<Statement> {
        fn from(builder: DiffSetBuilder<ChangesetFormat, CreateTable>) -> Self {
            (&builder).into()
        }
    }
}
