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

use alloc::vec;
use alloc::vec::Vec;
use core::fmt::Debug;
use core::hash::Hash;

use crate::{
    builders::{
        format::Format, ChangeDelete, ChangesetFormat, Insert, Operation, PatchDelete,
        PatchsetFormat, Update,
    },
    encoding::{
        encode_defined_value, encode_undefined, encode_value, markers, op_codes, MaybeValue, Value,
    },
    SchemaWithPK,
};

/// `IndexMap` alias using hashbrown's default hasher for `no_std` compatibility.
type IndexMap<K, V> = IndexMapRaw<K, V, hashbrown::DefaultHashBuilder>;

/// Type alias for the row map in a table.
type RowMap<F, S, B> = IndexMap<Vec<Value<S, B>>, Operation<F, S, B>>;

/// Type alias for the table map.
type TableMap<F, T, S, B> = IndexMap<T, RowMap<F, S, B>>;

/// Type alias for a vector of rows in a table.
type RowVec<F, S, B> = Vec<(Vec<Value<S, B>>, Operation<F, S, B>)>;

/// Type alias for a vector of tables with their rows.
type TableVec<F, T, S, B> = Vec<(T, RowVec<F, S, B>)>;

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
fn session_hash_pk<S: AsRef<str>, B: AsRef<[u8]>>(pk: &[Value<S, B>]) -> u32 {
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
                h = session_hash_append_blob(h, s.as_ref().as_bytes());
            }
            Value::Blob(b) => {
                h = hash_append(h, 4); // SQLITE_BLOB
                h = session_hash_append_blob(h, b.as_ref());
            }
            Value::Null => {
                // NULL PKs: SQLite skips hashing for these.
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
fn session_row_order<S: AsRef<str>, B: AsRef<[u8]>, V>(
    rows: &IndexMap<Vec<Value<S, B>>, V>,
) -> Vec<usize> {
    let n = rows.len();
    if n == 0 {
        return Vec::new();
    }

    let pks: Vec<&Vec<Value<S, B>>> = rows.keys().collect();

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

// ============================================================================
// Shared encoding helpers
// ============================================================================

/// Write a table header to the output buffer.
///
/// Format:
/// - Table marker byte (`'T'` for changeset, `'P'` for patchset)
/// - Column count (1 byte)
/// - PK flags (1 byte per column: non-zero = PK ordinal, 0 = not PK)
/// - Table name (null-terminated UTF-8)
fn write_table_header<T: SchemaWithPK>(out: &mut Vec<u8>, marker: u8, table: &T) {
    out.push(marker);

    let num_cols = table.number_of_columns();
    out.push(u8::try_from(num_cols).unwrap());

    let pk_start = out.len();
    out.resize(pk_start + num_cols, 0);
    table.write_pk_flags(&mut out[pk_start..]);

    out.extend(table.name().as_bytes());
    out.push(0);
}

/// Build the column-index → PK-vector-position mapping used by patchset serialization.
///
/// Returns `(pk_flags, pk_col_to_pk_pos)` where `pk_col_to_pk_pos[col_idx]`
/// gives the index into the PK vector for PK columns, or `None` for non-PK columns.
fn patchset_pk_mapping<T: SchemaWithPK>(table: &T) -> (Vec<u8>, Vec<Option<usize>>) {
    let num_cols = table.number_of_columns();
    let mut pk_flags = alloc::vec![0u8; num_cols];
    table.write_pk_flags(&mut pk_flags);

    let mut pk_col_to_pk_pos: Vec<Option<usize>> = alloc::vec![None; num_cols];
    let mut pk_cols: Vec<(usize, u8)> = pk_flags
        .iter()
        .enumerate()
        .filter_map(|(i, &ord)| if ord > 0 { Some((i, ord)) } else { None })
        .collect();
    pk_cols.sort_by_key(|(_, ord)| *ord);
    for (pos, (col_idx, _)) in pk_cols.into_iter().enumerate() {
        pk_col_to_pk_pos[col_idx] = Some(pos);
    }

    (pk_flags, pk_col_to_pk_pos)
}

/// Encode patchset DELETE old values: PK columns get their values, non-PK columns are skipped.
fn encode_patchset_delete_values<S: AsRef<str>, B: AsRef<[u8]>>(
    out: &mut Vec<u8>,
    pk_flags: &[u8],
    pk_col_to_pk_pos: &[Option<usize>],
    pk: &[Value<S, B>],
) {
    for (col_idx, &pk_flag) in pk_flags.iter().enumerate() {
        if pk_flag > 0 {
            if let Some(pk_pos) = pk_col_to_pk_pos[col_idx] {
                encode_value(out, Some(&pk[pk_pos]));
            } else {
                encode_value::<S, B>(out, None);
            }
        }
    }
}

/// Encode patchset UPDATE old values: PK columns get their values, non-PK columns are undefined.
fn encode_patchset_update_old_values<S: AsRef<str>, B: AsRef<[u8]>>(
    out: &mut Vec<u8>,
    pk_flags: &[u8],
    pk_col_to_pk_pos: &[Option<usize>],
    pk: &[Value<S, B>],
) {
    for (col_idx, &pk_flag) in pk_flags.iter().enumerate() {
        if pk_flag > 0 {
            if let Some(pk_pos) = pk_col_to_pk_pos[col_idx] {
                encode_defined_value(out, &pk[pk_pos]);
            } else {
                encode_undefined(out);
            }
        } else {
            encode_undefined(out);
        }
    }
}

// ============================================================================
// DiffSetBuilder — mutable builder (DML insertion order, hash-simulated build)
// ============================================================================

/// Builder for constructing changeset or patchset binary data.
///
/// `DiffSetBuilder` tracks rows in DML insertion order. When [`build`](Self::build)
/// is called, it simulates SQLite's session-extension hash table to produce
/// byte-identical output.
///
/// For parsed (frozen) data that should be emitted in its original order,
/// see [`DiffSet`].
///
/// Generic over the format `F` (Changeset or Patchset), table schema `T`, and value types `S`, `B`.
#[derive(Debug, Clone)]
pub struct DiffSetBuilder<F: Format<S, B>, T: SchemaWithPK, S, B> {
    pub(crate) tables: TableMap<F, T, S, B>,
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
impl<F: Format<S, B>, T: SchemaWithPK, S, B> PartialEq for DiffSetBuilder<F, T, S, B>
where
    S: PartialEq + Eq + Hash + AsRef<str>,
    B: PartialEq + Eq + Hash + AsRef<[u8]>,
    F::Old: PartialEq,
    F::DeleteData: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        // Filter out tables with empty operations, then compare element by element.
        // IndexMap preserves insertion order, so this also checks table ordering.
        self.tables
            .iter()
            .filter(|(_, ops)| !ops.is_empty())
            .eq(other.tables.iter().filter(|(_, ops)| !ops.is_empty()))
    }
}

impl<F: Format<S, B>, T: SchemaWithPK, S, B> Eq for DiffSetBuilder<F, T, S, B>
where
    S: Eq + Hash + AsRef<str>,
    B: Eq + Hash + AsRef<[u8]>,
    F::Old: Eq,
    F::DeleteData: Eq,
{
}

/// Type alias for building changesets.
pub type ChangeSet<T, S, B> = DiffSetBuilder<ChangesetFormat, T, S, B>;
/// Type alias for building patchsets.
pub type PatchSet<T, S, B> = DiffSetBuilder<PatchsetFormat, T, S, B>;

impl<F: Format<S, B>, T: SchemaWithPK, S: AsRef<str> + Hash + Eq, B: AsRef<[u8]> + Hash + Eq>
    Default for DiffSetBuilder<F, T, S, B>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<
        T: SchemaWithPK,
        S: Clone + Debug + Hash + Eq + AsRef<str>,
        B: Clone + Debug + Hash + Eq + AsRef<[u8]>,
    > From<&DiffSetBuilder<ChangesetFormat, T, S, B>> for Vec<u8>
{
    #[inline]
    fn from(builder: &DiffSetBuilder<ChangesetFormat, T, S, B>) -> Self {
        builder.build()
    }
}

impl<
        T: SchemaWithPK,
        S: Clone + Debug + Hash + Eq + AsRef<str>,
        B: Clone + Debug + Hash + Eq + AsRef<[u8]>,
    > From<DiffSetBuilder<ChangesetFormat, T, S, B>> for Vec<u8>
{
    #[inline]
    fn from(builder: DiffSetBuilder<ChangesetFormat, T, S, B>) -> Self {
        builder.build()
    }
}

impl<T: SchemaWithPK, S: AsRef<str> + Clone + Hash + Eq, B: AsRef<[u8]> + Clone + Hash + Eq>
    From<&DiffSetBuilder<PatchsetFormat, T, S, B>> for Vec<u8>
{
    #[inline]
    fn from(builder: &DiffSetBuilder<PatchsetFormat, T, S, B>) -> Self {
        builder.build()
    }
}

impl<T: SchemaWithPK, S: AsRef<str> + Clone + Hash + Eq, B: AsRef<[u8]> + Clone + Hash + Eq>
    From<DiffSetBuilder<PatchsetFormat, T, S, B>> for Vec<u8>
{
    #[inline]
    fn from(builder: DiffSetBuilder<PatchsetFormat, T, S, B>) -> Self {
        builder.build()
    }
}

impl<F: Format<S, B>, T: SchemaWithPK, S, B> DiffSetBuilder<F, T, S, B> {
    /// Returns the table corresponding to the given name, if it exists in the builder.
    pub(super) fn table<'builder>(&'builder self, name: &str) -> Option<&'builder T> {
        self.tables.keys().find(|t| t.name() == name)
    }
}

impl<F: Format<S, B>, T: SchemaWithPK, S: AsRef<str> + Hash + Eq, B: AsRef<[u8]> + Hash + Eq>
    DiffSetBuilder<F, T, S, B>
{
    /// Create a new builder.
    #[inline]
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
    #[inline]
    fn ensure_table(&mut self, table: &T) -> &mut RowMap<F, S, B> {
        self.tables.entry(table.clone()).or_default()
    }

    /// Register a table schema without adding any operations.
    ///
    /// This is useful when you need the table present (e.g. before calling
    /// [`DiffSetBuilder::digest_sql`]) but don't have operations yet.
    /// If the table is already registered, this is a no-op.
    pub fn add_table(&mut self, table: &T) -> &mut Self {
        self.ensure_table(table);
        self
    }

    /// Returns true if the builder has no operations.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.values().all(IndexMap::is_empty)
    }

    /// Returns the number of operations across all tables.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.tables.values().map(IndexMap::len).sum()
    }

    /// Add any operation, consolidating with existing operations on the same row.
    ///
    /// The table schema is passed separately — operations are schema-less.
    pub(crate) fn add_operation(
        &mut self,
        table: &T,
        pk: Vec<Value<S, B>>,
        new_op: Operation<F, S, B>,
    ) -> &mut Self
    where
        S: Clone,
        B: Clone,
        Operation<F, S, B>: core::ops::Add<Output = Option<Operation<F, S, B>>>,
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
// DiffOps trait — unified insert / delete / update for DiffSetBuilder & DiffSet
// ============================================================================

/// Trait for adding DML operations (INSERT, DELETE, UPDATE) to a diff set.
///
/// Implemented for both [`DiffSetBuilder`] and [`DiffSet`], allowing
/// operations to be added to either type. Methods consume `self` and
/// return a [`DiffSetBuilder`].
pub trait DiffOps<T: SchemaWithPK, S, B>: Sized {
    /// The format (changeset or patchset) of the diff set.
    type Format: Format<S, B>;

    /// The argument type for the [`delete`](Self::delete) operation.
    ///
    /// * Changeset: [`ChangeDelete<T, S, B>`]
    /// * Patchset: [`PatchDelete<T, S, B>`]
    type DeleteArg;

    /// Add an INSERT operation.
    fn insert(self, insert: Insert<T, S, B>) -> DiffSetBuilder<Self::Format, T, S, B>;

    /// Add a DELETE operation.
    fn delete(self, delete: Self::DeleteArg) -> DiffSetBuilder<Self::Format, T, S, B>;

    /// Add an UPDATE operation.
    fn update(self, update: Update<T, Self::Format, S, B>)
        -> DiffSetBuilder<Self::Format, T, S, B>;
}

// -- DiffOps for DiffSetBuilder<ChangesetFormat> ------------------------------

impl<
        T: SchemaWithPK,
        S: Clone + Debug + Hash + Eq + AsRef<str>,
        B: Clone + Debug + Hash + Eq + AsRef<[u8]>,
    > DiffOps<T, S, B> for DiffSetBuilder<ChangesetFormat, T, S, B>
{
    type Format = ChangesetFormat;
    type DeleteArg = ChangeDelete<T, S, B>;

    fn insert(mut self, insert: Insert<T, S, B>) -> Self {
        let pk = insert.extract_pk();
        let table = insert.as_ref().clone();
        self.add_operation(&table, pk, Operation::Insert(insert.into_values()));
        self
    }

    fn delete(mut self, delete: ChangeDelete<T, S, B>) -> Self {
        let pk = delete.as_ref().extract_pk(&delete.values);
        let table = delete.as_ref().clone();
        self.add_operation(&table, pk, Operation::Delete(delete.into_values()));
        self
    }

    fn update(mut self, update: Update<T, ChangesetFormat, S, B>) -> Self {
        let old_values: Vec<_> = update
            .values()
            .iter()
            .map(|(old, _): &(_, _)| old.clone().unwrap_or(Value::Null))
            .collect();
        let pk = update.as_ref().extract_pk(&old_values);
        let table = update.as_ref().clone();
        let values: Vec<(MaybeValue<S, B>, MaybeValue<S, B>)> = update.into();
        self.add_operation(&table, pk, Operation::Update(values));
        self
    }
}

// -- DiffOps for DiffSetBuilder<PatchsetFormat> -------------------------------

impl<T: SchemaWithPK, S: Clone + Hash + Eq + AsRef<str>, B: Clone + Hash + Eq + AsRef<[u8]>>
    DiffOps<T, S, B> for DiffSetBuilder<PatchsetFormat, T, S, B>
{
    type Format = PatchsetFormat;
    type DeleteArg = PatchDelete<T, S, B>;

    fn insert(mut self, insert: Insert<T, S, B>) -> Self {
        let pk = insert.extract_pk();
        let table = insert.as_ref().clone();
        self.add_operation(&table, pk, Operation::Insert(insert.into_values()));
        self
    }

    /// Delete by primary key.
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_diff_rs::{DiffOps, PatchDelete, PatchSet, SchemaWithPK, TableSchema};
    ///
    /// // CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)
    /// let schema: TableSchema<String> = TableSchema::new("users".into(), 2, vec![1, 0]);
    ///
    /// // Delete row where id = 1
    /// let patchset = PatchSet::<_, String, Vec<u8>>::new()
    ///     .delete(PatchDelete::new(schema, vec![1i64.into()]));
    /// ```
    fn delete(mut self, delete: PatchDelete<T, S, B>) -> Self {
        self.add_operation(&delete.table, delete.pk, Operation::Delete(()));
        self
    }

    /// Update by primary key.
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_diff_rs::{DiffOps, PatchSet, PatchsetFormat, Update, TableSchema};
    ///
    /// // CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)
    /// let schema: TableSchema<String> = TableSchema::new("users".into(), 2, vec![1, 0]);
    ///
    /// // UPDATE users SET name = 'Bob' WHERE id = 1
    /// let update = Update::<_, PatchsetFormat, String, Vec<u8>>::from(schema)
    ///     .set(0, 1i64).unwrap()  // PK value
    ///     .set(1, "Bob").unwrap();
    ///
    /// let patchset = PatchSet::<_, String, Vec<u8>>::new()
    ///     .update(update);
    /// ```
    fn update(mut self, update: Update<T, PatchsetFormat, S, B>) -> Self {
        let pk = update.extract_pk();
        let table = update.as_ref().clone();
        let values: Vec<((), MaybeValue<S, B>)> = update.into();
        self.add_operation(&table, pk, Operation::Update(values));
        self
    }
}

// -- DiffOps for DiffSet<ChangesetFormat> -------------------------------------

impl<
        T: SchemaWithPK,
        S: Clone + Debug + Hash + Eq + AsRef<str>,
        B: Clone + Debug + Hash + Eq + AsRef<[u8]>,
    > DiffOps<T, S, B> for DiffSet<ChangesetFormat, T, S, B>
{
    type Format = ChangesetFormat;
    type DeleteArg = ChangeDelete<T, S, B>;

    fn insert(self, insert: Insert<T, S, B>) -> DiffSetBuilder<ChangesetFormat, T, S, B> {
        let builder: DiffSetBuilder<ChangesetFormat, T, S, B> = self.into();
        builder.insert(insert)
    }

    fn delete(self, delete: ChangeDelete<T, S, B>) -> DiffSetBuilder<ChangesetFormat, T, S, B> {
        let builder: DiffSetBuilder<ChangesetFormat, T, S, B> = self.into();
        builder.delete(delete)
    }

    fn update(
        self,
        update: Update<T, ChangesetFormat, S, B>,
    ) -> DiffSetBuilder<ChangesetFormat, T, S, B> {
        let builder: DiffSetBuilder<ChangesetFormat, T, S, B> = self.into();
        builder.update(update)
    }
}

// -- DiffOps for DiffSet<PatchsetFormat> --------------------------------------

impl<T: SchemaWithPK, S: Clone + Hash + Eq + AsRef<str>, B: Clone + Hash + Eq + AsRef<[u8]>>
    DiffOps<T, S, B> for DiffSet<PatchsetFormat, T, S, B>
{
    type Format = PatchsetFormat;
    type DeleteArg = PatchDelete<T, S, B>;

    fn insert(self, insert: Insert<T, S, B>) -> DiffSetBuilder<PatchsetFormat, T, S, B> {
        let builder: DiffSetBuilder<PatchsetFormat, T, S, B> = self.into();
        builder.insert(insert)
    }

    fn delete(self, delete: PatchDelete<T, S, B>) -> DiffSetBuilder<PatchsetFormat, T, S, B> {
        let builder: DiffSetBuilder<PatchsetFormat, T, S, B> = self.into();
        builder.delete(delete)
    }

    fn update(
        self,
        update: Update<T, PatchsetFormat, S, B>,
    ) -> DiffSetBuilder<PatchsetFormat, T, S, B> {
        let builder: DiffSetBuilder<PatchsetFormat, T, S, B> = self.into();
        builder.update(update)
    }
}

impl<T: crate::schema::NamedColumns, S: Clone + Hash + Eq + AsRef<str> + for<'a> From<&'a str>>
    DiffSetBuilder<PatchsetFormat, T, S, Vec<u8>>
{
    /// Digest a SQL string containing INSERT, UPDATE, and DELETE statements
    /// into this patchset builder.
    ///
    /// The SQL statements are parsed and their effects are directly applied
    /// to the builder, consolidating operations by primary key as usual.
    ///
    /// # Errors
    ///
    /// Returns a [`crate::builders::sql::ParseError`] if the SQL cannot be parsed.
    pub fn digest_sql<'input>(
        &mut self,
        input: &'input str,
    ) -> Result<&mut Self, crate::builders::sql::ParseError<'input>> {
        let mut parser = crate::builders::sql::Parser::new(input, self);
        parser.digest_all()?;
        Ok(self)
    }
}

// ============================================================================
// Unified build implementation
// ============================================================================

impl<
        T: SchemaWithPK,
        S: Clone + Debug + Hash + Eq + AsRef<str>,
        B: Clone + Debug + Hash + Eq + AsRef<[u8]>,
    > DiffSetBuilder<ChangesetFormat, T, S, B>
{
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

            write_table_header(&mut out, markers::CHANGESET, table);

            for idx in session_row_order(rows) {
                let (_pk, op) = rows.get_index(idx).unwrap();
                match op {
                    Operation::Insert(values) => {
                        out.push(op_codes::INSERT);
                        out.push(0);
                        for value in values {
                            encode_value(&mut out, Some(value));
                        }
                    }
                    Operation::Delete(values) => {
                        out.push(op_codes::DELETE);
                        out.push(0);
                        for value in values {
                            encode_value(&mut out, Some(value));
                        }
                    }
                    Operation::Update(values) => {
                        out.push(op_codes::UPDATE);
                        out.push(0);
                        // Write old values, then new values
                        for (old, _new) in values {
                            encode_value(&mut out, old.as_ref());
                        }
                        for (_old, new) in values {
                            encode_value(&mut out, new.as_ref());
                        }
                    }
                }
            }
        }

        out
    }
}

impl<T: SchemaWithPK, S: Clone + Hash + Eq + AsRef<str>, B: Clone + Hash + Eq + AsRef<[u8]>>
    DiffSetBuilder<PatchsetFormat, T, S, B>
{
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

            write_table_header(&mut out, markers::PATCHSET, table);

            let (pk_flags, pk_col_to_pk_pos) = patchset_pk_mapping(table);

            for idx in session_row_order(rows) {
                let (pk, op) = rows.get_index(idx).unwrap();
                match op {
                    Operation::Insert(values) => {
                        out.push(op_codes::INSERT);
                        out.push(0);
                        for value in values {
                            encode_value(&mut out, Some(value));
                        }
                    }
                    Operation::Delete(()) => {
                        out.push(op_codes::DELETE);
                        out.push(0);
                        encode_patchset_delete_values(&mut out, &pk_flags, &pk_col_to_pk_pos, pk);
                    }
                    Operation::Update(values) => {
                        out.push(op_codes::UPDATE);
                        out.push(0);
                        encode_patchset_update_old_values(
                            &mut out,
                            &pk_flags,
                            &pk_col_to_pk_pos,
                            pk,
                        );
                        // Write new values
                        for ((), new) in values {
                            encode_value(&mut out, new.as_ref());
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

impl<
        T: SchemaWithPK,
        S: Clone + Debug + Hash + Eq + AsRef<str>,
        B: Clone + Debug + Hash + Eq + AsRef<[u8]>,
    > Reverse for DiffSetBuilder<ChangesetFormat, T, S, B>
{
    type Output = DiffSetBuilder<ChangesetFormat, T, S, B>;

    fn reverse(self) -> Self::Output {
        let mut reversed: DiffSetBuilder<ChangesetFormat, T, S, B> = DiffSetBuilder::new();

        for (table, rows) in self.tables {
            for (pk, op) in rows {
                let rev_op = op.reverse();

                reversed.add_operation(&table, pk, rev_op);
            }
        }

        reversed
    }
}

// ============================================================================
// DiffSet — frozen (parsed) changeset/patchset with sequential row order
// ============================================================================

/// A frozen changeset or patchset whose rows are emitted in stored order.
///
/// `DiffSet` is produced by the binary parser (via [`ParsedDiffSet`](crate::parser::ParsedDiffSet))
/// or by calling [`DiffSetBuilder::freeze`].  Unlike [`DiffSetBuilder`], it
/// stores tables and rows in a plain `Vec`, reflecting the fact that no
/// further mutation or PK-based lookup is needed.
///
/// [`build`](Self::build) serializes rows in the order they are stored — no
/// session hash-table simulation is applied.  This preserves the original
/// row order of parsed binary data across roundtrips.
///
/// To modify a `DiffSet`, convert it back to a [`DiffSetBuilder`] with
/// [`into_builder`](Self::into_builder).
#[derive(Debug, Clone)]
pub struct DiffSet<F: Format<S, B>, T: SchemaWithPK, S, B> {
    /// Tables and their rows, stored in order.  Each row is a `(pk, operation)` pair.
    pub(crate) tables: TableVec<F, T, S, B>,
}

/// Custom `PartialEq` that ignores tables with no operations (same semantics
/// as `DiffSetBuilder`).
impl<F: Format<S, B>, T: SchemaWithPK, S, B> PartialEq for DiffSet<F, T, S, B>
where
    S: PartialEq + Eq + Hash + AsRef<str>,
    B: PartialEq + Eq + Hash + AsRef<[u8]>,
    F::Old: PartialEq,
    F::DeleteData: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.tables
            .iter()
            .filter(|(_, ops)| !ops.is_empty())
            .eq(other.tables.iter().filter(|(_, ops)| !ops.is_empty()))
    }
}

impl<F: Format<S, B>, T: SchemaWithPK, S, B> Eq for DiffSet<F, T, S, B>
where
    S: Eq + Hash + AsRef<str>,
    B: Eq + Hash + AsRef<[u8]>,
    F::Old: Eq,
    F::DeleteData: Eq,
{
}

impl<F: Format<S, B>, T: SchemaWithPK, S: AsRef<str> + Hash + Eq, B: AsRef<[u8]> + Hash + Eq>
    Default for DiffSet<F, T, S, B>
{
    fn default() -> Self {
        Self { tables: Vec::new() }
    }
}

impl<F: Format<S, B>, T: SchemaWithPK, S: AsRef<str> + Hash + Eq, B: AsRef<[u8]> + Hash + Eq>
    DiffSet<F, T, S, B>
{
    /// Returns `true` if there are no operations in any table.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.iter().all(|(_, rows)| rows.is_empty())
    }

    /// Returns the total number of operations across all tables.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.tables.iter().map(|(_, rows)| rows.len()).sum()
    }
}

// -- Changeset build (DiffSet) ------------------------------------------------

impl<
        T: SchemaWithPK,
        S: Clone + Debug + Hash + Eq + AsRef<str>,
        B: Clone + Debug + Hash + Eq + AsRef<[u8]>,
    > DiffSet<ChangesetFormat, T, S, B>
{
    /// Serialize the changeset to binary.
    ///
    /// Rows are emitted in stored order (no hash simulation).
    #[must_use]
    pub fn build(&self) -> Vec<u8> {
        let mut out = Vec::new();

        for (table, rows) in &self.tables {
            if rows.is_empty() {
                continue;
            }

            write_table_header(&mut out, markers::CHANGESET, table);

            for (_pk, op) in rows {
                match op {
                    Operation::Insert(values) => {
                        out.push(op_codes::INSERT);
                        out.push(0);
                        for value in values {
                            encode_value(&mut out, Some(value));
                        }
                    }
                    Operation::Delete(values) => {
                        out.push(op_codes::DELETE);
                        out.push(0);
                        for value in values {
                            encode_value(&mut out, Some(value));
                        }
                    }
                    Operation::Update(values) => {
                        out.push(op_codes::UPDATE);
                        out.push(0);
                        for (old, _new) in values {
                            encode_value(&mut out, old.as_ref());
                        }
                        for (_old, new) in values {
                            encode_value(&mut out, new.as_ref());
                        }
                    }
                }
            }
        }

        out
    }
}

// -- Patchset build (DiffSet) -------------------------------------------------

impl<T: SchemaWithPK, S: Clone + Hash + Eq + AsRef<str>, B: Clone + Hash + Eq + AsRef<[u8]>>
    DiffSet<PatchsetFormat, T, S, B>
{
    /// Serialize the patchset to binary.
    ///
    /// Rows are emitted in stored order (no hash simulation).
    #[must_use]
    pub fn build(&self) -> Vec<u8> {
        let mut out = Vec::new();

        for (table, rows) in &self.tables {
            if rows.is_empty() {
                continue;
            }

            write_table_header(&mut out, markers::PATCHSET, table);

            let (pk_flags, pk_col_to_pk_pos) = patchset_pk_mapping(table);

            for (pk, op) in rows {
                match op {
                    Operation::Insert(values) => {
                        out.push(op_codes::INSERT);
                        out.push(0);
                        for value in values {
                            encode_value(&mut out, Some(value));
                        }
                    }
                    Operation::Delete(()) => {
                        out.push(op_codes::DELETE);
                        out.push(0);
                        encode_patchset_delete_values(&mut out, &pk_flags, &pk_col_to_pk_pos, pk);
                    }
                    Operation::Update(values) => {
                        out.push(op_codes::UPDATE);
                        out.push(0);
                        encode_patchset_update_old_values(
                            &mut out,
                            &pk_flags,
                            &pk_col_to_pk_pos,
                            pk,
                        );
                        for ((), new) in values {
                            encode_value(&mut out, new.as_ref());
                        }
                    }
                }
            }
        }

        out
    }
}

// -- From<DiffSet> for Vec<u8> ------------------------------------------------

impl<
        T: SchemaWithPK,
        S: Clone + Debug + Hash + Eq + AsRef<str>,
        B: Clone + Debug + Hash + Eq + AsRef<[u8]>,
    > From<&DiffSet<ChangesetFormat, T, S, B>> for Vec<u8>
{
    #[inline]
    fn from(diffset: &DiffSet<ChangesetFormat, T, S, B>) -> Self {
        diffset.build()
    }
}

impl<
        T: SchemaWithPK,
        S: Clone + Debug + Hash + Eq + AsRef<str>,
        B: Clone + Debug + Hash + Eq + AsRef<[u8]>,
    > From<DiffSet<ChangesetFormat, T, S, B>> for Vec<u8>
{
    #[inline]
    fn from(diffset: DiffSet<ChangesetFormat, T, S, B>) -> Self {
        diffset.build()
    }
}

impl<T: SchemaWithPK, S: AsRef<str> + Clone + Hash + Eq, B: AsRef<[u8]> + Clone + Hash + Eq>
    From<&DiffSet<PatchsetFormat, T, S, B>> for Vec<u8>
{
    #[inline]
    fn from(diffset: &DiffSet<PatchsetFormat, T, S, B>) -> Self {
        diffset.build()
    }
}

impl<T: SchemaWithPK, S: AsRef<str> + Clone + Hash + Eq, B: AsRef<[u8]> + Clone + Hash + Eq>
    From<DiffSet<PatchsetFormat, T, S, B>> for Vec<u8>
{
    #[inline]
    fn from(diffset: DiffSet<PatchsetFormat, T, S, B>) -> Self {
        diffset.build()
    }
}

// -- Reverse for DiffSet<ChangesetFormat> -------------------------------------

impl<
        T: SchemaWithPK,
        S: Clone + Debug + Hash + Eq + AsRef<str>,
        B: Clone + Debug + Hash + Eq + AsRef<[u8]>,
    > Reverse for DiffSet<ChangesetFormat, T, S, B>
{
    type Output = DiffSet<ChangesetFormat, T, S, B>;

    fn reverse(self) -> Self::Output {
        DiffSet {
            tables: self
                .tables
                .into_iter()
                .map(|(table, rows)| {
                    let rev_rows = rows
                        .into_iter()
                        .map(|(pk, op)| (pk, op.reverse()))
                        .collect();
                    (table, rev_rows)
                })
                .collect(),
        }
    }
}

// -- From conversions between DiffSetBuilder and DiffSet ----------------------

impl<F: Format<S, B>, T: SchemaWithPK, S: Hash + Eq + AsRef<str>, B: Hash + Eq + AsRef<[u8]>>
    From<DiffSetBuilder<F, T, S, B>> for DiffSet<F, T, S, B>
{
    fn from(builder: DiffSetBuilder<F, T, S, B>) -> Self {
        Self {
            tables: builder
                .tables
                .into_iter()
                .map(|(table, rows)| {
                    let ordered_rows: RowVec<F, S, B> = rows.into_iter().collect();
                    (table, ordered_rows)
                })
                .collect(),
        }
    }
}

impl<F: Format<S, B>, T: SchemaWithPK, S: Hash + Eq + AsRef<str>, B: Hash + Eq + AsRef<[u8]>>
    From<DiffSet<F, T, S, B>> for DiffSetBuilder<F, T, S, B>
{
    fn from(diffset: DiffSet<F, T, S, B>) -> Self {
        let mut builder = Self::new();
        for (table, rows) in diffset.tables {
            let map: IndexMap<Vec<Value<S, B>>, Operation<F, S, B>> = rows.into_iter().collect();
            builder.tables.insert(table, map);
        }
        builder
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
        fn number_of_primary_keys(&self) -> usize {
            1
        }

        fn primary_key_index(&self, col_idx: usize) -> Option<usize> {
            if col_idx == self.pk_column {
                Some(0)
            } else {
                None
            }
        }

        fn extract_pk<S: Clone, B: Clone>(
            &self,
            values: &impl crate::IndexableValues<Text = S, Binary = B>,
        ) -> alloc::vec::Vec<Value<S, B>> {
            alloc::vec![values
                .get(self.pk_column)
                .expect("primary key column index out of bounds — values shorter than schema")]
        }
    }

    // Type alias for cleaner test code
    type ChangesetBuilder = DiffSetBuilder<ChangesetFormat, TestTable, String, Vec<u8>>;

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

        let update = Update::<TestTable, ChangesetFormat, String, Vec<u8>>::from(table.clone())
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

        let update1 = Update::<TestTable, ChangesetFormat, String, Vec<u8>>::from(table.clone())
            .set(0, 1i64, 1i64)
            .unwrap()
            .set(1, "alice", "bob")
            .unwrap();

        let update2 = Update::<TestTable, ChangesetFormat, String, Vec<u8>>::from(table.clone())
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
        let op: Operation<ChangesetFormat, String, Vec<u8>> =
            Operation::Insert(vec![Value::Integer(1), Value::Text("alice".into())]);
        let reversed = op.reverse();
        assert!(matches!(reversed, Operation::Delete(_)));
        if let Operation::Delete(values) = reversed {
            assert_eq!(
                values,
                vec![
                    Value::<String, Vec<u8>>::Integer(1),
                    Value::Text("alice".into())
                ]
            );
        }
    }

    #[test]
    fn test_reverse_operation_delete_becomes_insert() {
        let op: Operation<ChangesetFormat, String, Vec<u8>> =
            Operation::Delete(vec![Value::Integer(1), Value::Text("alice".into())]);
        let reversed = op.reverse();
        assert!(matches!(reversed, Operation::Insert(_)));
        if let Operation::Insert(values) = reversed {
            assert_eq!(
                values,
                vec![
                    Value::<String, Vec<u8>>::Integer(1),
                    Value::Text("alice".into())
                ]
            );
        }
    }

    #[test]
    fn test_reverse_operation_update_swaps_old_new() {
        let op: Operation<ChangesetFormat, String, Vec<u8>> = Operation::Update(vec![
            (Some(Value::Integer(1)), Some(Value::Integer(1))),
            (
                Some(Value::Text("alice".into())),
                Some(Value::Text("bob".into())),
            ),
        ]);
        let reversed = op.reverse();
        if let Operation::Update(values) = reversed {
            assert_eq!(
                values[0],
                (Some(Value::Integer(1)), Some(Value::Integer(1)))
            );
            assert_eq!(
                values[1],
                (
                    Some(Value::Text("bob".into())),
                    Some(Value::Text("alice".into()))
                )
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
        let update = Update::<TestTable, ChangesetFormat, String, Vec<u8>>::from(table.clone())
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
                (
                    Some(Value::Text("bob".into())),
                    Some(Value::Text("alice".into()))
                )
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

        let original_values = insert.into_values();
        let insert2 = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();
        let builder = ChangesetBuilder::new().insert(insert2);
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

        let builder = ChangesetBuilder::new().insert(insert);
        let bytes = builder.build();

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

        let builder = ChangesetBuilder::new().delete(delete);
        let bytes = builder.build();

        assert!(!bytes.is_empty());
        assert_eq!(bytes[0], b'T');
        // Operation code: DELETE = 0x09
        assert_eq!(bytes[6], 0x09);
    }

    #[test]
    fn test_build_update_format() {
        let table = TestTable::new("t", 2, 0);
        let update = Update::<TestTable, ChangesetFormat, String, Vec<u8>>::from(table.clone())
            .set(0, 1i64, 1i64)
            .unwrap()
            .set(1, "a", "b")
            .unwrap();

        let builder = ChangesetBuilder::new().update(update);
        let bytes = builder.build();

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

        let builder = ChangesetBuilder::new().insert(insert).insert(insert2);
        let bytes = builder.build();

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

        let builder = ChangesetBuilder::new().insert(insert).delete(delete);
        let bytes = builder.build();

        // INSERT + DELETE with same values cancels out
        assert!(bytes.is_empty());
    }
}
