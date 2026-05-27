//! `DiffSet` builder for constructing changeset/patchset binary data.
//!
//! [`DiffSetBuilder`] builds `SQLite` session-extension changesets and
//! patchsets. It tracks row state and consolidates operations according to
//! `SQLite`'s changegroup semantics.
//!
//! # Terminology
//!
//! A *changeset* carries full row data for all operations and is invertible.
//! A *patchset* carries only the PK for deletes and only the changed columns
//! for updates. *`DiffSet`* is the generic term for either.
//!
//! # Consolidation Rules
//!
//! When multiple operations affect the same row (by primary key), they are
//! consolidated according to `SQLite`'s `sqlite3changegroup_add()` semantics:
//!
//! | Existing | New | Result |
//! |----------|--------|--------|
//! | INSERT | INSERT | Ignore new |
//! | INSERT | UPDATE | INSERT with updated values |
//! | INSERT | DELETE | Remove both (no-op) |
//! | UPDATE | INSERT | Ignore new |
//! | UPDATE | UPDATE | Single UPDATE original to final |
//! | UPDATE | DELETE | DELETE of original |
//! | DELETE | INSERT | UPDATE if different, no-op if same |
//! | DELETE | UPDATE | Ignore new |
//! | DELETE | DELETE | Ignore new |
//!
//! # Merging Changesets / Patchsets
//!
//! Two changesets or patchsets can be merged using the `|` (BitOr) operator,
//! which is equivalent to SQLite's `sqlite3changeset_concat()`:
//!
//! ```ignore
//! let combined = changeset_a | changeset_b;
//! // or in-place:
//! changeset_a |= changeset_b;
//! ```
//!
//! Operations affecting the same row are consolidated using the rules above.

use indexmap::IndexMap as IndexMapRaw;

use alloc::vec;
use alloc::vec::Vec;
use core::fmt::Debug;
use core::hash::Hash;
use core::ops::{BitOr, BitOrAssign};

use crate::{
    SchemaWithPK,
    builders::{
        ChangeDelete, ChangesetFormat, ChangesetOp, Insert, Operation, PatchDelete, PatchsetFormat,
        PatchsetOp, Update, format::Format,
    },
    encoding::{
        MaybeValue, Value, encode_defined_value, encode_undefined, encode_value, markers, op_codes,
    },
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

/// The core hash-combine step used throughout `SQLite`'s session extension.
///
/// Matches the C macro: `#define HASH_APPEND(hash, add) ((hash) << 3) ^ (hash) ^ (unsigned int)(add)`
const fn hash_append(h: u32, add: u32) -> u32 {
    (h << 3) ^ h ^ add
}

/// Hash a 64-bit integer using `SQLite`'s `sessionHashAppendI64`.
///
/// Hashes the lower 32 bits first, then the upper 32 bits.
#[allow(clippy::cast_sign_loss)]
fn session_hash_append_i64(h: u32, i: i64) -> u32 {
    let lo = (i as u64 & 0xFFFF_FFFF) as u32;
    let hi = ((i as u64 >> 32) & 0xFFFF_FFFF) as u32;
    let h = hash_append(h, lo);
    hash_append(h, hi)
}

/// Hash a blob using `SQLite`'s `sessionHashAppendBlob`.
///
/// Applies `HASH_APPEND` to each byte.
fn session_hash_append_blob(mut h: u32, data: &[u8]) -> u32 {
    for &byte in data {
        h = hash_append(h, u32::from(byte));
    }
    h
}

/// Hash a primary key using `SQLite`'s `sessionPreupdateHash` algorithm.
///
/// For each PK value: `h = HASH_APPEND(h, type_code)`, then hash the value.
/// Type codes match `SQLite`: INTEGER=1, FLOAT=2, TEXT=3, BLOB=4.
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

/// Simulate `SQLite`'s session extension hash table to determine row output order.
///
/// `SQLite`'s session extension tracks changes in a hash table where:
/// - New entries are prepended to their bucket (most recent at list head)
/// - The table starts at 256 buckets and doubles when entries >= buckets/2
/// - Changeset iteration walks buckets 0..n-1, following each linked list
///
/// This function returns indices into `rows` in the order that `SQLite`'s
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

/// Build the column-index to PK-vector-position mapping used by patchset serialization.
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

/// Encode a single changeset operation (op_code, indirect byte, then row payload).
fn encode_changeset_op<S: AsRef<str> + Clone + Debug, B: AsRef<[u8]> + Clone + Debug>(
    out: &mut Vec<u8>,
    op: &Operation<ChangesetFormat, S, B>,
) {
    match op {
        Operation::Insert { values, indirect } => {
            out.push(op_codes::INSERT);
            out.push(u8::from(*indirect));
            for value in values {
                encode_value(out, Some(value));
            }
        }
        Operation::Delete {
            data: values,
            indirect,
        } => {
            out.push(op_codes::DELETE);
            out.push(u8::from(*indirect));
            for value in values {
                encode_value(out, Some(value));
            }
        }
        Operation::Update { values, indirect } => {
            out.push(op_codes::UPDATE);
            out.push(u8::from(*indirect));
            for (old, _new) in values {
                encode_value(out, old.as_ref());
            }
            for (_old, new) in values {
                encode_value(out, new.as_ref());
            }
        }
    }
}

/// Encode a single patchset operation. The PK and per-table PK mapping are
/// supplied because DELETE/UPDATE rows in patchset format derive their old-value
/// section from the row's PK rather than from data carried on the operation.
fn encode_patchset_op<S: AsRef<str>, B: AsRef<[u8]>>(
    out: &mut Vec<u8>,
    op: &Operation<PatchsetFormat, S, B>,
    pk: &[Value<S, B>],
    pk_flags: &[u8],
    pk_col_to_pk_pos: &[Option<usize>],
) {
    match op {
        Operation::Insert { values, indirect } => {
            out.push(op_codes::INSERT);
            out.push(u8::from(*indirect));
            for value in values {
                encode_value(out, Some(value));
            }
        }
        Operation::Delete { data: (), indirect } => {
            out.push(op_codes::DELETE);
            out.push(u8::from(*indirect));
            encode_patchset_delete_values(out, pk_flags, pk_col_to_pk_pos, pk);
        }
        Operation::Update { values, indirect } => {
            out.push(op_codes::UPDATE);
            out.push(u8::from(*indirect));
            encode_patchset_update_old_values(out, pk_flags, pk_col_to_pk_pos, pk);
            for ((), new) in values {
                encode_value(out, new.as_ref());
            }
        }
    }
}

// ============================================================================
// DiffSetBuilder: mutable builder (DML insertion order, hash-simulated build)
// ============================================================================

/// Builder for constructing changeset or patchset binary data.
///
/// `DiffSetBuilder` tracks rows in DML insertion order. When [`build`](Self::build)
/// is called, it simulates `SQLite`'s session-extension hash table to produce
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

/// Custom `PartialEq` that ignores tables with empty operations.
///
/// Tables with no operations are not serialized (skipped in `build()`), so after
/// roundtrip they won't exist. This makes empty tables semantically equivalent
/// to non-existent tables for comparison purposes.
///
/// Verified: `SQLite`'s session extension does NOT include empty table entries in
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
    /// The table schema is passed separately, operations are schema-less.
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
                    (Operation::Insert { .. }, Operation::Update { .. }) => {
                        // Apply update to insert values, then re-extract PK
                        if let Some(combined) = existing + new_op
                            && let Operation::Insert { values, .. } = &combined
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
// DiffOps trait: unified insert / delete / update for DiffSetBuilder & DiffSet
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
        let indirect = insert.indirect;
        self.add_operation(
            &table,
            pk,
            Operation::Insert {
                values: insert.into_values(),
                indirect,
            },
        );
        self
    }

    fn delete(mut self, delete: ChangeDelete<T, S, B>) -> Self {
        let pk = delete.as_ref().extract_pk(&delete.values);
        let table = delete.as_ref().clone();
        let indirect = delete.indirect;
        self.add_operation(
            &table,
            pk,
            Operation::Delete {
                data: delete.into_values(),
                indirect,
            },
        );
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
        let indirect = update.indirect;
        let values: Vec<(MaybeValue<S, B>, MaybeValue<S, B>)> = update.into();
        self.add_operation(&table, pk, Operation::Update { values, indirect });
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
        let indirect = insert.indirect;
        self.add_operation(
            &table,
            pk,
            Operation::Insert {
                values: insert.into_values(),
                indirect,
            },
        );
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
        let indirect = delete.indirect;
        self.add_operation(
            &delete.table,
            delete.pk,
            Operation::Delete { data: (), indirect },
        );
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
        let indirect = update.indirect;
        let values: Vec<((), MaybeValue<S, B>)> = update.into();
        self.add_operation(&table, pk, Operation::Update { values, indirect });
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
    /// Returns the binary representation compatible with `SQLite`'s session extension.
    ///
    /// # Panics
    ///
    /// This function does not panic under normal usage. Internal indexing is guaranteed
    /// to be within bounds.
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
                encode_changeset_op(&mut out, op);
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
    /// Returns the binary representation compatible with `SQLite`'s session extension.
    ///
    /// # Panics
    ///
    /// This function does not panic under normal usage. Internal indexing is guaranteed
    /// to be within bounds.
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
                encode_patchset_op(&mut out, op, pk, &pk_flags, &pk_col_to_pk_pos);
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
// BitOr / BitOrAssign for DiffSetBuilder (changeset/patchset concatenation,
// equivalent to SQLite's `sqlite3changeset_concat()`)
// ============================================================================

impl<
    F: Format<S, B>,
    T: SchemaWithPK,
    S: Clone + Hash + Eq + AsRef<str>,
    B: Clone + Hash + Eq + AsRef<[u8]>,
> BitOrAssign for DiffSetBuilder<F, T, S, B>
where
    Operation<F, S, B>: core::ops::Add<Output = Option<Operation<F, S, B>>>,
{
    /// Merge another diff set into this one, consolidating operations on the same row.
    fn bitor_assign(&mut self, rhs: Self) {
        for (table, rows) in rhs.tables {
            for (pk, op) in rows {
                self.add_operation(&table, pk, op);
            }
        }
    }
}

impl<
    F: Format<S, B>,
    T: SchemaWithPK,
    S: Clone + Hash + Eq + AsRef<str>,
    B: Clone + Hash + Eq + AsRef<[u8]>,
> BitOr for DiffSetBuilder<F, T, S, B>
where
    Operation<F, S, B>: core::ops::Add<Output = Option<Operation<F, S, B>>>,
{
    type Output = Self;

    /// Merge two diff sets, consolidating operations on the same row.
    #[inline]
    fn bitor(mut self, rhs: Self) -> Self::Output {
        self |= rhs;
        self
    }
}

// ============================================================================
// DiffSet: frozen (parsed) changeset/patchset with sequential row order
// ============================================================================

/// A frozen changeset or patchset whose rows are emitted in stored order.
///
/// `DiffSet` is produced by the binary parser (via [`ParsedDiffSet`](crate::parser::ParsedDiffSet))
/// or by converting from a [`DiffSetBuilder`] using `Into::into`.  Unlike
/// [`DiffSetBuilder`], it stores tables and rows in a plain `Vec`, reflecting
/// the fact that no further mutation or PK-based lookup is needed.
///
/// [`build`](Self::build) serializes rows in the order they are stored. No
/// session hash-table simulation is applied. This preserves the original
/// row order of parsed binary data across roundtrips.
///
/// To modify a `DiffSet`, convert it back to a [`DiffSetBuilder`] using
/// `Into::into`.
#[derive(Debug, Clone)]
pub struct DiffSet<F: Format<S, B>, T: SchemaWithPK, S, B> {
    /// Tables and their rows, stored in order. Each row is a `(pk, operation)` pair.
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

// -- Changeset iter (DiffSet) -------------------------------------------------

impl<F: Format<S, B>, T: SchemaWithPK, S: AsRef<str> + Hash + Eq, B: AsRef<[u8]> + Hash + Eq>
    DiffSet<F, T, S, B>
{
    /// Returns the schema of each table that holds at least one operation,
    /// in stored order.
    pub fn tables(&self) -> impl Iterator<Item = &T> {
        self.tables
            .iter()
            .filter(|(_, rows)| !rows.is_empty())
            .map(|(t, _)| t)
    }
}

impl<T: SchemaWithPK, S: Clone + Debug + AsRef<str>, B: Clone + Debug + AsRef<[u8]>>
    DiffSet<ChangesetFormat, T, S, B>
{
    /// Iterate over every operation in the changeset.
    ///
    /// Operations are yielded in stored order, grouped by table. Each
    /// [`ChangesetOp`] borrows from this `DiffSet`, so the returned
    /// iterator is invalidated when the `DiffSet` is dropped or mutated.
    pub fn iter(&self) -> impl Iterator<Item = ChangesetOp<'_, T, S, B>> {
        self.tables.iter().flat_map(|(table, rows)| {
            rows.iter().map(move |(_pk, op)| match op {
                Operation::Insert { values, indirect } => ChangesetOp::Insert {
                    table,
                    values: values.as_slice(),
                    indirect: *indirect,
                },
                Operation::Update { values, indirect } => ChangesetOp::Update {
                    table,
                    values: values.as_slice(),
                    indirect: *indirect,
                },
                Operation::Delete { data, indirect } => ChangesetOp::Delete {
                    table,
                    old_values: data.as_slice(),
                    indirect: *indirect,
                },
            })
        })
    }
}

impl<T: SchemaWithPK, S: Clone + AsRef<str>, B: Clone + AsRef<[u8]>>
    DiffSet<PatchsetFormat, T, S, B>
{
    /// Iterate over every operation in the patchset.
    ///
    /// Operations are yielded in stored order, grouped by table. Each
    /// [`PatchsetOp`] borrows from this `DiffSet`. For DELETE and UPDATE
    /// ops only the primary-key columns are available (patchset format
    /// does not carry full old-row values).
    pub fn iter(&self) -> impl Iterator<Item = PatchsetOp<'_, T, S, B>> {
        self.tables.iter().flat_map(|(table, rows)| {
            rows.iter().map(move |(pk, op)| match op {
                Operation::Insert { values, indirect } => PatchsetOp::Insert {
                    table,
                    values: values.as_slice(),
                    indirect: *indirect,
                },
                Operation::Update { values, indirect } => PatchsetOp::Update {
                    table,
                    pk: pk.as_slice(),
                    entries: values.as_slice(),
                    indirect: *indirect,
                },
                Operation::Delete { indirect, .. } => PatchsetOp::Delete {
                    table,
                    pk: pk.as_slice(),
                    indirect: *indirect,
                },
            })
        })
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
                encode_changeset_op(&mut out, op);
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
                encode_patchset_op(&mut out, op, pk, &pk_flags, &pk_col_to_pk_pos);
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
    use crate::builders::operation::Indirect;
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
            alloc::vec![
                values
                    .get(self.pk_column)
                    .expect("primary key column index out of bounds, values shorter than schema")
            ]
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
        let op: Operation<ChangesetFormat, String, Vec<u8>> = Operation::Insert {
            values: vec![Value::Integer(1), Value::Text("alice".into())],
            indirect: false,
        };
        let reversed = op.reverse();
        let Operation::Delete { data, .. } = reversed else {
            panic!("Expected Delete operation");
        };
        assert_eq!(
            data,
            vec![
                Value::<String, Vec<u8>>::Integer(1),
                Value::Text("alice".into())
            ]
        );
    }

    #[test]
    fn test_reverse_operation_delete_becomes_insert() {
        let op: Operation<ChangesetFormat, String, Vec<u8>> = Operation::Delete {
            data: vec![Value::Integer(1), Value::Text("alice".into())],
            indirect: false,
        };
        let reversed = op.reverse();
        let Operation::Insert { values, .. } = reversed else {
            panic!("Expected Insert operation");
        };
        assert_eq!(
            values,
            vec![
                Value::<String, Vec<u8>>::Integer(1),
                Value::Text("alice".into())
            ]
        );
    }

    #[test]
    fn test_reverse_operation_update_swaps_old_new() {
        let op: Operation<ChangesetFormat, String, Vec<u8>> = Operation::Update {
            values: vec![
                (Some(Value::Integer(1)), Some(Value::Integer(1))),
                (
                    Some(Value::Text("alice".into())),
                    Some(Value::Text("bob".into())),
                ),
            ],
            indirect: false,
        };
        let reversed = op.reverse();
        let Operation::Update { values, .. } = reversed else {
            panic!("Expected Update operation");
        };
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
            Operation::Delete { .. }
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
            Operation::Insert { .. }
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
        let Operation::Update { values, .. } = rows.values().next().unwrap() else {
            panic!("Expected Update operation");
        };
        assert_eq!(
            values[1],
            (
                Some(Value::Text("bob".into())),
                Some(Value::Text("alice".into()))
            )
        );
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
        let Operation::Insert { values, .. } = rows.values().next().unwrap() else {
            panic!("Expected Insert operation");
        };
        assert_eq!(values, &original_values);
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

    // ========================================================================
    // BitOr / BitOrAssign tests
    // ========================================================================

    #[test]
    fn test_bitor_changeset_disjoint_rows() {
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

        let cs1 = ChangesetBuilder::new().insert(insert1);
        let cs2 = ChangesetBuilder::new().insert(insert2);

        let merged = cs1 | cs2;
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_bitor_changeset_consolidates_same_row() {
        let table = TestTable::new("users", 2, 0);

        // First changeset: INSERT row 1
        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();

        // Second changeset: DELETE row 1
        let delete = ChangeDelete::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();

        let cs1 = ChangesetBuilder::new().insert(insert);
        let cs2 = ChangesetBuilder::new().delete(delete);

        // INSERT + DELETE with same values should cancel out
        let merged = cs1 | cs2;
        assert_eq!(merged.len(), 0);
        assert!(merged.is_empty());
    }

    #[test]
    fn test_bitor_assign_changeset() {
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

        let mut cs = ChangesetBuilder::new().insert(insert1);
        cs |= ChangesetBuilder::new().insert(insert2);

        assert_eq!(cs.len(), 2);
    }

    #[test]
    fn test_bitor_patchset_disjoint_rows() {
        type PatchsetBuilder = DiffSetBuilder<PatchsetFormat, TestTable, String, Vec<u8>>;

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

        let ps1 = PatchsetBuilder::new().insert(insert1);
        let ps2 = PatchsetBuilder::new().insert(insert2);

        let merged = ps1 | ps2;
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_bitor_patchset_consolidates_same_row() {
        type PatchsetBuilder = DiffSetBuilder<PatchsetFormat, TestTable, String, Vec<u8>>;

        let table = TestTable::new("users", 2, 0);

        // First patchset: INSERT row 1
        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();

        // Second patchset: DELETE row 1
        let delete = PatchDelete::new(table.clone(), vec![Value::Integer(1)]);

        let ps1 = PatchsetBuilder::new().insert(insert);
        let ps2 = PatchsetBuilder::new().delete(delete);

        // INSERT + DELETE should cancel out
        let merged = ps1 | ps2;
        assert_eq!(merged.len(), 0);
        assert!(merged.is_empty());
    }

    #[test]
    fn test_bitor_multiple_tables() {
        let table1 = TestTable::new("users", 2, 0);
        let table2 = TestTable::new("posts", 2, 0);

        let insert1 = Insert::from(table1.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();

        let insert2 = Insert::from(table2.clone())
            .set(0, 100i64)
            .unwrap()
            .set(1, "first post")
            .unwrap();

        let cs1 = ChangesetBuilder::new().insert(insert1);
        let cs2 = ChangesetBuilder::new().insert(insert2);

        let merged = cs1 | cs2;
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_bitor_insert_then_update_consolidates() {
        let table = TestTable::new("users", 2, 0);

        // First changeset: INSERT row 1
        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();

        // Second changeset: UPDATE row 1
        let update = Update::<TestTable, ChangesetFormat, String, Vec<u8>>::from(table.clone())
            .set(0, 1i64, 1i64)
            .unwrap()
            .set(1, "alice", "alicia")
            .unwrap();

        let cs1 = ChangesetBuilder::new().insert(insert);
        let cs2 = ChangesetBuilder::new().update(update);

        // INSERT + UPDATE should consolidate to INSERT with final values
        let merged = cs1 | cs2;
        assert_eq!(merged.len(), 1);
    }

    #[test]
    fn test_session_hash_growth_with_many_rows() {
        // The simulated session hash table grows from 256 buckets when entries
        // reach 128. Insert 200 rows so the rehash branch in session_row_order
        // is exercised. Builds the binary and reparses to confirm consistency.
        let table = TestTable::new("many", 2, 0);
        let mut cs = ChangesetBuilder::new();
        for i in 0..200i64 {
            let insert = Insert::from(table.clone())
                .set(0, i)
                .unwrap()
                .set(1, alloc::format!("row-{i}"))
                .unwrap();
            cs = cs.insert(insert);
        }
        assert_eq!(cs.len(), 200);

        let bytes = cs.build();
        assert!(!bytes.is_empty());

        // Reparse the binary to make sure 200 ops survived the round-trip.
        let reparsed = crate::parser::ParsedDiffSet::try_from(bytes.as_slice()).unwrap();
        let reparsed_bytes: Vec<u8> = reparsed.into();
        assert_eq!(bytes, reparsed_bytes);
    }

    // ========================================================================
    // Indirect-flag tests
    // ========================================================================

    /// Header offset of the indirect byte within a single-op single-table
    /// changeset. Layout: 'T' marker (1) + col_count (1) + pk_flags (2)
    /// + table_name "t\0" (2) + op_code (1) = 7. Patchset is identical.
    const INDIRECT_BYTE_OFFSET: usize = 7;

    #[test]
    fn test_build_insert_indirect_byte_set() {
        let table = TestTable::new("t", 2, 0);
        let insert = Insert::from(table)
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap()
            .indirect(true);

        let bytes = ChangesetBuilder::new().insert(insert).build();
        assert_eq!(bytes[INDIRECT_BYTE_OFFSET], 1);
    }

    #[test]
    fn test_build_delete_indirect_byte_set() {
        let table = TestTable::new("t", 2, 0);
        let delete = ChangeDelete::from(table)
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap()
            .indirect(true);

        let bytes = ChangesetBuilder::new().delete(delete).build();
        assert_eq!(bytes[INDIRECT_BYTE_OFFSET], 1);
    }

    #[test]
    fn test_build_update_indirect_byte_set() {
        let table = TestTable::new("t", 2, 0);
        let update = Update::<TestTable, ChangesetFormat, String, Vec<u8>>::from(table)
            .set(0, 1i64, 1i64)
            .unwrap()
            .set(1, "a", "b")
            .unwrap()
            .indirect(true);

        let bytes = ChangesetBuilder::new().update(update).build();
        assert_eq!(bytes[INDIRECT_BYTE_OFFSET], 1);
    }

    #[test]
    fn test_build_patchset_insert_indirect_byte() {
        let table = TestTable::new("t", 2, 0);
        let insert = Insert::from(table)
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap()
            .indirect(true);

        let patchset: PatchSet<TestTable, String, Vec<u8>> = PatchSet::new().insert(insert);
        let bytes = patchset.build();
        assert_eq!(bytes[INDIRECT_BYTE_OFFSET], 1);
    }

    #[test]
    fn test_reverse_preserves_indirect() {
        // INSERT -> DELETE: indirect carries
        let insert: Operation<ChangesetFormat, String, Vec<u8>> = Operation::Insert {
            values: vec![Value::Integer(1)],
            indirect: true,
        };
        let reversed = insert.reverse();
        assert!(reversed.indirect());
        assert!(matches!(reversed, Operation::Delete { .. }));

        // DELETE -> INSERT: indirect carries
        let delete: Operation<ChangesetFormat, String, Vec<u8>> = Operation::Delete {
            data: vec![Value::Integer(1)],
            indirect: true,
        };
        let reversed = delete.reverse();
        assert!(reversed.indirect());
        assert!(matches!(reversed, Operation::Insert { .. }));

        // UPDATE -> UPDATE swapped: indirect carries
        let update: Operation<ChangesetFormat, String, Vec<u8>> = Operation::Update {
            values: vec![(Some(Value::Integer(1)), Some(Value::Integer(2)))],
            indirect: true,
        };
        let reversed = update.reverse();
        assert!(reversed.indirect());
        assert!(matches!(reversed, Operation::Update { .. }));
    }

    #[test]
    fn test_patchdelete_indirect_byte_set() {
        let table = TestTable::new("t", 2, 0);
        let delete: PatchDelete<TestTable, String, Vec<u8>> =
            PatchDelete::new(table, vec![Value::Integer(1)]).indirect(true);
        let bytes = PatchSet::new().delete(delete).build();
        assert_eq!(bytes[INDIRECT_BYTE_OFFSET], 1);
    }

    #[test]
    fn test_operation_eq_indirect_differs() {
        // Two ops with identical payload but different indirect flags must not be equal.
        let a: Operation<ChangesetFormat, String, Vec<u8>> = Operation::Insert {
            values: vec![Value::Integer(1)],
            indirect: false,
        };
        let b: Operation<ChangesetFormat, String, Vec<u8>> = Operation::Insert {
            values: vec![Value::Integer(1)],
            indirect: true,
        };
        assert_ne!(a, b);
    }

    #[test]
    fn test_operation_eq_variant_mismatch() {
        // Different variants must compare unequal regardless of payload.
        let insert: Operation<ChangesetFormat, String, Vec<u8>> = Operation::Insert {
            values: vec![Value::Integer(1)],
            indirect: false,
        };
        let delete: Operation<ChangesetFormat, String, Vec<u8>> = Operation::Delete {
            data: vec![Value::Integer(1)],
            indirect: false,
        };
        assert_ne!(insert, delete);
    }

    #[test]
    fn test_bitor_indirect_rhs_wins() {
        // Two INSERTs on the same PK with opposite indirect bits. The merged
        // op should carry the rhs's bit (last-write-wins).
        let table = TestTable::new("t", 2, 0);
        let lhs_insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap()
            .indirect(true);
        let rhs_insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap()
            .indirect(false);

        let merged =
            ChangesetBuilder::new().insert(lhs_insert) | ChangesetBuilder::new().insert(rhs_insert);
        let rows = merged.tables.get(&table).unwrap();
        let op = rows.values().next().unwrap();
        assert!(!op.indirect(), "rhs (false) should win over lhs (true)");

        // Reverse direction: rhs=true wins over lhs=false.
        let lhs_insert = Insert::from(table.clone())
            .set(0, 2i64)
            .unwrap()
            .set(1, "b")
            .unwrap()
            .indirect(false);
        let rhs_insert = Insert::from(table.clone())
            .set(0, 2i64)
            .unwrap()
            .set(1, "b")
            .unwrap()
            .indirect(true);

        let merged =
            ChangesetBuilder::new().insert(lhs_insert) | ChangesetBuilder::new().insert(rhs_insert);
        let rows = merged.tables.get(&table).unwrap();
        let op = rows.values().next().unwrap();
        assert!(op.indirect(), "rhs (true) should win over lhs (false)");
    }

    #[test]
    fn test_roundtrip_indirect_changeset() {
        let table = TestTable::new("t", 2, 0);
        let insert = Insert::from(table)
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap()
            .indirect(true);

        let bytes = ChangesetBuilder::new().insert(insert).build();
        let reparsed = crate::parser::ParsedDiffSet::try_from(bytes.as_slice()).unwrap();
        let reparsed_bytes: Vec<u8> = reparsed.into();
        assert_eq!(bytes, reparsed_bytes);
        assert_eq!(reparsed_bytes[INDIRECT_BYTE_OFFSET], 1);
    }

    #[test]
    fn test_roundtrip_indirect_patchset() {
        let table = TestTable::new("t", 2, 0);
        let insert = Insert::from(table)
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap()
            .indirect(true);

        let patchset: PatchSet<TestTable, String, Vec<u8>> = PatchSet::new().insert(insert);
        let bytes = patchset.build();
        let reparsed = crate::parser::ParsedDiffSet::try_from(bytes.as_slice()).unwrap();
        let reparsed_bytes: Vec<u8> = reparsed.into();
        assert_eq!(bytes, reparsed_bytes);
        assert_eq!(reparsed_bytes[INDIRECT_BYTE_OFFSET], 1);
    }

    #[test]
    fn test_indirect_full_pipeline_roundtrip() {
        // Serialize -> parse -> reverse -> reverse -> BitOr(empty) -> re-serialize.
        // The indirect bit must survive every stage.
        let table = TestTable::new("t", 2, 0);
        let insert = Insert::from(table)
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap()
            .indirect(true);
        let original = ChangesetBuilder::new().insert(insert);
        let bytes_a = original.build();
        assert_eq!(bytes_a[INDIRECT_BYTE_OFFSET], 1);

        // Parse the bytes back into a builder over TableSchema<String>.
        let parsed = crate::parser::ParsedDiffSet::try_from(bytes_a.as_slice()).unwrap();
        let crate::parser::ParsedDiffSet::Changeset(parsed_set) = parsed else {
            panic!("expected Changeset variant");
        };

        let parsed_builder: DiffSetBuilder<
            ChangesetFormat,
            crate::parser::TableSchema<String>,
            String,
            Vec<u8>,
        > = parsed_set.into();
        let empty: DiffSetBuilder<
            ChangesetFormat,
            crate::parser::TableSchema<String>,
            String,
            Vec<u8>,
        > = DiffSetBuilder::new();
        let doubled = parsed_builder.reverse().reverse();
        let merged = doubled | empty;

        let bytes_b = merged.build();
        assert_eq!(bytes_a, bytes_b);
        assert_eq!(bytes_b[INDIRECT_BYTE_OFFSET], 1);
    }

    // ========================================================================
    // Operation merge (Add) arms
    // ========================================================================

    fn changeset_insert(v: i64) -> Operation<ChangesetFormat, String, Vec<u8>> {
        Operation::Insert {
            values: vec![Value::Integer(v), Value::Text("a".into())],
            indirect: false,
        }
    }

    fn changeset_delete(v: i64) -> Operation<ChangesetFormat, String, Vec<u8>> {
        Operation::Delete {
            data: vec![Value::Integer(v), Value::Text("a".into())],
            indirect: false,
        }
    }

    fn changeset_update(old: i64, new: i64) -> Operation<ChangesetFormat, String, Vec<u8>> {
        Operation::Update {
            values: vec![
                (Some(Value::Integer(old)), Some(Value::Integer(new))),
                (Some(Value::Text("a".into())), Some(Value::Text("b".into()))),
            ],
            indirect: false,
        }
    }

    #[test]
    fn test_add_changeset_insert_plus_update() {
        let merged = (changeset_insert(1) + changeset_update(1, 2)).unwrap();
        let Operation::Insert { values, .. } = merged else {
            panic!("expected Insert");
        };
        assert_eq!(values[0], Value::Integer(2));
        assert_eq!(values[1], Value::Text("b".into()));
    }

    #[test]
    fn test_add_changeset_update_plus_insert() {
        // UPDATE wins, values are the UPDATE's, indirect is rhs (=false here).
        let merged = (changeset_update(1, 2) + changeset_insert(99)).unwrap();
        assert!(matches!(merged, Operation::Update { .. }));
    }

    #[test]
    fn test_add_changeset_update_plus_update() {
        // First old, last new.
        let merged = (changeset_update(1, 2) + changeset_update(2, 3)).unwrap();
        let Operation::Update { values, .. } = merged else {
            panic!("expected Update");
        };
        assert_eq!(values[0].0, Some(Value::Integer(1)));
        assert_eq!(values[0].1, Some(Value::Integer(3)));
    }

    #[test]
    fn test_add_changeset_update_plus_delete() {
        // UPDATE+DELETE collapses to DELETE carrying the UPDATE's old values.
        let merged = (changeset_update(1, 2) + changeset_delete(99)).unwrap();
        let Operation::Delete { data, .. } = merged else {
            panic!("expected Delete");
        };
        assert_eq!(data[0], Value::Integer(1));
        assert_eq!(data[1], Value::Text("a".into()));
    }

    #[test]
    fn test_add_changeset_delete_plus_update_keeps_delete() {
        let merged = (changeset_delete(1) + changeset_update(1, 2)).unwrap();
        assert!(matches!(merged, Operation::Delete { .. }));
    }

    #[test]
    fn test_add_changeset_delete_plus_delete_keeps_first() {
        let merged = (changeset_delete(1) + changeset_delete(2)).unwrap();
        let Operation::Delete { data, .. } = merged else {
            panic!("expected Delete");
        };
        assert_eq!(data[0], Value::Integer(1));
    }

    fn patchset_insert(v: i64) -> Operation<PatchsetFormat, String, Vec<u8>> {
        Operation::Insert {
            values: vec![Value::Integer(v), Value::Text("a".into())],
            indirect: false,
        }
    }

    fn patchset_update(new: i64) -> Operation<PatchsetFormat, String, Vec<u8>> {
        Operation::Update {
            values: vec![
                ((), Some(Value::Integer(new))),
                ((), Some(Value::Text("b".into()))),
            ],
            indirect: false,
        }
    }

    fn patchset_delete() -> Operation<PatchsetFormat, String, Vec<u8>> {
        Operation::Delete {
            data: (),
            indirect: false,
        }
    }

    #[test]
    fn test_add_patchset_insert_plus_update() {
        let merged = (patchset_insert(1) + patchset_update(2)).unwrap();
        let Operation::Insert { values, .. } = merged else {
            panic!("expected Insert");
        };
        assert_eq!(values[0], Value::Integer(2));
        assert_eq!(values[1], Value::Text("b".into()));
    }

    #[test]
    fn test_add_patchset_update_plus_insert() {
        let merged = (patchset_update(2) + patchset_insert(99)).unwrap();
        assert!(matches!(merged, Operation::Update { .. }));
    }

    #[test]
    fn test_add_patchset_update_plus_update() {
        let merged = (patchset_update(2) + patchset_update(3)).unwrap();
        let Operation::Update { values, .. } = merged else {
            panic!("expected Update");
        };
        assert_eq!(values[0].1, Some(Value::Integer(3)));
    }

    #[test]
    fn test_add_patchset_update_plus_delete() {
        let merged = (patchset_update(2) + patchset_delete()).unwrap();
        assert!(matches!(merged, Operation::Delete { .. }));
    }

    #[test]
    fn test_add_patchset_delete_plus_insert_promotes_to_update() {
        // Patchset can't compare old values, so DELETE + INSERT always becomes UPDATE.
        let merged = (patchset_delete() + patchset_insert(1)).unwrap();
        assert!(matches!(merged, Operation::Update { .. }));
    }

    #[test]
    fn test_add_patchset_delete_plus_update_keeps_delete() {
        let merged = (patchset_delete() + patchset_update(2)).unwrap();
        assert!(matches!(merged, Operation::Delete { .. }));
    }

    #[test]
    fn test_add_patchset_delete_plus_delete_keeps_first() {
        let merged = (patchset_delete() + patchset_delete()).unwrap();
        assert!(matches!(merged, Operation::Delete { .. }));
    }

    // ========================================================================
    // Session-hash coverage: PKs of every Value type
    // ========================================================================

    #[test]
    fn test_session_hash_real_pk() {
        let table = TestTable::new("t", 2, 0);
        let insert = Insert::from(table)
            .set(0, 2.5f64)
            .unwrap()
            .set(1, "a")
            .unwrap();
        let bytes = ChangesetBuilder::new().insert(insert).build();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_session_hash_text_pk() {
        let table = TestTable::new("t", 2, 0);
        let insert = Insert::from(table)
            .set(0, "alice")
            .unwrap()
            .set(1, 42i64)
            .unwrap();
        let bytes = ChangesetBuilder::new().insert(insert).build();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_session_hash_blob_pk() {
        let table = TestTable::new("t", 2, 0);
        let insert = Insert::from(table)
            .set(0, alloc::vec![0xDE_u8, 0xAD, 0xBE, 0xEF])
            .unwrap()
            .set(1, "a")
            .unwrap();
        let bytes = ChangesetBuilder::new().insert(insert).build();
        assert!(!bytes.is_empty());
    }

    // ========================================================================
    // session_row_order empty-rows short-circuit
    // ========================================================================

    #[test]
    fn test_session_row_order_empty() {
        // An empty builder builds to empty bytes (no headers, no ops).
        let cs: ChangesetBuilder = ChangesetBuilder::new();
        let bytes = cs.build();
        assert!(bytes.is_empty());
    }

    #[test]
    fn test_session_row_order_empty_rows_returns_empty_vec() {
        // Direct exercise of the empty-rows short-circuit (line 157-158).
        let rows: RowMap<ChangesetFormat, String, Vec<u8>> = IndexMap::default();
        assert!(session_row_order(&rows).is_empty());
    }

    #[test]
    fn test_diffset_patchset_build_skips_empty_table() {
        // A patchset DiffSet with a registered-but-empty table builds to nothing.
        let table = TestTable::new("t", 2, 0);
        let mut builder: PatchSet<TestTable, String, Vec<u8>> = PatchSet::new();
        builder.add_table(&table);
        let frozen: DiffSet<PatchsetFormat, TestTable, String, Vec<u8>> = builder.into();
        let bytes = frozen.build();
        assert!(bytes.is_empty());
    }

    // ========================================================================
    // From<DiffSetBuilder> / From<&DiffSetBuilder> / From<DiffSet> / From<&DiffSet> for Vec<u8>
    // ========================================================================

    #[test]
    fn test_from_changeset_builder_into_vec() {
        let table = TestTable::new("t", 2, 0);
        let insert = Insert::from(table)
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap();
        let builder = ChangesetBuilder::new().insert(insert);
        let bytes_owned: Vec<u8> = builder.clone().into();
        let bytes_ref: Vec<u8> = (&builder).into();
        assert_eq!(bytes_owned, bytes_ref);
        let frozen: DiffSet<ChangesetFormat, TestTable, String, Vec<u8>> = builder.into();
        let bytes_frozen_owned: Vec<u8> = frozen.clone().into();
        let bytes_frozen_ref: Vec<u8> = (&frozen).into();
        assert_eq!(bytes_frozen_owned, bytes_frozen_ref);
    }

    #[test]
    fn test_from_patchset_builder_into_vec() {
        let table = TestTable::new("t", 2, 0);
        let insert = Insert::from(table)
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap();
        let builder: PatchSet<TestTable, String, Vec<u8>> = PatchSet::new().insert(insert);
        let bytes_owned: Vec<u8> = builder.clone().into();
        let bytes_ref: Vec<u8> = (&builder).into();
        assert_eq!(bytes_owned, bytes_ref);
        let frozen: DiffSet<PatchsetFormat, TestTable, String, Vec<u8>> = builder.into();
        let bytes_frozen_owned: Vec<u8> = frozen.clone().into();
        let bytes_frozen_ref: Vec<u8> = (&frozen).into();
        assert_eq!(bytes_frozen_owned, bytes_frozen_ref);
    }

    // ========================================================================
    // add_operation INSERT+UPDATE pk-change branch
    // ========================================================================

    #[test]
    fn test_add_operation_insert_then_update_changes_pk() {
        // Insert id=1, then update id=1 to id=2. Triggers the special-case branch
        // in add_operation that re-extracts the PK from the merged INSERT values.
        let table = TestTable::new("t", 2, 0);
        let insert = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "alice")
            .unwrap();
        let update = Update::<TestTable, ChangesetFormat, String, Vec<u8>>::from(table.clone())
            .set(0, 1i64, 2i64)
            .unwrap()
            .set(1, "alice", "bob")
            .unwrap();
        let builder = ChangesetBuilder::new().insert(insert).update(update);
        let rows = builder.tables.get(&table).unwrap();
        assert_eq!(rows.len(), 1);
        // The row should now be keyed by id=2.
        let (pk, op) = rows.iter().next().unwrap();
        assert_eq!(pk[0], Value::Integer(2));
        let Operation::Insert { values, .. } = op else {
            panic!("expected merged INSERT");
        };
        assert_eq!(values[0], Value::Integer(2));
        assert_eq!(values[1], Value::Text("bob".into()));
    }

    // ========================================================================
    // DiffOps for DiffSet<F> wrappers
    // ========================================================================

    #[test]
    fn test_diffset_changeset_diffops_wrappers() {
        let table = TestTable::new("t", 2, 0);
        let initial = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap();
        let frozen: DiffSet<ChangesetFormat, TestTable, String, Vec<u8>> =
            ChangesetBuilder::new().insert(initial).into();

        let insert2 = Insert::from(table.clone())
            .set(0, 2i64)
            .unwrap()
            .set(1, "b")
            .unwrap();
        let after_insert = <_ as DiffOps<_, _, _>>::insert(frozen.clone(), insert2);
        assert_eq!(after_insert.len(), 2);

        let delete = ChangeDelete::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap();
        let after_delete = <_ as DiffOps<_, _, _>>::delete(frozen.clone(), delete);
        assert_eq!(after_delete.len(), 0);

        let update = Update::<TestTable, ChangesetFormat, String, Vec<u8>>::from(table)
            .set(0, 1i64, 1i64)
            .unwrap()
            .set(1, "a", "z")
            .unwrap();
        let after_update = <_ as DiffOps<_, _, _>>::update(frozen, update);
        assert_eq!(after_update.len(), 1);
    }

    #[test]
    fn test_diffset_patchset_diffops_wrappers() {
        let table = TestTable::new("t", 2, 0);
        let initial = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap();
        let frozen: DiffSet<PatchsetFormat, TestTable, String, Vec<u8>> =
            PatchSet::new().insert(initial).into();

        let insert2 = Insert::from(table.clone())
            .set(0, 2i64)
            .unwrap()
            .set(1, "b")
            .unwrap();
        let after_insert = <_ as DiffOps<_, _, _>>::insert(frozen.clone(), insert2);
        assert_eq!(after_insert.len(), 2);

        let delete: PatchDelete<TestTable, String, Vec<u8>> =
            PatchDelete::new(table.clone(), vec![Value::Integer(1)]);
        let after_delete = <_ as DiffOps<_, _, _>>::delete(frozen.clone(), delete);
        // Insert(id=1) + PatchDelete(id=1) cancel out.
        assert_eq!(after_delete.len(), 0);

        let update = Update::<TestTable, PatchsetFormat, String, Vec<u8>>::from(table)
            .set(0, 1i64)
            .unwrap()
            .set(1, "z")
            .unwrap();
        let after_update = <_ as DiffOps<_, _, _>>::update(frozen, update);
        assert_eq!(after_update.len(), 1);
    }

    #[test]
    fn test_diffset_changeset_iter_yields_inserts_and_indirect_flag() {
        let table = TestTable::new("t", 2, 0);
        let direct = Insert::from(table.clone()).set(0, 1i64).unwrap();
        let indirect = Insert::from(table.clone())
            .set(0, 2i64)
            .unwrap()
            .indirect(true);
        let frozen: DiffSet<ChangesetFormat, TestTable, String, Vec<u8>> = ChangesetBuilder::new()
            .insert(direct)
            .insert(indirect)
            .into();

        let ops: Vec<_> = frozen.iter().collect();
        assert_eq!(ops.len(), 2);
        for op in &ops {
            assert_eq!(crate::DynTable::name(op.table()), "t");
        }
        assert!(matches!(
            ops[0],
            ChangesetOp::Insert {
                indirect: false,
                ..
            }
        ));
        assert!(matches!(ops[1], ChangesetOp::Insert { indirect: true, .. }));
    }

    #[test]
    fn test_diffset_changeset_iter_yields_update_and_delete() {
        let table = TestTable::new("t", 2, 0);
        let starting = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap();
        let update = Update::<TestTable, ChangesetFormat, String, Vec<u8>>::from(table.clone())
            .set(0, 2i64, 2i64)
            .unwrap()
            .set(1, "before", "after")
            .unwrap();
        let delete = ChangeDelete::from(table.clone())
            .set(0, 3i64)
            .unwrap()
            .set(1, "gone")
            .unwrap();
        let frozen: DiffSet<ChangesetFormat, TestTable, String, Vec<u8>> = ChangesetBuilder::new()
            .insert(starting)
            .update(update)
            .delete(delete)
            .into();

        let kinds: Vec<&'static str> = frozen
            .iter()
            .map(|op| match op {
                ChangesetOp::Insert { .. } => "insert",
                ChangesetOp::Update { .. } => "update",
                ChangesetOp::Delete { .. } => "delete",
            })
            .collect();
        assert_eq!(kinds, ["insert", "update", "delete"]);
    }

    #[test]
    fn test_diffset_patchset_iter_exposes_pk_for_delete_and_update() {
        let table = TestTable::new("t", 2, 0);
        let starting = Insert::from(table.clone())
            .set(0, 1i64)
            .unwrap()
            .set(1, "a")
            .unwrap();
        let update = Update::<TestTable, PatchsetFormat, String, Vec<u8>>::from(table.clone())
            .set(0, 5i64)
            .unwrap()
            .set(1, "z")
            .unwrap();
        let delete: PatchDelete<TestTable, String, Vec<u8>> =
            PatchDelete::new(table.clone(), vec![Value::Integer(7)]);

        let frozen: DiffSet<PatchsetFormat, TestTable, String, Vec<u8>> = PatchSet::new()
            .insert(starting)
            .update(update)
            .delete(delete)
            .into();

        let mut saw_insert = false;
        let mut saw_update_pk: Option<i64> = None;
        let mut saw_delete_pk: Option<i64> = None;
        for op in frozen.iter() {
            match op {
                PatchsetOp::Insert { values, .. } => {
                    saw_insert = true;
                    assert!(matches!(values[0], Value::Integer(1)));
                }
                PatchsetOp::Update { pk, .. } => {
                    if let Value::Integer(id) = pk[0] {
                        saw_update_pk = Some(id);
                    }
                }
                PatchsetOp::Delete { pk, .. } => {
                    if let Value::Integer(id) = pk[0] {
                        saw_delete_pk = Some(id);
                    }
                }
            }
        }
        assert!(saw_insert);
        assert_eq!(saw_update_pk, Some(5));
        assert_eq!(saw_delete_pk, Some(7));
    }

    #[test]
    fn test_diffset_tables_skips_empty() {
        let t1 = TestTable::new("t1", 2, 0);
        let t2 = TestTable::new("t2", 2, 0);
        let insert_t1 = Insert::from(t1.clone()).set(0, 1i64).unwrap();
        let insert_then_delete_t2 = Insert::from(t2.clone()).set(0, 9i64).unwrap();
        let delete_t2 = ChangeDelete::from(t2.clone()).set(0, 9i64).unwrap();
        let frozen: DiffSet<ChangesetFormat, TestTable, String, Vec<u8>> = ChangesetBuilder::new()
            .insert(insert_t1)
            .insert(insert_then_delete_t2)
            .delete(delete_t2)
            .into();

        let names: Vec<&str> = frozen.tables().map(crate::DynTable::name).collect();
        assert_eq!(names, ["t1"]);
    }
}
