//! Schema-less operation enum for internal use in `DiffSetBuilder`.
//!
//! Operations store only row data (values), not the table schema `T`.
//! The schema lives as the key of the outer `IndexMap` in `DiffSetBuilder`.
//! All consolidation logic (Operation + Operation) is defined here.

use alloc::vec::Vec;
use core::fmt::Debug;

use crate::{
    builders::{ChangesetFormat, PatchsetFormat, format::Format},
    encoding::{MaybeValue, Value},
};

/// A schema-less database operation, parameterized by format `F` and value types `S`, `B`.
///
/// The table schema `T` is NOT stored here — it lives as the key in
/// `DiffSetBuilder`'s `IndexMap<T, IndexMap<Vec<Value<S, B>>, Operation<F, S, B>>>`.
#[derive(Debug, Clone)]
pub(crate) enum Operation<F: Format<S, B>, S: AsRef<str>, B: AsRef<[u8]>> {
    /// A row was inserted. Stores all column values.
    Insert(Vec<Value<S, B>>),
    /// A row was deleted. Stores format-specific delete data:
    /// - Changeset: `Vec<Value<S, B>>` (full old-row values)
    /// - Patchset: `()` (PK is stored as the IndexMap key)
    Delete(F::DeleteData),
    /// A row was updated. Stores `(old, new)` pairs per column.
    /// - Changeset: `(MaybeValue<S, B>, MaybeValue<S, B>)` per column (None = undefined)
    /// - Patchset: `((), MaybeValue<S, B>)` per column
    Update(Vec<(F::Old, MaybeValue<S, B>)>),
}

/// Implement PartialEq for Operation where needed.
impl<F: Format<S, B>, S: PartialEq + AsRef<str>, B: PartialEq + AsRef<[u8]>> PartialEq for Operation<F, S, B>
where
    F::DeleteData: PartialEq,
    F::Old: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Insert(a), Self::Insert(b)) => a == b,
            (Self::Delete(a), Self::Delete(b)) => a == b,
            (Self::Update(a), Self::Update(b)) => a == b,
            _ => false,
        }
    }
}

impl<F: Format<S, B>, S: Eq + AsRef<str>, B: Eq + AsRef<[u8]>> Eq for Operation<F, S, B>
where
    F::DeleteData: Eq,
    F::Old: Eq,
{}

/// Trait for reversing operations.
///
/// This trait allows reversing database operations, which is useful for:
/// - Creating inverse changesets (undo operations)
/// - Conflict resolution in distributed systems
/// - Testing bidirectional synchronization
pub trait Reverse {
    /// The reverse of this operation.
    type Output;

    /// Returns the reverse of this operation.
    fn reverse(self) -> Self::Output;
}

impl<S: Clone + Debug + AsRef<str>, B: Clone + Debug + AsRef<[u8]>> Reverse for Operation<ChangesetFormat, S, B> {
    type Output = Self;

    fn reverse(self) -> Self::Output {
        match self {
            // INSERT reversed → DELETE (same values)
            Operation::Insert(values) => Operation::Delete(values),
            // DELETE reversed → INSERT (same values)
            Operation::Delete(values) => Operation::Insert(values),
            // UPDATE reversed → UPDATE with old/new swapped
            Operation::Update(values) => {
                Operation::Update(values.into_iter().map(|(old, new)| (new, old)).collect())
            }
        }
    }
}

// ============================================================================
// Operation + Operation for Changeset
// ============================================================================

impl<S: Clone + Debug + PartialEq + AsRef<str>, B: Clone + Debug + PartialEq + AsRef<[u8]>> core::ops::Add
    for Operation<ChangesetFormat, S, B>
{
    type Output = Option<Self>;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            // INSERT + INSERT: keep first
            (Operation::Insert(lhs), Operation::Insert(_rhs)) => Some(Operation::Insert(lhs)),

            // INSERT + UPDATE: apply update to insert values
            (Operation::Insert(mut values), Operation::Update(updates)) => {
                for (idx, (_old, new)) in updates.into_iter().enumerate() {
                    if let Some(new_val) = new {
                        values[idx] = new_val;
                    }
                }
                Some(Operation::Insert(values))
            }

            // INSERT + DELETE: cancel out
            (Operation::Insert(_), Operation::Delete(_)) => None,

            // UPDATE + INSERT: keep update
            (Operation::Update(lhs), Operation::Insert(_rhs)) => Some(Operation::Update(lhs)),

            // UPDATE + UPDATE: keep original old, use final new
            (Operation::Update(lhs), Operation::Update(rhs)) => {
                let merged = lhs
                    .into_iter()
                    .zip(rhs)
                    .map(|((old, _mid), (_mid2, new))| (old, new))
                    .collect();
                Some(Operation::Update(merged))
            }

            // UPDATE + DELETE: delete with original old values
            (Operation::Update(upd), Operation::Delete(_del)) => {
                // Collect old values, converting MaybeValue to Value (None becomes Null)
                let old_values = upd
                    .into_iter()
                    .map(|(old, _new)| old.unwrap_or(Value::Null))
                    .collect();
                Some(Operation::Delete(old_values))
            }

            // DELETE + INSERT: update if different, cancel if same
            (Operation::Delete(del_values), Operation::Insert(ins_values)) => {
                if del_values == ins_values {
                    None // Same values — cancel out
                } else {
                    // Different — becomes UPDATE from old to new
                    let update_values = del_values
                        .into_iter()
                        .zip(ins_values)
                        .map(|(old, new)| (Some(old), Some(new)))
                        .collect();
                    Some(Operation::Update(update_values))
                }
            }

            // DELETE + UPDATE: keep delete
            (Operation::Delete(lhs), Operation::Update(_rhs)) => Some(Operation::Delete(lhs)),

            // DELETE + DELETE: keep first
            (Operation::Delete(lhs), Operation::Delete(_rhs)) => Some(Operation::Delete(lhs)),
        }
    }
}

// ============================================================================
// Operation + Operation for Patchset
// ============================================================================

impl<S: Clone + PartialEq + AsRef<str>, B: Clone + PartialEq + AsRef<[u8]>> core::ops::Add for Operation<PatchsetFormat, S, B> {
    type Output = Option<Self>;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            // INSERT + INSERT: keep first
            (Operation::Insert(lhs), Operation::Insert(_rhs)) => Some(Operation::Insert(lhs)),

            // INSERT + UPDATE: apply update to insert values
            (Operation::Insert(mut values), Operation::Update(updates)) => {
                for (idx, ((), new)) in updates.into_iter().enumerate() {
                    if let Some(new_val) = new {
                        values[idx] = new_val;
                    }
                }
                Some(Operation::Insert(values))
            }

            // INSERT + DELETE: cancel out
            (Operation::Insert(_), Operation::Delete(())) => None,

            // UPDATE + INSERT: keep update
            (Operation::Update(lhs), Operation::Insert(_rhs)) => Some(Operation::Update(lhs)),

            // UPDATE + UPDATE: keep original old (unit), use final new
            (Operation::Update(lhs), Operation::Update(rhs)) => {
                let merged = lhs
                    .into_iter()
                    .zip(rhs)
                    .map(|((old, _mid), (_mid2, new))| (old, new))
                    .collect();
                Some(Operation::Update(merged))
            }

            // UPDATE + DELETE: keep delete (patchset doesn't need old values)
            (Operation::Update(_upd), Operation::Delete(del)) => Some(Operation::Delete(del)),

            // DELETE + INSERT: always becomes update (patchset can't compare old values)
            (Operation::Delete(()), Operation::Insert(ins_values)) => {
                let update_values = ins_values.into_iter().map(|new| ((), Some(new))).collect();
                Some(Operation::Update(update_values))
            }

            // DELETE + UPDATE: keep delete
            (Operation::Delete(lhs), Operation::Update(_rhs)) => Some(Operation::Delete(lhs)),

            // DELETE + DELETE: keep first
            (Operation::Delete(lhs), Operation::Delete(_rhs)) => Some(Operation::Delete(lhs)),
        }
    }
}
