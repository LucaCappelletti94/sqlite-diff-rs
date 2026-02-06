//! Schema-less operation enum for internal use in `DiffSetBuilder`.
//!
//! Operations store only row data (values), not the table schema `T`.
//! The schema lives as the key of the outer `IndexMap` in `DiffSetBuilder`.
//! All consolidation logic (Operation + Operation) is defined here.

use alloc::vec::Vec;

use crate::{
    builders::{ChangesetFormat, PatchsetFormat, format::Format},
    encoding::Value,
};

/// A schema-less database operation, parameterized only by the format `F`.
///
/// The table schema `T` is NOT stored here — it lives as the key in
/// `DiffSetBuilder`'s `IndexMap<T, IndexMap<Vec<Value>, Operation<F>>>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Operation<F: Format> {
    /// A row was inserted. Stores all column values.
    Insert(Vec<Value>),
    /// A row was deleted. Stores format-specific delete data:
    /// - Changeset: `Vec<Value>` (full old-row values)
    /// - Patchset: `()` (PK is stored as the IndexMap key)
    Delete(F::DeleteData),
    /// A row was updated. Stores `(old, new)` pairs per column.
    /// - Changeset: `(Value, Value)` per column
    /// - Patchset: `((), Value)` per column
    Update(Vec<(F::Old, Value)>),
}

/// Trait for reversing operations.
pub trait Reverse {
    /// The reverse of this operation.
    type Output;

    /// Returns the reverse of this operation.
    fn reverse(self) -> Self::Output;
}

impl Reverse for Operation<ChangesetFormat> {
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

impl core::ops::Add for Operation<ChangesetFormat> {
    type Output = Option<Self>;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            // INSERT + INSERT: keep first
            (Operation::Insert(lhs), Operation::Insert(_rhs)) => Some(Operation::Insert(lhs)),

            // INSERT + UPDATE: apply update to insert values
            (Operation::Insert(mut values), Operation::Update(updates)) => {
                for (idx, (_old, new)) in updates.into_iter().enumerate() {
                    if !matches!(new, Value::Undefined) {
                        values[idx] = new;
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
                let old_values = upd.into_iter().map(|(old, _new)| old).collect();
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
                        .map(|(old, new)| (old, new))
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

impl core::ops::Add for Operation<PatchsetFormat> {
    type Output = Option<Self>;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            // INSERT + INSERT: keep first
            (Operation::Insert(lhs), Operation::Insert(_rhs)) => Some(Operation::Insert(lhs)),

            // INSERT + UPDATE: apply update to insert values
            (Operation::Insert(mut values), Operation::Update(updates)) => {
                for (idx, ((), new)) in updates.into_iter().enumerate() {
                    if !matches!(new, Value::Undefined) {
                        values[idx] = new;
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
                let update_values = ins_values.into_iter().map(|new| ((), new)).collect();
                Some(Operation::Update(update_values))
            }

            // DELETE + UPDATE: keep delete
            (Operation::Delete(lhs), Operation::Update(_rhs)) => Some(Operation::Delete(lhs)),

            // DELETE + DELETE: keep first
            (Operation::Delete(lhs), Operation::Delete(_rhs)) => Some(Operation::Delete(lhs)),
        }
    }
}
