//! Enumeration of operations (insert, delete, update) for changesets and patchsets,
//! defined so that only valid operations can be stored in the enum.

use crate::{
    DynTable,
    builders::{
        ChangeDelete, ChangesetFormat, Insert, PatchDelete, PatchsetFormat, Update, format::Format,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// The type of database operation.
pub(crate) enum Operation<T: DynTable, F: Format> {
    /// A row was inserted, or it was inserted and the updated,
    /// which makes it indistinguishable from a pure insert in patchsets.
    Insert(Insert<T>),
    /// A row was deleted.
    Delete(F::DeleteOps<T>),
    /// A row was updated.
    Update(Update<T, F>),
}

impl<T: DynTable, F: Format> From<Insert<T>> for Operation<T, F> {
    fn from(insert: Insert<T>) -> Self {
        Self::Insert(insert)
    }
}
impl<T: DynTable, F: Format> From<Update<T, F>> for Operation<T, F> {
    fn from(update: Update<T, F>) -> Self {
        Self::Update(update)
    }
}
impl<T: DynTable, F: Format<DeleteOps<T> = PatchDelete<T>>> From<PatchDelete<T>>
    for Operation<T, F>
{
    fn from(delete: PatchDelete<T>) -> Self {
        Self::Delete(delete)
    }
}
impl<T: DynTable, F: Format<DeleteOps<T> = ChangeDelete<T>>> From<ChangeDelete<T>>
    for Operation<T, F>
{
    fn from(delete: ChangeDelete<T>) -> Self {
        Self::Delete(delete)
    }
}

/// Trait for reversing operations.
pub trait Reverse {
    /// The reverse of this operation.
    type Output;

    /// Returns the reverse of this operation.
    fn reverse(self) -> Self::Output;
}

impl<T: DynTable> Reverse for Operation<T, ChangesetFormat> {
    type Output = Operation<T, ChangesetFormat>;

    fn reverse(self) -> Self::Output {
        match self {
            Operation::Insert(insert) => insert.reverse().into(),
            Operation::Delete(delete) => delete.reverse().into(),
            Operation::Update(update) => update.reverse().into(),
        }
    }
}

// ============================================================================
// Operation + Operation for Changeset
// ============================================================================

impl<T: DynTable + Clone> core::ops::Add for Operation<T, ChangesetFormat> {
    type Output = Option<Self>;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            // INSERT + INSERT: keep first
            (Operation::Insert(lhs), Operation::Insert(rhs)) => Some(Operation::Insert(lhs + rhs)),
            // INSERT + UPDATE: apply update to insert
            (Operation::Insert(lhs), Operation::Update(rhs)) => Some(Operation::Insert(lhs + rhs)),
            // INSERT + DELETE: cancel out
            (Operation::Insert(lhs), Operation::Delete(rhs)) => (lhs + rhs).map(Operation::Insert),
            // UPDATE + INSERT: keep update
            (Operation::Update(lhs), Operation::Insert(rhs)) => Some(Operation::Update(lhs + rhs)),
            // UPDATE + UPDATE: merge
            (Operation::Update(lhs), Operation::Update(rhs)) => Some(Operation::Update(lhs + rhs)),
            // UPDATE + DELETE: delete with original old values
            (Operation::Update(lhs), Operation::Delete(rhs)) => Some(Operation::Delete(lhs + rhs)),
            // DELETE + INSERT: update if different, cancel if same
            (Operation::Delete(lhs), Operation::Insert(rhs)) => (lhs + rhs).map(Operation::Update),
            // DELETE + UPDATE: keep delete
            (Operation::Delete(lhs), Operation::Update(rhs)) => Some(Operation::Delete(lhs + rhs)),
            // DELETE + DELETE: keep first
            (Operation::Delete(lhs), Operation::Delete(rhs)) => Some(Operation::Delete(lhs + rhs)),
        }
    }
}

// ============================================================================
// Operation + Operation for Patchset
// ============================================================================

impl<T: DynTable + Clone> core::ops::Add for Operation<T, PatchsetFormat> {
    type Output = Option<Self>;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            // INSERT + INSERT: keep first
            (Operation::Insert(lhs), Operation::Insert(rhs)) => Some(Operation::Insert(lhs + rhs)),
            // INSERT + UPDATE: apply update to insert
            (Operation::Insert(lhs), Operation::Update(rhs)) => Some(Operation::Insert(lhs + rhs)),
            // INSERT + DELETE: cancel out
            (Operation::Insert(lhs), Operation::Delete(rhs)) => (lhs + rhs).map(Operation::Insert),
            // UPDATE + INSERT: keep update
            (Operation::Update(lhs), Operation::Insert(rhs)) => Some(Operation::Update(lhs + rhs)),
            // UPDATE + UPDATE: merge
            (Operation::Update(lhs), Operation::Update(rhs)) => Some(Operation::Update(lhs + rhs)),
            // UPDATE + DELETE: keep delete (patchset doesn't need old values)
            (Operation::Update(lhs), Operation::Delete(rhs)) => Some(Operation::Delete(lhs + rhs)),
            // DELETE + INSERT: always becomes update (patchset can't compare)
            (Operation::Delete(lhs), Operation::Insert(rhs)) => Some(Operation::Update(lhs + rhs)),
            // DELETE + UPDATE: keep delete
            (Operation::Delete(lhs), Operation::Update(rhs)) => Some(Operation::Delete(lhs + rhs)),
            // DELETE + DELETE: keep first
            (Operation::Delete(lhs), Operation::Delete(rhs)) => Some(Operation::Delete(lhs + rhs)),
        }
    }
}
