//! Enumeration of operations (insert, delete, update) for changesets and patchsets,
//! defined so that only valid operations can be stored in the enum.

use crate::{
    DynTable,
    builders::{ChangeDelete, ChangesetFormat, Insert, PatchDelete, Update, format::Format},
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// The type of database operation.
pub enum Operation<T: DynTable, F: Format> {
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
