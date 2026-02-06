//! Format trait defining changeset vs patchset behavior.

use crate::{
    DynTable,
    builders::{ChangeDelete, Operation, PatchDelete},
    encoding::Value,
};
use core::fmt::Debug;

/// Trait defining the differences between changeset and patchset formats.
///
/// The key differences:
/// - **Changeset**: DELETE stores all column values, UPDATE stores old+new
/// - **Patchset**: DELETE stores only PK values, UPDATE stores only PK+new
pub(crate) trait Format: Default + Clone + Copy + PartialEq + Eq + 'static {
    /// The type representing old values in this format.
    type Old: Clone + Debug + Default + PartialEq + Eq;
    /// The type of delete operations for this format.
    type DeleteOps<T: DynTable>: Debug
        + Into<Operation<T, Self>>
        + AsRef<T>
        + Clone
        + Eq
        + PartialEq;

    /// Table header marker byte.
    /// Changesets use 'T' (0x54), patchsets use 'P' (0x50).
    const TABLE_MARKER: u8;
}

/// Changeset format marker.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ChangesetFormat;

impl Format for ChangesetFormat {
    const TABLE_MARKER: u8 = b'T';

    type Old = Value;
    type DeleteOps<T: DynTable> = ChangeDelete<T>;
}

/// Patchset format marker.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PatchsetFormat;

impl Format for PatchsetFormat {
    type Old = ();
    type DeleteOps<T: DynTable> = PatchDelete<T>;

    const TABLE_MARKER: u8 = b'P';
}
