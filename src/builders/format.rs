//! Format trait defining changeset vs patchset behavior.

use crate::encoding::{MaybeValue, Value};
use alloc::vec::Vec;
use core::fmt::Debug;

/// Trait defining the differences between changeset and patchset formats.
///
/// The key differences:
/// - **Changeset**: DELETE stores all column values, UPDATE stores old+new
/// - **Patchset**: DELETE stores only PK values (data lives externally), UPDATE stores only PK+new
pub(crate) trait Format<S, B>: Default + Clone + Copy + PartialEq + Eq + 'static {
    /// The type representing old values in this format.
    ///
    /// - Changeset: `MaybeValue<S, B>` (Option<Value<S, B>>, None = undefined/unchanged)
    /// - Patchset: `()` (old values not stored)
    type Old: Clone + Debug + Default;

    /// The data stored for a DELETE operation (beyond the PK which is always
    /// stored as the `IndexMap` key in `DiffSetBuilder`).
    ///
    /// - Changeset: `Vec<Value<S, B>>` — full old-row values
    /// - Patchset: `()` — only the PK matters (stored externally)
    type DeleteData: Clone + Debug + Default;
}

/// Changeset format marker.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ChangesetFormat;

impl<S: Clone + Debug + AsRef<str>, B: Clone + Debug + AsRef<[u8]>> Format<S, B>
    for ChangesetFormat
{
    type Old = MaybeValue<S, B>;
    type DeleteData = Vec<Value<S, B>>;
}

/// Patchset format marker.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PatchsetFormat;

impl<S, B> Format<S, B> for PatchsetFormat {
    type Old = ();
    type DeleteData = ();
}
