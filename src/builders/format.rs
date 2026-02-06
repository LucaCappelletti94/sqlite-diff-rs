//! Format trait defining changeset vs patchset behavior.

use crate::encoding::{MaybeValue, Value};
use alloc::vec::Vec;
use core::fmt::Debug;

/// Trait defining the differences between changeset and patchset formats.
///
/// The key differences:
/// - **Changeset**: DELETE stores all column values, UPDATE stores old+new
/// - **Patchset**: DELETE stores only PK values (data lives externally), UPDATE stores only PK+new
pub(crate) trait Format: Default + Clone + Copy + PartialEq + Eq + 'static {
    /// The type representing old values in this format.
    ///
    /// - Changeset: `MaybeValue` (Option<Value>, None = undefined/unchanged)
    /// - Patchset: `()` (old values not stored)
    type Old: Clone + Debug + Default + PartialEq + Eq;

    /// The data stored for a DELETE operation (beyond the PK which is always
    /// stored as the `IndexMap` key in `DiffSetBuilder`).
    ///
    /// - Changeset: `Vec<Value>` — full old-row values
    /// - Patchset: `()` — only the PK matters (stored externally)
    type DeleteData: Clone + Debug + Default + PartialEq + Eq;

    /// Table header marker byte.
    /// Changesets use 'T' (0x54), patchsets use 'P' (0x50).
    const TABLE_MARKER: u8;
}

/// Changeset format marker.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ChangesetFormat;

impl Format for ChangesetFormat {
    const TABLE_MARKER: u8 = b'T';

    type Old = MaybeValue;
    type DeleteData = Vec<Value>;
}

/// Patchset format marker.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PatchsetFormat;

impl Format for PatchsetFormat {
    type Old = ();
    type DeleteData = ();

    const TABLE_MARKER: u8 = b'P';
}
