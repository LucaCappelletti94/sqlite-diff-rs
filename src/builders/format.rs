//! Format trait defining changeset vs patchset behavior.

use crate::encoding::{MaybeValue, Value};
use alloc::vec::Vec;
use core::fmt::Debug;

/// Trait defining the differences between changeset and patchset formats.
///
/// A changeset DELETE stores all column values and a changeset UPDATE stores
/// both old and new values. A patchset DELETE stores only the PK (data lives
/// externally) and a patchset UPDATE stores only the PK plus new values.
pub(crate) trait Format<S, B>: Default + Clone + Copy + PartialEq + Eq + 'static {
    /// The type representing old values in this format.
    ///
    /// - Changeset: `MaybeValue<S, B>` (Option<Value<S, B>>, None = undefined/unchanged)
    /// - Patchset: `()` (old values not stored)
    type Old: Clone + Debug + Default;

    /// The data stored for a DELETE operation (beyond the PK which is always
    /// stored as the `IndexMap` key in `DiffSetBuilder`).
    ///
    /// - Changeset: `Vec<Value<S, B>>` (full old-row values)
    /// - Patchset: `()` (only the PK matters, stored externally)
    type DeleteData: Clone + Debug + Default;
}

/// Public, nameable bound for a diff format, either changeset or patchset.
///
/// This is the downstream-visible counterpart to the crate-private
/// [`Format`] trait. It carries no items of its own; the associated types
/// live on the sealed supertrait and stay private. Because [`Format`] is
/// crate-private, no external type can satisfy this bound, so it is sealed
/// in the same sense as a sealed trait: only [`ChangesetFormat`] and
/// [`PatchsetFormat`] implement it.
///
/// A consumer that only calls `new`, `digest`, and `build` can therefore
/// write one function generic over `F: DiffFormat<String, Vec<u8>>` and
/// fold a batch of wire events into either a changeset or a patchset,
/// instead of duplicating the body once per format.
pub trait DiffFormat<S, B>: Format<S, B> {}

impl<S, B, F: Format<S, B>> DiffFormat<S, B> for F {}

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
