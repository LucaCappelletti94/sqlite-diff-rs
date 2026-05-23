//! Read-only views of operations inside a [`DiffSet`].
//!
//! [`DiffSet::iter`] (changeset format) and [`DiffSet::iter`] (patchset
//! format) yield one of these per stored operation. The fields borrow
//! from the underlying [`DiffSet`], so consumers can render or inspect
//! the contents without copying any row data.
//!
//! Patchsets do not carry full old-row values, so [`PatchsetOp::Delete`]
//! and [`PatchsetOp::Update`] expose only the primary-key columns. The
//! [`PatchsetOp::Update`] `entries` slice is the raw `(F::Old, new)`
//! storage (with `F::Old = ()` for patchsets); call `.iter().map(|(_, v)| v)`
//! to drop the unit and walk just the new values.
//!
//! [`DiffSet`]: super::DiffSet
//! [`DiffSet::iter`]: super::DiffSet::iter

use alloc::vec::Vec;

use crate::encoding::Value;

/// `(old, new)` value pair stored per column in a changeset UPDATE.
/// Either slot may be `None`, which means the column was not part of
/// the diff.
pub type ChangesetUpdatePair<S, B> = (Option<Value<S, B>>, Option<Value<S, B>>);

/// Entry stored per column in a patchset UPDATE: a unit (the format
/// does not carry the old value) plus the new value (or `None` for
/// unchanged columns).
pub type PatchsetUpdateEntry<S, B> = ((), Option<Value<S, B>>);

/// View over one operation in a changeset.
#[derive(Debug)]
pub enum ChangesetOp<'a, T, S, B> {
    /// `INSERT`. Carries every column's value in column order.
    Insert {
        /// Table this row belongs to.
        table: &'a T,
        /// Full row values, one per column.
        values: &'a [Value<S, B>],
        /// SQLite session-extension indirect flag.
        indirect: bool,
    },
    /// `UPDATE`. Carries `(old, new)` pairs per column. `None` in either
    /// slot means "undefined" (the column was not part of the diff).
    Update {
        /// Table this row belongs to.
        table: &'a T,
        /// `(old, new)` pairs, one per column.
        values: &'a [ChangesetUpdatePair<S, B>],
        /// SQLite session-extension indirect flag.
        indirect: bool,
    },
    /// `DELETE`. Carries the full old-row values in column order.
    Delete {
        /// Table this row belongs to.
        table: &'a T,
        /// Full old-row values, one per column.
        old_values: &'a [Value<S, B>],
        /// SQLite session-extension indirect flag.
        indirect: bool,
    },
}

impl<'a, T, S, B> ChangesetOp<'a, T, S, B> {
    /// Returns the schema of the table this operation applies to.
    #[must_use]
    pub fn table(&self) -> &'a T {
        match self {
            Self::Insert { table, .. }
            | Self::Update { table, .. }
            | Self::Delete { table, .. } => table,
        }
    }

    /// Returns the SQLite session-extension indirect flag.
    #[must_use]
    pub fn indirect(&self) -> bool {
        match self {
            Self::Insert { indirect, .. }
            | Self::Update { indirect, .. }
            | Self::Delete { indirect, .. } => *indirect,
        }
    }
}

/// View over one operation in a patchset.
#[derive(Debug)]
pub enum PatchsetOp<'a, T, S, B> {
    /// `INSERT`. Carries every column's value in column order.
    Insert {
        /// Table this row belongs to.
        table: &'a T,
        /// Full row values, one per column.
        values: &'a [Value<S, B>],
        /// SQLite session-extension indirect flag.
        indirect: bool,
    },
    /// `UPDATE`. Carries primary-key values plus a `(unit, new)` entry
    /// per non-PK column. The unit reflects the patchset format's
    /// missing old-value storage; consumers can map `|(_, v)| v` to walk
    /// just the new values.
    Update {
        /// Table this row belongs to.
        table: &'a T,
        /// Primary-key column values for the row being updated.
        pk: &'a [Value<S, B>],
        /// `(unit, new)` entries, one per column.
        entries: &'a [PatchsetUpdateEntry<S, B>],
        /// SQLite session-extension indirect flag.
        indirect: bool,
    },
    /// `DELETE`. Carries only the primary-key columns; the rest of the
    /// old row is not stored in patchset format.
    Delete {
        /// Table this row belongs to.
        table: &'a T,
        /// Primary-key column values for the row being deleted.
        pk: &'a [Value<S, B>],
        /// SQLite session-extension indirect flag.
        indirect: bool,
    },
}

impl<'a, T, S, B> PatchsetOp<'a, T, S, B> {
    /// Returns the schema of the table this operation applies to.
    #[must_use]
    pub fn table(&self) -> &'a T {
        match self {
            Self::Insert { table, .. }
            | Self::Update { table, .. }
            | Self::Delete { table, .. } => table,
        }
    }

    /// Returns the SQLite session-extension indirect flag.
    #[must_use]
    pub fn indirect(&self) -> bool {
        match self {
            Self::Insert { indirect, .. }
            | Self::Update { indirect, .. }
            | Self::Delete { indirect, .. } => *indirect,
        }
    }

    /// For an `Update` op, returns the new values per column (with the
    /// unit dropped). Returns `None` for `Insert` and `Delete`.
    #[must_use]
    pub fn update_new_values(&self) -> Option<Vec<&'a Option<Value<S, B>>>> {
        match self {
            Self::Update { entries, .. } => Some(entries.iter().map(|((), v)| v).collect()),
            _ => None,
        }
    }
}
