//! [`WireSource`]: sealed marker trait for CDC wire formats.

use core::hash::Hash;

use super::sealed::Sealed;

/// Per-format marker naming a CDC wire source.
///
/// Implementors are unit structs owned by each format module
/// (`PgWalstream`, `Wal2Json`, `Maxwell`). The associated types describe
/// the format's per-column payload and its native type-identity key.
///
/// # Type keys
///
/// - `PgWalstream::TypeKey = pg_walstream::Oid` (u32 alias).
/// - `Wal2Json::TypeKey = alloc::sync::Arc<str>` (Postgres type name).
/// - `Maxwell::TypeKey = alloc::sync::Arc<str>` (MySQL type name; empty
///   string when the Maxwell daemon runs without `--include_types`).
pub trait WireSource: Sealed {
    /// Per-column payload the format hands to a decoder.
    ///
    /// Every payload struct carries a `column_name: &'a str` so decoder
    /// errors are self-describing without an outer wrapping layer.
    type Payload<'a>;

    /// Type identity as seen on this source's wire.
    type TypeKey: Hash + Eq + Clone;

    /// Extract the type key from a payload for dispatch.
    fn type_key(payload: &Self::Payload<'_>) -> Self::TypeKey;

    /// Extract the column name from a payload for diagnostic messages.
    fn column_name<'a>(payload: &'a Self::Payload<'_>) -> &'a str;
}
