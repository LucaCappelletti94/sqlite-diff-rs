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

/// Schema-side source-native type key for one column of one table.
pub trait WireColumnTypes<Src: WireSource> {
    /// Type key for the column at `column_index`.
    fn column_type_key(&self, column_index: usize) -> Src::TypeKey;
}

/// Table-name lookup for the [`DiffSetBuilder::digest`](crate::DiffSetBuilder::digest) entry point.
pub trait WireSchema<Src: WireSource> {
    /// Concrete schema type for one table.
    type Table: crate::schema::NamedColumns + WireColumnTypes<Src>;

    /// Resolve a table name to its schema entry.
    fn get(&self, table_name: &str) -> Option<&Self::Table>;
}

/// One CDC wire event digested via [`DiffSetBuilder::digest`](crate::DiffSetBuilder::digest).
///
/// Implemented in-crate for `pg_walstream::EventType`, `wal2json::MessageV2`,
/// `wal2json::ChangeV1`, and `maxwell::Message` (each times both formats).
pub trait Digestable<F, T, S, B>
where
    F: crate::builders::Format<S, B>,
    T: crate::schema::NamedColumns + WireColumnTypes<Self::Src>,
{
    /// Wire source this event came from.
    type Src: WireSource;

    /// Failure mode raised on schema lookup or decode failure.
    type Error;

    /// Fold this event into `builder`, resolving affected tables via `schema`
    /// and decoding column payloads via `adapter`.
    ///
    /// # Errors
    ///
    /// Any per-source `ConversionError`.
    fn digest_into<Sch, A>(
        &self,
        builder: crate::builders::DiffSetBuilder<F, T, S, B>,
        schema: &Sch,
        adapter: &A,
    ) -> Result<crate::builders::DiffSetBuilder<F, T, S, B>, Self::Error>
    where
        Sch: WireSchema<Self::Src, Table = T>,
        A: super::WireAdapter<Self::Src, S, B>;
}
