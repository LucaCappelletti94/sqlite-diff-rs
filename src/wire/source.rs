//! [`WireSource`]: sealed marker trait for CDC wire formats.

use super::sealed::Sealed;
use super::wire_type::WireType;

/// Per-format marker naming a CDC wire source.
///
/// Implementors are unit structs owned by each format module
/// (`PgWalstream`, `Wal2Json`, `Maxwell`). The associated payload type
/// describes the format's per-column wire data.
///
/// Type identity is no longer source-native. Every payload carries a
/// source-independent [`WireType`] that selects the decoder, so one
/// semantic catalog drives every source without a per-source
/// translation table.
pub trait WireSource: Sealed {
    /// Per-column payload the format hands to a decoder.
    ///
    /// Every payload struct carries a `column_name: &'a str` so decoder
    /// errors are self-describing without an outer wrapping layer.
    type Payload<'a>;

    /// Semantic type of the column carried by the payload, used for
    /// decoder dispatch.
    fn wire_type(payload: &Self::Payload<'_>) -> WireType;

    /// Extract the column name from a payload for diagnostic messages.
    fn column_name<'a>(payload: &'a Self::Payload<'_>) -> &'a str;
}

/// Schema-side semantic type for one column of one table.
pub trait WireColumnTypes {
    /// Semantic [`WireType`] for the column at `column_index`.
    fn column_type(&self, column_index: usize) -> WireType;
}

/// Table-name lookup for the [`DiffSetBuilder::digest`](crate::DiffSetBuilder::digest) entry point.
pub trait WireSchema {
    /// Concrete schema type for one table.
    type Table: crate::schema::NamedColumns + WireColumnTypes;

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
    T: crate::schema::NamedColumns + WireColumnTypes,
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
        Sch: WireSchema<Table = T>,
        A: super::WireAdapter<Self::Src, S, B>;
}
