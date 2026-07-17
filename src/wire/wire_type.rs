//! [`WireType`]: source-independent semantic column type.

/// Semantic column type used to select a decoder, independent of any
/// wire source's native type identity.
///
/// The schema side declares one of these per column (via
/// [`WireColumnTypes`](super::WireColumnTypes)) and
/// [`TypeMap`](super::TypeMap) dispatches on it. This replaces the older
/// per-source native key (`Oid` for pg_walstream, type-name strings for
/// wal2json and maxwell): a single catalog carrying semantic types now
/// drives every source without a per-source translation table.
///
/// The set covers every decoder the crate ships. `Json` and `Jsonb`
/// stay distinct so a downstream that treats them differently can
/// register different decoders. Decoder policy variants (for example
/// [`UuidText36Decoder`](super::UuidText36Decoder) versus
/// [`UuidBlob16Decoder`](super::UuidBlob16Decoder), or
/// [`JsonVerbatimDecoder`](super::JsonVerbatimDecoder) versus
/// [`JsonCanonicalDecoder`](super::JsonCanonicalDecoder)) are chosen at
/// registration under the one semantic key, not by adding more keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum WireType {
    /// Boolean.
    Bool,
    /// Signed integer fitting in `i64`.
    Int,
    /// Floating-point.
    Real,
    /// UTF-8 text.
    Text,
    /// Raw bytes.
    Bytes,
    /// UUID.
    Uuid,
    /// Arbitrary-precision decimal / numeric.
    Decimal,
    /// Timestamp without time zone.
    Timestamp,
    /// Timestamp with time zone.
    TimestampTz,
    /// Calendar date.
    Date,
    /// Time of day.
    Time,
    /// Interval / duration.
    Interval,
    /// JSON.
    Json,
    /// Binary JSON (`jsonb`).
    Jsonb,
}
