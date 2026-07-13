//! [`Decoder`]: per-(source, semantic) conversion from a payload into
//! [`Value`](crate::encoding::Value), plus every built-in decoder unit
//! type the crate ships.
//!
//! Phase 0 populates the trait and all unit types. Semantics are filled
//! in per phase: Phase 1 populates [`BoolDecoder`], Phase 2 populates
//! [`IntDecoder`] and [`Int64OverflowToTextDecoder`], and so on.
//! Skeleton implementations return
//! [`DecodeError::NotYetImplemented`](super::DecodeError::NotYetImplemented).

use super::adapter::WireAdapter;
use super::error::DecodeError;
use super::source::WireSource;
use crate::encoding::Value;

/// Converts a per-column payload into a [`Value`].
///
/// Zero-sized unit-struct implementations (the crate's built-ins) cover
/// the common cases. User-defined stateful decoders carry their own
/// fields (lookup tables, compiled regexes, per-adapter configuration)
/// and implement the same trait shape.
///
/// See `docs/schema-aware-forward-conversion.md` Section 3.1 for the
/// built-in decoder table.
pub trait Decoder<Src: WireSource, S, B> {
    /// Decode one payload.
    ///
    /// # Errors
    ///
    /// Returns a [`DecodeError`] variant describing the failure. Every
    /// variant carries the offending column name so the outer
    /// [`WireAdapter`] impl does not have to wrap.
    fn decode(&self, payload: Src::Payload<'_>) -> Result<Value<S, B>, DecodeError>;
}

// ------------------------------------------------------------------
// Skeleton decoder unit types.
//
// Every one is a zero-sized marker whose `impl Decoder<Src, S, B>` for
// each `Src` this crate supports lives in the format module
// (`pg_walstream`, `wal2json`, `maxwell`). Phase 0 ships every impl
// returning `DecodeError::NotYetImplemented { decoder: "..." }` except
// for `NullDecoder` (which is trivial) and `SnifferDecoder` (which
// reproduces 0.1.4 behavior for migration).
// ------------------------------------------------------------------

/// Decoder producing `Value::Null` regardless of the payload's non-null
/// contents. Registered internally when the payload's null discriminator
/// is set. Users normally do not register this directly.
#[derive(Debug, Clone, Copy, Default)]
pub struct NullDecoder;

/// Decoder for boolean columns. Populates in Phase 1.
#[derive(Debug, Clone, Copy, Default)]
pub struct BoolDecoder;

/// Decoder for integer columns fitting in `i64`. Populates in Phase 2.
#[derive(Debug, Clone, Copy, Default)]
pub struct IntDecoder;

/// Decoder for integer columns whose wire values may exceed `i64`.
/// Values fitting `i64` produce `Value::Integer`, values above produce
/// `Value::Text` with the base-10 digits preserved. Populates in Phase 2.
#[derive(Debug, Clone, Copy, Default)]
pub struct Int64OverflowToTextDecoder;

/// Decoder for floating-point columns. Populates in Phase 3.
#[derive(Debug, Clone, Copy, Default)]
pub struct RealDecoder;

/// Decoder for text-affinity columns. Populates in Phase 4.
#[derive(Debug, Clone, Copy, Default)]
pub struct TextDecoder;

/// Decoder for pg_walstream binary-mode BYTEA. Populates in Phase 5.
#[derive(Debug, Clone, Copy, Default)]
pub struct PgByteaBinaryDecoder;

/// Decoder for pg_walstream text-mode BYTEA (`\xHEX` form).
/// Populates in Phase 5.
#[derive(Debug, Clone, Copy, Default)]
pub struct PgByteaTextModeDecoder;

/// Decoder for MySQL binary-family columns (base64 on the wire).
/// Populates in Phase 5.
#[derive(Debug, Clone, Copy, Default)]
pub struct MySqlBinaryDecoder;

/// Decoder rendering UUID wire text into a 16-byte `Value::Blob`.
/// Populates in Phase 6.
#[derive(Debug, Clone, Copy, Default)]
pub struct UuidBlob16Decoder;

/// Decoder rendering UUID wire text into a verbatim `Value::Text(36)`.
/// Populates in Phase 6.
#[derive(Debug, Clone, Copy, Default)]
pub struct UuidText36Decoder;

/// Decoder for `numeric`/`decimal` columns, preserving precision as
/// `Value::Text`. Populates in Phase 7.
#[derive(Debug, Clone, Copy, Default)]
pub struct DecimalTextDecoder;

/// Decoder for `timestamp` (without time zone). Preserves the wire text
/// verbatim. Populates in Phase 8.
#[derive(Debug, Clone, Copy, Default)]
pub struct TimestampVerbatimDecoder;

/// Decoder for `timestamptz`. Preserves the wire text verbatim.
/// Populates in Phase 8.
#[derive(Debug, Clone, Copy, Default)]
pub struct TimestampTzVerbatimDecoder;

/// Decoder for `date`. Preserves the wire text verbatim.
/// Populates in Phase 8.
#[derive(Debug, Clone, Copy, Default)]
pub struct DateVerbatimDecoder;

/// Decoder for `time`/`timetz`. Preserves the wire text verbatim.
/// Populates in Phase 8.
#[derive(Debug, Clone, Copy, Default)]
pub struct TimeVerbatimDecoder;

/// Decoder for `interval`. Preserves the wire text verbatim.
/// Populates in Phase 8.
#[derive(Debug, Clone, Copy, Default)]
pub struct IntervalVerbatimDecoder;

/// Decoder for `json`/`jsonb`. Preserves the wire form verbatim as
/// `Value::Text`. Populates in Phase 9.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonVerbatimDecoder;

/// Decoder for `json`/`jsonb` that canonicalizes into sorted-key
/// compact JSON text. Populates in Phase 9.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonCanonicalDecoder;

/// 0.1.4 content sniffer preserved as an explicit escape hatch.
///
/// Existing callers whose `TryFrom` impls were removed can restore
/// behavior with one line. Slated for removal in 0.3.0.
#[derive(Debug, Clone, Copy, Default)]
#[deprecated(
    since = "0.2.0",
    note = "sniffer-based decoding is unsound; register schema-aware decoders on a TypeMap instead"
)]
pub struct SnifferDecoder;

/// [`WireAdapter`] wrapper around [`SnifferDecoder`] for one-line
/// migration from 0.1.4.
///
/// Downstream callers whose `patchset = event.try_into()?` calls
/// disappeared in 0.2.0 recover with
/// `patchset.digest_pg_walstream(&event, &relation, &SnifferAdapter)`.
/// Slated for removal in 0.3.0.
#[derive(Debug, Clone, Copy, Default)]
#[deprecated(
    since = "0.2.0",
    note = "sniffer-based decoding is unsound; use a TypeMap with schema-aware decoders"
)]
pub struct SnifferAdapter;

#[allow(deprecated)]
impl<Src: WireSource, S, B> WireAdapter<Src, S, B> for SnifferAdapter
where
    SnifferDecoder: Decoder<Src, S, B>,
{
    fn decode(&self, payload: Src::Payload<'_>) -> Result<Value<S, B>, DecodeError> {
        SnifferDecoder.decode(payload)
    }
}
