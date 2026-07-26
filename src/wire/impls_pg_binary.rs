//! `Decoder` implementations and `TypeMapDefaults` for the [`PgBinary`]
//! source: decoding PostgreSQL binary result fields straight into
//! [`Value`].
//!
//! The Postgres binary send format is the same whether it arrives over
//! logical replication in binary mode or as a binary query result, so
//! these decoders mirror the binary arms of the `PgWalstream` impls and
//! produce byte-identical [`Value`]s.

use alloc::string::ToString;
use alloc::vec::Vec;

use super::decoder::{
    BoolDecoder, DateVerbatimDecoder, DecimalTextDecoder, Decoder, IntDecoder,
    IntervalVerbatimDecoder, JsonVerbatimDecoder, NullDecoder, PgByteaBinaryDecoder, RealDecoder,
    TextDecoder, TimeVerbatimDecoder, TimestampTzVerbatimDecoder, TimestampVerbatimDecoder,
    UuidBlob16Decoder,
};
use super::error::DecodeError;
use super::scalar_helpers::{decode_pg_bool_binary, decode_pg_int_binary, decode_pg_real_binary};
use super::source::{PgBinary, PgBinaryColumn};
use super::type_map::{TypeMap, TypeMapDefaults};
use super::wire_type::WireType;
use crate::encoding::Value;

impl<S, B> Decoder<PgBinary, S, B> for NullDecoder {
    fn decode(&self, _payload: PgBinaryColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        Ok(Value::Null)
    }
}

// ------------------------------------------------------------------
// BoolDecoder: single byte 0x01 -> 1, 0x00 -> 0. Null pass-through.
// ------------------------------------------------------------------

impl<S, B> Decoder<PgBinary, S, B> for BoolDecoder {
    fn decode(&self, payload: PgBinaryColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.raw {
            None => Ok(Value::Null),
            Some(bytes) => decode_pg_bool_binary(payload.column_name, bytes),
        }
    }
}

// ------------------------------------------------------------------
// IntDecoder: int2/int4/int8 as 2/4/8-byte big-endian two's complement.
// ------------------------------------------------------------------

impl<S, B> Decoder<PgBinary, S, B> for IntDecoder {
    fn decode(&self, payload: PgBinaryColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.raw {
            None => Ok(Value::Null),
            Some(bytes) => decode_pg_int_binary(payload.column_name, bytes),
        }
    }
}

// ------------------------------------------------------------------
// RealDecoder: float4/float8 as 4/8-byte big-endian IEEE 754. NaN
// normalizes to Null, -0.0 to 0.0, matching `decode_value`.
// ------------------------------------------------------------------

impl<S, B> Decoder<PgBinary, S, B> for RealDecoder {
    fn decode(&self, payload: PgBinaryColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.raw {
            None => Ok(Value::Null),
            Some(bytes) => decode_pg_real_binary(payload.column_name, bytes),
        }
    }
}

// ------------------------------------------------------------------
// TextDecoder: UTF-8 bytes verbatim. Invalid UTF-8 -> InvalidUtf8.
// ------------------------------------------------------------------

impl<S, B> Decoder<PgBinary, S, B> for TextDecoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: PgBinaryColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        let Some(bytes) = payload.raw else {
            return Ok(Value::Null);
        };
        match core::str::from_utf8(bytes) {
            Ok(s) => Ok(Value::Text(S::from(s.to_string()))),
            Err(_) => Err(DecodeError::InvalidUtf8 {
                column: payload.column_name.to_string(),
            }),
        }
    }
}

// ------------------------------------------------------------------
// PgByteaBinaryDecoder: raw bytes verbatim into Value::Blob.
// ------------------------------------------------------------------

impl<S, B> Decoder<PgBinary, S, B> for PgByteaBinaryDecoder
where
    B: From<Vec<u8>>,
{
    fn decode(&self, payload: PgBinaryColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        let Some(bytes) = payload.raw else {
            return Ok(Value::Null);
        };
        Ok(Value::Blob(B::from(bytes.to_vec())))
    }
}

// ------------------------------------------------------------------
// UuidBlob16Decoder: the source bytes are already the 16 raw uuid
// bytes, taken verbatim when the length is 16. Any other length errors.
// The output matches the CDC path's 16-byte blob, which is the point.
// ------------------------------------------------------------------

impl<S, B> Decoder<PgBinary, S, B> for UuidBlob16Decoder
where
    B: From<Vec<u8>>,
{
    fn decode(&self, payload: PgBinaryColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        let Some(bytes) = payload.raw else {
            return Ok(Value::Null);
        };
        if bytes.len() == 16 {
            Ok(Value::Blob(B::from(bytes.to_vec())))
        } else {
            Err(DecodeError::InvalidUuid {
                column: payload.column_name.to_string(),
                source_len: bytes.len(),
            })
        }
    }
}

// ------------------------------------------------------------------
// Deferred set. Their binary layouts are numeric and must be rendered
// back to text byte-identically to the verbatim/decimal CDC decoders
// before they can be enabled. Until then they return a clear error
// rather than a lossy or diverging value. The `defaults()` map already
// routes each deferred WireType to its eventual decoder so enabling one
// later is a body change, not a wiring change.
// ------------------------------------------------------------------

macro_rules! not_yet_impl {
    ($decoder:ty) => {
        impl<S, B> Decoder<PgBinary, S, B> for $decoder {
            fn decode(&self, payload: PgBinaryColumn<'_>) -> Result<Value<S, B>, DecodeError> {
                if payload.raw.is_none() {
                    return Ok(Value::Null);
                }
                Err(DecodeError::NotYetImplemented {
                    decoder: stringify!($decoder),
                })
            }
        }
    };
}

not_yet_impl!(DecimalTextDecoder);
not_yet_impl!(TimestampVerbatimDecoder);
not_yet_impl!(TimestampTzVerbatimDecoder);
not_yet_impl!(DateVerbatimDecoder);
not_yet_impl!(TimeVerbatimDecoder);
not_yet_impl!(IntervalVerbatimDecoder);
not_yet_impl!(JsonVerbatimDecoder);

impl<S, B> TypeMapDefaults<S, B> for PgBinary
where
    S: From<alloc::string::String>,
    B: From<Vec<u8>>,
{
    fn defaults() -> TypeMap<Self, S, B> {
        TypeMap::new()
            .with(WireType::Bool, BoolDecoder)
            .with(WireType::Int, IntDecoder)
            .with(WireType::Real, RealDecoder)
            .with(WireType::Text, TextDecoder)
            .with(WireType::Bytes, PgByteaBinaryDecoder)
            .with(WireType::Uuid, UuidBlob16Decoder)
            .with(WireType::Decimal, DecimalTextDecoder)
            .with(WireType::Timestamp, TimestampVerbatimDecoder)
            .with(WireType::TimestampTz, TimestampTzVerbatimDecoder)
            .with(WireType::Date, DateVerbatimDecoder)
            .with(WireType::Time, TimeVerbatimDecoder)
            .with(WireType::Interval, IntervalVerbatimDecoder)
            .with(WireType::Json, JsonVerbatimDecoder)
            .with(WireType::Jsonb, JsonVerbatimDecoder)
    }
}
