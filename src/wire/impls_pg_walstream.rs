//! `Decoder` implementations and `TypeMapDefaults` for the `PgWalstream` source.

use alloc::string::ToString;
use alloc::vec::Vec;

use super::decoder::{
    BoolDecoder, DateVerbatimDecoder, DecimalTextDecoder, Decoder, Int64OverflowToTextDecoder,
    IntDecoder, IntervalVerbatimDecoder, JsonCanonicalDecoder, JsonVerbatimDecoder,
    MySqlBinaryDecoder, NullDecoder, PgByteaBinaryDecoder, PgByteaTextModeDecoder, RealDecoder,
    TextDecoder, TimeVerbatimDecoder, TimestampTzVerbatimDecoder, TimestampVerbatimDecoder,
    UuidBlob16Decoder, UuidText36Decoder,
};
use super::error::DecodeError;
use super::type_map::{TypeMap, TypeMapDefaults};
use super::wire_type::WireType;
use crate::encoding::Value;
use crate::pg_walstream::{ColumnValue, PgWalstream, PgWalstreamColumn};

impl<S, B> Decoder<PgWalstream, S, B> for NullDecoder {
    fn decode(&self, _payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        Ok(Value::Null)
    }
}

// ------------------------------------------------------------------
// BoolDecoder
//
// pg_walstream text mode: `"t"` -> 1, `"f"` -> 0.
// pg_walstream binary mode: single byte 0x01 -> 1, 0x00 -> 0.
// Null pass-through.
// Anything else -> WrongPayloadKind.
// ------------------------------------------------------------------

impl<S, B> Decoder<PgWalstream, S, B> for BoolDecoder {
    fn decode(&self, payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.data {
            ColumnValue::Null => Ok(Value::Null),
            ColumnValue::Text(_) => match payload.data.as_str() {
                Some("t") => Ok(Value::Integer(1)),
                Some("f") => Ok(Value::Integer(0)),
                other => Err(DecodeError::WrongPayloadKind {
                    column: payload.column_name.to_string(),
                    expected: "\"t\" or \"f\"",
                    actual: match other {
                        Some(_) => "arbitrary text",
                        None => "non-utf8 bytes",
                    },
                }),
            },
            ColumnValue::Binary(b) => match b.as_ref() {
                [0x01] => Ok(Value::Integer(1)),
                [0x00] => Ok(Value::Integer(0)),
                _ => Err(DecodeError::WrongPayloadKind {
                    column: payload.column_name.to_string(),
                    expected: "single byte 0x00 or 0x01",
                    actual: "other binary contents",
                }),
            },
        }
    }
}

// ------------------------------------------------------------------
// IntDecoder
//
// Text mode: base-10 parse via `str::parse::<i64>`. Overflow raises
// `IntegerOverflow`. Binary mode: width inferred from OID (int2 = 2
// bytes, int4 = 4, int8 = 8), big-endian. Null pass-through.
// ------------------------------------------------------------------

impl<S, B> Decoder<PgWalstream, S, B> for IntDecoder {
    fn decode(&self, payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.data {
            ColumnValue::Null => Ok(Value::Null),
            ColumnValue::Text(_) => {
                let s = payload
                    .data
                    .as_str()
                    .ok_or_else(|| DecodeError::InvalidUtf8 {
                        column: payload.column_name.to_string(),
                    })?;
                match s.parse::<i64>() {
                    Ok(i) => Ok(Value::Integer(i)),
                    Err(_)
                        if s.trim_start_matches('-')
                            .chars()
                            .all(|c| c.is_ascii_digit()) =>
                    {
                        Err(DecodeError::IntegerOverflow {
                            column: payload.column_name.to_string(),
                            digits: s.to_string(),
                        })
                    }
                    Err(_) => Err(DecodeError::WrongPayloadKind {
                        column: payload.column_name.to_string(),
                        expected: "base-10 signed integer",
                        actual: "non-numeric text",
                    }),
                }
            }
            ColumnValue::Binary(b) => decode_pg_int_binary(payload.column_name, b.as_ref()),
        }
    }
}

fn decode_pg_int_binary<S, B>(column_name: &str, bytes: &[u8]) -> Result<Value<S, B>, DecodeError> {
    match bytes.len() {
        2 => {
            let arr: [u8; 2] = bytes.try_into().unwrap();
            Ok(Value::Integer(i16::from_be_bytes(arr).into()))
        }
        4 => {
            let arr: [u8; 4] = bytes.try_into().unwrap();
            Ok(Value::Integer(i32::from_be_bytes(arr).into()))
        }
        8 => {
            let arr: [u8; 8] = bytes.try_into().unwrap();
            Ok(Value::Integer(i64::from_be_bytes(arr)))
        }
        _ => Err(DecodeError::WrongPayloadKind {
            column: column_name.to_string(),
            expected: "int2, int4, or int8 binary (2, 4, or 8 bytes)",
            actual: "unexpected binary integer width",
        }),
    }
}

// ------------------------------------------------------------------
// Int64OverflowToTextDecoder
//
// pg_walstream rarely surfaces true bigint-unsigned overflow (Postgres
// has no such type), but this decoder tolerates the shape for
// symmetry with the Maxwell path. Wire text that does not fit i64
// stays as base-10 digits in Value::Text.
// ------------------------------------------------------------------

impl<S, B> Decoder<PgWalstream, S, B> for Int64OverflowToTextDecoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.data {
            ColumnValue::Null => Ok(Value::Null),
            ColumnValue::Text(_) => {
                let s = payload
                    .data
                    .as_str()
                    .ok_or_else(|| DecodeError::InvalidUtf8 {
                        column: payload.column_name.to_string(),
                    })?;
                match s.parse::<i64>() {
                    Ok(i) => Ok(Value::Integer(i)),
                    Err(_)
                        if s.trim_start_matches('-')
                            .chars()
                            .all(|c| c.is_ascii_digit()) =>
                    {
                        Ok(Value::Text(S::from(s.to_string())))
                    }
                    Err(_) => Err(DecodeError::WrongPayloadKind {
                        column: payload.column_name.to_string(),
                        expected: "base-10 integer text",
                        actual: "non-numeric text",
                    }),
                }
            }
            ColumnValue::Binary(_) => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "text-mode integer",
                actual: "binary payload",
            }),
        }
    }
}

// ------------------------------------------------------------------
// RealDecoder
//
// Text mode: `str::parse::<f64>` accepts "NaN"/"Infinity"/"-Infinity"
// and standard decimal / exponential forms. NaN normalizes to Null,
// -0.0 normalizes to 0.0 (matching the crate's `decode_value`).
// Binary mode: float4 = 4-byte big-endian IEEE 754, float8 = 8-byte.
// ------------------------------------------------------------------

impl<S, B> Decoder<PgWalstream, S, B> for RealDecoder {
    fn decode(&self, payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.data {
            ColumnValue::Null => Ok(Value::Null),
            ColumnValue::Text(_) => {
                let s = payload
                    .data
                    .as_str()
                    .ok_or_else(|| DecodeError::InvalidUtf8 {
                        column: payload.column_name.to_string(),
                    })?;
                match s.parse::<f64>() {
                    Ok(f) => Ok(normalize_real(f)),
                    Err(_) => Err(DecodeError::WrongPayloadKind {
                        column: payload.column_name.to_string(),
                        expected: "IEEE 754 float text",
                        actual: "non-numeric text",
                    }),
                }
            }
            ColumnValue::Binary(b) => decode_pg_real_binary(payload.column_name, b.as_ref()),
        }
    }
}

#[inline]
fn normalize_real<S, B>(f: f64) -> Value<S, B> {
    if f.is_nan() {
        Value::Null
    } else if f == 0.0 {
        Value::Real(0.0)
    } else {
        Value::Real(f)
    }
}

fn decode_pg_real_binary<S, B>(
    column_name: &str,
    bytes: &[u8],
) -> Result<Value<S, B>, DecodeError> {
    match bytes.len() {
        4 => {
            let arr: [u8; 4] = bytes.try_into().unwrap();
            Ok(normalize_real(f64::from(f32::from_be_bytes(arr))))
        }
        8 => {
            let arr: [u8; 8] = bytes.try_into().unwrap();
            Ok(normalize_real(f64::from_be_bytes(arr)))
        }
        _ => Err(DecodeError::WrongPayloadKind {
            column: column_name.to_string(),
            expected: "float4 or float8 binary (4 or 8 bytes)",
            actual: "unexpected binary float width",
        }),
    }
}

// ------------------------------------------------------------------
// TextDecoder
//
// Text mode: UTF-8 validate, pass through as Value::Text. Invalid
// UTF-8 raises `InvalidUtf8` rather than silently coercing to a Blob.
// Binary mode is rejected for text columns.
// ------------------------------------------------------------------

impl<S, B> Decoder<PgWalstream, S, B> for TextDecoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.data {
            ColumnValue::Null => Ok(Value::Null),
            ColumnValue::Text(_) => {
                let s = payload
                    .data
                    .as_str()
                    .ok_or_else(|| DecodeError::InvalidUtf8 {
                        column: payload.column_name.to_string(),
                    })?;
                Ok(Value::Text(S::from(s.to_string())))
            }
            ColumnValue::Binary(_) => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "UTF-8 text",
                actual: "binary payload",
            }),
        }
    }
}

// ------------------------------------------------------------------
// PgByteaBinaryDecoder
//
// Handles both `ColumnValue::Binary` (pass-through) and text-mode
// `\xHEX` (decoded via the vendored helper). Null pass-through.
// This is the recommended default for PG_BYTEA columns since it
// tolerates both transport modes.
// ------------------------------------------------------------------

impl<S, B> Decoder<PgWalstream, S, B> for PgByteaBinaryDecoder
where
    B: From<Vec<u8>>,
{
    fn decode(&self, payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.data {
            ColumnValue::Null => Ok(Value::Null),
            ColumnValue::Binary(b) => Ok(Value::Blob(B::from(b.to_vec()))),
            ColumnValue::Text(_) => {
                let s = payload
                    .data
                    .as_str()
                    .ok_or_else(|| DecodeError::InvalidUtf8 {
                        column: payload.column_name.to_string(),
                    })?;
                match super::bytes_helpers::decode_pg_hex_escape(s) {
                    Ok(bytes) => Ok(Value::Blob(B::from(bytes))),
                    Err(at) => Err(DecodeError::InvalidHexEscape {
                        column: payload.column_name.to_string(),
                        at,
                    }),
                }
            }
        }
    }
}

// ------------------------------------------------------------------
// UuidBlob16Decoder and UuidText36Decoder
//
// Both accept 36-character hyphenated and braced forms of a UUID
// wire text and produce `Value::Blob([u8; 16])` or `Value::Text(36)`
// respectively. Neither is registered in `defaults()`. Users pick
// per column.
// ------------------------------------------------------------------

impl<S, B> Decoder<PgWalstream, S, B> for UuidBlob16Decoder
where
    B: From<Vec<u8>>,
{
    fn decode(&self, payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        decode_pg_uuid_to_blob(payload)
    }
}

impl<S, B> Decoder<PgWalstream, S, B> for UuidText36Decoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        decode_pg_uuid_to_text(payload)
    }
}

fn decode_pg_uuid_to_blob<S, B>(payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError>
where
    B: From<Vec<u8>>,
{
    match payload.data {
        ColumnValue::Null => Ok(Value::Null),
        ColumnValue::Text(_) => {
            let s = payload
                .data
                .as_str()
                .ok_or_else(|| DecodeError::InvalidUtf8 {
                    column: payload.column_name.to_string(),
                })?;
            match super::uuid_helpers::parse_uuid(s) {
                Ok(bytes) => Ok(Value::Blob(B::from(bytes.to_vec()))),
                Err(source_len) => Err(DecodeError::InvalidUuid {
                    column: payload.column_name.to_string(),
                    source_len,
                }),
            }
        }
        ColumnValue::Binary(_) => Err(DecodeError::WrongPayloadKind {
            column: payload.column_name.to_string(),
            expected: "UUID text form",
            actual: "binary payload",
        }),
    }
}

fn decode_pg_uuid_to_text<S, B>(payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError>
where
    S: From<alloc::string::String>,
{
    match payload.data {
        ColumnValue::Null => Ok(Value::Null),
        ColumnValue::Text(_) => {
            let s = payload
                .data
                .as_str()
                .ok_or_else(|| DecodeError::InvalidUtf8 {
                    column: payload.column_name.to_string(),
                })?;
            match super::uuid_helpers::preserve_or_canonicalize_uuid_text(s) {
                Ok(canonical) => Ok(Value::Text(S::from(canonical))),
                Err(source_len) => Err(DecodeError::InvalidUuid {
                    column: payload.column_name.to_string(),
                    source_len,
                }),
            }
        }
        ColumnValue::Binary(_) => Err(DecodeError::WrongPayloadKind {
            column: payload.column_name.to_string(),
            expected: "UUID text form",
            actual: "binary payload",
        }),
    }
}

// ------------------------------------------------------------------
// DecimalTextDecoder
// ------------------------------------------------------------------

impl<S, B> Decoder<PgWalstream, S, B> for DecimalTextDecoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.data {
            ColumnValue::Null => Ok(Value::Null),
            ColumnValue::Text(_) => {
                let s = payload
                    .data
                    .as_str()
                    .ok_or_else(|| DecodeError::InvalidUtf8 {
                        column: payload.column_name.to_string(),
                    })?;
                Ok(Value::Text(S::from(s.to_string())))
            }
            ColumnValue::Binary(_) => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "text-mode numeric",
                actual: "binary payload",
            }),
        }
    }
}

// PgByteaTextModeDecoder and MySqlBinaryDecoder are wire-format
// specific to wal2json / maxwell respectively; on pg_walstream they
// stay NotYetImplemented.

macro_rules! not_yet_impl {
    ($decoder:ty) => {
        impl<S, B> Decoder<PgWalstream, S, B> for $decoder {
            fn decode(&self, _payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError> {
                Err(DecodeError::NotYetImplemented {
                    decoder: stringify!($decoder),
                })
            }
        }
    };
}

// ------------------------------------------------------------------
// Temporal verbatim decoders
//
// Preserve wire text form as `Value::Text`. Null pass-through.
// Reject binary payloads.
// ------------------------------------------------------------------

fn decode_pg_text_verbatim<S, B>(payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError>
where
    S: From<alloc::string::String>,
{
    match payload.data {
        ColumnValue::Null => Ok(Value::Null),
        ColumnValue::Text(_) => {
            let s = payload
                .data
                .as_str()
                .ok_or_else(|| DecodeError::InvalidUtf8 {
                    column: payload.column_name.to_string(),
                })?;
            Ok(Value::Text(S::from(s.to_string())))
        }
        ColumnValue::Binary(_) => Err(DecodeError::WrongPayloadKind {
            column: payload.column_name.to_string(),
            expected: "text form",
            actual: "binary payload",
        }),
    }
}

macro_rules! verbatim_impl {
    ($decoder:ty) => {
        impl<S, B> Decoder<PgWalstream, S, B> for $decoder
        where
            S: From<alloc::string::String>,
        {
            fn decode(&self, payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError> {
                decode_pg_text_verbatim(payload)
            }
        }
    };
}

verbatim_impl!(TimestampVerbatimDecoder);
verbatim_impl!(TimestampTzVerbatimDecoder);
verbatim_impl!(DateVerbatimDecoder);
verbatim_impl!(TimeVerbatimDecoder);
verbatim_impl!(IntervalVerbatimDecoder);

verbatim_impl!(JsonVerbatimDecoder);

// For pg_walstream, JSON canonical is the same as verbatim because
// the wire carries JSON as opaque text; canonicalization requires
// re-parsing which lives in the JSON helpers on the wal2json /
// maxwell paths.
impl<S, B> Decoder<PgWalstream, S, B> for JsonCanonicalDecoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.data {
            ColumnValue::Null => Ok(Value::Null),
            ColumnValue::Text(_) => {
                let s = payload
                    .data
                    .as_str()
                    .ok_or_else(|| DecodeError::InvalidUtf8 {
                        column: payload.column_name.to_string(),
                    })?;
                let canon = crate::wire::json_helpers::canonicalize_string(s);
                Ok(Value::Text(S::from(canon)))
            }
            ColumnValue::Binary(_) => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "text-mode JSON",
                actual: "binary payload",
            }),
        }
    }
}

not_yet_impl!(PgByteaTextModeDecoder);
not_yet_impl!(MySqlBinaryDecoder);

impl<S, B> TypeMapDefaults<S, B> for PgWalstream
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
            .with(WireType::Uuid, UuidText36Decoder)
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
