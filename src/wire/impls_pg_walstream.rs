//! `Decoder` implementations and `TypeMapDefaults` for the
//! `PgWalstream` source.
//!
//! Phase 0: every decoder except `NullDecoder` and `SnifferDecoder`
//! returns `DecodeError::NotYetImplemented { decoder }`. Later phases
//! populate the impls one payload family at a time.

use alloc::string::ToString;
use alloc::vec::Vec;

#[allow(deprecated)]
use super::decoder::SnifferDecoder;
use super::decoder::{
    BoolDecoder, DateVerbatimDecoder, DecimalTextDecoder, Decoder, Int64OverflowToTextDecoder,
    IntDecoder, IntervalVerbatimDecoder, JsonCanonicalDecoder, JsonVerbatimDecoder,
    MySqlBinaryDecoder, NullDecoder, PgByteaBinaryDecoder, PgByteaTextModeDecoder, RealDecoder,
    TextDecoder, TimeVerbatimDecoder, TimestampTzVerbatimDecoder, TimestampVerbatimDecoder,
    UuidBlob16Decoder, UuidText36Decoder,
};
use super::error::DecodeError;
use super::type_map::{TypeMap, TypeMapDefaults};
use crate::encoding::Value;
use crate::pg_walstream::{ColumnValue, PgWalstream, PgWalstreamColumn};

impl<S, B> Decoder<PgWalstream, S, B> for NullDecoder {
    fn decode(&self, _payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        Ok(Value::Null)
    }
}

#[allow(deprecated)]
impl Decoder<PgWalstream, alloc::string::String, Vec<u8>> for SnifferDecoder {
    fn decode(
        &self,
        payload: PgWalstreamColumn<'_>,
    ) -> Result<Value<alloc::string::String, Vec<u8>>, DecodeError> {
        match payload.data {
            ColumnValue::Null => Ok(Value::Null),
            ColumnValue::Binary(b) => Ok(Value::Blob(b.to_vec())),
            ColumnValue::Text(b) => {
                let Some(s) = payload.data.as_str() else {
                    return Ok(Value::Blob(b.to_vec()));
                };
                if s == "t" {
                    return Ok(Value::Integer(1));
                }
                if s == "f" {
                    return Ok(Value::Integer(0));
                }
                if let Ok(i) = s.parse::<i64>() {
                    return Ok(Value::Integer(i));
                }
                if let Ok(f) = s.parse::<f64>() {
                    return Ok(Value::Real(f));
                }
                Ok(Value::Text(s.to_string()))
            }
        }
    }
}

/// Postgres bool OID. Registered under this key by
/// [`TypeMapDefaults::defaults`].
pub const PG_BOOL: crate::pg_walstream::Oid = 16;

// ------------------------------------------------------------------
// BoolDecoder (Phase 1)
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

/// Postgres `int2` OID (SMALLINT).
pub const PG_INT2: crate::pg_walstream::Oid = 21;
/// Postgres `int4` OID (INTEGER).
pub const PG_INT4: crate::pg_walstream::Oid = 23;
/// Postgres `int8` OID (BIGINT).
pub const PG_INT8: crate::pg_walstream::Oid = 20;

// ------------------------------------------------------------------
// IntDecoder (Phase 2)
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
            ColumnValue::Binary(b) => {
                decode_pg_int_binary(payload.column_name, payload.oid, b.as_ref())
            }
        }
    }
}

fn decode_pg_int_binary<S, B>(
    column_name: &str,
    oid: crate::pg_walstream::Oid,
    bytes: &[u8],
) -> Result<Value<S, B>, DecodeError> {
    match (oid, bytes.len()) {
        (PG_INT2, 2) => {
            let arr: [u8; 2] = bytes.try_into().unwrap();
            Ok(Value::Integer(i16::from_be_bytes(arr).into()))
        }
        (PG_INT4, 4) => {
            let arr: [u8; 4] = bytes.try_into().unwrap();
            Ok(Value::Integer(i32::from_be_bytes(arr).into()))
        }
        (PG_INT8, 8) => {
            let arr: [u8; 8] = bytes.try_into().unwrap();
            Ok(Value::Integer(i64::from_be_bytes(arr)))
        }
        _ => Err(DecodeError::WrongPayloadKind {
            column: column_name.to_string(),
            expected: "int2, int4, or int8 binary with matching byte width",
            actual: "OID and byte width disagreement",
        }),
    }
}

// ------------------------------------------------------------------
// Int64OverflowToTextDecoder (Phase 2)
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

/// Postgres `float4` OID (REAL).
pub const PG_FLOAT4: crate::pg_walstream::Oid = 700;
/// Postgres `float8` OID (DOUBLE PRECISION).
pub const PG_FLOAT8: crate::pg_walstream::Oid = 701;

// ------------------------------------------------------------------
// RealDecoder (Phase 3)
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
            ColumnValue::Binary(b) => {
                decode_pg_real_binary(payload.column_name, payload.oid, b.as_ref())
            }
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
    oid: crate::pg_walstream::Oid,
    bytes: &[u8],
) -> Result<Value<S, B>, DecodeError> {
    match (oid, bytes.len()) {
        (PG_FLOAT4, 4) => {
            let arr: [u8; 4] = bytes.try_into().unwrap();
            Ok(normalize_real(f64::from(f32::from_be_bytes(arr))))
        }
        (PG_FLOAT8, 8) => {
            let arr: [u8; 8] = bytes.try_into().unwrap();
            Ok(normalize_real(f64::from_be_bytes(arr)))
        }
        _ => Err(DecodeError::WrongPayloadKind {
            column: column_name.to_string(),
            expected: "float4 or float8 binary with matching byte width",
            actual: "OID and byte width disagreement",
        }),
    }
}

/// Postgres `text` OID.
pub const PG_TEXT: crate::pg_walstream::Oid = 25;
/// Postgres `varchar` OID.
pub const PG_VARCHAR: crate::pg_walstream::Oid = 1043;
/// Postgres `bpchar` (character) OID.
pub const PG_BPCHAR: crate::pg_walstream::Oid = 1042;
/// Postgres `name` OID.
pub const PG_NAME: crate::pg_walstream::Oid = 19;

// ------------------------------------------------------------------
// TextDecoder (Phase 4)
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

not_yet_impl!(PgByteaBinaryDecoder);
not_yet_impl!(PgByteaTextModeDecoder);
not_yet_impl!(MySqlBinaryDecoder);
not_yet_impl!(UuidBlob16Decoder);
not_yet_impl!(UuidText36Decoder);
not_yet_impl!(DecimalTextDecoder);
not_yet_impl!(TimestampVerbatimDecoder);
not_yet_impl!(TimestampTzVerbatimDecoder);
not_yet_impl!(DateVerbatimDecoder);
not_yet_impl!(TimeVerbatimDecoder);
not_yet_impl!(IntervalVerbatimDecoder);
not_yet_impl!(JsonVerbatimDecoder);
not_yet_impl!(JsonCanonicalDecoder);

impl<S, B> TypeMapDefaults<S, B> for PgWalstream
where
    S: From<alloc::string::String>,
{
    fn defaults() -> TypeMap<Self, S, B> {
        TypeMap::new()
            .with(PG_BOOL, BoolDecoder)
            .with(PG_INT2, IntDecoder)
            .with(PG_INT4, IntDecoder)
            .with(PG_INT8, IntDecoder)
            .with(PG_FLOAT4, RealDecoder)
            .with(PG_FLOAT8, RealDecoder)
            .with(PG_TEXT, TextDecoder)
            .with(PG_VARCHAR, TextDecoder)
            .with(PG_BPCHAR, TextDecoder)
            .with(PG_NAME, TextDecoder)
    }
}
