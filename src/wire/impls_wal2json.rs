//! `Decoder` implementations and `TypeMapDefaults` for the `Wal2Json`
//! source.
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
use crate::wal2json::{Wal2Json, Wal2JsonColumn};

// ------------------------------------------------------------------
// NullDecoder: trivial, always Null.
// ------------------------------------------------------------------

impl<S, B> Decoder<Wal2Json, S, B> for NullDecoder {
    fn decode(&self, _payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        Ok(Value::Null)
    }
}

// ------------------------------------------------------------------
// SnifferDecoder: 0.1.4 shape-sniffer behavior, deprecated migration
// bridge.
//
// Restricted to `Value<String, Vec<u8>>` since it reproduces the exact
// `TryFrom` shape callers already relied on.
// ------------------------------------------------------------------

#[allow(deprecated)]
impl Decoder<Wal2Json, alloc::string::String, Vec<u8>> for SnifferDecoder {
    fn decode(
        &self,
        payload: Wal2JsonColumn<'_>,
    ) -> Result<Value<alloc::string::String, Vec<u8>>, DecodeError> {
        match payload.value {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Bool(b) => Ok(Value::Integer(i64::from(*b))),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Value::Integer(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(Value::Real(f))
                } else {
                    Err(DecodeError::WrongPayloadKind {
                        column: payload.column_name.to_string(),
                        expected: "i64 or f64 number",
                        actual: "arbitrary-precision number",
                    })
                }
            }
            serde_json::Value::String(s) => Ok(Value::Text(s.clone())),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                Err(DecodeError::WrongPayloadKind {
                    column: payload.column_name.to_string(),
                    expected: "scalar JSON value",
                    actual: "array or object",
                })
            }
        }
    }
}

// ------------------------------------------------------------------
// BoolDecoder (Phase 1)
//
// wal2json v2 delivers PG booleans as JSON `true`/`false`. `null` maps
// to Value::Null. Anything else -> WrongPayloadKind.
// ------------------------------------------------------------------

impl<S, B> Decoder<Wal2Json, S, B> for BoolDecoder {
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.value {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Bool(b) => Ok(Value::Integer(i64::from(*b))),
            serde_json::Value::Number(_) => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "JSON boolean",
                actual: "JSON number",
            }),
            serde_json::Value::String(_) => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "JSON boolean",
                actual: "JSON string",
            }),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                Err(DecodeError::WrongPayloadKind {
                    column: payload.column_name.to_string(),
                    expected: "JSON boolean",
                    actual: "JSON array or object",
                })
            }
        }
    }
}

// ------------------------------------------------------------------
// IntDecoder (Phase 2)
// ------------------------------------------------------------------

impl<S, B> Decoder<Wal2Json, S, B> for IntDecoder {
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.value {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Number(n) => match n.as_i64() {
                Some(i) => Ok(Value::Integer(i)),
                None => Err(DecodeError::IntegerOverflow {
                    column: payload.column_name.to_string(),
                    digits: n.to_string(),
                }),
            },
            serde_json::Value::Bool(_) => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "JSON integer number",
                actual: "JSON boolean",
            }),
            serde_json::Value::String(_) => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "JSON integer number",
                actual: "JSON string",
            }),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                Err(DecodeError::WrongPayloadKind {
                    column: payload.column_name.to_string(),
                    expected: "JSON integer number",
                    actual: "JSON array or object",
                })
            }
        }
    }
}

// ------------------------------------------------------------------
// Int64OverflowToTextDecoder (Phase 2)
// ------------------------------------------------------------------

impl<S, B> Decoder<Wal2Json, S, B> for Int64OverflowToTextDecoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.value {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Number(n) => match n.as_i64() {
                Some(i) => Ok(Value::Integer(i)),
                None => Ok(Value::Text(S::from(n.to_string()))),
            },
            serde_json::Value::String(s)
                if s.trim_start_matches('-')
                    .chars()
                    .all(|c| c.is_ascii_digit()) =>
            {
                match s.parse::<i64>() {
                    Ok(i) => Ok(Value::Integer(i)),
                    Err(_) => Ok(Value::Text(S::from(s.clone()))),
                }
            }
            _ => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "JSON integer number or numeric string",
                actual: "other JSON shape",
            }),
        }
    }
}

// ------------------------------------------------------------------
// RealDecoder (Phase 3)
//
// NaN normalizes to Null, -0.0 to 0.0. Matches the crate's
// `decode_value` invariant.
// ------------------------------------------------------------------

impl<S, B> Decoder<Wal2Json, S, B> for RealDecoder {
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.value {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Number(n) => match n.as_f64() {
                Some(f) => Ok(normalize_real(f)),
                None => Err(DecodeError::WrongPayloadKind {
                    column: payload.column_name.to_string(),
                    expected: "IEEE 754 float number",
                    actual: "arbitrary-precision JSON number",
                }),
            },
            serde_json::Value::Bool(_) => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "IEEE 754 float number",
                actual: "JSON boolean",
            }),
            serde_json::Value::String(_) => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "IEEE 754 float number",
                actual: "JSON string",
            }),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                Err(DecodeError::WrongPayloadKind {
                    column: payload.column_name.to_string(),
                    expected: "IEEE 754 float number",
                    actual: "JSON array or object",
                })
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

// ------------------------------------------------------------------
// TextDecoder (Phase 4)
// ------------------------------------------------------------------

impl<S, B> Decoder<Wal2Json, S, B> for TextDecoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.value {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::String(s) => Ok(Value::Text(S::from(s.clone()))),
            serde_json::Value::Bool(_) => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "JSON string",
                actual: "JSON boolean",
            }),
            serde_json::Value::Number(_) => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "JSON string",
                actual: "JSON number",
            }),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                Err(DecodeError::WrongPayloadKind {
                    column: payload.column_name.to_string(),
                    expected: "JSON string",
                    actual: "JSON array or object",
                })
            }
        }
    }
}

// ------------------------------------------------------------------
// Skeleton impls for the schema-aware decoders. Populated per phase.
// ------------------------------------------------------------------

macro_rules! not_yet_impl {
    ($decoder:ty) => {
        impl<S, B> Decoder<Wal2Json, S, B> for $decoder {
            fn decode(&self, _payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
                Err(DecodeError::NotYetImplemented {
                    decoder: stringify!($decoder),
                })
            }
        }
    };
}

// ------------------------------------------------------------------
// PgByteaTextModeDecoder (Phase 5)
//
// wal2json v2 emits PG BYTEA as a JSON string in `\xHEX` form.
// Null pass-through.
// ------------------------------------------------------------------

impl<S, B> Decoder<Wal2Json, S, B> for PgByteaTextModeDecoder
where
    B: From<Vec<u8>>,
{
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.value {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::String(s) => match super::bytes_helpers::decode_pg_hex_escape(s) {
                Ok(bytes) => Ok(Value::Blob(B::from(bytes))),
                Err(at) => Err(DecodeError::InvalidHexEscape {
                    column: payload.column_name.to_string(),
                    at,
                }),
            },
            _ => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "JSON string with \\xHEX prefix",
                actual: "other JSON shape",
            }),
        }
    }
}

// ------------------------------------------------------------------
// UuidBlob16Decoder and UuidText36Decoder (Phase 6)
// ------------------------------------------------------------------

impl<S, B> Decoder<Wal2Json, S, B> for UuidBlob16Decoder
where
    B: From<Vec<u8>>,
{
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.value {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::String(s) => match super::uuid_helpers::parse_uuid(s) {
                Ok(bytes) => Ok(Value::Blob(B::from(bytes.to_vec()))),
                Err(source_len) => Err(DecodeError::InvalidUuid {
                    column: payload.column_name.to_string(),
                    source_len,
                }),
            },
            _ => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "JSON UUID string",
                actual: "other JSON shape",
            }),
        }
    }
}

impl<S, B> Decoder<Wal2Json, S, B> for UuidText36Decoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.value {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::String(s) => {
                match super::uuid_helpers::preserve_or_canonicalize_uuid_text(s) {
                    Ok(canonical) => Ok(Value::Text(S::from(canonical))),
                    Err(source_len) => Err(DecodeError::InvalidUuid {
                        column: payload.column_name.to_string(),
                        source_len,
                    }),
                }
            }
            _ => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "JSON UUID string",
                actual: "other JSON shape",
            }),
        }
    }
}

// ------------------------------------------------------------------
// DecimalTextDecoder (Phase 7)
// ------------------------------------------------------------------

impl<S, B> Decoder<Wal2Json, S, B> for DecimalTextDecoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.value {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::String(s) => Ok(Value::Text(S::from(s.clone()))),
            serde_json::Value::Number(_) => Err(DecodeError::DecimalPrecisionLoss {
                column: payload.column_name.to_string(),
            }),
            _ => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "JSON string decimal",
                actual: "other JSON shape",
            }),
        }
    }
}

not_yet_impl!(PgByteaBinaryDecoder);
not_yet_impl!(MySqlBinaryDecoder);
not_yet_impl!(TimestampVerbatimDecoder);
not_yet_impl!(TimestampTzVerbatimDecoder);
not_yet_impl!(DateVerbatimDecoder);
not_yet_impl!(TimeVerbatimDecoder);
not_yet_impl!(IntervalVerbatimDecoder);
not_yet_impl!(JsonVerbatimDecoder);
not_yet_impl!(JsonCanonicalDecoder);

// ------------------------------------------------------------------
// TypeMapDefaults: empty at Phase 0.
// ------------------------------------------------------------------

impl<S, B> TypeMapDefaults<S, B> for Wal2Json
where
    S: From<alloc::string::String>,
    B: From<Vec<u8>>,
{
    fn defaults() -> TypeMap<Self, S, B> {
        TypeMap::new()
            .with(alloc::sync::Arc::from("boolean"), BoolDecoder)
            .with(alloc::sync::Arc::from("smallint"), IntDecoder)
            .with(alloc::sync::Arc::from("integer"), IntDecoder)
            .with(alloc::sync::Arc::from("bigint"), IntDecoder)
            .with(alloc::sync::Arc::from("real"), RealDecoder)
            .with(alloc::sync::Arc::from("double precision"), RealDecoder)
            .with(alloc::sync::Arc::from("float4"), RealDecoder)
            .with(alloc::sync::Arc::from("text"), TextDecoder)
            .with(alloc::sync::Arc::from("varchar"), TextDecoder)
            .with(alloc::sync::Arc::from("character varying"), TextDecoder)
            .with(alloc::sync::Arc::from("character"), TextDecoder)
            .with(alloc::sync::Arc::from("char"), TextDecoder)
            .with(alloc::sync::Arc::from("name"), TextDecoder)
            .with(alloc::sync::Arc::from("bytea"), PgByteaTextModeDecoder)
            .with(alloc::sync::Arc::from("numeric"), DecimalTextDecoder)
            .with(alloc::sync::Arc::from("decimal"), DecimalTextDecoder)
            .with(alloc::sync::Arc::from("float8"), RealDecoder)
    }
}
