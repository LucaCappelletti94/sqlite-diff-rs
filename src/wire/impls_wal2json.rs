//! `Decoder` implementations and `TypeMapDefaults` for the `Wal2Json` source.

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
// BoolDecoder
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
// IntDecoder
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
// Int64OverflowToTextDecoder
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
// RealDecoder
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
// TextDecoder
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
// PgByteaTextModeDecoder
//
// wal2json v2 emits PG BYTEA as a JSON string of bare lowercase hex
// (no `\x` prefix). An optional `\x` prefix is also accepted.
// Null pass-through.
// ------------------------------------------------------------------

impl<S, B> Decoder<Wal2Json, S, B> for PgByteaTextModeDecoder
where
    B: From<Vec<u8>>,
{
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.value {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::String(s) => {
                match super::bytes_helpers::decode_wal2json_bytea_hex(s) {
                    Ok(bytes) => Ok(Value::Blob(B::from(bytes))),
                    Err(at) => Err(DecodeError::InvalidHexEscape {
                        column: payload.column_name.to_string(),
                        at,
                    }),
                }
            }
            _ => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "JSON string of hex bytes",
                actual: "other JSON shape",
            }),
        }
    }
}

// ------------------------------------------------------------------
// UuidBlob16Decoder and UuidText36Decoder
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
// DecimalTextDecoder
// ------------------------------------------------------------------

impl<S, B> Decoder<Wal2Json, S, B> for DecimalTextDecoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.value {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::String(s) => Ok(Value::Text(S::from(s.clone()))),
            // wal2json emits small numerics as JSON `Number`. The
            // `serde_json::Number` display preserves the parsed
            // digits, so we can safely round-trip through
            // `to_string`. Callers who need arbitrary-precision
            // decimals should enable serde_json's `arbitrary_precision`
            // feature upstream so `Number::to_string` yields the raw
            // wire text.
            serde_json::Value::Number(n) => Ok(Value::Text(S::from(n.to_string()))),
            _ => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "JSON string or number decimal",
                actual: "other JSON shape",
            }),
        }
    }
}

// ------------------------------------------------------------------
// Temporal verbatim decoders
// ------------------------------------------------------------------

fn decode_wal2json_string_verbatim<S, B>(
    payload: Wal2JsonColumn<'_>,
) -> Result<Value<S, B>, DecodeError>
where
    S: From<alloc::string::String>,
{
    match payload.value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::String(s) => Ok(Value::Text(S::from(s.clone()))),
        _ => Err(DecodeError::WrongPayloadKind {
            column: payload.column_name.to_string(),
            expected: "JSON string",
            actual: "other JSON shape",
        }),
    }
}

macro_rules! verbatim_impl {
    ($decoder:ty) => {
        impl<S, B> Decoder<Wal2Json, S, B> for $decoder
        where
            S: From<alloc::string::String>,
        {
            fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
                decode_wal2json_string_verbatim(payload)
            }
        }
    };
}

verbatim_impl!(TimestampVerbatimDecoder);
verbatim_impl!(TimestampTzVerbatimDecoder);
verbatim_impl!(DateVerbatimDecoder);
verbatim_impl!(TimeVerbatimDecoder);
verbatim_impl!(IntervalVerbatimDecoder);

// ------------------------------------------------------------------
// JsonVerbatimDecoder / JsonCanonicalDecoder
//
// Verbatim: serialize Object/Array via serde_json::to_string (compact)
// or pass string sources through unchanged. Canonical: sort keys
// recursively, then serialize compactly.
// ------------------------------------------------------------------

impl<S, B> Decoder<Wal2Json, S, B> for JsonVerbatimDecoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.value {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::String(s) => Ok(Value::Text(S::from(s.clone()))),
            other => match crate::wire::json_helpers::serialize_verbatim(other) {
                Ok(text) => Ok(Value::Text(S::from(text))),
                Err(error) => Err(DecodeError::JsonNotSerializable {
                    column: payload.column_name.to_string(),
                    error,
                }),
            },
        }
    }
}

impl<S, B> Decoder<Wal2Json, S, B> for JsonCanonicalDecoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.value {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::String(s) => {
                let canon = crate::wire::json_helpers::canonicalize_string(s);
                Ok(Value::Text(S::from(canon)))
            }
            other => match crate::wire::json_helpers::canonicalize_to_string(other) {
                Ok(text) => Ok(Value::Text(S::from(text))),
                Err(error) => Err(DecodeError::JsonNotSerializable {
                    column: payload.column_name.to_string(),
                    error,
                }),
            },
        }
    }
}

not_yet_impl!(PgByteaBinaryDecoder);
not_yet_impl!(MySqlBinaryDecoder);

// ------------------------------------------------------------------
// TypeMapDefaults.
// ------------------------------------------------------------------

impl<S, B> TypeMapDefaults<S, B> for Wal2Json
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
            .with(WireType::Bytes, PgByteaTextModeDecoder)
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
