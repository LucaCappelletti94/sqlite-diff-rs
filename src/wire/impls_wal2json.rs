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
use super::scalar_helpers::normalize_real;
use super::type_map::{TypeMap, TypeMapDefaults};
use super::wire_type::WireType;
use crate::encoding::Value;
use crate::wal2json::{Wal2Json, Wal2JsonColumn};

impl super::json_decoders::AsJsonValue for crate::wal2json::Wal2JsonColumn<'_> {
    fn json_value(&self) -> &serde_json::Value {
        self.value
    }

    fn column_name(&self) -> &str {
        self.column_name
    }
}

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

// ------------------------------------------------------------------
// TextDecoder
// ------------------------------------------------------------------

impl<S, B> Decoder<Wal2Json, S, B> for TextDecoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        super::json_decoders::decode_json_text(&payload)
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
        super::json_decoders::decode_json_uuid_blob(&payload)
    }
}

impl<S, B> Decoder<Wal2Json, S, B> for UuidText36Decoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        super::json_decoders::decode_json_uuid_text(&payload)
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
        super::json_decoders::decode_json_decimal(&payload)
    }
}

// ------------------------------------------------------------------
// Temporal verbatim decoders
// ------------------------------------------------------------------

macro_rules! verbatim_impl {
    ($decoder:ty) => {
        impl<S, B> Decoder<Wal2Json, S, B> for $decoder
        where
            S: From<alloc::string::String>,
        {
            fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
                super::json_decoders::decode_json_text(&payload)
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
        super::json_decoders::decode_json_verbatim(&payload)
    }
}

impl<S, B> Decoder<Wal2Json, S, B> for JsonCanonicalDecoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        super::json_decoders::decode_json_canonical(&payload)
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
