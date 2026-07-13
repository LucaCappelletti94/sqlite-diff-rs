//! `Decoder` implementations and `TypeMapDefaults` for the `Maxwell`
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
use crate::maxwell::{Maxwell, MaxwellColumn};

impl<S, B> Decoder<Maxwell, S, B> for NullDecoder {
    fn decode(&self, _payload: MaxwellColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        Ok(Value::Null)
    }
}

#[allow(deprecated)]
impl Decoder<Maxwell, alloc::string::String, Vec<u8>> for SnifferDecoder {
    fn decode(
        &self,
        payload: MaxwellColumn<'_>,
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
// Maxwell delivers MySQL `tinyint(1)` bool values as either JSON
// `true`/`false` or as integer 0/1 (config-dependent). Both are
// accepted. Null pass-through. Anything else -> WrongPayloadKind.
// ------------------------------------------------------------------

impl<S, B> Decoder<Maxwell, S, B> for BoolDecoder {
    fn decode(&self, payload: MaxwellColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        match payload.value {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Bool(b) => Ok(Value::Integer(i64::from(*b))),
            serde_json::Value::Number(n) => match n.as_i64() {
                Some(0) => Ok(Value::Integer(0)),
                Some(1) => Ok(Value::Integer(1)),
                _ => Err(DecodeError::WrongPayloadKind {
                    column: payload.column_name.to_string(),
                    expected: "JSON bool or number 0/1",
                    actual: "number outside {0, 1}",
                }),
            },
            serde_json::Value::String(_) => Err(DecodeError::WrongPayloadKind {
                column: payload.column_name.to_string(),
                expected: "JSON bool or number 0/1",
                actual: "JSON string",
            }),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                Err(DecodeError::WrongPayloadKind {
                    column: payload.column_name.to_string(),
                    expected: "JSON bool or number 0/1",
                    actual: "JSON array or object",
                })
            }
        }
    }
}

// ------------------------------------------------------------------
// IntDecoder (Phase 2)
// ------------------------------------------------------------------

impl<S, B> Decoder<Maxwell, S, B> for IntDecoder {
    fn decode(&self, payload: MaxwellColumn<'_>) -> Result<Value<S, B>, DecodeError> {
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
//
// Load-bearing for MySQL `bigint unsigned` columns whose wire values
// can exceed `i64::MAX`.
// ------------------------------------------------------------------

impl<S, B> Decoder<Maxwell, S, B> for Int64OverflowToTextDecoder
where
    S: From<alloc::string::String>,
{
    fn decode(&self, payload: MaxwellColumn<'_>) -> Result<Value<S, B>, DecodeError> {
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
// ------------------------------------------------------------------

impl<S, B> Decoder<Maxwell, S, B> for RealDecoder {
    fn decode(&self, payload: MaxwellColumn<'_>) -> Result<Value<S, B>, DecodeError> {
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

macro_rules! not_yet_impl {
    ($decoder:ty) => {
        impl<S, B> Decoder<Maxwell, S, B> for $decoder {
            fn decode(&self, _payload: MaxwellColumn<'_>) -> Result<Value<S, B>, DecodeError> {
                Err(DecodeError::NotYetImplemented {
                    decoder: stringify!($decoder),
                })
            }
        }
    };
}

not_yet_impl!(TextDecoder);
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

impl<S, B> TypeMapDefaults<S, B> for Maxwell
where
    S: From<alloc::string::String>,
{
    fn defaults() -> TypeMap<Self, S, B> {
        TypeMap::new()
            .with(alloc::sync::Arc::from("tinyint(1)"), BoolDecoder)
            .with(alloc::sync::Arc::from("tinyint"), IntDecoder)
            .with(alloc::sync::Arc::from("smallint"), IntDecoder)
            .with(alloc::sync::Arc::from("mediumint"), IntDecoder)
            .with(alloc::sync::Arc::from("int"), IntDecoder)
            .with(alloc::sync::Arc::from("bigint"), IntDecoder)
            .with(alloc::sync::Arc::from("float"), RealDecoder)
            .with(alloc::sync::Arc::from("double"), RealDecoder)
            .with(alloc::sync::Arc::from("real"), RealDecoder)
            .with(
                alloc::sync::Arc::from("bigint unsigned"),
                Int64OverflowToTextDecoder,
            )
    }
}
