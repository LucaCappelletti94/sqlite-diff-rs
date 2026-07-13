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

not_yet_impl!(BoolDecoder);
not_yet_impl!(IntDecoder);
not_yet_impl!(Int64OverflowToTextDecoder);
not_yet_impl!(RealDecoder);
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

impl<S, B> TypeMapDefaults<S, B> for Maxwell {
    fn defaults() -> TypeMap<Self, S, B> {
        TypeMap::new()
    }
}
