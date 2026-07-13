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

impl<S, B> TypeMapDefaults<S, B> for PgWalstream {
    fn defaults() -> TypeMap<Self, S, B> {
        TypeMap::new()
    }
}
