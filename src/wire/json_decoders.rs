//! Decoders shared by the JSON-based wire sources (Maxwell, Wal2Json).
//!
//! Each source carries column values as `&serde_json::Value`. The six decoder
//! functions here are generic over any payload type that implements
//! [`AsJsonValue`], eliminating byte-identical duplication between the two
//! sources while preserving all existing behavior and error strings.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use super::error::DecodeError;
use crate::encoding::Value;

/// Abstracts access to the JSON payload value and column name for a CDC column.
pub(crate) trait AsJsonValue {
    /// Returns the raw JSON value for this column.
    fn json_value(&self) -> &serde_json::Value;
    /// Returns the column name.
    fn column_name(&self) -> &str;
}

/// Decodes a JSON string column into [`Value::Text`], or [`Value::Null`].
///
/// # Errors
///
/// Returns [`DecodeError::WrongPayloadKind`] if the JSON value is not a string or null.
pub(crate) fn decode_json_text<P: AsJsonValue, S, B>(
    payload: &P,
) -> Result<Value<S, B>, DecodeError>
where
    S: From<String>,
{
    match payload.json_value() {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::String(s) => Ok(Value::Text(S::from(s.clone()))),
        serde_json::Value::Bool(_) => Err(DecodeError::WrongPayloadKind {
            column: payload.column_name().to_string(),
            expected: "JSON string",
            actual: "JSON boolean",
        }),
        serde_json::Value::Number(_) => Err(DecodeError::WrongPayloadKind {
            column: payload.column_name().to_string(),
            expected: "JSON string",
            actual: "JSON number",
        }),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Err(DecodeError::WrongPayloadKind {
                column: payload.column_name().to_string(),
                expected: "JSON string",
                actual: "JSON array or object",
            })
        }
    }
}

/// Decodes a JSON string or number decimal column into [`Value::Text`], or [`Value::Null`].
///
/// # Errors
///
/// Returns [`DecodeError::WrongPayloadKind`] if the JSON value is not a string, number, or null.
pub(crate) fn decode_json_decimal<P: AsJsonValue, S, B>(
    payload: &P,
) -> Result<Value<S, B>, DecodeError>
where
    S: From<String>,
{
    match payload.json_value() {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::String(s) => Ok(Value::Text(S::from(s.clone()))),
        serde_json::Value::Number(n) => Ok(Value::Text(S::from(n.to_string()))),
        _ => Err(DecodeError::WrongPayloadKind {
            column: payload.column_name().to_string(),
            expected: "JSON string or number decimal",
            actual: "other JSON shape",
        }),
    }
}

/// Decodes a UUID JSON string into a 16-byte [`Value::Blob`], or [`Value::Null`].
///
/// # Errors
///
/// Returns [`DecodeError::InvalidUuid`] if the string is not a valid UUID.
/// Returns [`DecodeError::WrongPayloadKind`] if the JSON value is not a string or null.
pub(crate) fn decode_json_uuid_blob<P: AsJsonValue, S, B>(
    payload: &P,
) -> Result<Value<S, B>, DecodeError>
where
    B: From<Vec<u8>>,
{
    match payload.json_value() {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::String(s) => match super::uuid_helpers::parse_uuid(s) {
            Ok(bytes) => Ok(Value::Blob(B::from(bytes.to_vec()))),
            Err(source_len) => Err(DecodeError::InvalidUuid {
                column: payload.column_name().to_string(),
                source_len,
            }),
        },
        _ => Err(DecodeError::WrongPayloadKind {
            column: payload.column_name().to_string(),
            expected: "JSON UUID string",
            actual: "other JSON shape",
        }),
    }
}

/// Decodes a UUID JSON string into a canonical 36-char [`Value::Text`], or [`Value::Null`].
///
/// # Errors
///
/// Returns [`DecodeError::InvalidUuid`] if the string is not a valid UUID.
/// Returns [`DecodeError::WrongPayloadKind`] if the JSON value is not a string or null.
pub(crate) fn decode_json_uuid_text<P: AsJsonValue, S, B>(
    payload: &P,
) -> Result<Value<S, B>, DecodeError>
where
    S: From<String>,
{
    match payload.json_value() {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::String(s) => {
            match super::uuid_helpers::preserve_or_canonicalize_uuid_text(s) {
                Ok(canonical) => Ok(Value::Text(S::from(canonical))),
                Err(source_len) => Err(DecodeError::InvalidUuid {
                    column: payload.column_name().to_string(),
                    source_len,
                }),
            }
        }
        _ => Err(DecodeError::WrongPayloadKind {
            column: payload.column_name().to_string(),
            expected: "JSON UUID string",
            actual: "other JSON shape",
        }),
    }
}

/// Decodes a JSON value column verbatim into [`Value::Text`], or [`Value::Null`].
///
/// String values pass through unchanged; objects and arrays are serialized compactly.
///
/// # Errors
///
/// Returns [`DecodeError::JsonNotSerializable`] if the JSON value cannot be serialized.
pub(crate) fn decode_json_verbatim<P: AsJsonValue, S, B>(
    payload: &P,
) -> Result<Value<S, B>, DecodeError>
where
    S: From<String>,
{
    match payload.json_value() {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::String(s) => Ok(Value::Text(S::from(s.clone()))),
        other => match super::json_helpers::serialize_verbatim(other) {
            Ok(text) => Ok(Value::Text(S::from(text))),
            Err(error) => Err(DecodeError::JsonNotSerializable {
                column: payload.column_name().to_string(),
                error,
            }),
        },
    }
}

/// Decodes a JSON value column with canonical key ordering into [`Value::Text`], or [`Value::Null`].
///
/// # Errors
///
/// Returns [`DecodeError::JsonNotSerializable`] if the JSON value cannot be serialized.
pub(crate) fn decode_json_canonical<P: AsJsonValue, S, B>(
    payload: &P,
) -> Result<Value<S, B>, DecodeError>
where
    S: From<String>,
{
    match payload.json_value() {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::String(s) => {
            let canon = super::json_helpers::canonicalize_string(s);
            Ok(Value::Text(S::from(canon)))
        }
        other => match super::json_helpers::canonicalize_to_string(other) {
            Ok(text) => Ok(Value::Text(S::from(text))),
            Err(error) => Err(DecodeError::JsonNotSerializable {
                column: payload.column_name().to_string(),
                error,
            }),
        },
    }
}
