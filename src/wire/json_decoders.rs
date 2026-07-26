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

#[cfg(test)]
mod tests {
    use super::{
        AsJsonValue, decode_json_canonical, decode_json_decimal, decode_json_text,
        decode_json_uuid_blob, decode_json_uuid_text, decode_json_verbatim,
    };
    use crate::encoding::Value;
    use crate::wire::DecodeError;
    use alloc::string::{String, ToString};
    use alloc::vec::Vec;

    /// Minimal [`AsJsonValue`] payload for exercising the shared decoders.
    struct Col {
        value: serde_json::Value,
        name: &'static str,
    }

    impl AsJsonValue for Col {
        fn json_value(&self) -> &serde_json::Value {
            &self.value
        }
        fn column_name(&self) -> &str {
            self.name
        }
    }

    fn col(value: serde_json::Value) -> Col {
        Col { value, name: "c" }
    }

    const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

    fn obj() -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert("b".to_string(), serde_json::Value::Bool(true));
        map.insert(
            "a".to_string(),
            serde_json::Value::Number(serde_json::Number::from(1_i64)),
        );
        serde_json::Value::Object(map)
    }

    #[test]
    fn text_rejects_every_non_string_shape() {
        let cases = [
            (serde_json::Value::Bool(true), "JSON boolean"),
            (
                serde_json::Value::Number(serde_json::Number::from(1_i64)),
                "JSON number",
            ),
            (serde_json::Value::Array(Vec::new()), "JSON array or object"),
            (obj(), "JSON array or object"),
        ];
        for (value, want_actual) in cases {
            match decode_json_text::<_, String, Vec<u8>>(&col(value)).unwrap_err() {
                DecodeError::WrongPayloadKind { actual, .. } => assert_eq!(actual, want_actual),
                other => panic!("expected WrongPayloadKind, got {other:?}"),
            }
        }
    }

    #[test]
    fn decimal_rejects_non_scalar_shapes() {
        let err = decode_json_decimal::<_, String, Vec<u8>>(&col(serde_json::Value::Bool(true)))
            .unwrap_err();
        assert!(matches!(err, DecodeError::WrongPayloadKind { .. }));
        let err =
            decode_json_decimal::<_, String, Vec<u8>>(&col(serde_json::Value::Array(Vec::new())))
                .unwrap_err();
        assert!(matches!(err, DecodeError::WrongPayloadKind { .. }));
    }

    #[test]
    fn uuid_blob_error_and_ok_paths() {
        // Malformed UUID string.
        let err = decode_json_uuid_blob::<_, String, Vec<u8>>(&col(serde_json::Value::String(
            "not-a-uuid".to_string(),
        )))
        .unwrap_err();
        assert!(matches!(err, DecodeError::InvalidUuid { .. }));
        // Non-string shape.
        let err = decode_json_uuid_blob::<_, String, Vec<u8>>(&col(serde_json::Value::Number(
            serde_json::Number::from(5_i64),
        )))
        .unwrap_err();
        assert!(matches!(err, DecodeError::WrongPayloadKind { .. }));
        // Valid UUID string.
        let ok = decode_json_uuid_blob::<_, String, Vec<u8>>(&col(serde_json::Value::String(
            UUID.to_string(),
        )))
        .unwrap();
        assert!(matches!(ok, Value::Blob(b) if b.len() == 16));
    }

    #[test]
    fn uuid_text_error_paths() {
        let err = decode_json_uuid_text::<_, String, Vec<u8>>(&col(serde_json::Value::String(
            "bad".to_string(),
        )))
        .unwrap_err();
        assert!(matches!(err, DecodeError::InvalidUuid { .. }));
        let err = decode_json_uuid_text::<_, String, Vec<u8>>(&col(serde_json::Value::Bool(true)))
            .unwrap_err();
        assert!(matches!(err, DecodeError::WrongPayloadKind { .. }));
    }

    #[test]
    fn verbatim_and_canonical_serialize_non_string_values() {
        // Exercises the non-string ("other") serialization arm of both.
        let v = decode_json_verbatim::<_, String, Vec<u8>>(&col(obj())).unwrap();
        assert!(matches!(v, Value::Text(_)));
        let c = decode_json_canonical::<_, String, Vec<u8>>(&col(obj())).unwrap();
        // Canonical output sorts object keys.
        assert_eq!(c, Value::Text("{\"a\":1,\"b\":true}".to_string()));
    }
}
