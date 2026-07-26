//! Source-independent scalar decode primitives shared by the wire
//! [`Decoder`](super::Decoder) implementations.
//!
//! [`normalize_real`] matches SQLite's `decode_value` float handling and
//! is used by every source. The `decode_pg_*_binary` helpers decode the
//! PostgreSQL binary send format for integer, floating-point, and boolean
//! fields, which is identical whether the bytes arrive over logical
//! replication in binary mode ([`PgWalstream`](crate::pg_walstream::PgWalstream))
//! or as a binary query result ([`PgBinary`](super::PgBinary)).

use alloc::string::ToString;

use super::error::DecodeError;
use crate::encoding::Value;

/// Normalize a decoded float to match SQLite: NaN becomes `Value::Null`
/// and any zero (including `-0.0`) becomes `+0.0`.
#[inline]
pub(crate) fn normalize_real<S, B>(f: f64) -> Value<S, B> {
    if f.is_nan() {
        Value::Null
    } else if f == 0.0 {
        Value::Real(0.0)
    } else {
        Value::Real(f)
    }
}

/// Decode a PostgreSQL binary integer (`int2`/`int4`/`int8`) from its 2,
/// 4, or 8 byte big-endian two's complement form, widened to `i64`.
///
/// # Errors
///
/// [`DecodeError::WrongPayloadKind`] for any other byte count.
pub(crate) fn decode_pg_int_binary<S, B>(
    column_name: &str,
    bytes: &[u8],
) -> Result<Value<S, B>, DecodeError> {
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

/// Decode a PostgreSQL binary float (`float4`/`float8`) from its 4 or 8
/// byte big-endian IEEE 754 form, normalized via [`normalize_real`].
///
/// # Errors
///
/// [`DecodeError::WrongPayloadKind`] for any other byte count.
pub(crate) fn decode_pg_real_binary<S, B>(
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

/// Decode a PostgreSQL binary boolean: a single byte `0x01` (true) or
/// `0x00` (false).
///
/// # Errors
///
/// [`DecodeError::WrongPayloadKind`] for any other contents.
pub(crate) fn decode_pg_bool_binary<S, B>(
    column_name: &str,
    bytes: &[u8],
) -> Result<Value<S, B>, DecodeError> {
    match bytes {
        [0x01] => Ok(Value::Integer(1)),
        [0x00] => Ok(Value::Integer(0)),
        _ => Err(DecodeError::WrongPayloadKind {
            column: column_name.to_string(),
            expected: "single byte 0x00 or 0x01",
            actual: "other binary contents",
        }),
    }
}
