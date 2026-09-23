//! Source-independent scalar decode primitives shared by the wire
//! [`Decoder`](super::Decoder) implementations.
//!
//! [`normalize_real`] matches SQLite's `decode_value` float handling and
//! is used by every source. The `decode_pg_*_binary` helpers decode the
//! PostgreSQL binary send format for integer, floating-point, boolean,
//! numeric and JSON fields, which is identical whether the bytes arrive
//! over logical replication in binary mode ([`PgWalstream`](crate::pg_walstream::PgWalstream))
//! or as a binary query result ([`PgBinary`](super::PgBinary)).

use alloc::string::{String, ToString};
use core::fmt::Write;

use super::error::DecodeError;
use super::wire_type::WireType;
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

/// Render a PostgreSQL binary `numeric` as the text `numeric_out` prints,
/// so it matches the text a `numeric` carries over logical replication.
///
/// The layout is `ndigits`, `weight`, `sign` and `dscale` as big-endian
/// 16-bit fields, then `ndigits` base-10000 digits, the first of which is
/// multiplied by 10000 to the power `weight`.
///
/// # Errors
///
/// [`DecodeError::WrongPayloadKind`] for bytes `numeric_send` would not
/// produce.
pub(crate) fn decode_pg_numeric_binary<S, B>(
    column_name: &str,
    bytes: &[u8],
) -> Result<Value<S, B>, DecodeError>
where
    S: From<String>,
{
    render_pg_numeric(bytes)
        .map(|text| Value::Text(S::from(text)))
        .ok_or_else(|| DecodeError::WrongPayloadKind {
            column: column_name.to_string(),
            expected: "numeric binary",
            actual: "other binary contents",
        })
}

fn render_pg_numeric(bytes: &[u8]) -> Option<String> {
    const POSITIVE: u16 = 0x0000;
    const NEGATIVE: u16 = 0x4000;
    const NAN: u16 = 0xC000;
    const INFINITY: u16 = 0xD000;
    const NEGATIVE_INFINITY: u16 = 0xF000;
    const MAX_DSCALE: u16 = 0x3FFF;

    let field = |at: usize| Some(u16::from_be_bytes(bytes.get(at..at + 2)?.try_into().ok()?));
    let ndigits = usize::from(field(0)?);
    let weight = i64::from(field(2)?.cast_signed());
    let sign = field(4)?;
    let dscale = field(6)?;
    let digits = bytes.get(8..)?;
    if digits.len() != 2 * ndigits || dscale > MAX_DSCALE {
        return None;
    }
    let special = match sign {
        NAN => Some("NaN"),
        INFINITY => Some("Infinity"),
        NEGATIVE_INFINITY => Some("-Infinity"),
        POSITIVE | NEGATIVE => None,
        _ => return None,
    };
    if let Some(special) = special {
        return (ndigits == 0).then(|| String::from(special));
    }
    if digits
        .chunks_exact(2)
        .any(|d| u16::from_be_bytes([d[0], d[1]]) > 9999)
    {
        return None;
    }
    let digit = |index: i64| -> u16 {
        usize::try_from(index)
            .ok()
            .and_then(|i| field(8 + 2 * i))
            .unwrap_or(0)
    };

    let dscale = usize::from(dscale);
    let mut out = String::with_capacity(ndigits * 4 + dscale + 3);
    if sign == NEGATIVE {
        out.push('-');
    }
    if weight < 0 {
        out.push('0');
    } else {
        write!(out, "{}", digit(0)).unwrap();
        for index in 1..=weight {
            write!(out, "{:04}", digit(index)).unwrap();
        }
    }
    if dscale > 0 {
        out.push('.');
        let fraction_start = out.len();
        let mut index = weight + 1;
        while out.len() - fraction_start < dscale {
            write!(out, "{:04}", digit(index)).unwrap();
            index += 1;
        }
        out.truncate(fraction_start + dscale);
    }
    Some(out)
}

/// Decode a PostgreSQL binary `json` or `jsonb` into [`Value::Text`].
///
/// # Errors
///
/// As [`pg_json_binary_text`].
pub(crate) fn decode_pg_json_binary<S, B>(
    column_name: &str,
    wire_type: WireType,
    bytes: &[u8],
) -> Result<Value<S, B>, DecodeError>
where
    S: From<String>,
{
    pg_json_binary_text(column_name, wire_type, bytes)
        .map(|text| Value::Text(S::from(text.to_string())))
}

/// The JSON text of a PostgreSQL binary `json` (the UTF-8 text) or `jsonb`
/// (a version byte `1`, then the UTF-8 text).
///
/// # Errors
///
/// [`DecodeError::WrongPayloadKind`] for a `jsonb` version other than
/// `1`, [`DecodeError::InvalidUtf8`] for text that is not UTF-8.
pub(crate) fn pg_json_binary_text<'a>(
    column_name: &str,
    wire_type: WireType,
    bytes: &'a [u8],
) -> Result<&'a str, DecodeError> {
    let text = match (wire_type, bytes) {
        (WireType::Jsonb, [1, text @ ..]) => text,
        (WireType::Jsonb, _) => {
            return Err(DecodeError::WrongPayloadKind {
                column: column_name.to_string(),
                expected: "jsonb binary version 1",
                actual: "other jsonb binary version",
            });
        }
        _ => bytes,
    };
    core::str::from_utf8(text).map_err(|_| DecodeError::InvalidUtf8 {
        column: column_name.to_string(),
    })
}
