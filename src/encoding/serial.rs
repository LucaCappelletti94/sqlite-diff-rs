//! SQLite changeset value encoding.
//!
//! **IMPORTANT**: SQLite changesets use a DIFFERENT encoding than database records!
//!
//! Changeset value types (used in this module):
//! - 0: Undefined (special marker for unchanged columns in UPDATE)
//! - 1: INTEGER (always 8 bytes, big-endian i64)
//! - 2: FLOAT (8 bytes, big-endian IEEE 754)
//! - 3: TEXT (varint length + UTF-8 bytes)
//! - 4: BLOB (varint length + raw bytes)
//! - 5: NULL (no data follows)
//!
//! This is NOT the same as SQLite database record serial types!
//! Database records use types 0-9 plus computed types for variable-length data.

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::hash::{Hash, Hasher};

use super::varint::encode_varint_simple;

/// A value that can be encoded in SQLite changeset format.
#[derive(Debug, Clone, Default)]
pub enum Value {
    /// SQL NULL
    Null,
    /// Integer (any size, will be encoded optimally)
    Integer(i64),
    /// IEEE 754 floating point
    Real(f64),
    /// UTF-8 text
    Text(String),
    /// Binary blob
    Blob(Vec<u8>),
    #[default]
    /// Undefined (used in changesets for unchanged columns)
    Undefined,
}

impl Value {
    /// Check if the value is Undefined.
    pub(crate) fn is_undefined(&self) -> bool {
        matches!(self, Value::Undefined)
    }

    /// Check if the value is Null.
    #[must_use]
    pub(crate) fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Real(a), Value::Real(b)) => a.to_bits() == b.to_bits(),
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) => a == b,
            (Value::Null | Value::Undefined, Value::Null | Value::Undefined) => true,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // IMPORTANT: Hash must be consistent with PartialEq.
        // Since Null == Undefined, they must hash identically.
        // We use a single tag (5) for both, matching the Null discriminant
        // choice, rather than core::mem::discriminant which would differ.
        let tag: u8 = match self {
            Value::Null | Value::Undefined => 0,
            Value::Integer(_) => 1,
            Value::Real(_) => 2,
            Value::Text(_) => 3,
            Value::Blob(_) => 4,
        };
        tag.hash(state);
        match self {
            Value::Integer(v) => v.hash(state),
            Value::Real(v) => v.to_bits().hash(state),
            Value::Text(v) => v.hash(state),
            Value::Blob(v) => v.hash(state),
            Value::Null | Value::Undefined => {}
        }
    }
}

// From implementations for common types
impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Integer(i64::from(v))
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_string())
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Real(v)
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Blob(v)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

mod display;

#[cfg(feature = "sqlparser")]
pub mod sqlparser;

/// Encode a value into the changeset binary format.
///
/// SQLite changesets use a DIFFERENT encoding than database records:
/// - Type 0: Undefined (special marker for unchanged columns in UPDATE)
/// - Type 1: INTEGER (always 8 bytes, big-endian i64)
/// - Type 2: FLOAT (8 bytes, big-endian IEEE 754)  
/// - Type 3: TEXT (varint length + UTF-8 bytes)
/// - Type 4: BLOB (varint length + raw bytes)
/// - Type 5: NULL (no data follows)
///
/// This is NOT the same as SQLite serial types used in database records!
pub(crate) fn encode_value(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Undefined => {
            // Undefined marker in changeset format (type 0)
            out.push(0x00);
        }
        Value::Null => {
            // NULL is type 5 in changeset format
            out.push(0x05);
        }
        Value::Integer(v) => {
            // INTEGER is type 1, always 8 bytes big-endian
            out.push(0x01);
            out.extend(v.to_be_bytes());
        }
        Value::Real(v) => {
            // SQLite converts NaN to NULL, but preserves Infinity
            if v.is_nan() {
                out.push(0x05); // NULL
            } else {
                // FLOAT is type 2, 8 bytes big-endian IEEE 754
                // SQLite normalizes -0.0 to 0.0
                out.push(0x02);
                let normalized = if *v == 0.0 { 0.0 } else { *v };
                out.extend(normalized.to_be_bytes());
            }
        }
        Value::Text(s) => {
            // TEXT is type 3, varint length + UTF-8 bytes
            out.push(0x03);
            out.extend(encode_varint_simple(s.len() as u64));
            out.extend(s.as_bytes());
        }
        Value::Blob(b) => {
            // BLOB is type 4, varint length + raw bytes
            out.push(0x04);
            out.extend(encode_varint_simple(b.len() as u64));
            out.extend(b);
        }
    }
}

/// Decode a value from changeset binary format.
///
/// SQLite changesets use the following type codes:
/// - 0: Undefined (unchanged column in UPDATE)
/// - 1: INTEGER (8 bytes big-endian)
/// - 2: FLOAT (8 bytes big-endian IEEE 754)
/// - 3: TEXT (varint length + UTF-8)
/// - 4: BLOB (varint length + raw bytes)
/// - 5: NULL
///
/// Returns the value and number of bytes consumed.
#[must_use]
pub(crate) fn decode_value(data: &[u8]) -> Option<(Value, usize)> {
    use super::varint::decode_varint;

    if data.is_empty() {
        return None;
    }

    let type_code = data[0];
    let data = &data[1..];

    match type_code {
        0 => {
            // Undefined marker
            Some((Value::Undefined, 1))
        }
        1 => {
            // INTEGER: 8 bytes big-endian
            if data.len() < 8 {
                return None;
            }
            let v = i64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            Some((Value::Integer(v), 9))
        }
        2 => {
            // FLOAT: 8 bytes big-endian IEEE 754
            if data.len() < 8 {
                return None;
            }
            let v = f64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            // SQLite normalizes NaN to NULL and -0.0 to 0.0, so we do the same
            // during decoding to ensure roundtrip consistency
            if v.is_nan() {
                Some((Value::Null, 9))
            } else {
                // Normalize -0.0 to 0.0
                let normalized = if v == 0.0 { 0.0 } else { v };
                Some((Value::Real(normalized), 9))
            }
        }
        3 => {
            // TEXT: varint length + UTF-8 bytes
            let (len, len_bytes) = decode_varint(data)?;
            let len = usize::try_from(len).ok()?;
            let data = &data[len_bytes..];
            if data.len() < len {
                return None;
            }
            let text = String::from_utf8(data[..len].to_vec()).ok()?;
            Some((Value::Text(text), 1 + len_bytes + len))
        }
        4 => {
            // BLOB: varint length + raw bytes
            let (len, len_bytes) = decode_varint(data)?;
            let len = usize::try_from(len).ok()?;
            let data = &data[len_bytes..];
            if data.len() < len {
                return None;
            }
            Some((Value::Blob(data[..len].to_vec()), 1 + len_bytes + len))
        }
        5 => {
            // NULL
            Some((Value::Null, 1))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_encode_decode_null() {
        let mut buf = Vec::new();
        encode_value(&mut buf, &Value::Null);
        let (decoded, len) = decode_value(&buf).unwrap();
        assert_eq!(decoded, Value::Null);
        assert_eq!(len, buf.len());
    }

    #[test]
    fn test_encode_decode_integers() {
        for v in [
            0,
            1,
            -1,
            127,
            -128,
            32767,
            -32768,
            i64::from(i32::MAX),
            i64::MAX,
        ] {
            let mut buf = Vec::new();
            encode_value(&mut buf, &Value::Integer(v));
            let (decoded, len) = decode_value(&buf).unwrap();
            assert_eq!(decoded, Value::Integer(v), "Failed for {v}");
            assert_eq!(len, buf.len());
            // All integers should be encoded as type 1 + 8 bytes = 9 bytes total
            assert_eq!(buf.len(), 9, "Integer {v} should be 9 bytes");
        }
    }

    #[test]
    fn test_encode_decode_real() {
        let mut buf = Vec::new();
        encode_value(&mut buf, &Value::Real(6.14159));
        let (decoded, len) = decode_value(&buf).unwrap();
        assert_eq!(decoded, Value::Real(6.14159));
        assert_eq!(len, buf.len());
        // Float is type 2 + 8 bytes = 9 bytes total
        assert_eq!(buf.len(), 9);
    }

    #[test]
    fn test_encode_decode_text() {
        let mut buf = Vec::new();
        encode_value(&mut buf, &Value::Text("hello".to_string()));
        let (decoded, len) = decode_value(&buf).unwrap();
        assert_eq!(decoded, Value::Text("hello".to_string()));
        assert_eq!(len, buf.len());
        // Text is type 3 + varint(5) + "hello" = 1 + 1 + 5 = 7 bytes
        assert_eq!(buf.len(), 7);
    }

    #[test]
    fn test_encode_decode_blob() {
        let mut buf = Vec::new();
        encode_value(&mut buf, &Value::Blob(vec![1, 2, 3, 4, 5]));
        let (decoded, len) = decode_value(&buf).unwrap();
        assert_eq!(decoded, Value::Blob(vec![1, 2, 3, 4, 5]));
        assert_eq!(len, buf.len());
        // Blob is type 4 + varint(5) + data = 1 + 1 + 5 = 7 bytes
        assert_eq!(buf.len(), 7);
    }

    #[test]
    fn test_encode_decode_undefined() {
        let mut buf = Vec::new();
        encode_value(&mut buf, &Value::Undefined);
        let (decoded, len) = decode_value(&buf).unwrap();
        assert_eq!(decoded, Value::Undefined);
        assert_eq!(len, buf.len());
        // Undefined is type 0, just 1 byte
        assert_eq!(buf.len(), 1);
    }

    #[test]
    fn test_changeset_encoding_matches_rusqlite() {
        // Test that our encoding matches what rusqlite produces

        // Integer 1 should be: type 1 + 8 bytes (big-endian)
        let mut buf = Vec::new();
        encode_value(&mut buf, &Value::Integer(1));
        assert_eq!(
            buf,
            vec![0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
        );

        // Integer 100 should be: type 1 + 8 bytes (big-endian)
        let mut buf = Vec::new();
        encode_value(&mut buf, &Value::Integer(100));
        assert_eq!(
            buf,
            vec![0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64]
        );

        // Text "alice" should be: type 3 + varint(5) + "alice"
        let mut buf = Vec::new();
        encode_value(&mut buf, &Value::Text("alice".to_string()));
        assert_eq!(buf, vec![0x03, 0x05, b'a', b'l', b'i', b'c', b'e']);

        // NULL should be type 5
        let mut buf = Vec::new();
        encode_value(&mut buf, &Value::Null);
        assert_eq!(buf, vec![0x05]);
    }
}
