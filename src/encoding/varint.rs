//! SQLite varint encoding/decoding.
//!
//! SQLite changesets use a big-endian variable-length integer encoding where:
//! - The high bit of each byte is a continuation flag (1 = more bytes follow)
//! - The remaining 7 bits are data, with MSB first
//!
//! For example, 300 (0x12C) encodes as:
//! - Binary: 100101100 (9 bits)
//! - Split into 7-bit groups from MSB: 0000010, 0101100
//! - First byte: 0x80 | 2 = 0x82 (continuation)
//! - Second byte: 0x2c (no continuation)
//! - Result: [0x82, 0x2c]

use alloc::vec;
use alloc::vec::Vec;

/// Encode a u64 as a SQLite changeset varint (big-endian, 7-bit continuation).
///
/// This is the format used by SQLite's session extension for text/blob lengths.
#[must_use]
pub(crate) fn encode_varint(value: u64) -> Vec<u8> {
    if value < 128 {
        // Single byte, no continuation needed
        return vec![u8::try_from(value).unwrap()];
    }

    // Extract 7-bit chunks starting from LSB, then reverse for big-endian
    let mut temp = Vec::new();
    let mut v = value;

    while v > 0 {
        temp.push((v & 0x7f) as u8);
        v >>= 7;
    }

    // Build result: MSB first, with continuation flags
    let mut result = Vec::with_capacity(temp.len());
    for (i, &byte) in temp.iter().rev().enumerate() {
        if i == temp.len() - 1 {
            // Last byte: no continuation flag
            result.push(byte);
        } else {
            // Not last byte: add continuation flag
            result.push(byte | 0x80);
        }
    }

    result
}

/// Alias for encode_varint - kept for backwards compatibility.
#[inline]
#[must_use]
pub(crate) fn encode_varint_simple(value: u64) -> Vec<u8> {
    encode_varint(value)
}

/// Decode a SQLite changeset varint (big-endian, 7-bit continuation).
///
/// Returns the decoded value and number of bytes consumed.
#[must_use]
pub(crate) fn decode_varint(data: &[u8]) -> Option<(u64, usize)> {
    if data.is_empty() {
        return None;
    }

    let mut value = 0u64;
    let mut i = 0;

    loop {
        if i >= data.len() {
            return None; // Incomplete varint
        }
        if i >= 10 {
            return None; // Varint too long (u64 needs max 10 bytes at 7 bits each)
        }

        let byte = data[i];
        value = (value << 7) | u64::from(byte & 0x7f);
        i += 1;

        if byte & 0x80 == 0 {
            // No continuation flag - this is the last byte
            break;
        }
    }

    Some((value, i))
}

/// Calculate the length in bytes needed to encode a value as a varint.
#[must_use]
#[allow(dead_code)]
pub(crate) fn varint_len(value: u64) -> usize {
    if value < 128 {
        1
    } else if value < 16384 {
        2
    } else if value < 2097152 {
        3
    } else if value < 268435456 {
        4
    } else if value < 34359738368 {
        5
    } else if value < 4398046511104 {
        6
    } else if value < 562949953421312 {
        7
    } else if value < 72057594037927936 {
        8
    } else if value < 9223372036854775808 {
        9
    } else {
        10
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_roundtrip_small() {
        for v in 0..128 {
            let encoded = encode_varint(v);
            let (decoded, len) = decode_varint(&encoded).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(len, encoded.len());
            assert_eq!(len, 1);
        }
    }

    #[test]
    fn test_varint_roundtrip_medium() {
        for v in [128, 255, 256, 300, 1000, 16383, 16384, 100000, 2097151] {
            let encoded = encode_varint(v);
            let (decoded, len) = decode_varint(&encoded).unwrap();
            assert_eq!(decoded, v, "Failed for value {v}");
            assert_eq!(len, encoded.len());
        }
    }

    #[test]
    fn test_varint_roundtrip_large() {
        for v in [
            0xFF_FFFF,
            0xFFFF_FFFF,
            0xFF_FFFF_FFFF,
            0xFFFF_FFFF_FFFF,
            0xFF_FFFF_FFFF_FFFF,
            u64::MAX,
        ] {
            let encoded = encode_varint(v);
            let (decoded, len) = decode_varint(&encoded).unwrap();
            assert_eq!(decoded, v, "Failed for value {v}");
            assert_eq!(len, encoded.len());
        }
    }

    #[test]
    fn test_varint_len() {
        assert_eq!(varint_len(0), 1);
        assert_eq!(varint_len(127), 1);
        assert_eq!(varint_len(128), 2);
        assert_eq!(varint_len(16383), 2);
        assert_eq!(varint_len(16384), 3);
    }

    #[test]
    fn test_varint_300() {
        // 300 should encode as [0x82, 0x2c] (big-endian varint)
        let encoded = encode_varint(300);
        assert_eq!(encoded, vec![0x82, 0x2c]);

        let (decoded, len) = decode_varint(&encoded).unwrap();
        assert_eq!(decoded, 300);
        assert_eq!(len, 2);
    }
}
