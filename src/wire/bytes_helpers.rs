//! Byte-decoding helpers for binary and hex escape payloads.
//!
//! Two small `no_std`-clean routines: PG `\xHEX` decode for text-mode
//! BYTEA, and standard base64 (RFC 4648) decode for MySQL wire values
//! delivered as JSON strings. Both are vendored to avoid pulling
//! external deps for a hundred lines of parsing.

use alloc::vec::Vec;

/// Decode a PG `\xHEX` escape (produced by `\x` prefix + even-length
/// lowercase or uppercase hex) into raw bytes.
///
/// # Errors
///
/// Returns the zero-based byte offset (into `s`) of the first invalid
/// hex character. When the input does not begin with `\x` or has an
/// odd hex length, the offset points at that anomaly.
pub(crate) fn decode_pg_hex_escape(s: &str) -> Result<Vec<u8>, usize> {
    let bytes = s.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'\\' || bytes[1] != b'x' {
        return Err(0);
    }
    // Offsets stay relative to `s`, so shift past the `\x` prefix.
    decode_hex(&bytes[2..]).map_err(|at| at + 2)
}

/// Decode a wal2json BYTEA hex string into raw bytes.
///
/// wal2json emits BYTEA as bare lowercase hex with no `\x` prefix, so
/// accept both the bare form and a Postgres-style `\x`-prefixed form.
///
/// # Errors
///
/// Returns the zero-based byte offset (into the hex payload, after any
/// `\x` prefix) of the first invalid hex character, or of the odd
/// trailing nibble.
pub(crate) fn decode_wal2json_bytea_hex(s: &str) -> Result<Vec<u8>, usize> {
    let hex = s.strip_prefix("\\x").unwrap_or(s);
    decode_hex(hex.as_bytes())
}

/// Decode an even-length hex slice into raw bytes.
///
/// # Errors
///
/// Returns the byte offset within `hex` of the first invalid character,
/// or `hex.len() - 1` when the length is odd.
fn decode_hex(hex: &[u8]) -> Result<Vec<u8>, usize> {
    if hex.len() % 2 != 0 {
        return Err(hex.len().saturating_sub(1));
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    for (i, chunk) in hex.chunks_exact(2).enumerate() {
        let hi = hex_nibble(chunk[0]).ok_or(i * 2)?;
        let lo = hex_nibble(chunk[1]).ok_or(i * 2 + 1)?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

#[inline]
fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Decode a standard base64 (RFC 4648) string, tolerant of trailing
/// `=` padding. Returns `Err(())` on any invalid character or invalid
/// length.
pub(crate) fn decode_base64(s: &str) -> Result<Vec<u8>, ()> {
    let src = s.as_bytes();
    // Strip trailing '=' padding for length calc.
    let mut effective_len = src.len();
    while effective_len > 0 && src[effective_len - 1] == b'=' {
        effective_len -= 1;
    }
    // Every 4 base64 chars yield 3 output bytes. Reject non-multiple-of-4.
    if src.len() % 4 != 0 {
        return Err(());
    }
    let mut out = Vec::with_capacity((effective_len * 3) / 4);
    let mut buf = 0u32;
    let mut collected = 0u32;
    for &c in &src[..effective_len] {
        let v = base64_char(c).ok_or(())?;
        buf = (buf << 6) | u32::from(v);
        collected += 6;
        if collected >= 8 {
            collected -= 8;
            out.push(((buf >> collected) & 0xFF) as u8);
        }
    }
    Ok(out)
}

#[inline]
fn base64_char(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn hex_decode_deadbeef() {
        assert_eq!(
            decode_pg_hex_escape("\\xdeadbeef").unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
    }

    #[test]
    fn hex_decode_uppercase() {
        assert_eq!(
            decode_pg_hex_escape("\\xDEADBEEF").unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
    }

    #[test]
    fn hex_decode_empty() {
        assert_eq!(decode_pg_hex_escape("\\x").unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn hex_decode_missing_prefix() {
        assert!(decode_pg_hex_escape("deadbeef").is_err());
    }

    #[test]
    fn hex_decode_odd_length() {
        assert!(decode_pg_hex_escape("\\xdea").is_err());
    }

    #[test]
    fn wal2json_bytea_bare_hex() {
        assert_eq!(
            decode_wal2json_bytea_hex("0001deadff").unwrap(),
            vec![0x00, 0x01, 0xDE, 0xAD, 0xFF]
        );
    }

    #[test]
    fn wal2json_bytea_prefixed_hex() {
        assert_eq!(
            decode_wal2json_bytea_hex("\\xdeadbeef").unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
    }

    #[test]
    fn wal2json_bytea_odd_length() {
        assert!(decode_wal2json_bytea_hex("0001d").is_err());
    }

    #[test]
    fn wal2json_bytea_invalid_char() {
        assert!(decode_wal2json_bytea_hex("00zz").is_err());
    }

    #[test]
    fn hex_decode_invalid_char() {
        assert!(decode_pg_hex_escape("\\xzz").is_err());
    }

    #[test]
    fn base64_roundtrip() {
        assert_eq!(
            decode_base64("3q2+7w==").unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
        assert_eq!(decode_base64("AQID").unwrap(), vec![0x01, 0x02, 0x03]);
        assert_eq!(decode_base64("").unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn base64_invalid_char() {
        assert!(decode_base64("!!!!").is_err());
    }

    #[test]
    fn base64_invalid_length() {
        assert!(decode_base64("abc").is_err());
    }
}
