//! Cross-source parity guard: the whole reason `PgBinary` exists.
//!
//! For every v1 `WireType`, a value decoded through `PgBinary` must equal
//! the same logical value decoded through `PgWalstream`. Where both
//! sources carry the value in Postgres binary send format (bool, int,
//! real, bytea) the comparison is literal binary-mode-vs-binary-mode on
//! identical bytes. Text and uuid arrive over CDC as text, so parity is
//! asserted on the logical value: the `PgBinary` raw-byte form and the
//! `PgWalstream` text form must both land on the identical `Value`.
//!
//! This test is what keeps a future edit to one source from silently
//! diverging from the other.

#![cfg(feature = "pg-walstream")]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use bytes::Bytes;
use sqlite_diff_rs::pg_walstream::{ColumnValue, PgWalstream, PgWalstreamColumn};
use sqlite_diff_rs::{
    BoolDecoder, Decoder, IntDecoder, PgBinary, PgBinaryColumn, PgByteaBinaryDecoder, RealDecoder,
    TextDecoder, UuidBlob16Decoder, Value, WireType,
};

const UUID_BYTES: [u8; 16] = [
    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
];
const UUID_TEXT: &str = "550e8400-e29b-41d4-a716-446655440000";

/// Decode `bytes` through both sources' binary path with `decoder` and
/// assert the two `Value`s are identical.
fn assert_binary_parity<D>(wire_type: WireType, bytes: &[u8], decoder: &D)
where
    D: Decoder<PgBinary, String, Vec<u8>> + Decoder<PgWalstream, String, Vec<u8>>,
{
    let pg_binary: Value<String, Vec<u8>> = PgBinaryColumn {
        column_name: "c",
        wire_type,
        raw: Some(bytes),
    }
    .decoded_by(decoder)
    .unwrap();

    let cv = ColumnValue::Binary(Bytes::copy_from_slice(bytes));
    let pg_walstream: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "c",
        wire_type,
        data: &cv,
    }
    .decoded_by(decoder)
    .unwrap();

    assert_eq!(
        pg_binary, pg_walstream,
        "{wire_type:?} diverges between PgBinary and PgWalstream binary mode"
    );
}

#[test]
fn bool_parity() {
    assert_binary_parity(WireType::Bool, &[0x01], &BoolDecoder);
    assert_binary_parity(WireType::Bool, &[0x00], &BoolDecoder);
}

#[test]
fn int_parity() {
    assert_binary_parity(WireType::Int, &[0x00, 0x05], &IntDecoder);
    assert_binary_parity(WireType::Int, &[0, 0, 0, 42], &IntDecoder);
    assert_binary_parity(WireType::Int, &(-7_i64).to_be_bytes(), &IntDecoder);
}

#[test]
fn real_parity() {
    assert_binary_parity(WireType::Real, &1.5_f32.to_be_bytes(), &RealDecoder);
    assert_binary_parity(WireType::Real, &3.5_f64.to_be_bytes(), &RealDecoder);
}

#[test]
fn bytes_parity() {
    assert_binary_parity(
        WireType::Bytes,
        &[0xDE, 0xAD, 0xBE, 0xEF],
        &PgByteaBinaryDecoder,
    );
}

#[test]
fn text_parity_on_logical_value() {
    // PgBinary carries text as raw UTF-8 bytes; PgWalstream carries it as
    // text-mode ColumnValue. Both must land on the same Value::Text.
    let pg_binary: Value<String, Vec<u8>> = PgBinaryColumn {
        column_name: "c",
        wire_type: WireType::Text,
        raw: Some(b"hello"),
    }
    .decoded_by(&TextDecoder)
    .unwrap();

    let cv = ColumnValue::text("hello");
    let pg_walstream: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "c",
        wire_type: WireType::Text,
        data: &cv,
    }
    .decoded_by(&TextDecoder)
    .unwrap();

    assert_eq!(pg_binary, pg_walstream);
    assert_eq!(pg_binary, Value::Text(String::from("hello")));
}

#[test]
fn uuid_parity_on_logical_value() {
    // The motivating bug: a uuid must decode to the SAME 16-byte blob
    // whether it arrives as PgBinary raw bytes or PgWalstream text.
    let pg_binary: Value<String, Vec<u8>> = PgBinaryColumn {
        column_name: "c",
        wire_type: WireType::Uuid,
        raw: Some(&UUID_BYTES),
    }
    .decoded_by(&UuidBlob16Decoder)
    .unwrap();

    let cv = ColumnValue::text(UUID_TEXT);
    let pg_walstream: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "c",
        wire_type: WireType::Uuid,
        data: &cv,
    }
    .decoded_by(&UuidBlob16Decoder)
    .unwrap();

    assert_eq!(pg_binary, pg_walstream);
    assert_eq!(pg_binary, Value::Blob(UUID_BYTES.to_vec()));
}
