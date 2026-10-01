//! Tests for BYTEA and blob decoders.
//!
//! - `PgByteaBinaryDecoder` (pg_walstream): handles both `ColumnValue::Binary`
//!   (pass-through) and `ColumnValue::Text` with `\xHEX` prefix.
//! - `PgByteaTextModeDecoder` (wal2json): decodes bare-hex (and
//!   optionally `\xHEX`-prefixed) JSON strings into `Value::Blob`.
//! - `MySqlBinaryDecoder` (maxwell): base64-decodes JSON strings.

#![cfg(all(feature = "wal2json", feature = "pg-walstream", feature = "maxwell"))]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use bytes::Bytes;
use sqlite_diff_rs::maxwell::{Maxwell, MaxwellColumn};
use sqlite_diff_rs::pg_walstream::{ColumnValue, PgWalstream, PgWalstreamColumn};
use sqlite_diff_rs::wal2json::{Wal2Json, Wal2JsonColumn};
use sqlite_diff_rs::{
    DecodeError, MySqlBinaryDecoder, PgByteaBinaryDecoder, PgByteaTextModeDecoder, TypeMap, Value,
    WireAdapter, WireType,
};

// -- PgByteaBinaryDecoder: pg_walstream --------------------------------------

#[test]
fn pg_bytea_binary_decoder_pass_through_binary() {
    let cv = ColumnValue::binary_bytes(Bytes::from_static(&[0xDE, 0xAD, 0xBE, 0xEF]));
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "b",
        wire_type: WireType::Bytes,
        data: &cv,
    }
    .decoded_by(&PgByteaBinaryDecoder)
    .unwrap();
    assert_eq!(got, Value::Blob(alloc::vec![0xDE, 0xAD, 0xBE, 0xEF]));
}

#[test]
fn pg_bytea_binary_decoder_hex_escape_text_mode() {
    let cv = ColumnValue::text("\\xdeadbeef");
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "b",
        wire_type: WireType::Bytes,
        data: &cv,
    }
    .decoded_by(&PgByteaBinaryDecoder)
    .unwrap();
    assert_eq!(got, Value::Blob(alloc::vec![0xDE, 0xAD, 0xBE, 0xEF]));
}

#[test]
fn pg_bytea_binary_decoder_null() {
    let cv = ColumnValue::Null;
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "b",
        wire_type: WireType::Bytes,
        data: &cv,
    }
    .decoded_by(&PgByteaBinaryDecoder)
    .unwrap();
    assert_eq!(got, Value::Null);
}

#[test]
fn pg_bytea_binary_decoder_rejects_malformed_hex() {
    let cv = ColumnValue::text("\\xzz");
    let result: Result<Value<String, Vec<u8>>, _> = PgWalstreamColumn {
        column_name: "b",
        wire_type: WireType::Bytes,
        data: &cv,
    }
    .decoded_by(&PgByteaBinaryDecoder);
    assert!(matches!(
        result.unwrap_err(),
        DecodeError::InvalidHexEscape { .. }
    ));
}

// -- PgByteaTextModeDecoder: wal2json ---------------------------------------

#[test]
fn pg_bytea_text_mode_decoder_wal2json_hex() {
    let s = serde_json::Value::String("\\xcafef00d".into());
    let got: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "b",
        wire_type: WireType::Bytes,
        value: &s,
    }
    .decoded_by(&PgByteaTextModeDecoder)
    .unwrap();
    assert_eq!(got, Value::Blob(alloc::vec![0xCA, 0xFE, 0xF0, 0x0D]));
}

#[test]
fn pg_bytea_text_mode_decoder_wal2json_bare_hex() {
    // wal2json emits BYTEA as bare lowercase hex with no `\x` prefix.
    let s = serde_json::Value::String("0001deadff".into());
    let got: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "bin",
        wire_type: WireType::Bytes,
        value: &s,
    }
    .decoded_by(&PgByteaTextModeDecoder)
    .unwrap();
    assert_eq!(got, Value::Blob(alloc::vec![0x00, 0x01, 0xde, 0xad, 0xff]));
}

#[test]
fn pg_bytea_text_mode_decoder_wal2json_prefixed_hex() {
    // A Postgres-style `\x`-prefixed hex text form still decodes.
    let s = serde_json::Value::String("\\xdeadbeef".into());
    let got: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "bin",
        wire_type: WireType::Bytes,
        value: &s,
    }
    .decoded_by(&PgByteaTextModeDecoder)
    .unwrap();
    assert_eq!(got, Value::Blob(alloc::vec![0xde, 0xad, 0xbe, 0xef]));
}

#[test]
fn pg_bytea_text_mode_decoder_wal2json_odd_nibbles() {
    let s = serde_json::Value::String("0001d".into());
    let result: Result<Value<String, Vec<u8>>, _> = Wal2JsonColumn {
        column_name: "bin",
        wire_type: WireType::Bytes,
        value: &s,
    }
    .decoded_by(&PgByteaTextModeDecoder);
    match result {
        Err(DecodeError::InvalidHexEscape { column, .. }) => assert_eq!(column, "bin"),
        other => panic!("expected InvalidHexEscape, got {other:?}"),
    }
}

#[test]
fn pg_bytea_text_mode_decoder_wal2json_non_hex() {
    let s = serde_json::Value::String("00zz".into());
    let result: Result<Value<String, Vec<u8>>, _> = Wal2JsonColumn {
        column_name: "bin",
        wire_type: WireType::Bytes,
        value: &s,
    }
    .decoded_by(&PgByteaTextModeDecoder);
    match result {
        Err(DecodeError::InvalidHexEscape { column, .. }) => assert_eq!(column, "bin"),
        other => panic!("expected InvalidHexEscape, got {other:?}"),
    }
}

#[test]
fn pg_bytea_text_mode_decoder_wal2json_null() {
    let s = serde_json::Value::Null;
    let got: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "b",
        wire_type: WireType::Bytes,
        value: &s,
    }
    .decoded_by(&PgByteaTextModeDecoder)
    .unwrap();
    assert_eq!(got, Value::Null);
}

// -- MySqlBinaryDecoder: maxwell --------------------------------------------

#[test]
fn mysql_binary_decoder_maxwell_base64() {
    // "deadbeef" as base64 (raw bytes 0xDE 0xAD 0xBE 0xEF).
    let s = serde_json::Value::String("3q2+7w==".into());
    let got: Value<String, Vec<u8>> = MaxwellColumn {
        column_name: "b",
        wire_type: WireType::Bytes,
        value: &s,
    }
    .decoded_by(&MySqlBinaryDecoder)
    .unwrap();
    assert_eq!(got, Value::Blob(alloc::vec![0xDE, 0xAD, 0xBE, 0xEF]));
}

#[test]
fn mysql_binary_decoder_maxwell_empty_string() {
    let s = serde_json::Value::String(String::new());
    let got: Value<String, Vec<u8>> = MaxwellColumn {
        column_name: "b",
        wire_type: WireType::Bytes,
        value: &s,
    }
    .decoded_by(&MySqlBinaryDecoder)
    .unwrap();
    assert_eq!(got, Value::Blob(Vec::new()));
}

#[test]
fn mysql_binary_decoder_maxwell_null() {
    let s = serde_json::Value::Null;
    let got: Value<String, Vec<u8>> = MaxwellColumn {
        column_name: "b",
        wire_type: WireType::Bytes,
        value: &s,
    }
    .decoded_by(&MySqlBinaryDecoder)
    .unwrap();
    assert_eq!(got, Value::Null);
}

// -- Defaults ----------------------------------------------------------------

#[test]
fn type_map_defaults_route_byte_types() {
    let pg: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    let w2j: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let mx: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();

    // pg_walstream bytea binary + text modes
    let cv_bin = ColumnValue::binary_bytes(Bytes::from_static(&[0x01, 0x02, 0x03]));
    let got = pg
        .decode(PgWalstreamColumn {
            column_name: "b",
            wire_type: WireType::Bytes,
            data: &cv_bin,
        })
        .unwrap();
    assert_eq!(got, Value::Blob(alloc::vec![0x01, 0x02, 0x03]));

    let cv_txt = ColumnValue::text("\\x010203");
    let got = pg
        .decode(PgWalstreamColumn {
            column_name: "b",
            wire_type: WireType::Bytes,
            data: &cv_txt,
        })
        .unwrap();
    assert_eq!(got, Value::Blob(alloc::vec![0x01, 0x02, 0x03]));

    // wal2json bytea hex string
    let s = serde_json::Value::String("\\x010203".into());
    let got = w2j
        .decode(Wal2JsonColumn {
            column_name: "b",
            wire_type: WireType::Bytes,
            value: &s,
        })
        .unwrap();
    assert_eq!(got, Value::Blob(alloc::vec![0x01, 0x02, 0x03]));

    // maxwell blob base64 string
    let s = serde_json::Value::String("AQID".into()); // base64("\x01\x02\x03")
    let got = mx
        .decode(MaxwellColumn {
            column_name: "b",
            wire_type: WireType::Bytes,
            value: &s,
        })
        .unwrap();
    assert_eq!(got, Value::Blob(alloc::vec![0x01, 0x02, 0x03]));
}
