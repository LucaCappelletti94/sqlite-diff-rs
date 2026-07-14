//! Tests for `TextDecoder`.
//!
//! Cross-format contract: `TextDecoder::decode(payload)` preserves the
//! wire text verbatim as `Value::Text(String)`. Null pass-through.
//! Non-UTF-8 pg_walstream bytes surface as `DecodeError::InvalidUtf8`
//! (not silently coerced to `Blob` as the sniffer did).
//!
//! Also includes discriminator assertions: the same wire bytes
//! decoded via `TextDecoder` and via `BoolDecoder` produce different
//! `Value` variants, proving type-driven dispatch is load-bearing.

#![cfg(all(feature = "wal2json", feature = "pg-walstream", feature = "maxwell"))]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use bytes::Bytes;
use sqlite_diff_rs::maxwell::{Maxwell, MaxwellColumn};
use sqlite_diff_rs::pg_walstream::{ColumnValue, PgWalstream, PgWalstreamColumn};
use sqlite_diff_rs::wal2json::{Wal2Json, Wal2JsonColumn};
use sqlite_diff_rs::{
    BoolDecoder, DecodeError, IntDecoder, TextDecoder, TypeMap, Value, WireAdapter,
};

// -- TextDecoder: pg_walstream -----------------------------------------------

#[test]
fn text_decoder_pg_walstream_text_mode_utf8() {
    let cases = ["hello", "", "unicode: \u{1F600}", "with\nnewlines"];
    for wire in &cases {
        let cv = ColumnValue::text(wire);
        let got: Value<String, Vec<u8>> = PgWalstreamColumn {
            column_name: "s",
            oid: 25, // text
            type_modifier: -1,
            data: &cv,
        }
        .decoded_by(&TextDecoder)
        .unwrap();
        assert_eq!(got, Value::Text(String::from(*wire)));
    }
}

#[test]
fn text_decoder_pg_walstream_null() {
    let cv = ColumnValue::Null;
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "s",
        oid: 25,
        type_modifier: -1,
        data: &cv,
    }
    .decoded_by(&TextDecoder)
    .unwrap();
    assert_eq!(got, Value::Null);
}

#[test]
fn text_decoder_pg_walstream_rejects_non_utf8_text() {
    let cv = ColumnValue::text_bytes(Bytes::from_static(&[0xFF, 0xFE, 0xFD]));
    let result: Result<Value<String, Vec<u8>>, _> = PgWalstreamColumn {
        column_name: "s",
        oid: 25,
        type_modifier: -1,
        data: &cv,
    }
    .decoded_by(&TextDecoder);
    assert!(matches!(
        result.unwrap_err(),
        DecodeError::InvalidUtf8 { .. }
    ));
}

#[test]
fn text_decoder_pg_walstream_rejects_binary_payload() {
    let cv = ColumnValue::binary_bytes(Bytes::from_static(&[0x01, 0x02]));
    let result: Result<Value<String, Vec<u8>>, _> = PgWalstreamColumn {
        column_name: "s",
        oid: 25,
        type_modifier: -1,
        data: &cv,
    }
    .decoded_by(&TextDecoder);
    assert!(matches!(
        result.unwrap_err(),
        DecodeError::WrongPayloadKind { .. }
    ));
}

// -- TextDecoder: wal2json ---------------------------------------------------

#[test]
fn text_decoder_wal2json_string() {
    let s = serde_json::Value::String("verbatim".into());
    let got: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "s",
        pg_type_name: "text",
        value: &s,
    }
    .decoded_by(&TextDecoder)
    .unwrap();
    assert_eq!(got, Value::Text(String::from("verbatim")));
}

#[test]
fn text_decoder_wal2json_null() {
    let s = serde_json::Value::Null;
    let got: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "s",
        pg_type_name: "text",
        value: &s,
    }
    .decoded_by(&TextDecoder)
    .unwrap();
    assert_eq!(got, Value::Null);
}

#[test]
fn text_decoder_wal2json_rejects_non_string() {
    let n = serde_json::Value::Number(42.into());
    let result: Result<Value<String, Vec<u8>>, _> = Wal2JsonColumn {
        column_name: "s",
        pg_type_name: "text",
        value: &n,
    }
    .decoded_by(&TextDecoder);
    assert!(matches!(
        result.unwrap_err(),
        DecodeError::WrongPayloadKind { .. }
    ));
}

// -- TextDecoder: maxwell ----------------------------------------------------

#[test]
fn text_decoder_maxwell_string() {
    let s = serde_json::Value::String("hi".into());
    let got: Value<String, Vec<u8>> = MaxwellColumn {
        column_name: "s",
        mysql_type: Some("varchar"),
        value: &s,
    }
    .decoded_by(&TextDecoder)
    .unwrap();
    assert_eq!(got, Value::Text(String::from("hi")));
}

// -- Discriminator: same wire bytes, different decoders ----------------------

/// The same wire text `"t"` decoded via `TextDecoder` produces
/// `Text("t")`, but via `BoolDecoder` produces `Integer(1)`. Same wire
/// text `"42"` via `TextDecoder` produces `Text("42")`, but via
/// `IntDecoder` produces `Integer(42)`. This is the load-bearing
/// dispatch invariant: the type key on the payload picks the decoder,
/// not the shape of the wire bytes.
#[test]
fn dispatch_discriminator_pg_walstream() {
    let cv_t = ColumnValue::text("t");
    let payload_t = || PgWalstreamColumn {
        column_name: "col",
        oid: 25, // ignored by these tests: we're dispatching manually
        type_modifier: -1,
        data: &cv_t,
    };
    let as_text: Value<String, Vec<u8>> = payload_t().decoded_by(&TextDecoder).unwrap();
    let as_bool: Value<String, Vec<u8>> = payload_t().decoded_by(&BoolDecoder).unwrap();
    assert_eq!(as_text, Value::Text(String::from("t")));
    assert_eq!(as_bool, Value::Integer(1));

    let cv_42 = ColumnValue::text("42");
    let payload_42 = || PgWalstreamColumn {
        column_name: "col",
        oid: 23,
        type_modifier: -1,
        data: &cv_42,
    };
    let as_text: Value<String, Vec<u8>> = payload_42().decoded_by(&TextDecoder).unwrap();
    let as_int: Value<String, Vec<u8>> = payload_42().decoded_by(&IntDecoder).unwrap();
    assert_eq!(as_text, Value::Text(String::from("42")));
    assert_eq!(as_int, Value::Integer(42));
}

#[test]
fn dispatch_discriminator_wal2json() {
    let s_42 = serde_json::Value::String("42".into());
    let payload_str = || Wal2JsonColumn {
        column_name: "col",
        pg_type_name: "text",
        value: &s_42,
    };
    let as_text: Value<String, Vec<u8>> = payload_str().decoded_by(&TextDecoder).unwrap();
    assert_eq!(as_text, Value::Text(String::from("42")));
    let result: Result<Value<String, Vec<u8>>, _> = payload_str().decoded_by(&IntDecoder);
    assert!(matches!(
        result.unwrap_err(),
        DecodeError::WrongPayloadKind { .. }
    ));
}

// -- Defaults ----------------------------------------------------------------

#[test]
fn type_map_defaults_route_text_types() {
    let pg: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    let w2j: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let mx: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();

    // pg_walstream text OID
    let cv = ColumnValue::text("payload");
    let got = pg
        .decode(PgWalstreamColumn {
            column_name: "s",
            oid: 25, // text
            type_modifier: -1,
            data: &cv,
        })
        .unwrap();
    assert_eq!(got, Value::Text(String::from("payload")));

    // wal2json text
    let s = serde_json::Value::String("payload".into());
    let got = w2j
        .decode(Wal2JsonColumn {
            column_name: "s",
            pg_type_name: "text",
            value: &s,
        })
        .unwrap();
    assert_eq!(got, Value::Text(String::from("payload")));

    // maxwell varchar
    let got = mx
        .decode(MaxwellColumn {
            column_name: "s",
            mysql_type: Some("varchar"),
            value: &s,
        })
        .unwrap();
    assert_eq!(got, Value::Text(String::from("payload")));
}
