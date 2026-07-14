//! Tests for `RealDecoder`.
//!
//! Cross-format contract: `RealDecoder::decode(payload)` returns
//! `Value::Real(f)` for floating-point wire values, `Value::Null` for
//! null payloads, and additionally normalizes NaN to `Value::Null` and
//! `-0.0` to `Value::Real(0.0)` (matching the crate's `decode_value`
//! invariant).

#![cfg(all(feature = "wal2json", feature = "pg-walstream", feature = "maxwell"))]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use bytes::Bytes;
use sqlite_diff_rs::maxwell::{Maxwell, MaxwellColumn};
use sqlite_diff_rs::pg_walstream::{ColumnValue, PgWalstream, PgWalstreamColumn};
use sqlite_diff_rs::wal2json::{Wal2Json, Wal2JsonColumn};
use sqlite_diff_rs::{DecodeError, RealDecoder, TypeMap, Value, WireAdapter};

// -- RealDecoder: pg_walstream text mode -------------------------------------

#[test]
fn real_decoder_pg_walstream_text_basic() {
    let cases = [
        ("2.5", Value::Real(2.5_f64)),
        ("-2.5", Value::Real(-2.5_f64)),
        ("0.0", Value::Real(0.0_f64)),
        ("-0.0", Value::Real(0.0_f64)), // normalized
        ("1e10", Value::Real(1e10_f64)),
    ];
    for (wire, expected) in &cases {
        let cv = ColumnValue::text(wire);
        let got: Value<String, Vec<u8>> = PgWalstreamColumn {
            column_name: "x",
            oid: 701,
            type_modifier: -1,
            data: &cv,
        }
        .decoded_by(&RealDecoder)
        .unwrap();
        assert_eq!(got, *expected, "wire {wire}");
    }
}

#[test]
fn real_decoder_pg_walstream_text_nan_becomes_null() {
    let cv = ColumnValue::text("NaN");
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "x",
        oid: 701,
        type_modifier: -1,
        data: &cv,
    }
    .decoded_by(&RealDecoder)
    .unwrap();
    assert_eq!(got, Value::Null);
}

#[test]
fn real_decoder_pg_walstream_text_infinities() {
    let cv_pos = ColumnValue::text("Infinity");
    let cv_neg = ColumnValue::text("-Infinity");

    let got_pos: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "x",
        oid: 701,
        type_modifier: -1,
        data: &cv_pos,
    }
    .decoded_by(&RealDecoder)
    .unwrap();
    let got_neg: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "x",
        oid: 701,
        type_modifier: -1,
        data: &cv_neg,
    }
    .decoded_by(&RealDecoder)
    .unwrap();
    assert_eq!(got_pos, Value::Real(f64::INFINITY));
    assert_eq!(got_neg, Value::Real(f64::NEG_INFINITY));
}

#[test]
fn real_decoder_pg_walstream_binary_float4_and_float8() {
    // float4 (OID 700): 4-byte big-endian IEEE 754
    let f4 = 3.5_f32.to_be_bytes();
    let cv_f4 = ColumnValue::binary_bytes(Bytes::copy_from_slice(&f4));
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "x",
        oid: 700,
        type_modifier: -1,
        data: &cv_f4,
    }
    .decoded_by(&RealDecoder)
    .unwrap();
    assert_eq!(got, Value::Real(3.5_f64));

    // float8 (OID 701): 8-byte big-endian IEEE 754
    let f8 = 1234.5678_f64.to_be_bytes();
    let cv_f8 = ColumnValue::binary_bytes(Bytes::copy_from_slice(&f8));
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "x",
        oid: 701,
        type_modifier: -1,
        data: &cv_f8,
    }
    .decoded_by(&RealDecoder)
    .unwrap();
    assert_eq!(got, Value::Real(1234.5678_f64));
}

#[test]
fn real_decoder_pg_walstream_null() {
    let cv = ColumnValue::Null;
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "x",
        oid: 701,
        type_modifier: -1,
        data: &cv,
    }
    .decoded_by(&RealDecoder)
    .unwrap();
    assert_eq!(got, Value::Null);
}

#[test]
fn real_decoder_pg_walstream_rejects_non_float_text() {
    let cv = ColumnValue::text("not a number");
    let result: Result<Value<String, Vec<u8>>, _> = PgWalstreamColumn {
        column_name: "x",
        oid: 701,
        type_modifier: -1,
        data: &cv,
    }
    .decoded_by(&RealDecoder);
    assert!(matches!(
        result.unwrap_err(),
        DecodeError::WrongPayloadKind { .. }
    ));
}

// -- RealDecoder: wal2json ---------------------------------------------------

#[test]
fn real_decoder_wal2json_number() {
    let n = serde_json::Value::Number(serde_json::Number::from_f64(0.25).unwrap());
    let got: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "x",
        pg_type_name: "double precision",
        value: &n,
    }
    .decoded_by(&RealDecoder)
    .unwrap();
    assert_eq!(got, Value::Real(0.25_f64));
}

#[test]
fn real_decoder_wal2json_null() {
    let n = serde_json::Value::Null;
    let got: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "x",
        pg_type_name: "double precision",
        value: &n,
    }
    .decoded_by(&RealDecoder)
    .unwrap();
    assert_eq!(got, Value::Null);
}

// -- RealDecoder: maxwell ----------------------------------------------------

#[test]
fn real_decoder_maxwell_number() {
    let n = serde_json::Value::Number(serde_json::Number::from_f64(-1.5).unwrap());
    let got: Value<String, Vec<u8>> = MaxwellColumn {
        column_name: "x",
        mysql_type: Some("double"),
        value: &n,
    }
    .decoded_by(&RealDecoder)
    .unwrap();
    assert_eq!(got, Value::Real(-1.5_f64));
}

// -- Defaults ----------------------------------------------------------------

#[test]
fn type_map_defaults_route_real_types() {
    let pg: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    let w2j: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let mx: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();

    // pg_walstream float4
    let cv_f4 = ColumnValue::text("1.25");
    let got = pg
        .decode(PgWalstreamColumn {
            column_name: "x",
            oid: 700,
            type_modifier: -1,
            data: &cv_f4,
        })
        .unwrap();
    assert_eq!(got, Value::Real(1.25_f64));

    // pg_walstream float8
    let cv_f8 = ColumnValue::text("1.5");
    let got = pg
        .decode(PgWalstreamColumn {
            column_name: "x",
            oid: 701,
            type_modifier: -1,
            data: &cv_f8,
        })
        .unwrap();
    assert_eq!(got, Value::Real(1.5_f64));

    // wal2json real
    let n = serde_json::Value::Number(serde_json::Number::from_f64(0.5).unwrap());
    let got = w2j
        .decode(Wal2JsonColumn {
            column_name: "x",
            pg_type_name: "real",
            value: &n,
        })
        .unwrap();
    assert_eq!(got, Value::Real(0.5_f64));

    // maxwell float
    let got = mx
        .decode(MaxwellColumn {
            column_name: "x",
            mysql_type: Some("float"),
            value: &n,
        })
        .unwrap();
    assert_eq!(got, Value::Real(0.5_f64));
}
