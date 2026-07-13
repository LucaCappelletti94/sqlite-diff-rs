//! Phase 2 tests for `IntDecoder` and `Int64OverflowToTextDecoder`.
//!
//! Cross-format contract:
//! - `IntDecoder` decodes wire integer representations (Postgres text
//!   base-10, Postgres binary big-endian by width, or JSON `i64`) into
//!   `Value::Integer`.
//! - `Int64OverflowToTextDecoder` accepts the same input plus JSON
//!   arbitrary-precision numbers and text-encoded digits, producing
//!   `Value::Integer` when they fit in `i64` and
//!   `Value::Text(digits)` when they overflow. Used for MySQL
//!   `bigint unsigned`.

#![cfg(all(feature = "wal2json", feature = "pg-walstream", feature = "maxwell"))]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use bytes::Bytes;
use sqlite_diff_rs::maxwell::{Maxwell, MaxwellColumn};
use sqlite_diff_rs::pg_walstream::{ColumnValue, PgWalstream, PgWalstreamColumn};
use sqlite_diff_rs::wal2json::{Wal2Json, Wal2JsonColumn};
use sqlite_diff_rs::{
    DecodeError, Int64OverflowToTextDecoder, IntDecoder, TypeMap, Value, WireAdapter,
};

// -- IntDecoder: pg_walstream ------------------------------------------------

#[test]
fn int_decoder_pg_walstream_text_positive_and_negative() {
    let cv_pos = ColumnValue::text("42");
    let cv_neg = ColumnValue::text("-17");

    let got_pos: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "n",
        oid: 23,
        type_modifier: -1,
        data: &cv_pos,
    }
    .decoded_by(&IntDecoder)
    .unwrap();
    let got_neg: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "n",
        oid: 23,
        type_modifier: -1,
        data: &cv_neg,
    }
    .decoded_by(&IntDecoder)
    .unwrap();
    assert_eq!(got_pos, Value::Integer(42));
    assert_eq!(got_neg, Value::Integer(-17));
}

#[test]
fn int_decoder_pg_walstream_text_i64_bounds() {
    let cv_max = ColumnValue::text("9223372036854775807"); // i64::MAX
    let cv_min = ColumnValue::text("-9223372036854775808"); // i64::MIN
    let cv_over = ColumnValue::text("9223372036854775808"); // i64::MAX + 1

    let got_max: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "n",
        oid: 20,
        type_modifier: -1,
        data: &cv_max,
    }
    .decoded_by(&IntDecoder)
    .unwrap();
    let got_min: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "n",
        oid: 20,
        type_modifier: -1,
        data: &cv_min,
    }
    .decoded_by(&IntDecoder)
    .unwrap();
    assert_eq!(got_max, Value::Integer(i64::MAX));
    assert_eq!(got_min, Value::Integer(i64::MIN));

    let result: Result<Value<String, Vec<u8>>, _> = PgWalstreamColumn {
        column_name: "n",
        oid: 20,
        type_modifier: -1,
        data: &cv_over,
    }
    .decoded_by(&IntDecoder);
    assert!(matches!(
        result.unwrap_err(),
        DecodeError::IntegerOverflow { .. }
    ));
}

#[test]
fn int_decoder_pg_walstream_binary_int2_int4_int8() {
    // int2 (OID 21): 2-byte big-endian
    let two = 12345_i16.to_be_bytes();
    let cv_two = ColumnValue::binary_bytes(Bytes::copy_from_slice(&two));
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "n",
        oid: 21,
        type_modifier: -1,
        data: &cv_two,
    }
    .decoded_by(&IntDecoder)
    .unwrap();
    assert_eq!(got, Value::Integer(12345));

    // int4 (OID 23): 4-byte big-endian
    let four = 1_234_567_i32.to_be_bytes();
    let cv_four = ColumnValue::binary_bytes(Bytes::copy_from_slice(&four));
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "n",
        oid: 23,
        type_modifier: -1,
        data: &cv_four,
    }
    .decoded_by(&IntDecoder)
    .unwrap();
    assert_eq!(got, Value::Integer(1_234_567));

    // int8 (OID 20): 8-byte big-endian
    let eight = 42_000_000_000_i64.to_be_bytes();
    let cv_eight = ColumnValue::binary_bytes(Bytes::copy_from_slice(&eight));
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "n",
        oid: 20,
        type_modifier: -1,
        data: &cv_eight,
    }
    .decoded_by(&IntDecoder)
    .unwrap();
    assert_eq!(got, Value::Integer(42_000_000_000));
}

#[test]
fn int_decoder_pg_walstream_null() {
    let cv = ColumnValue::Null;
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "n",
        oid: 23,
        type_modifier: -1,
        data: &cv,
    }
    .decoded_by(&IntDecoder)
    .unwrap();
    assert_eq!(got, Value::Null);
}

#[test]
fn int_decoder_pg_walstream_rejects_non_int_text() {
    let cv = ColumnValue::text("hello");
    let result: Result<Value<String, Vec<u8>>, _> = PgWalstreamColumn {
        column_name: "n",
        oid: 23,
        type_modifier: -1,
        data: &cv,
    }
    .decoded_by(&IntDecoder);
    assert!(matches!(
        result.unwrap_err(),
        DecodeError::WrongPayloadKind { .. }
    ));
}

// -- IntDecoder: wal2json ----------------------------------------------------

#[test]
fn int_decoder_wal2json_valid_i64() {
    let n = serde_json::Value::Number(42.into());
    let got: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "n",
        pg_type_name: "integer",
        value: &n,
    }
    .decoded_by(&IntDecoder)
    .unwrap();
    assert_eq!(got, Value::Integer(42));
}

#[test]
fn int_decoder_wal2json_null() {
    let n = serde_json::Value::Null;
    let got: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "n",
        pg_type_name: "integer",
        value: &n,
    }
    .decoded_by(&IntDecoder)
    .unwrap();
    assert_eq!(got, Value::Null);
}

#[test]
fn int_decoder_wal2json_rejects_non_number() {
    let s = serde_json::Value::String("42".into());
    let result: Result<Value<String, Vec<u8>>, _> = Wal2JsonColumn {
        column_name: "n",
        pg_type_name: "integer",
        value: &s,
    }
    .decoded_by(&IntDecoder);
    assert!(matches!(
        result.unwrap_err(),
        DecodeError::WrongPayloadKind { .. }
    ));
}

// -- IntDecoder: maxwell -----------------------------------------------------

#[test]
fn int_decoder_maxwell_valid_i64() {
    let n = serde_json::Value::Number((-9999_i64).into());
    let got: Value<String, Vec<u8>> = MaxwellColumn {
        column_name: "n",
        mysql_type: Some("int"),
        value: &n,
    }
    .decoded_by(&IntDecoder)
    .unwrap();
    assert_eq!(got, Value::Integer(-9999));
}

// -- Int64OverflowToTextDecoder ----------------------------------------------

#[test]
fn overflow_decoder_fits_i64_maxwell() {
    let at_max = serde_json::Value::Number(i64::MAX.into());
    let got: Value<String, Vec<u8>> = MaxwellColumn {
        column_name: "big",
        mysql_type: Some("bigint unsigned"),
        value: &at_max,
    }
    .decoded_by(&Int64OverflowToTextDecoder)
    .unwrap();
    assert_eq!(got, Value::Integer(i64::MAX));
}

#[test]
fn overflow_decoder_overflows_to_text_maxwell() {
    // serde_json parses > i64::MAX as u64; construct a Number that
    // holds one.
    let over_json = serde_json::from_str::<serde_json::Value>("9223372036854775808").unwrap();
    let got: Value<String, Vec<u8>> = MaxwellColumn {
        column_name: "big",
        mysql_type: Some("bigint unsigned"),
        value: &over_json,
    }
    .decoded_by(&Int64OverflowToTextDecoder)
    .unwrap();
    assert_eq!(got, Value::Text(String::from("9223372036854775808")));

    let very_large = serde_json::from_str::<serde_json::Value>("18000000000000000000").unwrap();
    let got: Value<String, Vec<u8>> = MaxwellColumn {
        column_name: "big",
        mysql_type: Some("bigint unsigned"),
        value: &very_large,
    }
    .decoded_by(&Int64OverflowToTextDecoder)
    .unwrap();
    assert_eq!(got, Value::Text(String::from("18000000000000000000")));
}

#[test]
fn overflow_decoder_null() {
    let n = serde_json::Value::Null;
    let got: Value<String, Vec<u8>> = MaxwellColumn {
        column_name: "big",
        mysql_type: Some("bigint unsigned"),
        value: &n,
    }
    .decoded_by(&Int64OverflowToTextDecoder)
    .unwrap();
    assert_eq!(got, Value::Null);
}

// -- Defaults registration ---------------------------------------------------

#[test]
fn type_map_defaults_route_int_types() {
    let pg: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    let w2j: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let mx: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();

    // pg_walstream: int4 text mode "12345"
    let cv = ColumnValue::text("12345");
    let got = pg
        .decode(PgWalstreamColumn {
            column_name: "n",
            oid: 23,
            type_modifier: -1,
            data: &cv,
        })
        .unwrap();
    assert_eq!(got, Value::Integer(12345));

    // wal2json: integer JSON number
    let json = serde_json::Value::Number(12345.into());
    let got = w2j
        .decode(Wal2JsonColumn {
            column_name: "n",
            pg_type_name: "integer",
            value: &json,
        })
        .unwrap();
    assert_eq!(got, Value::Integer(12345));

    // maxwell: int JSON number
    let got = mx
        .decode(MaxwellColumn {
            column_name: "n",
            mysql_type: Some("int"),
            value: &json,
        })
        .unwrap();
    assert_eq!(got, Value::Integer(12345));

    // maxwell: bigint unsigned overflow via defaults
    let over = serde_json::from_str::<serde_json::Value>("9223372036854775808").unwrap();
    let got = mx
        .decode(MaxwellColumn {
            column_name: "big",
            mysql_type: Some("bigint unsigned"),
            value: &over,
        })
        .unwrap();
    assert_eq!(got, Value::Text(String::from("9223372036854775808")));
}
