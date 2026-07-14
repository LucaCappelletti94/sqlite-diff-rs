//! Comprehensive tests for `TypeMap::defaults()` registration.
//!
//! Verifies that every registered key produces the expected `Value` variant
//! for a known input, confirming that the crate's self-evident mappings are
//! correctly wired for all three sources.

#![cfg(all(feature = "pg-walstream", feature = "wal2json", feature = "maxwell"))]

extern crate alloc;

use alloc::vec::Vec;
use std::f64::consts::PI;

use sqlite_diff_rs::maxwell::{Maxwell, MaxwellColumn};
use sqlite_diff_rs::pg_walstream::{ColumnValue, PgWalstream, PgWalstreamColumn};
use sqlite_diff_rs::wal2json::{Wal2Json, Wal2JsonColumn};
use sqlite_diff_rs::{DecodeError, TypeMap, Value, WireAdapter};

// -- pg_walstream -----------------------------------------------------------

#[test]
fn pg_defaults_bool_key_produces_integer() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_BOOL = 16 -> BoolDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 16,
            type_modifier: -1,
            data: &ColumnValue::text("t"),
        })
        .unwrap();
    assert_eq!(val, Value::Integer(1), "PG bool 't' -> Integer(1)");
}

#[test]
fn pg_defaults_int2_produces_integer() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_INT2 = 21 -> IntDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 21,
            type_modifier: -1,
            data: &ColumnValue::text("42"),
        })
        .unwrap();
    assert_eq!(val, Value::Integer(42), "PG int2 text '42' -> Integer(42)");
}

#[test]
fn pg_defaults_int4_produces_integer() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_INT4 = 23 -> IntDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 23,
            type_modifier: -1,
            data: &ColumnValue::text("42"),
        })
        .unwrap();
    assert_eq!(val, Value::Integer(42), "PG int4 text '42' -> Integer(42)");
}

#[test]
fn pg_defaults_int8_produces_integer() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_INT8 = 20 -> IntDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 20,
            type_modifier: -1,
            data: &ColumnValue::text("42"),
        })
        .unwrap();
    assert_eq!(val, Value::Integer(42), "PG int8 text '42' -> Integer(42)");
}

#[test]
fn pg_defaults_float4_produces_real() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_FLOAT4 = 700 -> RealDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 700,
            type_modifier: -1,
            data: &ColumnValue::text("3.141592653589793"),
        })
        .unwrap();
    assert_eq!(val, Value::Real(PI), "PG float4 text -> Real(PI)");
}

#[test]
fn pg_defaults_float8_produces_real() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_FLOAT8 = 701 -> RealDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 701,
            type_modifier: -1,
            data: &ColumnValue::text("3.141592653589793"),
        })
        .unwrap();
    assert_eq!(val, Value::Real(PI), "PG float8 text -> Real(PI)");
}

#[test]
fn pg_defaults_text_produces_text() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_TEXT = 25 -> TextDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 25,
            type_modifier: -1,
            data: &ColumnValue::text("hello"),
        })
        .unwrap();
    assert_eq!(val, Value::Text("hello".into()), "PG text -> Text(hello)");
}

#[test]
fn pg_defaults_varchar_produces_text() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_VARCHAR = 1043 -> TextDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 1043,
            type_modifier: -1,
            data: &ColumnValue::text("hello"),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Text("hello".into()),
        "PG varchar -> Text(hello)"
    );
}

#[test]
fn pg_defaults_bpchar_produces_text() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_BPCHAR = 1042 -> TextDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 1042,
            type_modifier: -1,
            data: &ColumnValue::text("hello"),
        })
        .unwrap();
    assert_eq!(val, Value::Text("hello".into()), "PG bpchar -> Text(hello)");
}

#[test]
fn pg_defaults_name_produces_text() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_NAME = 19 -> TextDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 19,
            type_modifier: -1,
            data: &ColumnValue::text("hello"),
        })
        .unwrap();
    assert_eq!(val, Value::Text("hello".into()), "PG name -> Text(hello)");
}

#[test]
fn pg_defaults_bytea_produces_blob() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_BYTEA = 17 -> PgByteaBinaryDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 17,
            type_modifier: -1,
            data: &ColumnValue::text("\\xdeadbeef"),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Blob(alloc::vec![0xde, 0xad, 0xbe, 0xef]),
        "PG bytea \\xdeadbeef -> Blob"
    );
}

#[test]
fn pg_defaults_numeric_produces_text() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_NUMERIC = 1700 -> DecimalTextDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 1700,
            type_modifier: -1,
            data: &ColumnValue::text("123.456"),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Text("123.456".into()),
        "PG numeric -> Text(123.456)"
    );
}

#[test]
fn pg_defaults_timestamp_produces_text() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_TIMESTAMP = 1114 -> TimestampVerbatimDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 1114,
            type_modifier: -1,
            data: &ColumnValue::text("2024-01-15 10:30:00"),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Text("2024-01-15 10:30:00".into()),
        "PG timestamp -> verbatim"
    );
}

#[test]
fn pg_defaults_timestamptz_produces_text() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_TIMESTAMPTZ = 1184 -> TimestampTzVerbatimDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 1184,
            type_modifier: -1,
            data: &ColumnValue::text("2024-01-15 10:30:00+00"),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Text("2024-01-15 10:30:00+00".into()),
        "PG timestamptz -> verbatim with offset"
    );
}

#[test]
fn pg_defaults_date_produces_text() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_DATE = 1082 -> DateVerbatimDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 1082,
            type_modifier: -1,
            data: &ColumnValue::text("2024-01-15"),
        })
        .unwrap();
    assert_eq!(val, Value::Text("2024-01-15".into()), "PG date -> verbatim");
}

#[test]
fn pg_defaults_time_produces_text() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_TIME = 1083 -> TimeVerbatimDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 1083,
            type_modifier: -1,
            data: &ColumnValue::text("10:30:00"),
        })
        .unwrap();
    assert_eq!(val, Value::Text("10:30:00".into()), "PG time -> verbatim");
}

#[test]
fn pg_defaults_interval_produces_text() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_INTERVAL = 1186 -> IntervalVerbatimDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 1186,
            type_modifier: -1,
            data: &ColumnValue::text("1 day"),
        })
        .unwrap();
    assert_eq!(val, Value::Text("1 day".into()), "PG interval -> verbatim");
}

#[test]
fn pg_defaults_json_produces_text() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_JSON = 114 -> JsonVerbatimDecoder
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 114,
            type_modifier: -1,
            data: &ColumnValue::text("{\"k\": 1}"),
        })
        .unwrap();
    assert_eq!(val, Value::Text("{\"k\": 1}".into()), "PG json -> verbatim");
}

#[test]
fn pg_defaults_jsonb_produces_text() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    // PG_JSONB = 3802 -> JsonCanonicalDecoder, but for pg_walstream
    // canonicalization is the same as verbatim because the wire carries
    // JSON as opaque text; no re-parsing is done.
    let val = types
        .decode(PgWalstreamColumn {
            column_name: "c",
            oid: 3802,
            type_modifier: -1,
            data: &ColumnValue::text("{\"k\": 1}"),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Text("{\"k\": 1}".into()),
        "PG jsonb -> verbatim (canonical=verbatim for pg_walstream)"
    );
}

// -- wal2json ---------------------------------------------------------------

#[test]
fn w2j_defaults_boolean_produces_integer() {
    let types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(Wal2JsonColumn {
            column_name: "c",
            pg_type_name: "boolean",
            value: &serde_json::Value::Bool(true),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Integer(1),
        "wal2json boolean true -> Integer(1)"
    );
}

#[test]
fn w2j_defaults_integer_produces_integer() {
    let types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(Wal2JsonColumn {
            column_name: "c",
            pg_type_name: "integer",
            value: &serde_json::Value::Number(serde_json::Number::from(42)),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Integer(42),
        "wal2json integer 42 -> Integer(42)"
    );
}

#[test]
fn w2j_defaults_bigint_produces_integer() {
    let types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(Wal2JsonColumn {
            column_name: "c",
            pg_type_name: "bigint",
            value: &serde_json::Value::Number(serde_json::Number::from(42)),
        })
        .unwrap();
    assert_eq!(val, Value::Integer(42), "wal2json bigint 42 -> Integer(42)");
}

#[test]
fn w2j_defaults_smallint_produces_integer() {
    let types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(Wal2JsonColumn {
            column_name: "c",
            pg_type_name: "smallint",
            value: &serde_json::Value::Number(serde_json::Number::from(42)),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Integer(42),
        "wal2json smallint 42 -> Integer(42)"
    );
}

#[test]
fn w2j_defaults_real_produces_real() {
    let types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(Wal2JsonColumn {
            column_name: "c",
            pg_type_name: "real",
            value: &serde_json::Value::Number(serde_json::Number::from_f64(PI).unwrap()),
        })
        .unwrap();
    assert_eq!(val, Value::Real(PI), "wal2json real PI -> Real(PI)");
}

#[test]
fn w2j_defaults_double_precision_produces_real() {
    let types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(Wal2JsonColumn {
            column_name: "c",
            pg_type_name: "double precision",
            value: &serde_json::Value::Number(serde_json::Number::from_f64(PI).unwrap()),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Real(PI),
        "wal2json double precision PI -> Real(PI)"
    );
}

#[test]
fn w2j_defaults_text_produces_text() {
    let types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(Wal2JsonColumn {
            column_name: "c",
            pg_type_name: "text",
            value: &serde_json::Value::String("hello".into()),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Text("hello".into()),
        "wal2json text -> Text(hello)"
    );
}

#[test]
fn w2j_defaults_varchar_produces_text() {
    let types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(Wal2JsonColumn {
            column_name: "c",
            pg_type_name: "varchar",
            value: &serde_json::Value::String("hello".into()),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Text("hello".into()),
        "wal2json varchar -> Text(hello)"
    );
}

#[test]
fn w2j_defaults_bytea_produces_blob() {
    let types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(Wal2JsonColumn {
            column_name: "c",
            pg_type_name: "bytea",
            value: &serde_json::Value::String("\\xdeadbeef".into()),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Blob(alloc::vec![0xde, 0xad, 0xbe, 0xef]),
        "wal2json bytea \\xdeadbeef -> Blob"
    );
}

#[test]
fn w2j_defaults_numeric_produces_text() {
    let types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(Wal2JsonColumn {
            column_name: "c",
            pg_type_name: "numeric",
            value: &serde_json::Value::String("123.456".into()),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Text("123.456".into()),
        "wal2json numeric -> Text(123.456)"
    );
}

#[test]
fn w2j_defaults_timestamp_produces_text() {
    let types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(Wal2JsonColumn {
            column_name: "c",
            pg_type_name: "timestamp",
            value: &serde_json::Value::String("2024-01-15 10:30:00".into()),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Text("2024-01-15 10:30:00".into()),
        "wal2json timestamp -> verbatim"
    );
}

#[test]
fn w2j_defaults_json_produces_text() {
    let types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(Wal2JsonColumn {
            column_name: "c",
            pg_type_name: "json",
            value: &serde_json::Value::String("{\"k\": 1}".into()),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Text("{\"k\": 1}".into()),
        "wal2json json string -> verbatim"
    );
}

#[test]
fn w2j_defaults_jsonb_produces_canonical_text() {
    let types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    // JSON object as parsed value — jsonb canonicalizes
    let mut map = serde_json::Map::new();
    map.insert(
        "b".to_string(),
        serde_json::Value::Number(serde_json::Number::from(2)),
    );
    map.insert(
        "a".to_string(),
        serde_json::Value::Number(serde_json::Number::from(1)),
    );
    let val = types
        .decode(Wal2JsonColumn {
            column_name: "c",
            pg_type_name: "jsonb",
            value: &serde_json::Value::Object(map),
        })
        .unwrap();
    // Keys should be sorted: a then b
    assert_eq!(
        val,
        Value::Text(r#"{"a":1,"b":2}"#.into()),
        "wal2json jsonb -> canonical sorted keys"
    );
}

// -- maxwell ----------------------------------------------------------------

#[test]
fn maxwell_defaults_tinyint1_produces_integer() {
    let types: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(MaxwellColumn {
            column_name: "c",
            mysql_type: Some("tinyint(1)"),
            value: &serde_json::Value::Bool(true),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Integer(1),
        "maxwell tinyint(1) true -> Integer(1)"
    );
}

#[test]
fn maxwell_defaults_int_produces_integer() {
    let types: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(MaxwellColumn {
            column_name: "c",
            mysql_type: Some("int"),
            value: &serde_json::Value::Number(serde_json::Number::from(42)),
        })
        .unwrap();
    assert_eq!(val, Value::Integer(42), "maxwell int 42 -> Integer(42)");
}

#[test]
fn maxwell_defaults_bigint_produces_integer() {
    let types: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(MaxwellColumn {
            column_name: "c",
            mysql_type: Some("bigint"),
            value: &serde_json::Value::Number(serde_json::Number::from(42)),
        })
        .unwrap();
    assert_eq!(val, Value::Integer(42), "maxwell bigint 42 -> Integer(42)");
}

#[test]
fn maxwell_defaults_bigint_unsigned_overflows_to_text() {
    let types: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();
    // This value exceeds i64::MAX
    let val = types
        .decode(MaxwellColumn {
            column_name: "c",
            mysql_type: Some("bigint unsigned"),
            value: &serde_json::Value::String("9223372036854775808".into()),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Text("9223372036854775808".into()),
        "maxwell bigint unsigned overflow -> Text"
    );
}

#[test]
fn maxwell_defaults_float_produces_real() {
    let types: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(MaxwellColumn {
            column_name: "c",
            mysql_type: Some("float"),
            value: &serde_json::Value::Number(serde_json::Number::from_f64(PI).unwrap()),
        })
        .unwrap();
    assert_eq!(val, Value::Real(PI), "maxwell float PI -> Real(PI)");
}

#[test]
fn maxwell_defaults_varchar_produces_text() {
    let types: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(MaxwellColumn {
            column_name: "c",
            mysql_type: Some("varchar"),
            value: &serde_json::Value::String("hello".into()),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Text("hello".into()),
        "maxwell varchar -> Text(hello)"
    );
}

#[test]
fn maxwell_defaults_blob_produces_blob() {
    let types: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();
    // MySqlBinaryDecoder base64-decodes
    let val = types
        .decode(MaxwellColumn {
            column_name: "c",
            mysql_type: Some("blob"),
            value: &serde_json::Value::String("3q2+7w==".into()),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Blob(alloc::vec![0xde, 0xad, 0xbe, 0xef]),
        "maxwell blob base64 -> Blob"
    );
}

#[test]
fn maxwell_defaults_decimal_produces_text() {
    let types: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(MaxwellColumn {
            column_name: "c",
            mysql_type: Some("decimal"),
            value: &serde_json::Value::String("123.456".into()),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Text("123.456".into()),
        "maxwell decimal -> Text(123.456)"
    );
}

#[test]
fn maxwell_defaults_datetime_produces_text() {
    let types: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();
    let val = types
        .decode(MaxwellColumn {
            column_name: "c",
            mysql_type: Some("datetime"),
            value: &serde_json::Value::String("2024-01-15 10:30:00".into()),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Text("2024-01-15 10:30:00".into()),
        "maxwell datetime -> verbatim"
    );
}

#[test]
fn maxwell_defaults_json_produces_canonical_text() {
    let types: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();
    // maxwell uses JsonCanonicalDecoder for json
    let mut map = serde_json::Map::new();
    map.insert(
        "b".to_string(),
        serde_json::Value::Number(serde_json::Number::from(2)),
    );
    map.insert(
        "a".to_string(),
        serde_json::Value::Number(serde_json::Number::from(1)),
    );
    let val = types
        .decode(MaxwellColumn {
            column_name: "c",
            mysql_type: Some("json"),
            value: &serde_json::Value::Object(map),
        })
        .unwrap();
    assert_eq!(
        val,
        Value::Text(r#"{"a":1,"b":2}"#.into()),
        "maxwell json -> canonical sorted keys"
    );
}

// -- Empty TypeMap reports NoDecoderForType ---------------------------------

#[test]
fn empty_type_map_reports_no_decoder_for_any_pg_key() {
    let types: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::new();
    let result = types.decode(PgWalstreamColumn {
        column_name: "test_col",
        oid: 23,
        type_modifier: -1,
        data: &ColumnValue::text("42"),
    });
    match result {
        Err(DecodeError::NoDecoderForType { column }) => {
            assert_eq!(column, "test_col");
        }
        Err(other) => panic!("expected NoDecoderForType, got {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn empty_type_map_reports_no_decoder_for_any_w2j_key() {
    let types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::new();
    let result = types.decode(Wal2JsonColumn {
        column_name: "test_col",
        pg_type_name: "integer",
        value: &serde_json::Value::Number(serde_json::Number::from(42)),
    });
    match result {
        Err(DecodeError::NoDecoderForType { column }) => {
            assert_eq!(column, "test_col");
        }
        Err(other) => panic!("expected NoDecoderForType, got {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn empty_type_map_reports_no_decoder_for_any_maxwell_key() {
    let types: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::new();
    let result = types.decode(MaxwellColumn {
        column_name: "test_col",
        mysql_type: Some("int"),
        value: &serde_json::Value::Number(serde_json::Number::from(42)),
    });
    match result {
        Err(DecodeError::NoDecoderForType { column }) => {
            assert_eq!(column, "test_col");
        }
        Err(other) => panic!("expected NoDecoderForType, got {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}
