//! Phase 9 tests for `JsonVerbatimDecoder` and `JsonCanonicalDecoder`.
//!
//! Cross-format contract:
//! - `JsonVerbatimDecoder` preserves the source's JSON serialization as
//!   `Value::Text`. Objects/arrays are serialized via
//!   `serde_json::to_string`, so key order comes from serde_json's
//!   internal representation. String sources pass through verbatim.
//! - `JsonCanonicalDecoder` recursively sorts object keys and emits
//!   compact JSON. Two rows that differ only in key order produce
//!   byte-equal output.
//!
//! Kills the failure inventory item where wal2json v2 delivered
//! `{"k": 1}` as `serde_json::Value::Object` and the sniffer rejected
//! it wholesale.

#![cfg(all(feature = "wal2json", feature = "pg-walstream", feature = "maxwell"))]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use sqlite_diff_rs::maxwell::{Maxwell, MaxwellColumn};
use sqlite_diff_rs::pg_walstream::{ColumnValue, PgWalstream, PgWalstreamColumn};
use sqlite_diff_rs::wal2json::{Wal2Json, Wal2JsonColumn};
use sqlite_diff_rs::{JsonCanonicalDecoder, JsonVerbatimDecoder, TypeMap, Value, WireAdapter};

// -- JsonVerbatimDecoder -----------------------------------------------------

#[test]
fn json_verbatim_pg_walstream_text() {
    let wire = "{\"k\": 1, \"nested\": [1, 2, 3]}";
    let cv = ColumnValue::text(wire);
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "data",
        oid: 114,
        type_modifier: -1,
        data: &cv,
    }
    .decoded_by(&JsonVerbatimDecoder)
    .unwrap();
    assert_eq!(got, Value::Text(String::from(wire)));
}

#[test]
fn json_verbatim_wal2json_object() {
    let obj: serde_json::Value = serde_json::from_str("{\"k\": 1}").unwrap();
    let got: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "data",
        pg_type_name: "json",
        value: &obj,
    }
    .decoded_by(&JsonVerbatimDecoder)
    .unwrap();
    // serde_json's default output is compact when using to_string.
    assert_eq!(got, Value::Text(String::from("{\"k\":1}")));
}

#[test]
fn json_verbatim_wal2json_string_pass_through() {
    let s = serde_json::Value::String("{\"k\": 1}".into());
    let got: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "data",
        pg_type_name: "json",
        value: &s,
    }
    .decoded_by(&JsonVerbatimDecoder)
    .unwrap();
    // JSON string source is passed through verbatim (no re-serialize).
    assert_eq!(got, Value::Text(String::from("{\"k\": 1}")));
}

#[test]
fn json_verbatim_null_pass_through() {
    let cv = ColumnValue::Null;
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "data",
        oid: 114,
        type_modifier: -1,
        data: &cv,
    }
    .decoded_by(&JsonVerbatimDecoder)
    .unwrap();
    assert_eq!(got, Value::Null);
}

// -- JsonCanonicalDecoder ----------------------------------------------------

#[test]
fn json_canonical_sorts_keys() {
    let unsorted: serde_json::Value = serde_json::from_str("{\"z\": 1, \"a\": 2}").unwrap();
    let got: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "data",
        pg_type_name: "jsonb",
        value: &unsorted,
    }
    .decoded_by(&JsonCanonicalDecoder)
    .unwrap();
    assert_eq!(got, Value::Text(String::from("{\"a\":2,\"z\":1}")));
}

#[test]
fn json_canonical_recurses_into_nested_objects() {
    let src: serde_json::Value =
        serde_json::from_str("{\"z\": {\"b\": 1, \"a\": 2}, \"a\": [3, 4]}").unwrap();
    let got: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "data",
        pg_type_name: "jsonb",
        value: &src,
    }
    .decoded_by(&JsonCanonicalDecoder)
    .unwrap();
    assert_eq!(
        got,
        Value::Text(String::from("{\"a\":[3,4],\"z\":{\"a\":2,\"b\":1}}"))
    );
}

#[test]
fn json_canonical_string_source_reparses_and_sorts() {
    let s = serde_json::Value::String("{\"z\":1,\"a\":2}".into());
    let got: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "data",
        pg_type_name: "jsonb",
        value: &s,
    }
    .decoded_by(&JsonCanonicalDecoder)
    .unwrap();
    assert_eq!(got, Value::Text(String::from("{\"a\":2,\"z\":1}")));
}

// -- Discriminator ----------------------------------------------------------

/// Same wire JSON `{"z":1,"a":2}` as a STRING source produces
/// different `Value::Text` via the two decoders. Object sources are
/// pre-sorted by `serde_json::Map` (which is `BTreeMap` without the
/// `preserve_order` feature), so the discriminator only diverges when
/// the raw wire text preserves a non-sorted order.
#[test]
fn json_discriminator_verbatim_vs_canonical_string_source() {
    let raw = serde_json::Value::String("{\"z\":1,\"a\":2}".into());

    let verbatim: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "data",
        pg_type_name: "jsonb",
        value: &raw,
    }
    .decoded_by(&JsonVerbatimDecoder)
    .unwrap();
    let canonical: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "data",
        pg_type_name: "jsonb",
        value: &raw,
    }
    .decoded_by(&JsonCanonicalDecoder)
    .unwrap();

    assert_eq!(verbatim, Value::Text(String::from("{\"z\":1,\"a\":2}")));
    assert_eq!(canonical, Value::Text(String::from("{\"a\":2,\"z\":1}")));
    assert_ne!(verbatim, canonical);
}

// -- Defaults ---------------------------------------------------------------

#[test]
fn defaults_route_json_types_verbatim() {
    let pg: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    let w2j: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let mx: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();

    // pg_walstream json
    let cv = ColumnValue::text("{\"k\": 1}");
    let got = pg
        .decode(PgWalstreamColumn {
            column_name: "data",
            oid: 114,
            type_modifier: -1,
            data: &cv,
        })
        .unwrap();
    assert_eq!(got, Value::Text(String::from("{\"k\": 1}")));

    // wal2json json object
    let obj: serde_json::Value = serde_json::from_str("{\"k\": 1}").unwrap();
    let got = w2j
        .decode(Wal2JsonColumn {
            column_name: "data",
            pg_type_name: "json",
            value: &obj,
        })
        .unwrap();
    assert_eq!(got, Value::Text(String::from("{\"k\":1}")));

    // maxwell json string
    let s = serde_json::Value::String("{\"k\":1}".into());
    let got = mx
        .decode(MaxwellColumn {
            column_name: "data",
            mysql_type: Some("json"),
            value: &s,
        })
        .unwrap();
    assert_eq!(got, Value::Text(String::from("{\"k\":1}")));
}
