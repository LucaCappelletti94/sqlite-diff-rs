//! Tests for the temporal verbatim decoders.
//!
//! All five (`TimestampVerbatimDecoder`, `TimestampTzVerbatimDecoder`,
//! `DateVerbatimDecoder`, `TimeVerbatimDecoder`, `IntervalVerbatimDecoder`)
//! share one contract: preserve wire text verbatim as `Value::Text`.
//! Null pass-through. Non-string inputs are rejected.

#![cfg(all(feature = "wal2json", feature = "pg-walstream", feature = "maxwell"))]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use sqlite_diff_rs::maxwell::{Maxwell, MaxwellColumn};
use sqlite_diff_rs::pg_walstream::{ColumnValue, PgWalstream, PgWalstreamColumn};
use sqlite_diff_rs::wal2json::{Wal2Json, Wal2JsonColumn};
use sqlite_diff_rs::{
    DateVerbatimDecoder, IntervalVerbatimDecoder, TimeVerbatimDecoder, TimestampTzVerbatimDecoder,
    TimestampVerbatimDecoder, TypeMap, Value, WireAdapter,
};

// -- Timestamp ---------------------------------------------------------------

#[test]
fn timestamp_verbatim_pg_walstream_and_wal2json() {
    let wire = "2024-01-15 10:30:00.123456";

    let cv = ColumnValue::text(wire);
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "t",
        oid: 1114,
        type_modifier: -1,
        data: &cv,
    }
    .decoded_by(&TimestampVerbatimDecoder)
    .unwrap();
    assert_eq!(got, Value::Text(String::from(wire)));

    let s = serde_json::Value::String(wire.into());
    let got: Value<String, Vec<u8>> = Wal2JsonColumn {
        column_name: "t",
        pg_type_name: "timestamp without time zone",
        value: &s,
    }
    .decoded_by(&TimestampVerbatimDecoder)
    .unwrap();
    assert_eq!(got, Value::Text(String::from(wire)));
}

#[test]
fn timestamptz_verbatim_preserves_offset() {
    let wire = "2024-01-15 10:30:00.123456+02:00";
    let cv = ColumnValue::text(wire);
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "t",
        oid: 1184,
        type_modifier: -1,
        data: &cv,
    }
    .decoded_by(&TimestampTzVerbatimDecoder)
    .unwrap();
    assert_eq!(got, Value::Text(String::from(wire)));
}

// -- Date / Time / Interval --------------------------------------------------

#[test]
fn date_time_interval_verbatim() {
    let dates = ("2024-05-14", DateVerbatimDecoder);
    let times = ("15:30:45.123", TimeVerbatimDecoder);
    let intervals = ("1 year 2 months 3 days", IntervalVerbatimDecoder);

    let (wire, decoder) = dates;
    let cv = ColumnValue::text(wire);
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "d",
        oid: 1082,
        type_modifier: -1,
        data: &cv,
    }
    .decoded_by(&decoder)
    .unwrap();
    assert_eq!(got, Value::Text(String::from(wire)));

    let cv = ColumnValue::text(times.0);
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "t",
        oid: 1083,
        type_modifier: -1,
        data: &cv,
    }
    .decoded_by(&times.1)
    .unwrap();
    assert_eq!(got, Value::Text(String::from(times.0)));

    let cv = ColumnValue::text(intervals.0);
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "i",
        oid: 1186,
        type_modifier: -1,
        data: &cv,
    }
    .decoded_by(&intervals.1)
    .unwrap();
    assert_eq!(got, Value::Text(String::from(intervals.0)));
}

// -- Maxwell -----------------------------------------------------------------

#[test]
fn maxwell_datetime_verbatim() {
    let wire = "2024-01-15 10:30:00";
    let s = serde_json::Value::String(wire.into());
    let got: Value<String, Vec<u8>> = MaxwellColumn {
        column_name: "t",
        mysql_type: Some("datetime"),
        value: &s,
    }
    .decoded_by(&TimestampVerbatimDecoder)
    .unwrap();
    assert_eq!(got, Value::Text(String::from(wire)));
}

// -- Null pass-through -------------------------------------------------------

#[test]
fn temporal_null_pass_through() {
    let cv = ColumnValue::Null;
    let got: Value<String, Vec<u8>> = PgWalstreamColumn {
        column_name: "t",
        oid: 1114,
        type_modifier: -1,
        data: &cv,
    }
    .decoded_by(&TimestampVerbatimDecoder)
    .unwrap();
    assert_eq!(got, Value::Null);
}

// -- Defaults ----------------------------------------------------------------

#[test]
fn defaults_route_temporal_types() {
    let pg: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    let w2j: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let mx: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();

    let wire = "2024-01-15 10:30:00";

    // pg_walstream timestamp
    let cv = ColumnValue::text(wire);
    let got = pg
        .decode(PgWalstreamColumn {
            column_name: "t",
            oid: 1114,
            type_modifier: -1,
            data: &cv,
        })
        .unwrap();
    assert_eq!(got, Value::Text(String::from(wire)));

    // wal2json timestamp
    let s = serde_json::Value::String(wire.into());
    let got = w2j
        .decode(Wal2JsonColumn {
            column_name: "t",
            pg_type_name: "timestamp without time zone",
            value: &s,
        })
        .unwrap();
    assert_eq!(got, Value::Text(String::from(wire)));

    // maxwell datetime
    let got = mx
        .decode(MaxwellColumn {
            column_name: "t",
            mysql_type: Some("datetime"),
            value: &s,
        })
        .unwrap();
    assert_eq!(got, Value::Text(String::from(wire)));

    // pg_walstream date
    let d = "2024-05-14";
    let cv = ColumnValue::text(d);
    let got = pg
        .decode(PgWalstreamColumn {
            column_name: "d",
            oid: 1082,
            type_modifier: -1,
            data: &cv,
        })
        .unwrap();
    assert_eq!(got, Value::Text(String::from(d)));
}
