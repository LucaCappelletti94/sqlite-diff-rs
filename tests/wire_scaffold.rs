//! Smoke tests for the `wire` module scaffold.

#![cfg(all(feature = "wal2json", feature = "pg-walstream", feature = "maxwell"))]

extern crate alloc;

use alloc::sync::Arc;
use alloc::vec::Vec;

use sqlite_diff_rs::maxwell::{Maxwell, MaxwellColumn};
use sqlite_diff_rs::pg_walstream::{PgWalstream, PgWalstreamColumn};
use sqlite_diff_rs::wal2json::{Wal2Json, Wal2JsonColumn};
use sqlite_diff_rs::{DecodeError, TypeMap, Value, WireAdapter, WireSource};

/// `WireAdapter<Src, S, B>` is object-safe: `dyn WireAdapter<..>` compiles.
#[test]
fn wire_adapter_is_object_safe_for_every_source() {
    fn _assert_pg(_: &dyn WireAdapter<PgWalstream, String, Vec<u8>>) {}
    fn _assert_wal2json(_: &dyn WireAdapter<Wal2Json, String, Vec<u8>>) {}
    fn _assert_maxwell(_: &dyn WireAdapter<Maxwell, String, Vec<u8>>) {}
}

/// `TypeMap<Src, S, B>::defaults()` is callable for every source and
/// grows monotonically as later phases populate more decoders.
#[test]
fn type_map_defaults_are_callable_for_every_source() {
    let pg: TypeMap<PgWalstream, String, Vec<u8>> = TypeMap::defaults();
    let wal2json: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::defaults();
    let maxwell: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();

    // Assert only that the map is non-empty.
    assert!(!pg.is_empty(), "pg_walstream defaults should carry bool");
    assert!(!wal2json.is_empty(), "wal2json defaults should carry bool");
    assert!(!maxwell.is_empty(), "maxwell defaults should carry bool");
}

/// Empty `TypeMap` returns `NoDecoderForType` for any lookup, using the
/// column name from the payload's `column_name` field.
#[test]
fn empty_type_map_reports_missing_decoder_by_column_name() {
    let types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::new();
    let value = serde_json::Value::Null;
    let payload = Wal2JsonColumn {
        column_name: "my_column",
        pg_type_name: "integer",
        value: &value,
    };
    let err = types.decode(payload).unwrap_err();
    assert!(matches!(
        err,
        DecodeError::NoDecoderForType { ref column } if column == "my_column"
    ));
}

/// `WireSource::type_key` extracts the source's native identity from a
/// synthesized payload for each source.
#[test]
fn wire_source_type_keys_match_payload_metadata() {
    // pg_walstream: Oid.
    let cv = sqlite_diff_rs::pg_walstream::ColumnValue::Null;
    let pg_payload = PgWalstreamColumn {
        column_name: "id",
        oid: 2950,
        type_modifier: -1,
        data: &cv,
    };
    assert_eq!(PgWalstream::type_key(&pg_payload), 2950u32);

    // wal2json: Arc<str> of the pg type name.
    let val = serde_json::Value::Null;
    let w2j_payload = Wal2JsonColumn {
        column_name: "id",
        pg_type_name: "uuid",
        value: &val,
    };
    assert_eq!(Wal2Json::type_key(&w2j_payload), Arc::<str>::from("uuid"));

    // maxwell: Arc<str> of the mysql type, empty string when absent.
    let mx_payload = MaxwellColumn {
        column_name: "id",
        mysql_type: Some("varchar"),
        value: &val,
    };
    assert_eq!(Maxwell::type_key(&mx_payload), Arc::<str>::from("varchar"));

    let mx_payload_absent = MaxwellColumn {
        column_name: "id",
        mysql_type: None,
        value: &val,
    };
    assert_eq!(Maxwell::type_key(&mx_payload_absent), Arc::<str>::from(""));
}

/// `TypeMap::register` accepts any `Decoder<Src, S, B> + Send + Sync +
/// 'static` and the resulting entry decodes via the registered decoder.
#[test]
fn type_map_register_and_dispatch_via_null_decoder() {
    use sqlite_diff_rs::NullDecoder;

    let mut types: TypeMap<Wal2Json, String, Vec<u8>> = TypeMap::new();
    types.register(Arc::from("integer"), NullDecoder);

    let value = serde_json::Value::Number(42.into());
    let payload = Wal2JsonColumn {
        column_name: "n",
        pg_type_name: "integer",
        value: &value,
    };
    let got = types.decode(payload).unwrap();
    assert!(matches!(got, Value::Null));
}
