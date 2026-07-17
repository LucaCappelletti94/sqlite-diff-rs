# Changelog

## 0.3.0

`wal2json::MessageV2` now carries the optional wal2json LSN.

### Added

`lsn: Option<String>` field on `wal2json::MessageV2`, populated from the message's `lsn` when wal2json runs with `include-lsn=true` and `None` otherwise. The value stays a raw `hi/lo` hex string (for example `0/16B2270`) so the module carries no Postgres-specific numeric LSN type, and the consumer decides how to parse it. The `Digestable` wire output is byte-identical whether or not the field is present.

### Breaking

Struct-literal construction of `MessageV2` must now supply `lsn` (for example `lsn: None`). Deserialization from wal2json JSON is unaffected because the field is `#[serde(default)]`.

## 0.2.0

Schema-aware forward conversion for CDC wire formats. Downstream users register a type-to-decoder mapping once and consume `pg_walstream`, `wal2json`, or `maxwell` interchangeably.

### Removed

`debezium` module and feature. Users on Postgres have `pg_walstream` and `wal2json`. Users on MySQL have `maxwell`.

The 0.1.x `TryFrom<..>` impls on `Insert`, `Update`, `ChangeDelete`, and `PatchDelete` for pg_walstream, wal2json, and maxwell events. Use `builder.digest(&event, &schema, &adapter)` instead.

`SnifferDecoder` and `SnifferAdapter` (migration bridge for the deleted `TryFrom` impls).

Legacy `digest_pg_walstream`, `digest_wal2json_v2`, `digest_wal2json_v1_change`, and `digest_maxwell` methods on `DiffSetBuilder`.

Per-source `maxwell`, `pg_walstream`, and `wal2json` Criterion benches. They benched the deleted `TryFrom` path.

### Added

`sqlite_diff_rs::wire` module with:

- `WireSource` sealed trait implemented by `PgWalstream`, `Wal2Json`, `Maxwell`. Each carries an associated `Payload` struct and `TypeKey`.
- `Decoder<Src, S, B>` trait. Zero-sized unit types for the built-ins, state-carrying structs for custom user decoders.
- `WireAdapter<Src, S, B>` trait. Single-method dispatcher. `TypeMap` is the primary implementer.
- `TypeMap<Src, S, B>` generic hashmap-backed registry keyed by `Src::TypeKey`.
- `TypeMapDefaults<S, B>` companion trait giving `TypeMap::<Src>::defaults()` a pre-populated registry.
- `DecodeError` shared enum wrapped by each format's existing `ConversionError` via a new `Decode(_)` variant.

Built-in decoders shipping in 0.2.0:

- `BoolDecoder`
- `IntDecoder`, `Int64OverflowToTextDecoder`
- `RealDecoder` (NaN normalizes to `Null`, `-0.0` to `0.0`)
- `TextDecoder` (strict UTF-8)
- `PgByteaBinaryDecoder`, `PgByteaTextModeDecoder`, `MySqlBinaryDecoder` (vendored base64 and PG `\xHEX` decoders, no external deps)
- `UuidBlob16Decoder`, `UuidText36Decoder` (vendored parser, no `uuid` dep)
- `DecimalTextDecoder`
- `TimestampVerbatimDecoder`, `TimestampTzVerbatimDecoder`, `DateVerbatimDecoder`, `TimeVerbatimDecoder`, `IntervalVerbatimDecoder`
- `JsonVerbatimDecoder`, `JsonCanonicalDecoder` (canonical does recursive key sort)
- `NullDecoder`

New traits `WireColumnTypes<Src>` (schema-side per-column type key), `WireSchema<Src>` (table-name lookup), and `Digestable<F, T, S, B>` (event dispatch, implemented in-crate for `pg_walstream::EventType`, `wal2json::MessageV2`, `wal2json::ChangeV1`, `maxwell::Message` times both formats).

One unified digest entry point: `builder.digest(&event, &schema, &adapter)`. Replaces the 0.1.x `digest_pg_walstream` / `digest_wal2json_v2` / `digest_wal2json_v1_change` / `digest_maxwell` methods. `RelationInfo` is no longer a digest argument (OIDs come from the schema).

`ConversionError::TableNotFound(String)` added to each format's error type.

`IndexableValues` promoted from `pub(crate)` to `pub` so external code can implement `SchemaWithPK`.


### MSRV

Rust 1.85 (edition 2024). Unchanged from 0.1.x.

## 0.1.4 and earlier

See git history.
