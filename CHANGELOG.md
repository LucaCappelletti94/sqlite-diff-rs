# Changelog

## 0.2.0

Schema-aware forward conversion for CDC wire formats. Downstream users register a type-to-decoder mapping once and consume `pg_walstream`, `wal2json`, or `maxwell` interchangeably. See `docs/schema-aware-forward-conversion.md` for the full design.

### Removed

`debezium` module and feature. The 0.1.x sniffer-based converter was unsound (dropped the Kafka Connect schema section wholesale) and re-adding Debezium under the new schema-aware design would have required a special-case dispatch shape that no other source needed. Users on Postgres have `pg_walstream` and `wal2json`. Users on MySQL have `maxwell`.

### Added

`sqlite_diff_rs::wire` module with:

- `WireSource` sealed trait implemented by `PgWalstream`, `Wal2Json`, `Maxwell`. Each carries an associated `Payload` struct and `TypeKey`.
- `Decoder<Src, S, B>` trait. Zero-sized unit types for the built-ins, state-carrying structs for custom user decoders.
- `WireAdapter<Src, S, B>` trait. Single-method dispatcher. `TypeMap` is the primary implementer.
- `TypeMap<Src, S, B>` generic hashmap-backed registry keyed by `Src::TypeKey`.
- `TypeMapDefaults<S, B>` companion trait giving `TypeMap::<Src>::defaults()` a pre-populated registry.
- `DecodeError` shared enum wrapped by each format's existing `ConversionError` via a new `Decode(_)` variant.

Built-in decoders shipping in 0.2.0:

- `BoolDecoder` (Phase 1)
- `IntDecoder`, `Int64OverflowToTextDecoder` (Phase 2)
- `RealDecoder` (Phase 3, NaN normalizes to `Null`, `-0.0` to `0.0`)
- `TextDecoder` (Phase 4, strict UTF-8)
- `PgByteaBinaryDecoder`, `PgByteaTextModeDecoder`, `MySqlBinaryDecoder` (Phase 5, vendored base64 and PG `\xHEX` decoders, no external deps)
- `UuidBlob16Decoder`, `UuidText36Decoder` (Phase 6, vendored parser, no `uuid` dep)
- `DecimalTextDecoder` (Phase 7)
- `TimestampVerbatimDecoder`, `TimestampTzVerbatimDecoder`, `DateVerbatimDecoder`, `TimeVerbatimDecoder`, `IntervalVerbatimDecoder` (Phase 8)
- `JsonVerbatimDecoder`, `JsonCanonicalDecoder` (Phase 9, canonical does recursive key sort)
- `NullDecoder`
- `SnifferDecoder` and `SnifferAdapter` (deprecated, migration bridge reproducing 0.1.4 behavior)

New methods on `DiffSetBuilder`, symmetric with `digest_sql`:

- `digest_pg_walstream(&event, &relation, &table, &adapter)`
- `digest_wal2json_v2(&msg, &table, &adapter)`
- `digest_wal2json_v1_change(&change, &table, &adapter)`
- `digest_maxwell(&msg, &table, &adapter)`

Per-payload ergonomic helper `PgWalstreamColumn::decoded_by(&decoder)` (same shape on `Wal2JsonColumn` and `MaxwellColumn`) as a shortcut around Rust's GAT method resolution.

New field `maxwell::Message::columns_types: Option<BTreeMap<String, String>>` populated when the Maxwell daemon runs with `--include_types`.

`ConversionError::Decode(DecodeError)` added to each format's error type via `#[from]`.

### Legacy API

The 0.1.x `TryFrom<..>` impls on `Insert`, `Update`, `ChangeDelete`, and `PatchDelete` remain in place for pg_walstream, wal2json, and maxwell. They keep the content-sniffing behavior from 0.1.4 and continue to compile against existing callers. New code should use the digest methods above. The `TryFrom` impls are slated for removal in 0.3.0.

### MSRV

Rust 1.85 (edition 2024). Unchanged from 0.1.x.

## 0.1.4 and earlier

See git history.
