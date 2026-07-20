# Changelog

## 0.6.1

### Fixed

Changeset UPDATE digests now always capture the old primary key, so a changeset produced from a wire event applies through the diesel changeset path even when the update does not change the key. The wal2json digest previously dropped the old-row image entirely (it now reads `identity` in the v2 format and `oldkeys` in the v1 format). The Maxwell digest treats a column absent from `old` as unchanged, taking its old value from the new value, since Maxwell lists only changed columns in `old`. The pgoutput digest keeps a primary-key column's old value from the new tuple when `old_data` is absent under `REPLICA IDENTITY DEFAULT`. The patchset builders are unchanged, since they use only the new image.

## 0.6.0

### Added

The `diesel` feature now renders `ChangeSet` operations as backend-generic Diesel queries, alongside the existing `PatchSet` support. `ChangesetOp` implements `QueryFragment`, `QueryId`, and `RunQueryDsl`, and `ChangesetOp::with_adapter` builds a `BoundChangesetOp` just as `PatchsetOp::with_adapter` builds a `BoundPatchsetOp`. Both bound types are aliases of the new generic `BoundOp`, and `ApplyOps` drives either one. Because a changeset carries the old and new value of every column, it renders primary-key changes (including composite keys) as `UPDATE ... SET <changed columns> WHERE <old key>`, which a patchset cannot represent. A changeset `UPDATE` writes only the columns whose value actually changed, so applying it never rewrites (or spuriously triggers on) a column that did not move.

## 0.5.1

### Fixed

The wal2json `bytea` decoder (`PgByteaTextModeDecoder`) now accepts wal2json's bare lowercase hex form (for example `0001deadff`) in addition to the Postgres-style `\x`-prefixed hex form, so `bytea` columns round-trip on the wal2json vehicle. The pgoutput and Maxwell paths are unchanged.

## 0.5.0

The optional `pg_walstream` dependency moves from 0.7 to 0.8.

### Breaking

The `pg_walstream` and `pg_walstream_reverse` module re-exports (`EventType`, `RowData`, `ColumnValue`, `ChangeEvent`, `ReplicaIdentity`, `ColumnData`, `ColumnInfo`, `LogicalReplicationMessage`, `TupleData`, `Oid`, and the rest) now resolve to `pg_walstream` 0.8 types. Consumers of the `pg-walstream` feature that also depend on `pg_walstream` directly must move to 0.8. Enabling the feature raises the minimum supported Rust version to 1.87, which `pg_walstream` 0.8 requires.

## 0.4.0

Source-independent semantic type key for `digest`. A catalog carrying semantic column types now drives `DiffSetBuilder::digest` for every wire source without translating to a source-native key.

### Added

`WireType` enum (`Bool`, `Int`, `Real`, `Text`, `Bytes`, `Uuid`, `Decimal`, `Timestamp`, `TimestampTz`, `Date`, `Time`, `Interval`, `Json`, `Jsonb`), re-exported from the crate root. It is the single decoder-dispatch key shared by `pg_walstream`, `wal2json`, and `maxwell`.

`TypeMap::defaults()` now registers a `WireType::Uuid` decoder for every source (`UuidText36Decoder`), which fixes the previously missing wal2json `uuid` mapping.

### Breaking

`WireSource` drops the associated `TypeKey` and its `type_key` method in favor of `fn wire_type(payload) -> WireType`.

`WireColumnTypes` and `WireSchema` are no longer generic over the source. `WireColumnTypes::column_type_key(idx) -> Src::TypeKey` becomes `WireColumnTypes::column_type(idx) -> WireType`, and `WireSchema<Src>` becomes `WireSchema`. `Digestable` and `DiffSetBuilder::digest` drop `Src` from their schema and column-type bounds.

`TypeMap` is keyed by `WireType` instead of `Src::TypeKey`, so `register` and `with` take a `WireType`.

The per-column payload structs replace their native type field with `wire_type: WireType`: `PgWalstreamColumn` drops `oid` and `type_modifier`, `Wal2JsonColumn` drops `pg_type_name`, and `MaxwellColumn` drops `mysql_type`. Binary integer and float widths on `pg_walstream` are now inferred from the payload byte length. The wal2json paren-stripping and maxwell `tinyint(1)` type-name normalizations are gone because the schema declares the semantic type directly.

Downstreams migrate by implementing the source-independent `WireColumnTypes` and `WireSchema` and registering decoders under `WireType`.

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
