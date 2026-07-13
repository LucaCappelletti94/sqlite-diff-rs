# Schema-Aware Forward Conversion (0.2.0)

Status: source of truth, executing.
Target release: `sqlite-diff-rs` 0.2.0 (API-breaking, no partial cuts).
Scope: three CDC forward converters (`pg_walstream`, `wal2json`, `maxwell`), plus the shared `wire` module they funnel through. Debezium is dropped in 0.2.0 (see Section 8).

Every code change references a phase and passes that phase's tests before the next one starts. TDD throughout: failing test first, decoder impl second, green third.

---

## 1. The current state

The three surviving forward converters resolve `wire value` to `Value<S, B>` by inspecting the value's shape and ignoring the column's declared type. Every wire format actually carries the type metadata. None of it flows into the conversion.

### 1.1 Where each format carries type info

`pg_walstream` (grounded in `src/pg_walstream.rs` and `pg_walstream-0.7`):

The relation cache holds `RelationInfo { relation_id, namespace, relation_name, columns: Vec<ColumnInfo> }` where `ColumnInfo::type_id: Oid`. Orthogonal to `EventType`, which carries `RowData` (a bag of `(name, ColumnValue)` pairs). `ColumnValue` is `Null | Binary(Bytes) | Text(Bytes)`. The current `column_value_to_value(&ColumnValue) -> Value` never sees an `Oid` because the enclosing `TryFrom<(EventType, T)>` impls do not receive one.

`wal2json` (grounded in `src/wal2json.rs`):

v2 emits one `Column { name, type_name, value: serde_json::Value }` per column. v1 emits parallel `columnnames`/`columntypes`/`columnvalues` arrays on `ChangeV1`. Both shapes fully deserialize today. `json_to_value(&serde_json::Value, column_name: &str)` throws the type name away.

`maxwell` (grounded in `src/maxwell.rs`):

The current `Message` struct has no field for `columns_types`. Maxwell emits this map when the daemon runs with `--include_types`, but the parser drops it today. `json_to_value` is the same content sniffer.

### 1.2 Failure inventory

Grounded in concrete branches of the two sniffer fns.

`pg_walstream, msg TEXT, wire value "t"`: sniffer produces `Value::Integer(1)`. Text column receives integer on the wire.

`pg_walstream, handle TEXT, wire value "42"`: `Value::Integer(42)`. Same class.

`pg_walstream, amount NUMERIC(20, 8), wire value "123456789.12345678"`: `parse::<f64>()` truncates to about fifteen significant digits, producing `Value::Real(1.2345678912345678e8)`. Financial data corruption.

`wal2json, data JSON, wire value {"k": 1}`: v2 emits `serde_json::Value::Object(...)`. Sniffer hits `Array | Object => Err(UnsupportedType)`. Any Postgres table with a JSON or JSONB column is unconvertible.

`pg_walstream, payload BYTEA, wire value \xdeadbeef in text mode`: sniffer stores `Value::Text("\\xdeadbeef")`. The intended `Value::Blob(vec![0xde, 0xad, 0xbe, 0xef])` never materializes.

`pg_walstream, id UUID, wire value 550e8400-...`: sniffer produces `Value::Text` because 36-char UUID does not parse as int or float. Coincidentally correct today because subql's apply side dispatches on both text and blob.

`pg_walstream, active BOOLEAN, wire value "t"`: sniffer produces `Value::Integer(1)` via the `s == "t"` case. The one path that is right by design.

`maxwell, id UUID, wire value "550e8400-..."`: sniffer produces `Value::Text`. Same coincidence.

`maxwell, big BIGINT UNSIGNED, wire value 18000000000000000000`: `as_i64` returns None, `as_f64` returns Some with precision loss, sniffer produces `Value::Real(1.8e19)`. Precision lost.

### 1.3 The common thread

The type-metadata carrier is present in the parsed struct (or in a sibling struct for `pg_walstream`), and the conversion signature does not receive it. Fix requires: every callsite receives the column type as a first-class argument, and the shape of that argument is uniform across formats so downstream users maintain one mapping catalog.

---

## 2. Design principles

**Principle 1: uphill is superset-to-subset, no schema required.** Wire types are large (Postgres has 50 base types, MySQL 30, plus user-defined). SQLite `Value` has 5. Given a wire type, the decoder is a pure function of that type. Same UUID column, same target, same shape.  No `(table, column_index)` disambiguation needed.

**Principle 2: user configures a type-to-decoder map.** The crate ships defaults for every self-evident mapping. Users register overrides where the default is ambiguous (UUID, JSON canonicalization) or where their DB defines custom types (PG enums). This is the whole API surface for the primary path.

**Principle 3: policy is a trait generic, not a runtime dial.** UUID shape (`Blob(16)` vs `Text(36)`) is a compile-time decoder-type choice, not a `ConversionPrefs` field. Same for JSON canonicalization and every other per-family knob. A user picks `UuidBlob16Decoder` or `UuidText36Decoder` by name at registration time.

**Principle 4: one `TypeMap<Src, S, B>` generic over source.** The map is one struct, one impl, works for every source that implements `WireSource`. `Src::TypeKey` associated type carries the source-specific key (`Oid` for pg_walstream, `Arc<str>` for wal2json and maxwell).

**Principle 5: remove `TryFrom`.** The four operation types' `TryFrom<(&Event, &T)>` impls are convoluted for this use case. Public entry points become named methods on `DiffSetBuilder`, symmetric with `digest_sql`. Breaking change, welcomed.

**Principle 6: one payload family per phase, TDD strictly.** Bool ships and lands with tests before int, int before real, and so on. Each phase's PR starts with a failing test.

---

## 3. The proposed API

Namespace: `sqlite_diff_rs::wire` (new module).

### 3.1 `Decoder<Src, S, B>` trait

Mirror of `Binder<DB>`. Each concrete decoder is a type (usually a unit struct) implementing the trait once per source it supports.

```rust
pub trait Decoder<Src: WireSource, S, B> {
    fn decode(&self, payload: Src::Payload<'_>) -> Result<Value<S, B>, DecodeError>;
}
```

`&self` receiver so stateful decoders (adapter-scoped config, lookup tables, compiled regexes) fit the same shape. Object-safe: `dyn Decoder<Src, S, B>` works, which lets `TypeMap` box heterogeneous decoders in one `HashMap`.

Built-in decoders. Phase 0 ships skeletons that return `DecodeError::NotYetImplemented`. Each subsequent phase populates its own.

| Decoder type | Populated in | Produces |
|---|---|---|
| `BoolDecoder` | Phase 1 | `Integer(0 | 1)` |
| `IntDecoder` | Phase 2 | `Integer(i64)` |
| `Int64OverflowToTextDecoder` | Phase 2 | `Integer(i64)` on fit, else `Text(digits)` |
| `RealDecoder` | Phase 3 | `Real(f64)` |
| `TextDecoder` | Phase 4 | `Text(String)` verbatim |
| `PgByteaBinaryDecoder` | Phase 5 | `Blob(Vec<u8>)` pass-through |
| `PgByteaTextModeDecoder` | Phase 5 | `Blob(Vec<u8>)` after `\xHEX` decode |
| `MySqlBinaryDecoder` | Phase 5 | `Blob(Vec<u8>)` after base64 decode |
| `UuidBlob16Decoder` | Phase 6 | `Blob([u8; 16])` |
| `UuidText36Decoder` | Phase 6 | `Text(36)` verbatim |
| `DecimalTextDecoder` | Phase 7 | `Text(String)` verbatim |
| `TimestampVerbatimDecoder` | Phase 8 | `Text(String)` verbatim |
| `TimestampTzVerbatimDecoder` | Phase 8 | `Text(String)` verbatim |
| `DateVerbatimDecoder` | Phase 8 | `Text(String)` verbatim |
| `TimeVerbatimDecoder` | Phase 8 | `Text(String)` verbatim |
| `IntervalVerbatimDecoder` | Phase 8 | `Text(String)` verbatim |
| `JsonVerbatimDecoder` | Phase 9 | `Text(String)` verbatim |
| `JsonCanonicalDecoder` | Phase 9 | `Text(String)` sorted keys, compact |
| `NullDecoder` | Phase 0 | `Null` |
| `SnifferDecoder` (deprecated) | Phase 0 | 0.1.4 sniffer behavior |

Users write custom decoders by adding a type and `impl Decoder<Src, S, B>` for the sources they care about.

### 3.2 `WireSource` sealed trait

Per-source marker plus payload and key associated types.

```rust
pub trait WireSource: sealed::Sealed {
    type Payload<'a>;
    type TypeKey: Hash + Eq + Clone;

    /// Extract the type key from a payload for dispatch.
    fn type_key(payload: &Self::Payload<'_>) -> Self::TypeKey;
}

pub struct PgWalstream;
pub struct Wal2Json;
pub struct Maxwell;
```

Each source declares its payload struct and its key. Payload struct fields are `pub` so third-party decoders can read them.

```rust
pub struct PgWalstreamColumn<'a> {
    pub column_name: &'a str,
    pub oid: pg_walstream::Oid,
    pub type_modifier: i32,
    pub data: &'a pg_walstream::ColumnValue,
}

pub struct Wal2JsonColumn<'a> {
    pub column_name: &'a str,
    pub pg_type_name: &'a str,
    pub value: &'a serde_json::Value,
}

pub struct MaxwellColumn<'a> {
    pub column_name: &'a str,
    pub mysql_type: Option<&'a str>,
    pub value: &'a serde_json::Value,
}
```

`column_name` on every payload gives decoders self-describing errors without an outer wrapping layer.

Per-source `type_key`:

```rust
impl WireSource for PgWalstream {
    type Payload<'a> = PgWalstreamColumn<'a>;
    type TypeKey = pg_walstream::Oid;
    fn type_key(p: &PgWalstreamColumn<'_>) -> pg_walstream::Oid { p.oid }
}

impl WireSource for Wal2Json {
    type Payload<'a> = Wal2JsonColumn<'a>;
    type TypeKey = alloc::sync::Arc<str>;
    fn type_key(p: &Wal2JsonColumn<'_>) -> alloc::sync::Arc<str> { alloc::sync::Arc::from(p.pg_type_name) }
}

impl WireSource for Maxwell {
    type Payload<'a> = MaxwellColumn<'a>;
    type TypeKey = alloc::sync::Arc<str>;
    fn type_key(p: &MaxwellColumn<'_>) -> alloc::sync::Arc<str> {
        alloc::sync::Arc::from(p.mysql_type.unwrap_or(""))
    }
}
```

Maxwell without `--include_types` uses the empty-string key. `defaults()` does not register anything there, so the map lookup fails cleanly with `DecodeError::NoDecoderForType`, forcing the user to configure explicitly.

### 3.3 `WireAdapter<Src, S, B>` trait

One method. `TypeMap` implements it, and users can also implement it directly for per-column-override wrappers.

```rust
pub trait WireAdapter<Src: WireSource, S, B> {
    fn decode(&self, payload: Src::Payload<'_>) -> Result<Value<S, B>, DecodeError>;
}
```

### 3.4 `TypeMap<Src, S, B>` struct

The primary user-facing type. One generic struct, works for every source.

```rust
pub struct TypeMap<Src: WireSource, S, B> {
    entries: HashMap<Src::TypeKey, alloc::sync::Arc<dyn Decoder<Src, S, B> + Send + Sync>>,
}

impl<Src: WireSource, S, B> TypeMap<Src, S, B> {
    pub fn new() -> Self { Self { entries: HashMap::new() } }

    pub fn register<D>(&mut self, key: Src::TypeKey, decoder: D) -> &mut Self
    where D: Decoder<Src, S, B> + Send + Sync + 'static
    {
        self.entries.insert(key, alloc::sync::Arc::new(decoder));
        self
    }

    pub fn with<D>(mut self, key: Src::TypeKey, decoder: D) -> Self
    where D: Decoder<Src, S, B> + Send + Sync + 'static
    { self.register(key, decoder); self }
}

impl<Src: WireSource, S, B> WireAdapter<Src, S, B> for TypeMap<Src, S, B> {
    fn decode(&self, payload: Src::Payload<'_>) -> Result<Value<S, B>, DecodeError> {
        let key = Src::type_key(&payload);
        self.entries.get(&key).ok_or_else(|| DecodeError::NoDecoderForType {
            column: payload_column_name(&payload).into(),
        })?.decode(payload)
    }
}
```

`payload_column_name` is a private helper indexed off each source's payload struct.

### 3.5 `TypeMapDefaults<S, B>` trait for per-source defaults

Per-source companion trait. Phase 0 provides an empty impl for each source. Later phases add registrations as decoders are populated.

```rust
pub trait TypeMapDefaults<S, B>: WireSource + Sized {
    fn defaults() -> TypeMap<Self, S, B>;
}
```

Call: `TypeMap::<PgWalstream, String, Vec<u8>>::defaults()`.

### 3.6 `DecodeError`

Shared across all sources. Per-format `ConversionError` wraps this via `Decode(DecodeError)`.

```rust
#[non_exhaustive]
pub enum DecodeError {
    NoDecoderForType { column: String },
    NotYetImplemented { decoder: &'static str },
    InvalidUtf8 { column: String },
    InvalidUuid { column: String, source_len: usize },
    InvalidHexEscape { column: String, at: usize },
    IntegerOverflow { column: String, digits: String },
    DecimalPrecisionLoss { column: String },
    JsonNotSerializable { column: String, error: String },
    WrongPayloadKind { column: String, expected: &'static str, actual: &'static str },
    Custom { column: String, message: String },
}
```

### 3.7 Adding custom decoders

Zero-state decoders are unit structs implementing `Decoder<Src, S, B>` for the source(s) they support. Stateful decoders carry fields and hold config.

```rust
struct GeographicPointDecoder;
impl<S, B> Decoder<PgWalstream, S, B> for GeographicPointDecoder
where B: From<Vec<u8>>,
{
    fn decode(&self, payload: PgWalstreamColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        // parse "(x,y)", encode as 16 bytes
    }
}
// Registration: types.register(PG_POINT, GeographicPointDecoder);

struct EnumDecoder { forward: HashMap<Arc<str>, i64> }
impl<S, B> Decoder<Wal2Json, S, B> for EnumDecoder {
    fn decode(&self, payload: Wal2JsonColumn<'_>) -> Result<Value<S, B>, DecodeError> {
        let s = payload.value.as_str().ok_or_else(|| DecodeError::WrongPayloadKind { .. })?;
        self.forward.get(s).copied().map(Value::Integer).ok_or_else(|| ..)
    }
}
// Registration: types.register("my_role".into(), EnumDecoder { forward: role_map });
```

### 3.8 Per-column overrides (escape hatch)

Rare case: same wire type, different application semantics per column (two `text` columns where one is prose and one is a base64 blob). The crate ships a wrapper:

```rust
pub struct WithColumnOverrides<Src: WireSource, S, B, Inner: WireAdapter<Src, S, B>> {
    inner: Inner,
    overrides: HashMap<(alloc::sync::Arc<str>, usize), alloc::sync::Arc<dyn Decoder<Src, S, B> + Send + Sync>>,
}
```

`decode` checks the overrides first, falls through to inner. Requires each digest fn to plumb `(table_name, column_index)` alongside the payload. Considered for Phase 0 or Phase 11 (release polish). Decision deferred to Section 8.

### 3.9 `DiffSetBuilder` entry points

One method per wire format, guarded by the matching feature flag.

```rust
impl<F, T: NamedColumns, S, B> DiffSetBuilder<F, T, S, B> {
    #[cfg(feature = "pg-walstream")]
    pub fn digest_pg_walstream<A>(
        &mut self,
        event: &pg_walstream::EventType,
        relation: &pg_walstream::RelationInfo,
        adapter: &A,
    ) -> Result<&mut Self, pg_walstream::ConversionError>
    where A: WireAdapter<PgWalstream, S, B>;

    #[cfg(feature = "wal2json")]
    pub fn digest_wal2json_v2<A>(&mut self, message: &wal2json::MessageV2, adapter: &A)
        -> Result<&mut Self, wal2json::ConversionError>
    where A: WireAdapter<Wal2Json, S, B>;

    #[cfg(feature = "wal2json")]
    pub fn digest_wal2json_v1_change<A>(&mut self, change: &wal2json::ChangeV1, adapter: &A)
        -> Result<&mut Self, wal2json::ConversionError>
    where A: WireAdapter<Wal2Json, S, B>;

    #[cfg(feature = "maxwell")]
    pub fn digest_maxwell<A>(&mut self, message: &maxwell::Message, adapter: &A)
        -> Result<&mut Self, maxwell::ConversionError>
    where A: WireAdapter<Maxwell, S, B>;
}
```

---

## 4. Per-format specifics

### 4.1 `pg_walstream`

Before: `TryFrom<(EventType, T)>` for `Insert`/`Update`/`ChangeDelete`/`PatchDelete`, same for `ChangeEvent`. Eight impls.

After: methods on `DiffSetBuilder` per Section 3.9. All eight impls deleted, `column_value_to_value` deleted.

`digest_pg_walstream` takes `&RelationInfo` alongside the event to resolve per-column OIDs. `pg_walstream` users maintain a `RelationMap` for pgoutput parsing anyway, so passing `&RelationInfo` is free. The format-side wrapping converts each `(&ColumnValue, &ColumnInfo, &str)` into a `PgWalstreamColumn<'_>` and calls `adapter.decode(...)`.

### 4.2 `wal2json`

Before: `TryFrom<(&ChangeV1, &T)>` and `TryFrom<(&MessageV2, &T)>` for the four operation types. Sixteen impls.

After: methods on `DiffSetBuilder`. All sixteen impls deleted. `json_to_value` deleted.

wal2json v2 reads `type_name` from each `Column` and constructs `Wal2JsonColumn { column_name, pg_type_name, value }`. v1 does the same via parallel arrays.

### 4.3 `maxwell`

Before: `TryFrom<(&Message, &T)>` for the four operation types. Four impls.

After: methods on `DiffSetBuilder`. `Message` grows `columns_types: Option<BTreeMap<String, String>>`. When populated (daemon runs with `--include_types`), format wrapper populates `MaxwellColumn::mysql_type`. When absent, `mysql_type = None`, and `defaults()` does not carry an empty-string entry, so the map lookup fails with `NoDecoderForType`. User then either enables `--include_types` or registers explicit `""` mappings (not recommended).

---

## 5. Multi-phase implementation plan

Eleven phases. Each phase after Phase 0 owns exactly one payload family with a strict TDD template: failing tests first, decoder impl second, green third, cargo fmt / clippy / test --release fourth, next phase.

### Phase 0. Scaffold

Deliverable:
- Delete `src/debezium.rs`, `benches/debezium_benchmark.rs`.
- Remove `debezium` feature from `Cargo.toml`. Remove debezium bench entry.
- Update `src/lib.rs` accordingly.
- New module `sqlite_diff_rs::wire`:
  - `sealed` submodule
  - `WireSource` trait
  - `Decoder<Src, S, B>` trait
  - `WireAdapter<Src, S, B>` trait
  - `TypeMap<Src, S, B>` struct + `TypeMapDefaults<S, B>` trait
  - `DecodeError` enum
  - All decoder unit types listed in Section 3.1's table. `NullDecoder` and `SnifferDecoder` work end to end. Everything else returns `NotYetImplemented { decoder: "BoolDecoder" }` for now.
- Per-format additions:
  - `PgWalstream`, `Wal2Json`, `Maxwell` unit structs.
  - `PgWalstreamColumn`, `Wal2JsonColumn`, `MaxwellColumn` payload structs.
  - `WireSource` impls with concrete `TypeKey`.
  - `TypeMapDefaults` impls with `TypeMap::new()` (no registrations yet).
- Add `Maxwell::Message::columns_types` field.
- Add entry-point methods on `DiffSetBuilder` per Section 3.9. Implementations iterate wire rows, populate `Src::Payload`, invoke `adapter.decode(..)`, and build `Insert` / `Update` / `ChangeDelete` / `PatchDelete` operations that flow through the existing `DiffOps::insert`/`update`/`delete` path.
- Ship `SnifferAdapter` (deprecated) with `impl WireAdapter<Src, ..> for SnifferAdapter { fn decode(..) { SnifferDecoder.decode(..) } }` for each `Src`. Migration bridge for existing callers.
- Ship self-contained sniffer semantics in `SnifferDecoder` per source. Not tied to the surviving `json_to_value` / `column_value_to_value` helpers.

The following remain in place during 0.2.0 development and are deleted in Phase 11 as the release step:
- Existing `TryFrom<..>` impls on `Insert`, `Update`, `ChangeDelete`, `PatchDelete` in `pg_walstream.rs`, `wal2json.rs`, `maxwell.rs`.
- `column_value_to_value` (pg_walstream) and both `json_to_value` fns (wal2json, maxwell).
- All existing test/bench/integration callsites that use them.

This split keeps Phase 0's scope tight (foundation only) while preserving compile-time compatibility for every downstream throughout Phases 1 through 10. Migration of every callsite lands atomically in Phase 11 alongside the API deletion so the workspace never sits in a half-migrated state.

Tests (Phase 0's own):
- Compile-only test that `dyn WireAdapter<Src, S, B>` is object-safe for each `Src`.
- Compile-only test that `TypeMap<Src, S, B>` implements `WireAdapter<Src, S, B>` for each `Src`.
- Round-trip smoke test: existing `tests/wal2json_pg_walstream_equivalence.rs` continues to pass byte-for-byte through `SnifferAdapter`.

Acceptance: `cargo fmt --check`, `cargo clippy --all-features -- -D warnings`, `cargo test --all-features --release` all green.

### Phase 1. Bool

TDD template:
1. Write `tests/phase_01_bool.rs`. Registers `BoolDecoder` under the source's bool key. Feeds a wire event with a bool column. Asserts `Value::Integer(0 | 1)`.
2. Test fails: `NotYetImplemented { decoder: "BoolDecoder" }`.
3. Implement `Decoder<PgWalstream, S, B>`, `Decoder<Wal2Json, S, B>`, `Decoder<Maxwell, S, B>` for `BoolDecoder`.
4. Add `PG_BOOL` (Oid 16) to `PgWalstream`'s `TypeMapDefaults::defaults()`. Add `"boolean"` to `Wal2Json`'s. Add `"tinyint(1)"` to `Maxwell`'s (with the standard MySQL caveat).
5. Test passes.
6. Fmt / clippy / test --release green.

Test invariants:
- Round-trip through `pg_walstream_reverse::op_to_message` produces a wire event that re-digests to the same `PatchSet`.
- Cross-format equality: `PatchSet` from pg_walstream bool column byte-equals `PatchSet` from wal2json bool column with the same value.
- Type-dispatch discriminator: `TypeMap::defaults()` decodes wire value `"t"` (bool column) as `Integer(1)`. Same bytes as a text column (registered `"text" -> TextDecoder`, once Phase 4 lands) would decode as `Text("t")`. This test lives in Phase 4's file since it needs both decoders.

### Phase 2. Int

TDD template as above, per source:
- pg_walstream text: base-10 parse. pg_walstream binary: 2/4/8-byte big-endian by width (width inferred from OID).
- wal2json JSON number via `as_i64`.
- maxwell JSON number via `as_i64`.
- `Int64OverflowToTextDecoder` for MySQL `bigint unsigned`. Values fitting `i64` produce `Integer`, values above produce `Text(digits)`.

Registrations added to `defaults()`:
- pg_walstream: `PG_INT2` (21), `PG_INT4` (23), `PG_INT8` (20) → `IntDecoder`.
- wal2json: `"smallint"`, `"integer"`, `"bigint"` → `IntDecoder`.
- maxwell: `"tinyint"`, `"smallint"`, `"mediumint"`, `"int"`, `"bigint"` → `IntDecoder`. `"bigint unsigned"` → `Int64OverflowToTextDecoder`.

Test invariants:
- `MySQL bigint unsigned` at `9223372036854775807` → `Integer(i64::MAX)`. At `9223372036854775808` → `Text("9223372036854775808")`. At `18000000000000000000` → `Text("18000000000000000000")`.
- Round-trip via `pg_walstream_reverse`.

### Phase 3. Real

- pg_walstream text: `parse::<f64>`. pg_walstream binary: 4/8-byte IEEE 754.
- wal2json/maxwell JSON number via `as_f64`.
- NaN normalizes to `Null`, `-0.0` normalizes to `0.0` (matches existing `decode_value`).

Registrations:
- pg_walstream: `PG_FLOAT4` (700), `PG_FLOAT8` (701).
- wal2json: `"real"`, `"double precision"`, `"float4"`, `"float8"`.
- maxwell: `"float"`, `"double"`, `"real"`.

Test invariants:
- Special values: 0.0, -0.0, 1.0, -1.0, f64::MIN, f64::MAX, NaN, +inf, -inf.
- NaN → Null.
- Round-trip via reverse.

### Phase 4. Text

- pg_walstream text: UTF-8 validate. Invalid UTF-8 → `DecodeError::InvalidUtf8`.
- wal2json/maxwell JSON string pass-through.

Registrations:
- pg_walstream: `PG_TEXT` (25), `PG_VARCHAR` (1043), `PG_BPCHAR` (1042), `PG_NAME` (19).
- wal2json: `"text"`, `"varchar"`, `"character varying"`, `"character"`, `"char"`.
- maxwell: `"char"`, `"varchar"`, `"tinytext"`, `"text"`, `"mediumtext"`, `"longtext"`.

Test invariants:
- ASCII, multi-byte, zero-width joiners.
- Type-dispatch discriminator: wal2json wire `{"type_name": "text", "value": "42"}` decodes to `Text("42")` via `TextDecoder`. The same value with `"integer"` decodes to `Integer(42)` via `IntDecoder`. Kills the sniffer.
- Same wire value `"t"` decodes as `Text("t")` via `TextDecoder`, `Integer(1)` via `BoolDecoder`.
- Round-trip via reverse.

### Phase 5. Bytes

- `PgByteaBinaryDecoder`: pg_walstream `ColumnValue::Binary` pass-through.
- `PgByteaTextModeDecoder`: pg_walstream `ColumnValue::Text` starting with `\x` → hex decode.
- `MySqlBinaryDecoder`: wal2json/maxwell base64-encoded strings → decode. Base64 vendored in-crate (~40 lines, `no_std`), not an external dep.

Registrations:
- pg_walstream: `PG_BYTEA` (17) → `PgByteaTextModeDecoder` (with text-mode fallback logic that also handles Binary via a discriminator inside the decoder).
- wal2json: `"bytea"` → `PgByteaTextModeDecoder` if pg-source, but wal2json emits base64 strings so it's a separate decoder. Reconsider at implementation time.
- maxwell: `"binary"`, `"varbinary"`, `"tinyblob"`, `"blob"`, `"mediumblob"`, `"longblob"` → `MySqlBinaryDecoder`.

Test invariants:
- pg_walstream text-mode `"\xdeadbeef"` → `Blob([0xde, 0xad, 0xbe, 0xef])`.
- Same in binary mode → same `Blob`.
- Cross-format equality on the byte content.
- Round-trip via reverse.

### Phase 6. Uuid

- `UuidBlob16Decoder`: accepts 36-char hyphenated and braced `{...}` forms. Rejects everything else with `DecodeError::InvalidUuid`.
- `UuidText36Decoder`: verifies UUID format, passes through as `Text(36)`.
- No `uuid` crate dep. Inline hex-plus-hyphen parser (16 bytes + 4 hyphens, case-insensitive).

Registrations: NOT added to `defaults()`. `defaults()` deliberately omits UUID because there is no correct default. Users register explicitly.

Test invariants:
- User registers `UuidBlob16Decoder` for column A, `UuidText36Decoder` for column B (via `WithColumnOverrides` if it's in). If overrides are deferred: two `TypeMap`s in two `PatchSet`s, one per shape. Assert both shapes are produced.
- Round-trip via reverse preserves either shape.
- Type-dispatch discriminator: same UUID wire routed through the two decoders produces different `Value` variants.

### Phase 7. Decimal

- `DecimalTextDecoder`: pg_walstream text verbatim. wal2json/maxwell string source verbatim. Numeric JSON source rejected with `DecodeError::DecimalPrecisionLoss`.

Registrations:
- pg_walstream: `PG_NUMERIC` (1700).
- wal2json: `"numeric"`, `"decimal"`.
- maxwell: `"decimal"`, `"numeric"`.

Test invariants:
- `numeric(20, 8)` value `123456789.12345678` → `Text("123456789.12345678")`.
- Value near f64 boundary (`1e16 + 1`) preserves in `Text`, would corrupt via `RealDecoder`. Discriminator asserts both.
- Round-trip via reverse.

### Phase 8. Temporal

Decoders: `TimestampVerbatimDecoder`, `TimestampTzVerbatimDecoder`, `DateVerbatimDecoder`, `TimeVerbatimDecoder`, `IntervalVerbatimDecoder`. Every one preserves the wire text form.

Registrations:
- pg_walstream: `PG_TIMESTAMP` (1114), `PG_TIMESTAMPTZ` (1184), `PG_DATE` (1082), `PG_TIME` (1083), `PG_INTERVAL` (1186).
- wal2json: `"timestamp"`, `"timestamp without time zone"`, `"timestamp with time zone"`, `"date"`, `"time"`, `"time without time zone"`, `"time with time zone"`, `"interval"`.
- maxwell: `"datetime"`, `"timestamp"`, `"date"`, `"time"`, `"year"`.

Test invariants:
- One row per family per format.
- Cross-format equality on the same wire timestamp text.
- Round-trip via reverse.
- Timezone stability: `TimestampTzVerbatimDecoder` preserves the offset verbatim.

### Phase 9. Json / Jsonb

- `JsonVerbatimDecoder`: `serde_json::Value::Object`/`Array` serialize via `to_string()`. String source pass-through.
- `JsonCanonicalDecoder`: recursive key sort, compact whitespace via `to_string()` on the sorted value.

Registrations:
- pg_walstream: `PG_JSON` (114), `PG_JSONB` (3802) → `JsonVerbatimDecoder`.
- wal2json: `"json"`, `"jsonb"` → `JsonVerbatimDecoder`.
- maxwell: `"json"` → `JsonVerbatimDecoder`.

Users override with `JsonCanonicalDecoder` explicitly. `defaults()` does not choose.

Test invariants:
- `{"k": 1}` decodes to `Text("{\"k\":1}")` via verbatim. Kills failure inventory item 4.
- `{"z":1,"a":2}` via canonical decoder produces `Text("{\"a\":2,\"z\":1}")`. Discriminator against verbatim.
- Round-trip via reverse.

### Phase 10. Round-trip differential harness

New integration test crate `integration-tests/schema-aware-roundtrip/`:
- Spins Postgres via `testcontainers` with a table exercising every column type from Phases 1 through 9.
- Inserts one row with load-bearing values in each column.
- Drains through `pg_walstream` and `wal2json` in parallel with the same `TypeMap<*, _, _>` catalog.
- Asserts both `PatchSet`s byte-equal each other.
- Feeds one through `pg_walstream_reverse::op_to_message`, replays into `pg_walstream`, re-digests, asserts byte-equal to the original.
- Optional MySQL container path for `maxwell` (gated to opt-in due to container startup cost).

Regression floor for future releases.

### Phase 11. Release

API teardown:
- Delete every `TryFrom<..>` impl on `Insert`, `Update`, `ChangeDelete`, `PatchDelete` from `pg_walstream.rs`, `wal2json.rs`, `maxwell.rs`.
- Delete `column_value_to_value` (pg_walstream) and both `json_to_value` fns (wal2json, maxwell).
- Migrate every remaining callsite in `src/`, `tests/`, `benches/`, `fuzz/`, `integration-tests/` to `digest_*(&event, &SnifferAdapter)` (one-line change per callsite).

Release meta:
- Bump to 0.2.0 in `Cargo.toml`.
- CHANGELOG covering the breaking API, the migration bridge (`SnifferAdapter`), and the Debezium removal.
- README section on `TypeMap::defaults().with(...)` shape with a bool + uuid example.
- `SnifferAdapter` and `SnifferDecoder` remain `#[deprecated]` throughout 0.2.x. Removal ships in 0.3.0.
- CI green: `cargo semver-checks` against 0.1.4 baseline (documents the breakage), `cargo doc --all-features --no-deps` clean, full `cargo test --all-features --release` green, full `cargo clippy --all-features -- -D warnings` green, `cargo fmt --check` green.

---

## 6. Cross-cutting concerns

**Arbitrary impls.** Each remaining format's `arbitrary_impl` module generates payloads that carry consistent type keys plus wire bytes. Fuzz harnesses assert decode never panics and the returned `Value` matches the registered decoder's contract.

**Fuzz harnesses.** Existing per-format targets migrated to the new API. Debezium fuzz target deleted.

**Benchmarks.** Existing per-format benches migrated. Debezium bench deleted. New bench comparing sniffer-path against schema-aware-path to quantify the per-column virtual-call overhead against the removed `parse::<i64>` / `parse::<f64>` cost.

**Documentation.** Every `pub` item in `sqlite_diff_rs::wire` carries doc comments. Cross-linked from the three format modules. Rustdoc examples per format.

**Integration tests workspace.** `integration-tests/wal2json` and `integration-tests/pg-walstream` migrated with `SnifferAdapter` initially. Real adapters land as phases complete. `integration-tests/debezium` (if it exists) deleted. `integration-tests/schema-aware-roundtrip` new in Phase 10.

---

## 7. Migration for downstream consumers

`subql::pg_sqlite_emu`: uses `pg_walstream_reverse` only. Unaffected except through the version bump.

`connetto-rs::subscription_materializer`: not yet built. Consumes new API from day one.

Any downstream calling `Insert::try_from((&change, &table))` today: `cargo build` fails because the impls are deleted. Migration:
- One-liner: `patchset.digest_pg_walstream(&event, &relation, &SnifferAdapter)?` reproduces 0.1.4 behavior with a `#[deprecated]` warning per callsite.
- Real: `patchset.digest_pg_walstream(&event, &relation, &types)?` where `types = TypeMap::<PgWalstream, _, _>::defaults().with(PG_UUID, UuidBlob16Decoder)` (or however the user's catalog is expressed).

Anyone consuming `debezium`: removed with no replacement. Options are `pg_walstream`, `wal2json`, or `maxwell` depending on the source database. If Debezium support is wanted back later, it can return as its own crate outside 0.2.0.

---

## 8. Open decisions

Answered in previous conversation, folded in:
- No `ConversionPrefs`. Trait-generic dispatch instead.
- No `column_type` callback on `WireAdapter`. `TypeMap` handles dispatch.
- No `WireAdapter::column_name`. Digest fn resolves names via `T: NamedColumns`.
- No debezium. Dropped for 0.2.0.

Still pending user call:
1. **Ship `SnifferAdapter`/`SnifferDecoder`?** Migration bridge vs. clean cutover. Recommendation: ship as deprecated, remove in 0.3.0.
2. **`WithColumnOverrides` in 0.2.0 or 0.3.0?** Needed for the "same wire type, different app semantics" escape hatch. Recommendation: 0.2.0 if the roundtrip harness needs it. Otherwise 0.3.0.
3. **Base64: vendor or optional dep?** Recommendation: vendor (~40 lines, `no_std`).

---

## 9. PR structure

One PR per phase. Eleven PRs total. Phase 0 largest. Every subsequent PR: one payload family, decoder impl + registrations + test file. Reuses Phase 0 scaffold.

0.2.0 release tag lands after Phase 11 merges. No partial cuts.

---

## 10. Non-goals for 0.2.0

- No Debezium.
- No PostgreSQL array types. Users register `_int4` etc. against custom decoders if needed.
- No range, enum, composite, geometric, network, money, xml, bit-string types by default. Users register.
- No `Value` shape change. Every decoder chooses a variant of the existing `Value<S, B>` enum.
