# SQLite Changeset/Patchset Format Analysis

## Abstract

SQLite's session extension defines two binary wire formats, changesets and patchsets, for representing database mutations. This report analyzes those formats and the `sqlite-diff-rs` Rust implementation against alternative serialization approaches.

Changesets and patchsets can be applied directly to SQLite databases via `sqlite3changeset_apply()` with no schema compilation or code generation. At scale on a mixed workload, patchsets are 48% smaller than SQL and 43% smaller than JSON, approaching Protobuf efficiency (only 6% larger). Binary apply is 3.4x to 3.9x faster than executing equivalent SQL statements, and the format supports zero-copy borrowed parsing. The `sqlite-diff-rs` crate adds no C dependencies and works in WASM and embedded environments.

The downsides are limited compressibility (the binary encoding leaves little redundancy for general compressors), tight coupling to SQLite's type system and operational semantics, and a small ecosystem outside SQLite itself.

---

## Binary Format Overview

![Changeset vs Patchset binary wire format](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/docs/format_illustration.svg)

Both formats share the same container structure: one or more table sections, each with a header followed by change records.

| Aspect | Changeset (`'T'` / 0x54) | Patchset (`'P'` / 0x50) |
|--------|--------------------------|-------------------------|
| INSERT | All column values | All column values |
| DELETE | All old column values | PK values only |
| UPDATE old | All old column values | PK + undefined for non-PK |
| UPDATE new | All new column values | All new column values |
| Reversible | Yes | No |
| Relative size | Larger | ~23% smaller |

### Value Encoding

| Type byte | Type | Payload |
|-----------|------|---------|
| 0x00 | Undefined | none |
| 0x01 | INTEGER | 8 bytes big-endian |
| 0x02 | FLOAT | 8 bytes IEEE 754 |
| 0x03 | TEXT | varint length + UTF-8 |
| 0x04 | BLOB | varint length + raw |
| 0x05 | NULL | none |

---

## Payload Size Comparison

Comparison of seven serialization formats for identical database operations on a realistic chat schema with UUID primary keys.

![Payload Size vs Operation Count](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/payload-size-bench/plots/ops_scaling.svg)

### Single Operations (bytes)

| Operation | SQL | JSON | MsgPack | CBOR | Protobuf | Patchset | Changeset |
|-----------|----:|-----:|--------:|-----:|---------:|---------:|----------:|
| INSERT (4B body) | 223 | 205 | 136 | 134 | **88** | 104 | 104 |
| INSERT (124B body) | 343 | 325 | 257 | 255 | **209** | 224 | 224 |
| UPDATE (4B to 40B) | 117 | 105 | 79 | 78 | **62** | 103 | 108 |
| DELETE | 68 | 55 | 32 | 31 | **20** | 36 | 104 |

Protobuf wins on single operations because field numbers take 1 to 2 bytes instead of string keys. Patchset pays a fixed table header of about 16 bytes that amortizes at scale.

### Mixed Workload at Scale (1,000 operations: 60% INSERT, 25% UPDATE, 15% DELETE)

| Format | Total bytes | x raw content | Overhead |
|--------|------------:|--------------:|---------:|
| Raw content | 91,430 | 1.0x | - |
| SQL | 203,979 | 2.2x | +123% |
| JSON | 187,931 | 2.1x | +106% |
| MsgPack | 136,003 | 1.5x | +49% |
| CBOR | 134,463 | 1.5x | +47% |
| Changeset | 138,566 | 1.5x | +52% |
| **Patchset** | **106,996** | **1.2x** | **+17%** |
| **Protobuf** | **101,090** | **1.1x** | **+11%** |

### Patchset Savings vs Alternatives

| Compared to | Size reduction |
|-------------|---------------:|
| SQL | **48%** |
| JSON | **43%** |
| MsgPack | **21%** |
| CBOR | **20%** |
| Changeset | **23%** |
| Protobuf | -6% (Protobuf smaller) |

### Compression Behavior

![Deflate compression](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/payload-size-bench/plots/deflate.svg)

![LZ4 compression](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/payload-size-bench/plots/lz4.svg)

![Zstd compression](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/payload-size-bench/plots/zstd.svg)

![Compressor comparison for patchset](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/payload-size-bench/plots/compressor_comparison_patchset.svg)

The binary format's compact encoding leaves limited redundancy for compressors. Text-based formats compress more dramatically but still end up larger than uncompressed patchsets. For bandwidth-constrained scenarios, patchsets offer a good balance without compression overhead.

---

## Apply Performance

Benchmarks comparing four methods for applying changes to SQLite databases.

### Method Comparison (1,000 ops, populated database)

#### Integer Primary Key

![Method comparison int_pk](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/apply-bench-report/plots/method_int_pk.svg)

| Method | Median | Speedup vs SQL |
|--------|-------:|---------------:|
| SQL (autocommit) | 2.35 ms | 1.00x |
| SQL (transaction) | 1.92 ms | 1.22x |
| **Patchset** | **605.7 us** | **3.87x** |
| Changeset | 698.6 us | 3.36x |

#### UUID Primary Key

![Method comparison uuid_pk](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/apply-bench-report/plots/method_uuid_pk.svg)

| Method | Median | Speedup vs SQL |
|--------|-------:|---------------:|
| SQL (autocommit) | 3.16 ms | 1.00x |
| SQL (transaction) | 2.65 ms | 1.20x |
| **Patchset** | **938.4 us** | **3.37x** |
| Changeset | 1.04 ms | 3.03x |

### Scaling Behavior

#### Integer PK

![Scaling int_pk empty](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/apply-bench-report/plots/scaling_int_pk_empty.svg)
![Scaling int_pk populated](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/apply-bench-report/plots/scaling_int_pk_populated.svg)

#### UUID PK

![Scaling uuid_pk empty](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/apply-bench-report/plots/scaling_uuid_pk_empty.svg)
![Scaling uuid_pk populated](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/apply-bench-report/plots/scaling_uuid_pk_populated.svg)

### Configuration Impact (indexes, triggers, foreign keys)

#### Integer PK

![Config variants int_pk](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/apply-bench-report/plots/config_int_pk.svg)

| Config | SQL overhead | Patchset overhead |
|--------|-------------:|------------------:|
| base | - | - |
| indexed | +23.6% | +44.0% |
| triggers | +26.2% | +61.8% |
| foreign keys | +9.4% | +18.2% |

#### UUID PK

![Config variants uuid_pk](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/apply-bench-report/plots/config_uuid_pk.svg)

### Primary Key Type Impact

![PK comparison](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/apply-bench-report/plots/pk_comparison.svg)

| Method | int_pk | uuid_pk | Overhead |
|--------|-------:|--------:|---------:|
| Patchset | 605.7 us | 938.4 us | +54.9% |
| Changeset | 698.6 us | 1.04 ms | +49.5% |

UUID and BLOB primary keys incur about 50% overhead compared to integer PKs due to comparison costs.

---

## Generation Performance

| Operation | rusqlite | sqlite-diff-rs | Speedup |
|-----------|----------|----------------|--------:|
| Changeset generation | 204.1 us | 6.9 us | **30x** |
| Patchset generation | 205.7 us | 6.5 us | **32x** |

The pure-Rust builder skips SQLite's session machinery, which makes changeset construction significantly faster.

---

## CDC Format Conversion Performance

`sqlite-diff-rs` converts common Change Data Capture (CDC) formats into SQLite patchsets, enabling PostgreSQL-to-SQLite replication pipelines.

### Throughput Benchmarks

Each benchmark parses CDC JSON messages and converts them to patchset operations, using realistic multi-column table operations with various data types.

| Format | Throughput | Notes |
|--------|------------|-------|
| **pg_walstream** | 350 to 477 MiB/s | PostgreSQL logical replication format |
| **Debezium** | 310 to 313 MiB/s | Kafka Connect CDC format |
| **wal2json** | 303 to 311 MiB/s | PostgreSQL wal2json plugin (v1/v2) |
| **Maxwell** | 236 to 239 MiB/s | MySQL CDC format |

### Supported Formats

| Format | Feature Flag | Source Database | Use Case |
|--------|--------------|-----------------|----------|
| wal2json | `wal2json` | PostgreSQL | Direct logical replication |
| pg_walstream | `pg-walstream` | PostgreSQL | Streaming replication |
| Debezium | `debezium` | PostgreSQL/MySQL | Kafka-based CDC pipelines |
| Maxwell | `maxwell` | MySQL | Lightweight MySQL CDC |

All conversions are zero-copy where possible, parsing directly into patchset operations without intermediate allocations.

---

## Compile Time & Artifact Size

Cold-build benchmarks comparing rusqlite (bundled SQLite C library) vs sqlite-diff-rs (pure Rust).

| Approach | Profile | Compile Time | Artifact Size |
|----------|---------|-------------:|--------------:|
| rusqlite | debug | 11.5s | 8.86 MiB |
| rusqlite | release | 42.4s | 2.19 MiB |
| sqlite-diff-rs | debug | 3.1s | 8.03 MiB |
| sqlite-diff-rs | release | 7.1s | 383.4 KiB |

`sqlite-diff-rs` compiles 6x faster and produces a 6x smaller release artifact, which is useful in CI/CD pipelines where build time matters, on WASM targets where artifact size impacts load time, and on embedded systems with flash constraints.

---

## When to Use Each Format

Use patchsets when network bandwidth is constrained, forward-only sync is acceptable, undo and revert are not required, and maximum performance is needed.

Use changesets when undo and rollback are required, when conflict detection needs the full row context, or when audit logging requires complete before-and-after state.

Consider alternatives when cross-schema portability is required (Protobuf or JSON), or when human readability matters and the schema is not available to the reader (SQL or JSON).

---

## Conclusion

SQLite's changeset and patchset formats deliver near-Protobuf compactness with no schema compilation, 3x to 4x faster apply than SQL execution, and direct database applicability without intermediate parsing. The main limitation is poor compressibility, because the dense binary encoding does not benefit much from general-purpose compression. For bandwidth-critical applications where every byte matters, Protobuf with compression may win, at the cost of requiring schema synchronization and code generation.

`sqlite-diff-rs` brings these benefits to Rust with pure-Rust, `no_std` compatibility, making the format accessible in environments where linking SQLite's C library is impractical.
