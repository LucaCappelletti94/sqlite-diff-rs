# SQLite Changeset/Patchset Format Analysis

## Abstract

SQLite's session extension defines two binary wire formats—**changesets** and **patchsets**—for representing database mutations. This report analyzes these formats and the `sqlite-diff-rs` Rust implementation, comparing them against alternative serialization approaches.

**Strengths:**

- **Direct SQLite applicability**: Changesets and patchsets can be applied directly to SQLite databases via `sqlite3changeset_apply()`, requiring no schema compilation or code generation
- **Compact wire size**: Patchsets achieve 48% smaller payloads than SQL and 43% smaller than JSON at scale, approaching Protobuf efficiency (only 6% larger)
- **High apply performance**: Binary format application is 3.4–3.9× faster than executing equivalent SQL statements
- **Zero-copy potential**: The format supports borrowed parsing without allocation
- **Pure Rust, no_std**: `sqlite-diff-rs` requires no C dependencies and works in WASM/embedded environments

**Weaknesses:**

- **Poor compressibility**: The format's already-compact binary encoding leaves little redundancy for general-purpose compressors to exploit; compression ratios are modest compared to text formats
- **SQLite-specific**: Unlike Protobuf or JSON, the format is tied to SQLite's type system and operational semantics
- **Limited tooling**: No widespread ecosystem support outside SQLite itself

---

## Binary Format Overview

![Changeset vs Patchset binary wire format](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/docs/format_illustration.svg)

Both formats share the same container structure: one or more *table sections*, each with a header followed by change records.

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
| UPDATE (4B→40B) | 117 | 105 | 79 | 78 | **62** | 103 | 108 |
| DELETE | 68 | 55 | 32 | 31 | **20** | 36 | 104 |

Protobuf wins on single operations due to field numbers (1–2 bytes) instead of string keys. Patchset pays a fixed table header (~16 bytes) that amortizes at scale.

### Mixed Workload at Scale (1,000 operations: 60% INSERT, 25% UPDATE, 15% DELETE)

| Format | Total bytes | × raw content | Overhead |
|--------|------------:|--------------:|---------:|
| Raw content | 91,430 | 1.0× | — |
| SQL | 203,979 | 2.2× | +123% |
| JSON | 187,931 | 2.1× | +106% |
| MsgPack | 136,003 | 1.5× | +49% |
| CBOR | 134,463 | 1.5× | +47% |
| Changeset | 138,566 | 1.5× | +52% |
| **Patchset** | **106,996** | **1.2×** | **+17%** |
| **Protobuf** | **101,090** | **1.1×** | **+11%** |

### Patchset Savings vs Alternatives

| Compared to | Size reduction |
|-------------|---------------:|
| SQL | **48%** |
| JSON | **43%** |
| MsgPack | **21%** |
| CBOR | **20%** |
| Changeset | **23%** |
| Protobuf | −6% (Protobuf smaller) |

### Compression Behavior

![Deflate compression](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/payload-size-bench/plots/deflate.svg)

![LZ4 compression](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/payload-size-bench/plots/lz4.svg)

![Zstd compression](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/payload-size-bench/plots/zstd.svg)

![Compressor comparison for patchset](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/payload-size-bench/plots/compressor_comparison_patchset.svg)

The binary format's compact encoding leaves limited redundancy for compressors. Text-based formats (SQL, JSON) compress more dramatically but still end up larger than uncompressed patchsets. For bandwidth-constrained scenarios, patchsets offer a good balance without compression overhead.

---

## Apply Performance

Benchmarks comparing four methods for applying changes to SQLite databases.

### Method Comparison (1,000 ops, populated database)

#### Integer Primary Key

![Method comparison int_pk](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/apply-bench-report/plots/method_int_pk.svg)

| Method | Median | Speedup vs SQL |
|--------|-------:|---------------:|
| SQL (autocommit) | 2.35 ms | 1.00× |
| SQL (transaction) | 1.92 ms | 1.22× |
| **Patchset** | **605.7 µs** | **3.87×** |
| Changeset | 698.6 µs | 3.36× |

#### UUID Primary Key

![Method comparison uuid_pk](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/apply-bench-report/plots/method_uuid_pk.svg)

| Method | Median | Speedup vs SQL |
|--------|-------:|---------------:|
| SQL (autocommit) | 3.16 ms | 1.00× |
| SQL (transaction) | 2.65 ms | 1.20× |
| **Patchset** | **938.4 µs** | **3.37×** |
| Changeset | 1.04 ms | 3.03× |

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
| base | — | — |
| indexed | +23.6% | +44.0% |
| triggers | +26.2% | +61.8% |
| foreign keys | +9.4% | +18.2% |

#### UUID PK

![Config variants uuid_pk](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/apply-bench-report/plots/config_uuid_pk.svg)

### Primary Key Type Impact

![PK comparison](https://raw.githubusercontent.com/LucaCappelletti94/sqlite-diff-rs/main/integration-tests/apply-bench-report/plots/pk_comparison.svg)

| Method | int_pk | uuid_pk | Overhead |
|--------|-------:|--------:|---------:|
| Patchset | 605.7 µs | 938.4 µs | +54.9% |
| Changeset | 698.6 µs | 1.04 ms | +49.5% |

UUID/BLOB primary keys incur ~50% overhead compared to integer PKs due to comparison costs.

---

## Generation Performance

| Operation | rusqlite | sqlite-diff-rs | Speedup |
|-----------|----------|----------------|--------:|
| Changeset generation | 204.1 µs | 6.9 µs | **30×** |
| Patchset generation | 205.7 µs | 6.5 µs | **32×** |

The pure-Rust builder avoids SQLite's session machinery overhead, enabling significantly faster changeset construction.

---

## Compile Time & Artifact Size

Cold-build benchmarks comparing rusqlite (bundled SQLite C library) vs sqlite-diff-rs (pure Rust).

| Approach | Profile | Compile Time | Artifact Size |
|----------|---------|-------------:|--------------:|
| rusqlite | debug | 11.5s | 8.86 MiB |
| rusqlite | release | 42.4s | 2.19 MiB |
| sqlite-diff-rs | debug | 3.1s | 8.03 MiB |
| sqlite-diff-rs | release | 7.1s | 383.4 KiB |

**sqlite-diff-rs** compiles **6× faster** and produces **6× smaller** release artifacts. This makes it particularly suitable for:

- CI/CD pipelines where build time matters
- WASM targets where artifact size impacts load time
- Embedded systems with flash constraints

---

## When to Use Each Format

### Use Patchset when

- Network bandwidth is constrained
- Forward-only sync is acceptable
- Undo/revert is not required
- Maximum performance is needed

### Use Changeset when

- Undo/rollback capability is required
- Conflict detection needs full row context
- Audit logging requires complete before/after state

### Consider alternatives when

- Cross-schema portability is required (use Protobuf/JSON)
- Human readability is important and schema is not available (use SQL/JSON)

---

## Conclusion

SQLite's changeset/patchset formats offer an compelling balance of:

- **Efficiency**: Near-Protobuf compactness without schema compilation
- **Performance**: 3–4× faster apply than SQL execution
- **Simplicity**: Direct database applicability without intermediate parsing

The primary limitation is poor compressibility—the already-dense binary encoding doesn't benefit much from general-purpose compression. For bandwidth-critical applications where every byte matters, Protobuf with compression may win, but at the cost of requiring schema synchronization and code generation.

`sqlite-diff-rs` brings these benefits to Rust with pure-Rust, no_std compatibility, making the format accessible in environments where linking SQLite's C library is impractical.
