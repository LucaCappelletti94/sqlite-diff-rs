# Apply Benchmark Report

## Methodology

This report compares four methods for applying changes to an SQLite database:

| Method | Description |
|--------|-------------|
| **SQL (autocommit)** | Execute raw SQL statements one at a time (implicit autocommit) |
| **SQL (transaction)** | Same SQL wrapped in a single `BEGIN…COMMIT` transaction |
| **Patchset** | Apply a binary patchset via `conn.apply_strm()` |
| **Changeset** | Apply a binary changeset via `conn.apply_strm()` |

All times are from Criterion.rs (95% confidence level). Median is used as the primary metric. Benchmarks run on in-memory SQLite databases.

## Summary Table

| PK Type | State | Ops | Config | SQL (autocommit) | SQL (transaction) | Patchset | Changeset |
|---------|-------|-----|--------|------|------|------|------|
| int_pk | empty | 30 | base | 75.2 µs ± 0.7 µs | **64.9 µs ± 1.2 µs** | 70.9 µs ± 1.1 µs | 70.5 µs ± 1.6 µs |
| int_pk | empty | 100 | base | 205.1 µs ± 1.4 µs | 165.5 µs ± 1.2 µs | **102.4 µs ± 0.9 µs** | 102.7 µs ± 1.7 µs |
| int_pk | empty | 1000 | base | 2.46 ms ± 16.7 µs | 2.06 ms ± 7.3 µs | 436.2 µs ± 3.7 µs | **430.2 µs ± 3.7 µs** |
| int_pk | populated | 30 | base | 69.6 µs ± 23.3 µs | **65.2 µs ± 112.2 µs** | 87.5 µs ± 1.1 µs | 93.4 µs ± 32.0 µs |
| int_pk | populated | 100 | base | 191.7 µs ± 2.6 µs | 161.4 µs ± 1.5 µs | **134.8 µs ± 1.7 µs** | 150.7 µs ± 1.3 µs |
| int_pk | populated | 1000 | base | 2.33 ms ± 12.4 µs | 1.96 ms ± 8.3 µs | **608.9 µs ± 5.0 µs** | 704.8 µs ± 3.3 µs |
| int_pk | populated | 1000 | fk | 2.60 ms ± 20.6 µs | 2.17 ms ± 18.5 µs | **724.1 µs ± 4.7 µs** | 799.6 µs ± 4.6 µs |
| int_pk | populated | 1000 | indexed | 2.95 ms ± 16.4 µs | 2.29 ms ± 15.3 µs | **886.4 µs ± 5.6 µs** | 984.1 µs ± 7.7 µs |
| int_pk | populated | 1000 | triggers | 2.98 ms ± 20.8 µs | 2.66 ms ± 15.0 µs | **987.5 µs ± 7.5 µs** | 1.07 ms ± 5.1 µs |
| uuid_pk | empty | 30 | base | 92.1 µs ± 8.4 µs | **78.5 µs ± 10.0 µs** | 89.9 µs ± 7.8 µs | 89.8 µs ± 8.2 µs |
| uuid_pk | empty | 100 | base | 258.9 µs ± 1.3 µs | 200.9 µs ± 1.2 µs | 126.6 µs ± 1.2 µs | **125.8 µs ± 11.8 µs** |
| uuid_pk | empty | 1000 | base | 3.31 ms ± 24.7 µs | 2.72 ms ± 18.6 µs | **579.4 µs ± 8.4 µs** | 581.9 µs ± 6.0 µs |
| uuid_pk | populated | 30 | base | 96.5 µs ± 158.3 µs | **92.4 µs ± 127.1 µs** | 125.7 µs ± 34.9 µs | 130.3 µs ± 40.5 µs |
| uuid_pk | populated | 100 | base | 258.6 µs ± 73.1 µs | 225.6 µs ± 98.9 µs | **195.6 µs ± 41.5 µs** | 211.6 µs ± 66.7 µs |
| uuid_pk | populated | 1000 | base | 3.20 ms ± 17.1 µs | 2.65 ms ± 16.4 µs | **952.9 µs ± 11.5 µs** | 1.06 ms ± 17.8 µs |
| uuid_pk | populated | 1000 | fk | 3.49 ms ± 12.0 µs | 2.93 ms ± 25.3 µs | **1.08 ms ± 14.4 µs** | 1.19 ms ± 8.1 µs |
| uuid_pk | populated | 1000 | indexed | 3.81 ms ± 23.8 µs | 3.04 ms ± 12.4 µs | **1.25 ms ± 8.3 µs** | 1.35 ms ± 10.0 µs |
| uuid_pk | populated | 1000 | triggers | 3.83 ms ± 15.4 µs | 3.41 ms ± 15.6 µs | **1.46 ms ± 11.2 µs** | 1.58 ms ± 9.4 µs |

## Scaling Analysis

How each apply method scales as the number of operations increases (30 → 100 → 1000).

### int pk, empty

![Scaling int_pk empty](scaling_int_pk_empty.svg)

### int pk, populated

![Scaling int_pk populated](scaling_int_pk_populated.svg)

### uuid pk, empty

![Scaling uuid_pk empty](scaling_uuid_pk_empty.svg)

### uuid pk, populated

![Scaling uuid_pk populated](scaling_uuid_pk_populated.svg)

## Method Comparison (populated/1000, base config)

### int pk

![Method comparison int_pk](method_int_pk.svg)

| Method | Median | Speedup vs SQL (autocommit) |
|--------|--------|----------------------------|
| SQL (autocommit) | 2.33 ms | 1.00× |
| SQL (transaction) | 1.96 ms | 1.19× |
| Patchset | 608.9 µs | 3.83× |
| Changeset | 704.8 µs | 3.31× |

### uuid pk

![Method comparison uuid_pk](method_uuid_pk.svg)

| Method | Median | Speedup vs SQL (autocommit) |
|--------|--------|----------------------------|
| SQL (autocommit) | 3.20 ms | 1.00× |
| SQL (transaction) | 2.65 ms | 1.21× |
| Patchset | 952.9 µs | 3.36× |
| Changeset | 1.06 ms | 3.03× |

## Configuration Variant Impact

How secondary indexes, triggers, and foreign keys affect apply performance (populated/1000 scenario).

### int pk

![Config variants int_pk](config_int_pk.svg)

| Method | base | indexed | triggers | fk |
|--------|------|---------|----------|----|
| SQL (autocommit) | 2.33 ms | 2.95 ms (+26.7%) | 2.98 ms (+27.9%) | 2.60 ms (+11.4%) |
| SQL (transaction) | 1.96 ms | 2.29 ms (+16.7%) | 2.66 ms (+35.3%) | 2.17 ms (+10.5%) |
| Patchset | 608.9 µs | 886.4 µs (+45.6%) | 987.5 µs (+62.2%) | 724.1 µs (+18.9%) |
| Changeset | 704.8 µs | 984.1 µs (+39.6%) | 1.07 ms (+52.3%) | 799.6 µs (+13.4%) |

### uuid pk

![Config variants uuid_pk](config_uuid_pk.svg)

| Method | base | indexed | triggers | fk |
|--------|------|---------|----------|----|
| SQL (autocommit) | 3.20 ms | 3.81 ms (+19.1%) | 3.83 ms (+19.8%) | 3.49 ms (+9.1%) |
| SQL (transaction) | 2.65 ms | 3.04 ms (+14.6%) | 3.41 ms (+28.4%) | 2.93 ms (+10.5%) |
| Patchset | 952.9 µs | 1.25 ms (+31.6%) | 1.46 ms (+53.2%) | 1.08 ms (+13.1%) |
| Changeset | 1.06 ms | 1.35 ms (+28.2%) | 1.58 ms (+49.3%) | 1.19 ms (+12.2%) |

## Primary Key Type Impact

Comparison of INTEGER PK vs UUID BLOB PK (populated/1000, base config).

![PK comparison](pk_comparison.svg)

| Method | int_pk | uuid_pk | Δ% |
|--------|--------|---------|------|
| SQL (autocommit) | 2.33 ms | 3.20 ms | +37.3% |
| SQL (transaction) | 1.96 ms | 2.65 ms | +35.0% |
| Patchset | 608.9 µs | 952.9 µs | +56.5% |
| Changeset | 704.8 µs | 1.06 ms | +49.9% |

## Generation Benchmarks

Time to generate a changeset/patchset from a database diff.

| Benchmark | Method | Median | Std Dev |
|-----------|--------|--------|---------|
| changeset_generation | rusqlite | 201.6 µs | 0.8 µs |
| changeset_generation | builder_api | 6.6 µs | 0.0 µs |
| patchset_generation | rusqlite | 203.1 µs | 1.1 µs |
| patchset_generation | builder_api | 6.6 µs | 0.1 µs |
| patchset_generation | sql_parser | 28.4 µs | 0.2 µs |

## Key Findings

- **int_pk**: Changeset apply is **3.3×** faster than autocommit SQL at 1000 ops
- **int_pk**: Patchset apply is **3.8×** faster than autocommit SQL at 1000 ops
- **int_pk**: Wrapping SQL in a transaction gives a **1.2×** speedup over autocommit
- **uuid_pk**: Changeset apply is **3.0×** faster than autocommit SQL at 1000 ops
- **uuid_pk**: Patchset apply is **3.4×** faster than autocommit SQL at 1000 ops
- **uuid_pk**: Wrapping SQL in a transaction gives a **1.2×** speedup over autocommit

