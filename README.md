# sqlite-diff-rs

[![Crates.io](https://img.shields.io/crates/v/sqlite-diff-rs.svg)](https://crates.io/crates/sqlite-diff-rs)
[![Documentation](https://docs.rs/sqlite-diff-rs/badge.svg)](https://docs.rs/sqlite-diff-rs)
[![CI](https://github.com/LucaCappelletti94/sqlite-diff-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/LucaCappelletti94/sqlite-diff-rs/actions/workflows/ci.yml)
[![Codecov](https://codecov.io/gh/LucaCappelletti94/sqlite-diff-rs/branch/main/graph/badge.svg)](https://codecov.io/gh/LucaCappelletti94/sqlite-diff-rs)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/LucaCappelletti94/sqlite-diff-rs/blob/main/LICENSE)

A Rust library for building SQLite [changeset and patchset](https://www.sqlite.org/sessionintro.html) binary formats programmatically.

## Overview

SQLite's [session extension](https://www.sqlite.org/session.html) provides a powerful mechanism for tracking and applying database changes. This crate enables you to **construct changeset/patchset binary data without requiring SQLite** — useful for:

- **Offline sync**: Build changesets on a server to apply to client SQLite databases
- **Testing**: Generate test fixtures for changeset processing code
- **CDC pipelines**: Produce the binary input for `sqlite3_changeset_apply()` from your own change events
- **Cross-database sync**: Convert change events from PostgreSQL (via wal2json, Debezium, Maxwell) to SQLite format

> **Note:** This crate is different from SQLite's [`sqldiff`](https://sqlite.org/sqldiff.html) command-line tool, which compares two existing database files. This library instead lets you *construct* the changeset/patchset binary format programmatically from your own change data, without needing SQLite.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
sqlite-diff-rs = "0.1"
```

## Quick Start

```rust
use sqlite_diff_rs::{DiffOps, Insert, PatchSet, SimpleTable};

// Define a table schema: "users" with columns (id, name), PK at index 0
let users = SimpleTable::new("users", &["id", "name"], &[0]);

// Build a patchset with an INSERT
let patchset = PatchSet::<_, String, Vec<u8>>::new()
    .insert(
        Insert::from(users)
            .set(0, 1i64).unwrap()      // id = 1
            .set(1, "Alice").unwrap()   // name = "Alice"
    );

// Encode to binary format
let bytes: Vec<u8> = patchset.into();

// Apply with sqlite3_changeset_apply() or sqlite3session_patchset_apply()
```

## Features

| Feature | Description |
|---------|-------------|
| `testing` | Enables `rusqlite` integration for differential testing |
| `wal2json` | Parse PostgreSQL wal2json output into changesets |
| `pg-walstream` | Integration with `pg_walstream` crate |
| `debezium` | Parse Debezium CDC JSON events |
| `maxwell` | Parse Maxwell CDC JSON events |

Enable features in `Cargo.toml`:

```toml
[dependencies]
sqlite-diff-rs = { version = "0.1", features = ["wal2json"] }
```

## Binary Format Reference

<p align="center">
  <img src="docs/format_illustration.svg" alt="Changeset vs Patchset binary wire format" width="720" />
</p>

Both formats share the same container structure: one or more *table sections*,
each with a table header followed by change records. The key difference is how
much old-row data each operation carries.

**Changesets** (`'T'` / `0x54`) store the complete old state of every column,
making them **reversible** — INSERTs can be turned into DELETEs and vice-versa.

**Patchsets** (`'P'` / `0x50`) omit old values for non-PK columns, producing
a **smaller, forward-only** encoding that cannot be reversed.

| Aspect | Changeset (`'T'`) | Patchset (`'P'`) |
|--------|-------------------|------------------|
| INSERT | All column values | All column values |
| DELETE | All old column values | PK values only |
| UPDATE old | All old column values | PK values + Undefined for non-PK |
| UPDATE new | All new column values | All new column values |
| Reversible | Yes | No |
| Wire size | Larger (carries full old state) | Smaller (omits non-PK old values) |

> See the [SQLite session extension docs](https://www.sqlite.org/session.html)
> for the full specification.

## `no_std` Support

This crate is `no_std` compatible (requires `alloc`). It can be used in embedded or WebAssembly environments.

## License

MIT License — see [LICENSE](https://github.com/LucaCappelletti94/sqlite-diff-rs/blob/main/LICENSE) for details.
