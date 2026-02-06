# sqlite-diff-rs

A Rust library for building SQLite [changeset and patchset](https://www.sqlite.org/sessionintro.html) binary formats programmatically.

[![Crates.io](https://img.shields.io/crates/v/sqlite-diff-rs.svg)](https://crates.io/crates/sqlite-diff-rs)
[![Documentation](https://docs.rs/sqlite-diff-rs/badge.svg)](https://docs.rs/sqlite-diff-rs)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Overview

SQLite's [session extension](https://www.sqlite.org/session.html) provides a powerful mechanism for tracking and applying database changes. This crate enables you to **construct changeset/patchset binary data without requiring SQLite** — useful for:

- **Change Data Capture (CDC)**: Translate changes from PostgreSQL logical replication, Kafka, or other sources into SQLite-compatible formats
- **Offline sync**: Build changesets on a server to apply to client SQLite databases
- **Testing**: Generate test fixtures for changeset processing code
- **Cross-database sync**: Bridge between different database systems using SQLite's format as an interchange

## License

MIT License — see [LICENSE](LICENSE) for details.
