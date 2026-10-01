# Integration Tests

Auxiliary workspace crates that exercise `sqlite-diff-rs` against real external systems and gather data for benchmarking. None of these are required to use the main crate.

| Crate | Purpose |
|---|---|
| `payload-size-bench` | Measures the on-wire size of changeset and patchset binaries for a range of synthetic workloads. |
| `apply-bench-report` | Compares the cost of applying changesets via this crate's builders vs SQLite's own session apply. |
| `wal2json` | Integration test that consumes a real PostgreSQL `wal2json` stream and verifies the conversion to a SQLite-equivalent changeset. Requires Docker. |
| `pg-walstream` | Same idea as `wal2json` but against the raw `pg_walstream` logical replication output. |

The end-to-end browser demo that previously lived here as `frontend`, `backend`, and `shared` has moved to `examples/web-demo/`, a serverless GitHub Pages app that exchanges changesets peer-to-peer over WebRTC. See its own README for usage.
