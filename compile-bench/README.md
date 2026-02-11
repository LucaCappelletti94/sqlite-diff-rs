# compile-bench

Measures **compile time** and **compiled artifact size** for two approaches to generating SQLite changesets/patchsets:

| Feature | Approach |
|---------|----------|
| `rusqlite` | rusqlite's native session extension API (requires bundled SQLite C library) |
| `builder` | sqlite-diff-rs pure-Rust builder API |

Both features are enabled by default so that any API breakage is caught immediately by `cargo check`.

## Quick check

```bash
# Verify everything compiles (all features, default)
cargo check -p compile-bench
```

## Running the benchmark

The benchmark script builds each feature **individually** (with `--no-default-features`) in both debug and release profiles, reporting wall-clock compile time and `.so` artifact size:

```bash
cd compile-bench
bash bench.sh
```

| Feature | Profile | Compile Time | Artifact Size |
|:--------|:--------|-------------:|--------------:|
| rusqlite | debug | 11.5s | 8.86 MiB |
| rusqlite | release | 42.4s | 2.19 MiB |
| sqlite-diff-rs | debug | 3.1s | 8.03 MiB |
| sqlite-diff-rs | release | 7.1s | 383.4 KiB |

Each row is a **cold build** — the crate is cleaned before every measurement so the time reflects a full recompilation of `compile-bench` and its unique dependencies.

**sqlite-diff-rs** compiles **6× faster** and produces **6× smaller artifacts** than rusqlite. The rusqlite approach pays the cost of compiling the bundled C SQLite library (~42s release, 2.2 MiB). sqlite-diff-rs is pure Rust with minimal dependencies, resulting in ~7s release compile time and a 383 KiB artifact.
