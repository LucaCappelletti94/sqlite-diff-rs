# compile-bench

Measures **compile time** and **compiled artifact size** for three approaches to generating SQLite changesets/patchsets:

|   Feature   | Approach                                |
|-------------|-----------------------------------------|
| `rusqlite`  | rusqlite's native session extension API |
| `builder`   | sqlite-diff-rs programmatic builder API |
| `sqlparser` | sqlite-diff-rs SQL parser integration   |

All three features are enabled by default so that any API breakage is caught immediately by `cargo check`.

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

| Feature        | Profile    |   Compile Time |  Artifact Size |
|:---------------|:-----------|---------------:|---------------:|
| rusqlite       | debug      |         7.971s |       9.48 MiB |
| rusqlite       | release    |        29.013s |       2.17 MiB |
| builder        | debug      |         1.973s |       8.01 MiB |
| builder        | release    |         4.567s |      375.7 KiB |
| sqlparser      | debug      |         7.211s |      49.86 MiB |
| sqlparser      | release    |        36.895s |       4.03 MiB |

Each row is a **cold build** — the crate is cleaned before every measurement so the time reflects a full recompilation of `compile-bench` and its unique dependencies.

The **builder** approach is the clear winner: ~2s debug / ~5s release compile time and the smallest artifact (376 KiB release). **rusqlite** pays the cost of compiling the bundled C SQLite library (~29s release, 2.2 MiB). **sqlparser** is the heaviest due to the large sqlparser crate AST — the debug artifact is notably large (50 MiB) because of all the generic monomorphisation, though it strips down to 4 MiB in release.
