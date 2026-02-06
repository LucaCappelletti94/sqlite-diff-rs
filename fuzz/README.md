# Fuzzing for sqlite-diff-rs

This directory contains harnesses for fuzz testing the `sqlite-diff-rs` crate.

## What is Fuzzing?

[Fuzzing](https://rust-fuzz.github.io/book/) is an automated testing technique that feeds random, invalid, or unexpected inputs into your program to find bugs, crashes, or security vulnerabilities. We use [Honggfuzz](https://github.com/google/honggfuzz) (via [honggfuzz-rs](https://github.com/rust-fuzz/honggfuzz-rs)) as our fuzzing engine.

## Getting Started

1. **Install Prerequisites (Linux/WSL)**

   ```bash
   sudo apt install build-essential binutils-dev libunwind-dev
   cargo install honggfuzz
   ```

2. **Run a Fuzzer**

   ```bash
   cd fuzz
   HFUZZ_RUN_ARGS="--timeout 5 --linux_perf_instr --linux_perf_branch" cargo hfuzz run roundtrip
   HFUZZ_RUN_ARGS="--timeout 5 --linux_perf_instr --linux_perf_branch" cargo hfuzz run sql_roundtrip
   HFUZZ_RUN_ARGS="--timeout 5 --linux_perf_instr --linux_perf_branch" cargo hfuzz run apply_roundtrip
   ```

3. **Debugging Crashes**

   If a crash is found, the input is saved in `hfuzz_workspace/<target>/`. You can replay it with:

   ```bash
   cargo hfuzz run-debug roundtrip hfuzz_workspace/roundtrip/*.fuzz
   ```

4. **Cleaning Up**

   ```bash
   cargo hfuzz clean
   ```

## Fuzz Targets

- **`roundtrip`** — Binary round-trip stability: parses arbitrary bytes into a
  `ParsedDiffSet`, serializes back to bytes, re-parses, and re-serializes.
  Asserts the two serialized byte sequences are identical (i.e. one
  normalization pass produces stable output). Also tests parser robustness
  on arbitrary input.

- **`sql_roundtrip`** — SQL Display round-trip: parses SQL into a
  `ChangeSet`/`PatchSet`, converts to SQL via `Display`, re-parses, and
  compares the in-memory structures.

- **`apply_roundtrip`** — End-to-end bit-parity: parses SQL into
  `ChangeSet`/`PatchSet` via `FromStr`, serializes to bytes, and compares
  byte-for-byte with rusqlite's session extension output for the same SQL.
  Also applies both changesets to empty databases and verifies identical
  final state.
