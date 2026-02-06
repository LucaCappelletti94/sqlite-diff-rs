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
   HFUZZ_RUN_ARGS="--linux_perf_instr --linux_perf_branch --sanitizers" cargo hfuzz run roundtrip   # Test build -> serialize -> parse roundtrip
   cargo hfuzz run encoding    # Test varint and value encoding
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

- `roundtrip`: Tests that changesets/patchsets built with `DiffSetBuilder` can be
  serialized, parsed back with `ParsedDiffSet`, and re-serialized to produce
  identical bytes. Also tests parser robustness on arbitrary input.

- `encoding`: Tests varint and value encoding/decoding roundtrips. Verifies that
  `encode_varint`/`decode_varint` and `encode_value`/`decode_value` are consistent.

