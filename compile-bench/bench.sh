#!/usr/bin/env bash
# compile-bench/bench.sh — Measure compile time and artifact size for each approach.
#
# Usage:
#   cd compile-bench && bash bench.sh
#
# Output: a table with feature × profile × compile-time × artifact-size.

set -euo pipefail

FEATURES=("rusqlite" "builder" "sqlparser")
PROFILES=("debug" "release")
CRATE_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_TARGET_BASE="$CRATE_DIR/target-bench"

# Clean up previous benchmark target dirs
rm -rf "$BENCH_TARGET_BASE"

printf "\n| %-14s | %-10s | %14s | %14s |\n" "Feature" "Profile" "Compile Time" "Artifact Size"
printf "|:%s|:%s|%s:|%s:|\n" "---------------" "-----------" "---------------" "---------------"

for feature in "${FEATURES[@]}"; do
  for profile in "${PROFILES[@]}"; do
    # Use an isolated target directory per feature×profile for a truly cold build
    target_dir="$BENCH_TARGET_BASE/$feature-$profile"

    # Build args
    build_args=(
      "build"
      "--lib"
      "--manifest-path" "$CRATE_DIR/Cargo.toml"
      "--target-dir" "$target_dir"
      "--no-default-features"
      "--features" "$feature"
    )
    if [[ "$profile" == "release" ]]; then
      build_args+=("--release")
    fi

    # Time the build (wall-clock, silent)
    start=$(date +%s%N)
    cargo "${build_args[@]}" >/dev/null 2>&1
    end=$(date +%s%N)
    elapsed_ms=$(( (end - start) / 1000000 ))
    elapsed_s="$((elapsed_ms / 1000)).$( printf '%03d' $((elapsed_ms % 1000)) )s"

    # Find the artifact
    if [[ "$profile" == "release" ]]; then
      profile_dir="$target_dir/release"
    else
      profile_dir="$target_dir/debug"
    fi

    # Look for cdylib (.so on Linux, .dylib on macOS, .dll on Windows)
    artifact=$(find "$profile_dir" -maxdepth 1 \
      \( -name 'libcompile_bench.so' -o -name 'libcompile_bench.dylib' -o -name 'compile_bench.dll' \) \
      -print -quit 2>/dev/null || true)

    if [[ -n "$artifact" && -f "$artifact" ]]; then
      size_bytes=$(stat -c%s "$artifact" 2>/dev/null || stat -f%z "$artifact" 2>/dev/null)
      # Human-readable
      if (( size_bytes >= 1048576 )); then
        size_hr="$(awk "BEGIN{printf \"%.2f MiB\", $size_bytes/1048576}")"
      elif (( size_bytes >= 1024 )); then
        size_hr="$(awk "BEGIN{printf \"%.1f KiB\", $size_bytes/1024}")"
      else
        size_hr="${size_bytes} B"
      fi
    else
      size_hr="(not found)"
    fi

    printf "| %-14s | %-10s | %14s | %14s |\n" "$feature" "$profile" "$elapsed_s" "$size_hr"
  done
done

# Clean up benchmark target dirs
rm -rf "$BENCH_TARGET_BASE"

printf "\n"
