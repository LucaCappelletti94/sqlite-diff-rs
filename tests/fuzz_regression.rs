//! Regression tests from fuzzing crashes.
//!
//! These tests ensure that bugs found by fuzzing don't regress.
//!
//! Each fuzz harness has a corresponding crash-input directory under
//! `tests/crash_inputs/<harness>/`. The directory-based tests auto-copy new
//! `.fuzz` files from the honggfuzz workspace and replay every file through
//! the same shared helper the harness uses.
//!
//! | Harness              | Input type                       | Crash directory                          |
//! |----------------------|----------------------------------|------------------------------------------|
//! | `roundtrip`          | `&[u8]`                          | `tests/crash_inputs/roundtrip/`          |
//! | `reverse_idempotent` | `&[u8]`                          | `tests/crash_inputs/reverse_idempotent/` |
//! | `apply_roundtrip`    | `(TypedSimpleTable, Vec<u8>)`    | `tests/crash_inputs/apply_roundtrip/`    |
//! | `sql_roundtrip`      | `(TypedSimpleTable, String)`     | `tests/crash_inputs/sql_roundtrip/`      |
//! | `differential`       | `(TypedSimpleTable, String)`     | `tests/crash_inputs/differential/`       |
//!
//! Structured-input harnesses (`apply_roundtrip`, `sql_roundtrip`, `differential`)
//! store honggfuzz crash files as raw `arbitrary`-encoded bytes. The regression
//! tests deserialize them via [`arbitrary::Unstructured`] before calling the
//! shared test function. If deserialization fails the file is silently skipped
//! (it may be a legacy file from before the structured-input migration).

use sqlite_diff_rs::testing::{
    TypedSimpleTable, run_crash_dir_regression, test_apply_roundtrip, test_differential,
    test_reverse_idempotent, test_roundtrip, test_sql_roundtrip,
};
use std::time::Duration;

/// Maximum time allowed for a single crash input before we flag it as a
/// timeout-class bug. Honggfuzz uses 1 s by default; we use 2 s to account
/// for debug-mode overhead while still catching algorithmic slowness.
const PER_INPUT_TIME_LIMIT: Duration = Duration::from_secs(2);

/// Crash 1: Empty patchset vs empty changeset equality.
///
/// Input: Patchset marker 'P' (0x50) with minimal table header.
/// Bug: Empty patchset serializes to [], which parses as empty changeset.
/// Fix: ParsedDiffSet::eq treats all empty builders as equal.
#[test]
fn fuzz_regression_empty_patchset_changeset_equality() {
    // P, 1 col, pk_flags, name ";", null term, ...
    let input = [0x50, 0x01, 0x01, 0x3b, 0x01, 0x3d, 0x00];
    test_roundtrip(&input);
}

/// Crash 2: NaN in FLOAT value becomes NULL after roundtrip.
///
/// Input: Changeset with FLOAT containing NaN bit pattern.
/// Bug: decode_value returned Real(NaN), but encode_value converts NaN to NULL.
/// Fix: decode_value now normalizes NaN to Null (matching SQLite behavior).
#[test]
fn fuzz_regression_nan_normalized_to_null() {
    // T, 4 cols, pk_flags, name "\x11", operations with NaN float
    let input = [
        0x54, 0x04, 0x2d, 0x93, 0xf8, 0xff, 0x11, 0x00, 0x09, 0x08, 0x00, 0x02, 0x7f, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00,
    ];
    test_roundtrip(&input);
}

/// Crash 3: Latest crash - needs investigation.
#[test]
/// Crash 3: PatchDelete + Insert with Undefined values.
///
/// Bug: PatchDelete + Insert calls update.set() with Undefined values which errors.
/// Fix: Skip Undefined values in the combination loop.
fn fuzz_regression_crash_3() {
    let input = [
        0x50, 0x01, 0x00, 0x02, 0x02, 0x2d, 0x35, 0x31, 0x38, 0x50, 0x02, 0x00, 0x09, 0x09, 0x09,
        0x00, 0x12, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x01, 0x03, 0x20, 0x00, 0x00,
        0x00, 0x00,
    ];
    test_roundtrip(&input);
}

/// Crash 4: Negative zero (-0.0) not normalized during decoding.
///
/// Bug: decode_value returned Real(-0.0), but encode_value normalizes to 0.0.
/// Fix: decode_value now normalizes -0.0 to 0.0 (matching SQLite behavior).
#[test]
fn fuzz_regression_crash_4() {
    let input = [
        0x54, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x01, 0x08, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x02, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00,
    ];
    test_roundtrip(&input);
}

/// Crash 5: Patchset UPDATE losing PK values during serialization.
///
/// Bug: Patchset UPDATE serialization wrote Undefined for ALL old values,
///      including PK columns. When re-parsed, extract_pk got all Undefined.
/// Fix: Serialize PK values from HashMap key into the old_values PK positions.
#[test]
fn fuzz_regression_crash_5() {
    let input = [
        0x50, 0x02, 0xff, 0x40, 0x00, 0x17, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0x09, 0x00, 0x00, 0x00, 0x50, 0x02, 0xff, 0x40, 0x00,
    ];
    test_roundtrip(&input);
}

/// Crash 6: ChangeDelete + Insert with Undefined values.
///
/// Bug: ChangeDelete + Insert calls update.set(old, new) with Undefined values.
/// Fix: Skip columns where either old or new is Undefined.
#[test]
fn fuzz_regression_crash_6() {
    let input = [
        0x54, 0x05, 0x48, 0x00, 0x00, 0xf5, 0x00, 0x00, 0x09, 0x7e, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x12, 0xf8, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x80, 0x00, 0x00,
        0x00, 0x00,
    ];
    test_roundtrip(&input);
}

/// Automatically test all roundtrip crash files in the crash_inputs/roundtrip directory.
///
/// This test also copies any new crash files from the fuzz workspace.
///
/// Each input is timed against [`PER_INPUT_TIME_LIMIT`] to catch timeout-class
/// bugs that honggfuzz would kill but `cargo test` would silently pass.
#[test]
fn fuzz_regression_roundtrip_crash_inputs_dir() {
    run_crash_dir_regression(
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/crash_inputs/roundtrip"),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fuzz/hfuzz_workspace/roundtrip"
        ),
        PER_INPUT_TIME_LIMIT,
        test_roundtrip,
    );
}

/// Automatically test all reverse_idempotent crash files.
///
/// Raw `&[u8]` input — same simple pattern as the roundtrip test.
#[test]
fn fuzz_regression_reverse_idempotent_crash_inputs_dir() {
    run_crash_dir_regression(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/crash_inputs/reverse_idempotent"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fuzz/hfuzz_workspace/reverse_idempotent"
        ),
        PER_INPUT_TIME_LIMIT,
        test_reverse_idempotent,
    );
}

/// Automatically test all apply_roundtrip crash files.
///
/// Since these crash files are raw bytes (not structured `(TypedSimpleTable, Vec<u8>)`
/// tuples), we parse the changeset first to extract table schemas via
/// [`TypedSimpleTable::from_table_schema`], then apply against each table.
#[test]
fn fuzz_regression_apply_roundtrip_crash_inputs_dir() {
    run_crash_dir_regression(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/crash_inputs/apply_roundtrip"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fuzz/hfuzz_workspace/apply_roundtrip"
        ),
        PER_INPUT_TIME_LIMIT,
        |data| {
            // Always do a roundtrip check
            test_roundtrip(data);

            // If it parses, try to apply against synthesized schemas
            let Ok(parsed) = sqlite_diff_rs::ParsedDiffSet::try_from(data) else {
                return;
            };

            let schemas: Vec<TypedSimpleTable> = parsed
                .table_schemas()
                .into_iter()
                .map(TypedSimpleTable::from_table_schema)
                .collect();

            let serialized: Vec<u8> = parsed.into();
            for schema in &schemas {
                test_apply_roundtrip(schema, &serialized);
            }
        },
    );
}

/// Automatically test all sql_roundtrip crash files.
///
/// Crash files contain `arbitrary`-encoded `(TypedSimpleTable, String)` tuples.
/// Files that fail to deserialize are skipped (legacy or corrupt inputs).
#[test]
fn fuzz_regression_sql_roundtrip_crash_inputs_dir() {
    run_crash_dir_regression(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/crash_inputs/sql_roundtrip"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fuzz/hfuzz_workspace/sql_roundtrip"
        ),
        PER_INPUT_TIME_LIMIT,
        |data| {
            let Ok((schema, sql)) =
                arbitrary::Unstructured::new(data).arbitrary::<(TypedSimpleTable, String)>()
            else {
                return;
            };
            test_sql_roundtrip(&schema, &sql);
        },
    );
}

/// Automatically test all differential crash files.
///
/// Crash files contain `arbitrary`-encoded `(TypedSimpleTable, String)` tuples.
/// Files that fail to deserialize are skipped (legacy or corrupt inputs).
#[test]
fn fuzz_regression_differential_crash_inputs_dir() {
    run_crash_dir_regression(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/crash_inputs/differential"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fuzz/hfuzz_workspace/differential"
        ),
        PER_INPUT_TIME_LIMIT,
        |data| {
            let Ok((schema, sql)) =
                arbitrary::Unstructured::new(data).arbitrary::<(TypedSimpleTable, String)>()
            else {
                return;
            };
            test_differential(&schema, &sql);
        },
    );
}
