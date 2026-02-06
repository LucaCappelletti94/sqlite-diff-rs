//! Regression tests from fuzzing crashes.
//!
//! These tests ensure that bugs found by fuzzing don't regress.
//!
//! To add a new roundtrip crash, copy the .fuzz file to `tests/crash_inputs/roundtrip/`.
//! To add a new differential crash, copy the .fuzz file to `tests/crash_inputs/differential/`.

use sqlite_diff_rs::ParsedDiffSet;
use std::fs;

/// Helper to test roundtrip: parse -> serialize -> parse -> compare
fn test_roundtrip(input: &[u8]) {
    let Ok(parsed) = ParsedDiffSet::try_from(input) else {
        return; // Invalid input is fine, we just shouldn't crash
    };

    let serialized: Vec<u8> = parsed.clone().into();
    let reparsed = ParsedDiffSet::try_from(serialized.as_slice())
        .expect("Re-parsing our own output should never fail");

    assert_eq!(parsed, reparsed, "Roundtrip mismatch");
}

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
#[test]
fn fuzz_regression_roundtrip_crash_inputs_dir() {
    let crash_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/crash_inputs/roundtrip");
    let fuzz_crash_dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fuzz/hfuzz_workspace/roundtrip"
    );

    // Ensure crash_inputs directory exists
    let _ = fs::create_dir_all(crash_dir);

    // Copy any new crash files from fuzz workspace
    if let Ok(fuzz_entries) = fs::read_dir(fuzz_crash_dir) {
        for entry in fuzz_entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "fuzz") {
                let dest = format!(
                    "{}/{}",
                    crash_dir,
                    path.file_name().unwrap().to_string_lossy()
                );
                if !std::path::Path::new(&dest).exists() {
                    let _ = fs::copy(&path, &dest);
                }
            }
        }
    }

    let Ok(entries) = fs::read_dir(crash_dir) else {
        return;
    }; // Directory doesn't exist or is empty, that's fine

    let mut tested = 0;
    let mut failures = Vec::new();

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(e) => {
                failures.push(format!("{}: read error: {}", path.display(), e));
                continue;
            }
        };

        test_roundtrip(&data);

        tested += 1;
    }

    assert!(
        failures.is_empty(),
        "Roundtrip failures in {} of {} crash files:\n{}",
        failures.len(),
        tested,
        failures.join("\n")
    );

    eprintln!("Tested {tested} roundtrip crash input files");
}

// ============================================================================
// Differential fuzzing regression tests
// ============================================================================

/// Automatically test all differential crash files in the crash_inputs/differential directory.
///
/// This test also copies any new crash files from the fuzz workspace.
/// Requires the `fuzzing` feature (enables rusqlite + sqlparser).
#[test]
#[cfg(all(feature = "sqlparser", feature = "rusqlite"))]
fn fuzz_regression_differential_crash_inputs_dir() {
    use sqlite_diff_rs::differential_testing::run_differential_test;

    let crash_dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/crash_inputs/differential"
    );
    let fuzz_crash_dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fuzz/hfuzz_workspace/differential"
    );

    // Ensure crash_inputs directory exists
    fs::create_dir_all(crash_dir).expect("Failed to create crash_inputs/differential directory");

    // Copy any new crash files from fuzz workspace
    let mut copied = 0;
    if let Ok(fuzz_entries) = fs::read_dir(fuzz_crash_dir) {
        for entry in fuzz_entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "fuzz") {
                let filename = path.file_name().unwrap().to_string_lossy();
                let dest = format!("{crash_dir}/{filename}");
                if !std::path::Path::new(&dest).exists() {
                    fs::copy(&path, &dest)
                        .unwrap_or_else(|e| panic!("Failed to copy {filename}: {e}"));
                    eprintln!("Copied new crash file: {filename}");
                    copied += 1;
                }
            }
        }
    }
    if copied > 0 {
        eprintln!("Copied {copied} new crash files from fuzz workspace");
    }

    let Ok(entries) = fs::read_dir(crash_dir) else {
        return;
    };

    let mut tested = 0;

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(e) => panic!("{}: read error: {}", path.display(), e),
        };

        // Use lossy conversion since crash files may have invalid UTF-8
        // (honggfuzz captures raw bytes on SIGSEGV before String validation)
        let sql = String::from_utf8_lossy(&data);

        let preview: String = sql.chars().take(120).collect();
        eprintln!(
            "[{tested}] {}: ({} bytes) {preview:?}",
            path.file_name().unwrap().to_string_lossy(),
            sql.len(),
        );

        run_differential_test(&sql);
        tested += 1;
    }

    eprintln!("Tested {tested} differential crash input files");
}
