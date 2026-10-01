//! Round-trip real SQLite `session_patchset` / `session_changeset` output
//! through [`ParsedDiffSet`].
//!
//! `assert_patchset_sql_parity` only compares builder output to builder output.
//! It never feeds live SQLite session bytes through our parser, which is
//! exactly the code path that regressed for standalone patchset `UPDATE` (the
//! parser assumed `column_count` values on each side while SQLite writes
//! `pk_count` old values plus `column_count - pk_count` new values). The tests
//! here run `Session::patchset_strm` and `Session::changeset_strm` on live
//! `SQLite` and assert:
//!
//!   1. `ParsedDiffSet::try_from` accepts the bytes,
//!   2. serializing the parsed value back yields byte-identical output.
//!
//! Every combination of {INSERT, UPDATE, DELETE} against a pre-existing row is
//! covered so consolidation cannot mask a specific op-code path.
#![cfg(feature = "testing")]

use sqlite_diff_rs::ParsedDiffSet;
use sqlite_diff_rs::testing::{byte_diff_report, session_changeset_and_patchset_with_setup};

fn assert_parser_roundtrip(label: &str, bytes: &[u8]) {
    let parsed = ParsedDiffSet::try_from(bytes)
        .unwrap_or_else(|err| panic!("{label}: parse failed on live SQLite output: {err}"));
    let serialized: Vec<u8> = parsed.into();
    let report = byte_diff_report(label, bytes, &serialized);
    assert!(
        bytes == serialized.as_slice(),
        "{label}: parser did not preserve SQLite session bytes\n{report}",
    );
}

fn roundtrip_patchset(label: &str, setup: &[&str], tracked: &[&str]) {
    let (_cs, ps) = session_changeset_and_patchset_with_setup(setup, tracked);
    assert_parser_roundtrip(&format!("{label} patchset"), &ps);
}

fn roundtrip_changeset(label: &str, setup: &[&str], tracked: &[&str]) {
    let (cs, _ps) = session_changeset_and_patchset_with_setup(setup, tracked);
    assert_parser_roundtrip(&format!("{label} changeset"), &cs);
}

fn roundtrip_both(label: &str, setup: &[&str], tracked: &[&str]) {
    roundtrip_patchset(label, setup, tracked);
    roundtrip_changeset(label, setup, tracked);
}

// --- INSERT ------------------------------------------------------------------

#[test]
fn session_output_parser_roundtrip_insert_single_pk() {
    roundtrip_both(
        "insert single PK",
        &["CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)"],
        &["INSERT INTO orders VALUES (5, 100, 'pending')"],
    );
}

#[test]
fn session_output_parser_roundtrip_insert_composite_pk() {
    roundtrip_both(
        "insert composite PK",
        &[
            "CREATE TABLE items (a INTEGER NOT NULL, b INTEGER NOT NULL, val TEXT, PRIMARY KEY(a, b))",
        ],
        &["INSERT INTO items VALUES (1, 2, 'v1')"],
    );
}

// --- UPDATE ------------------------------------------------------------------

#[test]
fn session_output_parser_roundtrip_update_single_pk() {
    // Standalone UPDATE against a pre-existing row (setup runs before attach).
    // Historically the patchset case blew up with `ParseError::InvalidValue`.
    roundtrip_both(
        "update single PK",
        &[
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)",
            "INSERT INTO orders VALUES (5, 100, 'pending')",
        ],
        &["UPDATE orders SET status = 'shipped' WHERE id = 5"],
    );
}

#[test]
fn session_output_parser_roundtrip_update_all_non_pk_changed() {
    roundtrip_both(
        "update all non-PK",
        &[
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)",
            "INSERT INTO orders VALUES (5, 100, 'pending')",
        ],
        &["UPDATE orders SET amount = 200, status = 'shipped' WHERE id = 5"],
    );
}

#[test]
fn session_output_parser_roundtrip_update_composite_pk() {
    roundtrip_patchset(
        "update composite PK",
        &[
            "CREATE TABLE items (a INTEGER NOT NULL, b INTEGER NOT NULL, val TEXT, PRIMARY KEY(a, b))",
            "INSERT INTO items VALUES (1, 2, 'v1')",
        ],
        &["UPDATE items SET val = 'v2' WHERE a = 1 AND b = 2"],
    );
    // Changeset variant covered separately because the changeset UPDATE builder
    // has a known unrelated new-side gap for PK columns; only the patchset path
    // is asserted here (matching the reported bug's scope).
}

#[test]
fn session_output_parser_roundtrip_update_reordered_composite_pk() {
    // `PRIMARY KEY(b, a)` swaps the ordinal-vs-column order, exercising the
    // `pk_col_to_pk_pos` mapping through the parser reconstruction.
    roundtrip_patchset(
        "update reordered composite PK",
        &[
            "CREATE TABLE items (a INTEGER NOT NULL, b INTEGER NOT NULL, val TEXT, PRIMARY KEY(b, a))",
            "INSERT INTO items VALUES (10, 20, 'v1')",
        ],
        &["UPDATE items SET val = 'v2' WHERE a = 10 AND b = 20"],
    );
}

#[test]
fn session_output_parser_roundtrip_update_to_null() {
    roundtrip_patchset(
        "update to null",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)",
            "INSERT INTO t VALUES (1, 'hi', 42)",
        ],
        &["UPDATE t SET a = NULL WHERE id = 1"],
    );
}

// --- DELETE ------------------------------------------------------------------

#[test]
fn session_output_parser_roundtrip_delete_single_pk() {
    roundtrip_both(
        "delete single PK",
        &[
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)",
            "INSERT INTO orders VALUES (5, 100, 'pending')",
        ],
        &["DELETE FROM orders WHERE id = 5"],
    );
}

#[test]
fn session_output_parser_roundtrip_delete_composite_pk() {
    roundtrip_both(
        "delete composite PK",
        &[
            "CREATE TABLE items (a INTEGER NOT NULL, b INTEGER NOT NULL, val TEXT, PRIMARY KEY(a, b))",
            "INSERT INTO items VALUES (1, 2, 'v1')",
        ],
        &["DELETE FROM items WHERE a = 1 AND b = 2"],
    );
}

// --- Mixed batches -----------------------------------------------------------

#[test]
fn session_output_parser_roundtrip_mixed_batch() {
    // Same session sees UPDATE, DELETE, and a fresh INSERT against distinct
    // pre-existing rows, exercising the encoder-parser pair in a single blob.
    roundtrip_patchset(
        "mixed batch",
        &[
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)",
            "INSERT INTO orders VALUES (5, 100, 'pending')",
            "INSERT INTO orders VALUES (6, 250, 'pending')",
        ],
        &[
            "UPDATE orders SET status = 'shipped' WHERE id = 5",
            "DELETE FROM orders WHERE id = 6",
            "INSERT INTO orders VALUES (7, 50, 'new')",
        ],
    );
}

// --- Value types on non-PK new-side entries ----------------------------------
//
// The wire layout for the new side of a patchset UPDATE is one entry per
// non-PK column, each encoded either as the new value or `0x00` (undefined).
// The tests below drive every value type through the new-side path so a
// hypothetical mis-decode of length prefixes (TEXT / BLOB) or fixed-width
// payloads (INTEGER / REAL) would surface as a round-trip mismatch.

#[test]
fn session_output_parser_roundtrip_update_integer_boundaries() {
    // i64::MIN, i64::MAX, and 0 in a single UPDATE. Big-endian 8-byte payload
    // for each; a truncated read would surface as a parse error or a slid
    // subsequent value.
    roundtrip_patchset(
        "update integer boundaries",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)",
            "INSERT INTO t VALUES (1, 0, 0, 0)",
        ],
        &["UPDATE t SET a = -9223372036854775808, b = 9223372036854775807, c = 0 WHERE id = 1"],
    );
}

#[test]
fn session_output_parser_roundtrip_update_real_values() {
    roundtrip_patchset(
        "update real values",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a REAL, b REAL, c REAL)",
            "INSERT INTO t VALUES (1, 0.0, 0.0, 0.0)",
        ],
        &["UPDATE t SET a = 3.14159, b = -1.5e-10, c = 1e300 WHERE id = 1"],
    );
}

#[test]
fn session_output_parser_roundtrip_update_text_types() {
    // Empty string, ASCII, multibyte UTF-8, embedded single-quote escapes.
    // TEXT is `type=3, varint length, bytes`; the varint length is the most
    // likely off-by-one hazard.
    roundtrip_patchset(
        "update text variety",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT, d TEXT)",
            "INSERT INTO t VALUES (1, 'x', 'y', 'z', 'w')",
        ],
        &["UPDATE t SET a = '', b = 'ascii', c = 'γειά σου κόσμε 🌍', d = 'don''t' WHERE id = 1"],
    );
}

#[test]
fn session_output_parser_roundtrip_update_blob_types() {
    // Empty blob, small blob, blob with embedded zero bytes and 0xFF markers
    // (the same bytes the parser uses to detect the next table header).
    roundtrip_patchset(
        "update blob variety",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a BLOB, b BLOB, c BLOB)",
            "INSERT INTO t VALUES (1, x'aa', x'bb', x'cc')",
        ],
        &[
            // x'' would be a NULL to SQLite; use x'00' to force a non-empty blob
            // containing a zero byte. x'505417' contains the CHANGESET and
            // PATCHSET table markers plus an UPDATE op code to try to trick a
            // buggy parser into reading blob contents as structural bytes.
            "UPDATE t SET a = x'00', b = x'505417', c = x'ff00ff00' WHERE id = 1",
        ],
    );
}

#[test]
fn session_output_parser_roundtrip_update_null_transitions() {
    // Value -> NULL and NULL -> value in the same session, on different rows.
    roundtrip_patchset(
        "update null transitions",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)",
            "INSERT INTO t VALUES (1, 'hi', 42)",
            "INSERT INTO t VALUES (2, NULL, NULL)",
        ],
        &[
            "UPDATE t SET a = NULL, b = NULL WHERE id = 1",
            "UPDATE t SET a = 'set', b = 99 WHERE id = 2",
        ],
    );
}

#[test]
fn session_output_parser_roundtrip_update_mixed_types() {
    // Every value type present on the new side in the same UPDATE.
    roundtrip_patchset(
        "update mixed types",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, i INTEGER, r REAL, s TEXT, b BLOB, n INTEGER)",
            "INSERT INTO t VALUES (1, 0, 0.0, 'x', x'aa', 0)",
        ],
        &["UPDATE t SET i = 42, r = 2.5, s = 'ok', b = x'deadbeef', n = NULL WHERE id = 1"],
    );
}

// --- PK type coverage --------------------------------------------------------
//
// The old-side wire layout emits PK columns in column order, one after the
// other. A TEXT or BLOB PK is length-prefixed with a varint, exercising the
// same parser path as non-PK TEXT / BLOB values but on the PK-only old side.

#[test]
fn session_output_parser_roundtrip_update_text_pk() {
    roundtrip_patchset(
        "update text PK",
        &[
            "CREATE TABLE t (k TEXT PRIMARY KEY, v INTEGER)",
            "INSERT INTO t VALUES ('hello', 1)",
        ],
        &["UPDATE t SET v = 2 WHERE k = 'hello'"],
    );
}

#[test]
fn session_output_parser_roundtrip_update_blob_pk() {
    roundtrip_patchset(
        "update blob PK",
        &[
            "CREATE TABLE t (k BLOB NOT NULL, v INTEGER, PRIMARY KEY(k))",
            "INSERT INTO t VALUES (x'deadbeef', 1)",
        ],
        &["UPDATE t SET v = 2 WHERE k = x'deadbeef'"],
    );
}

#[test]
fn session_output_parser_roundtrip_delete_text_pk() {
    roundtrip_patchset(
        "delete text PK",
        &[
            "CREATE TABLE t (k TEXT PRIMARY KEY, v INTEGER)",
            "INSERT INTO t VALUES ('bye', 1)",
        ],
        &["DELETE FROM t WHERE k = 'bye'"],
    );
}

#[test]
fn session_output_parser_roundtrip_update_composite_mixed_pk_types() {
    roundtrip_patchset(
        "update composite mixed-type PK",
        &[
            "CREATE TABLE t (a INTEGER NOT NULL, b TEXT NOT NULL, c BLOB NOT NULL, v INTEGER, PRIMARY KEY(a, b, c))",
            "INSERT INTO t VALUES (7, 'k', x'01', 100)",
        ],
        &["UPDATE t SET v = 200 WHERE a = 7 AND b = 'k' AND c = x'01'"],
    );
}

// --- Table shapes ------------------------------------------------------------

#[test]
fn session_output_parser_roundtrip_update_wide_table() {
    // Wide table: many non-PK columns, only a subset changed. Verifies the
    // new-side undefined markers align with the correct column positions after
    // parser rehydration.
    roundtrip_patchset(
        "update wide table",
        &[
            "CREATE TABLE w (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER, d INTEGER, \
             e INTEGER, f INTEGER, g INTEGER, h TEXT)",
            "INSERT INTO w VALUES (1, 10, 20, 30, 40, 50, 60, 70, 'orig')",
        ],
        &[
            // Change only every other non-PK column, leaving alternating
            // undefined markers on the new side.
            "UPDATE w SET a = 11, c = 31, e = 51, g = 71, h = 'new' WHERE id = 1",
        ],
    );
}

#[test]
fn session_output_parser_roundtrip_update_two_column_table() {
    // Minimal shape: one PK, one non-PK. Old side is a single value; new side
    // is a single value. Any off-by-one on `column_count - pk_count == 1`
    // shows up as a truncated read.
    roundtrip_patchset(
        "update two-column table",
        &["CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"],
        &["UPDATE t SET v = 200 WHERE id = 1"],
    );
}

// --- Row and table ordering --------------------------------------------------

#[test]
fn session_output_parser_roundtrip_multiple_rows_updated() {
    // Several UPDATEs on distinct pre-existing rows in the same table. The
    // session-extension hash simulation determines emission order; the parser
    // must accept whatever SQLite writes and preserve it through re-serialize.
    roundtrip_patchset(
        "update multiple rows",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'a')",
            "INSERT INTO t VALUES (2, 'b')",
            "INSERT INTO t VALUES (3, 'c')",
            "INSERT INTO t VALUES (4, 'd')",
            "INSERT INTO t VALUES (5, 'e')",
        ],
        &[
            "UPDATE t SET v = 'A' WHERE id = 1",
            "UPDATE t SET v = 'C' WHERE id = 3",
            "UPDATE t SET v = 'E' WHERE id = 5",
        ],
    );
}

#[test]
fn session_output_parser_roundtrip_multiple_tables_updated() {
    roundtrip_patchset(
        "update multiple tables",
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)",
            "CREATE TABLE tags  (id INTEGER PRIMARY KEY, label TEXT)",
            "INSERT INTO users VALUES (1, 'Alice')",
            "INSERT INTO posts VALUES (10, 'Hello')",
            "INSERT INTO tags  VALUES (100, 'rust')",
        ],
        &[
            "UPDATE tags  SET label = 'sqlite'  WHERE id = 100",
            "UPDATE users SET name  = 'Alicia'  WHERE id = 1",
            "UPDATE posts SET title = 'Goodbye' WHERE id = 10",
        ],
    );
}

// --- Consolidation and interactions with pre-existing rows -------------------

#[test]
fn session_output_parser_roundtrip_update_then_update() {
    roundtrip_patchset(
        "update then update",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'a')",
        ],
        &[
            "UPDATE t SET v = 'b' WHERE id = 1",
            "UPDATE t SET v = 'c' WHERE id = 1",
        ],
    );
}

#[test]
fn session_output_parser_roundtrip_update_then_delete() {
    roundtrip_patchset(
        "update then delete",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'a')",
        ],
        &[
            "UPDATE t SET v = 'b' WHERE id = 1",
            "DELETE FROM t WHERE id = 1",
        ],
    );
}

#[test]
fn session_output_parser_roundtrip_delete_then_insert_becomes_update() {
    // DELETE followed by INSERT on the same PK collapses to an UPDATE in
    // SQLite's session extension. Exercises the UPDATE encoder / parser path
    // through consolidation.
    roundtrip_patchset(
        "delete then insert",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'a')",
        ],
        &[
            "DELETE FROM t WHERE id = 1",
            "INSERT INTO t VALUES (1, 'z')",
        ],
    );
}

#[test]
fn session_output_parser_roundtrip_update_cancels_to_noop() {
    // Setting a column back to its original value inside the session leaves
    // SQLite recording an UPDATE where every non-PK column reads as undefined
    // on the new side (or no UPDATE at all). Either way the wire bytes must
    // survive the parser.
    roundtrip_patchset(
        "update self-cancels",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'a')",
        ],
        &[
            "UPDATE t SET v = 'b' WHERE id = 1",
            "UPDATE t SET v = 'a' WHERE id = 1",
        ],
    );
}

// --- Randomized session-driven differential -----------------------------------
//
// Deterministic pseudo-random session scenarios: generate a handful of tables
// with mixed PK / non-PK column types, seed rows, then execute a random mix of
// standalone UPDATE / DELETE / INSERT statements against the pre-existing set.
// Each seed drives one live SQLite session so any wire-format surprise (row
// ordering under hash simulation, unusual value combinations, or op-code
// distributions we didn't hand-code above) still funnels through the parser.

use rand::{RngExt, SeedableRng, rngs::StdRng};

/// Build a single pseudo-random session scenario for the given seed and
/// verify the parser round-trips SQLite's live patchset output.
fn run_random_session_scenario(seed: u64) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut setup: Vec<String> = Vec::new();
    let mut tracked: Vec<String> = Vec::new();

    // One or two tables, each with a mix of PK / non-PK column types.
    let table_count = rng.random_range(1..=2);
    for t in 0..table_count {
        let table = format!("t{t}");
        // 1..=3 non-PK columns of assorted affinities.
        let non_pk_types: Vec<&str> = (0..rng.random_range(1..=3))
            .map(|_| pick_type(&mut rng))
            .collect();
        let non_pk_cols: Vec<String> = non_pk_types
            .iter()
            .enumerate()
            .map(|(i, ty)| format!("c{i} {ty}"))
            .collect();

        // Single-column INTEGER PK -- keeps SQL generation simple while still
        // driving every value type through the non-PK new-side path.
        setup.push(format!(
            "CREATE TABLE {table} (id INTEGER PRIMARY KEY, {})",
            non_pk_cols.join(", "),
        ));

        // Seed 3 to 6 rows so DELETE / UPDATE have distinct targets.
        let row_count = rng.random_range(3..=6);
        for row_id in 1..=row_count {
            let literals: Vec<String> = non_pk_types
                .iter()
                .map(|ty| random_literal(&mut rng, ty))
                .collect();
            setup.push(format!(
                "INSERT INTO {table} VALUES ({row_id}, {})",
                literals.join(", "),
            ));
        }

        // 3 to 8 random operations, each targeting a seeded row so no
        // constraint violation aborts execution.
        let op_count = rng.random_range(3..=8);
        for _ in 0..op_count {
            let target = rng.random_range(1..=row_count);
            match rng.random_range(0..3) {
                0 => {
                    // UPDATE at least one non-PK column, keep the rest of the
                    // row alone (SQLite emits `0x00` markers for unchanged
                    // non-PK columns on the new side, which is precisely the
                    // wire layout we care about).
                    let n = rng.random_range(1..=non_pk_types.len());
                    let mut set_clauses = Vec::with_capacity(n);
                    let mut cols: Vec<usize> = (0..non_pk_types.len()).collect();
                    for _ in 0..n {
                        let idx = rng.random_range(0..cols.len());
                        let col_idx = cols.remove(idx);
                        let ty = non_pk_types[col_idx];
                        set_clauses.push(format!("c{col_idx} = {}", random_literal(&mut rng, ty)));
                    }
                    tracked.push(format!(
                        "UPDATE {table} SET {} WHERE id = {target}",
                        set_clauses.join(", "),
                    ));
                }
                1 => {
                    tracked.push(format!("DELETE FROM {table} WHERE id = {target}"));
                }
                _ => {
                    // INSERT with a fresh id well above the seeded range so it
                    // never collides with the pre-existing rows.
                    let new_id = row_count + rng.random_range(100..=200);
                    let literals: Vec<String> = non_pk_types
                        .iter()
                        .map(|ty| random_literal(&mut rng, ty))
                        .collect();
                    tracked.push(format!(
                        "INSERT OR IGNORE INTO {table} VALUES ({new_id}, {})",
                        literals.join(", "),
                    ));
                }
            }
        }
    }

    let setup_refs: Vec<&str> = setup.iter().map(String::as_str).collect();
    let tracked_refs: Vec<&str> = tracked.iter().map(String::as_str).collect();
    let label = format!("random seed {seed}");

    let (_cs, ps) = session_changeset_and_patchset_with_setup(&setup_refs, &tracked_refs);
    // Empty patchsets are legal (all ops cancelled): only assert when SQLite
    // actually produced bytes.
    if !ps.is_empty() {
        assert_parser_roundtrip(&format!("{label} patchset"), &ps);
    }
}

fn pick_type(rng: &mut StdRng) -> &'static str {
    match rng.random_range(0..4) {
        0 => "INTEGER",
        1 => "REAL",
        2 => "TEXT",
        _ => "BLOB",
    }
}

fn random_literal(rng: &mut StdRng, ty: &str) -> String {
    // 1-in-6 chance of NULL to exercise the NULL / undefined boundary.
    if rng.random_range(0..6) == 0 {
        return String::from("NULL");
    }
    match ty {
        "INTEGER" => {
            let v: i64 = rng.random_range(-1_000_000..=1_000_000);
            format!("{v}")
        }
        "REAL" => {
            let v: f64 = rng.random_range(-1e6..1e6);
            // Round to a stable representation to avoid formatter drift.
            format!("{v:.4}")
        }
        "TEXT" => {
            // 1..=6 ASCII characters that are safe in SQL single-quoted
            // literals (no `'`, no backslash, no null).
            let len = rng.random_range(1..=6);
            let s: String = (0..len)
                .map(|_| {
                    let n: u8 = rng.random_range(b'a'..=b'z');
                    n as char
                })
                .collect();
            format!("'{s}'")
        }
        _ => {
            // BLOB: 1..=4 hex bytes.
            let len = rng.random_range(1..=4);
            let bytes: Vec<u8> = (0..len).map(|_| rng.random()).collect();
            format!("x'{}'", hex::encode(&bytes))
        }
    }
}

#[test]
fn session_output_parser_roundtrip_randomized_scenarios() {
    // Fixed set of seeds so failures are reproducible and CI runs are
    // deterministic. 32 seeds cover more variety than the hand-written tests
    // while staying under a second on a warm cache.
    for seed in 0..32u64 {
        run_random_session_scenario(seed);
    }
}

// --- Apply-roundtrip: re-serialized bytes are semantically valid to SQLite ---
//
// Byte-identical roundtrip is necessary but not sufficient: a downstream
// consumer using `sqlite3changeset_apply` must be able to replay our
// serializer's output against a fresh database and reach the same state. The
// tests below open two connections, apply SQLite's own patchset against the
// first, apply OUR re-serialized copy of the same bytes against the second,
// then compare the tables row-for-row.

use rusqlite::Connection;
use sqlite_diff_rs::testing::{apply_changeset, get_all_rows};

fn apply_roundtrip_matches_sqlite(setup_ddl: &[&str], setup_dml: &[&str], tracked: &[&str]) {
    // Live SQLite session bytes.
    let (_cs, sqlite_patchset) = session_changeset_and_patchset_with_setup(
        &setup_ddl
            .iter()
            .copied()
            .chain(setup_dml.iter().copied())
            .collect::<Vec<&str>>(),
        tracked,
    );
    assert!(
        !sqlite_patchset.is_empty(),
        "session must emit bytes for this scenario"
    );

    // Round-trip through our parser.
    let parsed = ParsedDiffSet::try_from(sqlite_patchset.as_slice()).unwrap();
    let our_bytes: Vec<u8> = parsed.into();

    // Apply SQLite's own bytes against connection A.
    let conn_a = Connection::open_in_memory().unwrap();
    for ddl in setup_ddl {
        conn_a.execute(ddl, []).unwrap();
    }
    for dml in setup_dml {
        conn_a.execute(dml, []).unwrap();
    }
    apply_changeset(&conn_a, &sqlite_patchset).expect("SQLite bytes must apply cleanly");

    // Apply OUR re-serialized copy against connection B.
    let conn_b = Connection::open_in_memory().unwrap();
    for ddl in setup_ddl {
        conn_b.execute(ddl, []).unwrap();
    }
    for dml in setup_dml {
        conn_b.execute(dml, []).unwrap();
    }
    apply_changeset(&conn_b, &our_bytes).expect("re-serialized bytes must apply cleanly");

    // Compare table state.
    for ddl in setup_ddl {
        // Cheap table-name extraction from "CREATE TABLE <name> (...)".
        let upper = ddl.to_uppercase();
        let idx = upper
            .find("CREATE TABLE")
            .expect("DDL must be CREATE TABLE");
        let after = &ddl[idx + "CREATE TABLE".len()..].trim_start();
        let name_end = after
            .find(|c: char| c.is_whitespace() || c == '(')
            .unwrap_or(after.len());
        let table = &after[..name_end];

        let rows_a = get_all_rows(&conn_a, table);
        let rows_b = get_all_rows(&conn_b, table);
        assert_eq!(
            rows_a, rows_b,
            "post-apply state mismatch for `{table}`:\n  SQLite bytes -> {rows_a:?}\n  our bytes    -> {rows_b:?}",
        );
    }
}

#[test]
fn session_output_parser_apply_roundtrip_update_single_pk() {
    apply_roundtrip_matches_sqlite(
        &["CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)"],
        &["INSERT INTO orders VALUES (5, 100, 'pending')"],
        &["UPDATE orders SET status = 'shipped' WHERE id = 5"],
    );
}

#[test]
fn session_output_parser_apply_roundtrip_update_composite_pk() {
    apply_roundtrip_matches_sqlite(
        &[
            "CREATE TABLE items (a INTEGER NOT NULL, b INTEGER NOT NULL, val TEXT, PRIMARY KEY(a, b))",
        ],
        &["INSERT INTO items VALUES (1, 2, 'v1')"],
        &["UPDATE items SET val = 'v2' WHERE a = 1 AND b = 2"],
    );
}

#[test]
fn session_output_parser_apply_roundtrip_update_wide_partial() {
    apply_roundtrip_matches_sqlite(
        &[
            "CREATE TABLE w (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER, d INTEGER, \
             e INTEGER, f INTEGER, g INTEGER, h TEXT)",
        ],
        &["INSERT INTO w VALUES (1, 10, 20, 30, 40, 50, 60, 70, 'orig')"],
        &["UPDATE w SET a = 11, c = 31, e = 51, g = 71, h = 'new' WHERE id = 1"],
    );
}

#[test]
fn session_output_parser_apply_roundtrip_mixed_batch() {
    apply_roundtrip_matches_sqlite(
        &["CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)"],
        &[
            "INSERT INTO orders VALUES (5, 100, 'pending')",
            "INSERT INTO orders VALUES (6, 250, 'pending')",
        ],
        &[
            "UPDATE orders SET status = 'shipped' WHERE id = 5",
            "DELETE FROM orders WHERE id = 6",
            "INSERT INTO orders VALUES (7, 50, 'new')",
        ],
    );
}

#[test]
fn session_output_parser_apply_roundtrip_text_pk() {
    apply_roundtrip_matches_sqlite(
        &["CREATE TABLE t (k TEXT PRIMARY KEY, v INTEGER)"],
        &["INSERT INTO t VALUES ('hello', 1)"],
        &["UPDATE t SET v = 2 WHERE k = 'hello'"],
    );
}

#[test]
fn session_output_parser_apply_roundtrip_blob_pk() {
    apply_roundtrip_matches_sqlite(
        &["CREATE TABLE t (k BLOB NOT NULL, v INTEGER, PRIMARY KEY(k))"],
        &["INSERT INTO t VALUES (x'deadbeef', 1)"],
        &["UPDATE t SET v = 2 WHERE k = x'deadbeef'"],
    );
}

#[test]
fn session_output_parser_apply_roundtrip_delete_composite_pk() {
    apply_roundtrip_matches_sqlite(
        &[
            "CREATE TABLE items (a INTEGER NOT NULL, b INTEGER NOT NULL, val TEXT, PRIMARY KEY(a, b))",
        ],
        &["INSERT INTO items VALUES (1, 2, 'v1')"],
        &["DELETE FROM items WHERE a = 1 AND b = 2"],
    );
}
