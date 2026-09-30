//! `digest_sql` records literals as SQLite stores them under each column's
//! type affinity, compared byte for byte with rusqlite's session extension.

#![cfg(feature = "testing")]

use sqlite_diff_rs::testing::{
    byte_diff_report, session_changeset_and_patchset, session_changeset_and_patchset_with_setup,
};
use sqlite_diff_rs::{PatchSet, SimpleTable};

/// Digests `tracked` into a fresh patchset and compares it with the session
/// rusqlite records for the same statements after running `setup`.
fn digest_parity_with_setup(schema: &SimpleTable, setup: &[&str], tracked: &[&str]) {
    let mut ours = PatchSet::<SimpleTable, String, Vec<u8>>::new();
    ours.add_table(schema);
    for sql in tracked {
        ours.digest_sql(sql).unwrap();
    }
    let our_ps = ours.build();
    let (_, sqlite_ps) = session_changeset_and_patchset_with_setup(setup, tracked);
    let report = byte_diff_report("patchset", &sqlite_ps, &our_ps);
    assert!(sqlite_ps == our_ps, "{tracked:?}\n{report}");
}

const AFFINITY_LITERALS: &[&str] = &[
    "5",
    "-5",
    "5.0",
    "-5.0",
    "-0.0",
    "5.5",
    "0.1",
    "1e5",
    "1e-5",
    "1e16",
    "1e17",
    "123456789012345678",
    "9223372036854775807",
    "9223372036854775808",
    "-9223372036854775808",
    "12345678901234567890123",
    "3500000000000000.2500001",
    "'5'",
    "' 5 '",
    "'+7'",
    "'00012'",
    "'5.'",
    "'3.0e+5'",
    "'.5'",
    "'9223372036854775808'",
    "'1e400'",
    "'0x10'",
    "'abc'",
    "'5abc'",
    "X'35'",
    "NULL",
];

#[test]
fn bit_parity_insert_converts_literals_by_column_affinity() {
    let mut mismatches = Vec::new();
    for declared in [
        "INTEGER",
        "REAL",
        "TEXT",
        "NUMERIC",
        "VARCHAR(10)",
        "",
        "BLOB",
    ] {
        let schema = SimpleTable::with_rowid_alias("t", &[("id", "INTEGER"), ("c", declared)], 0);
        let create = format!("CREATE TABLE t (id INTEGER PRIMARY KEY, c {declared})");
        for literal in AFFINITY_LITERALS {
            let insert = format!("INSERT INTO t (id, c) VALUES (1, {literal})");
            let mut ours = PatchSet::<SimpleTable, String, Vec<u8>>::new();
            ours.add_table(&schema);
            ours.digest_sql(&insert).unwrap();
            let our_ps = ours.build();
            let (_, sqlite_ps) = session_changeset_and_patchset(&[&create, &insert]);
            if our_ps != sqlite_ps {
                mismatches.push(format!(
                    "{literal} into {declared:?}\n{}",
                    byte_diff_report("patchset", &sqlite_ps, &our_ps)
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[test]
fn bit_parity_update_set_converts_by_column_affinity() {
    let schema = SimpleTable::with_rowid_alias(
        "t",
        &[
            ("id", "INTEGER"),
            ("s", "TEXT"),
            ("i", "INTEGER"),
            ("r", "REAL"),
        ],
        0,
    );
    digest_parity_with_setup(
        &schema,
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT, i INTEGER, r REAL)",
            "INSERT INTO t VALUES (1, 'a', 1, 1.5)",
        ],
        &["UPDATE t SET s = 5, i = '7', r = 5 WHERE id = 1"],
    );
    digest_parity_with_setup(
        &schema,
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT, i INTEGER, r REAL)",
            "INSERT INTO t VALUES (1, 'a', 1, 1.5)",
        ],
        &["UPDATE t SET s = 1e-5, i = 5.0, r = '2.5' WHERE id = 1"],
    );
}

#[test]
fn bit_parity_where_number_on_text_key() {
    let schema = SimpleTable::new("k", &[("id", "TEXT"), ("v", "")], &[0]);
    let setup = [
        "CREATE TABLE k (id TEXT PRIMARY KEY, v)",
        "INSERT INTO k VALUES ('5', 1)",
    ];
    digest_parity_with_setup(&schema, &setup, &["DELETE FROM k WHERE id = 5"]);
    digest_parity_with_setup(&schema, &setup, &["UPDATE k SET v = 2 WHERE id = 5"]);
}

#[test]
fn bit_parity_where_text_on_rowid_key() {
    let schema = SimpleTable::with_rowid_alias("k", &[("id", "INTEGER"), ("v", "")], 0);
    let setup = [
        "CREATE TABLE k (id INTEGER PRIMARY KEY, v)",
        "INSERT INTO k VALUES (5, 1)",
    ];
    digest_parity_with_setup(&schema, &setup, &["DELETE FROM k WHERE id = '5'"]);
    digest_parity_with_setup(&schema, &setup, &["UPDATE k SET v = 2 WHERE id = ' 5 '"]);
}

#[test]
fn bit_parity_where_integer_on_real_key() {
    let schema = SimpleTable::new("r", &[("x", "REAL"), ("v", "")], &[0]);
    digest_parity_with_setup(
        &schema,
        &[
            "CREATE TABLE r (x REAL PRIMARY KEY, v)",
            "INSERT INTO r VALUES (5.0, 1)",
        ],
        &["DELETE FROM r WHERE x = 5"],
    );
}

#[test]
fn bit_parity_where_mixed_composite_key() {
    let schema = SimpleTable::new("c", &[("a", "TEXT"), ("b", "INTEGER"), ("v", "")], &[0, 1]);
    digest_parity_with_setup(
        &schema,
        &[
            "CREATE TABLE c (a TEXT, b INTEGER, v, PRIMARY KEY (a, b))",
            "INSERT INTO c VALUES ('1', 2, 0)",
        ],
        &["DELETE FROM c WHERE a = 1 AND b = '2'"],
    );
}

#[test]
fn bit_parity_rowid_key_accepts_integral_text_and_real() {
    let schema = SimpleTable::with_rowid_alias("k", &[("id", "INTEGER"), ("v", "")], 0);
    let create = "CREATE TABLE k (id INTEGER PRIMARY KEY, v)";
    for insert in [
        "INSERT INTO k VALUES ('7', 1)",
        "INSERT INTO k VALUES (7.0, 1)",
    ] {
        digest_parity_with_setup(&schema, &[create], &[insert]);
    }
}

#[test]
fn bit_parity_rowid_where_that_cannot_match_records_nothing() {
    let schema = SimpleTable::with_rowid_alias("k", &[("id", "INTEGER"), ("v", "")], 0);
    let setup = [
        "CREATE TABLE k (id INTEGER PRIMARY KEY, v)",
        "INSERT INTO k VALUES (5, 1)",
    ];
    digest_parity_with_setup(&schema, &setup, &["DELETE FROM k WHERE id = 'abc'"]);
    digest_parity_with_setup(&schema, &setup, &["UPDATE k SET v = 2 WHERE id = 5.5"]);
    digest_parity_with_setup(
        &schema,
        &setup,
        &[
            "INSERT INTO k VALUES (6, 1)",
            "DELETE FROM k WHERE id = 'abc'",
        ],
    );
}

#[test]
fn bit_parity_without_rowid_integer_key_stores_text() {
    let schema = SimpleTable::new("w", &[("id", "INTEGER"), ("v", "")], &[0]);
    digest_parity_with_setup(
        &schema,
        &["CREATE TABLE w (id INTEGER PRIMARY KEY, v) WITHOUT ROWID"],
        &[
            "INSERT INTO w VALUES ('abc', 1)",
            "INSERT INTO w VALUES (7.5, 2)",
        ],
    );
}

#[test]
fn bit_parity_mixed_affinity_statement_list() {
    let schema = SimpleTable::with_rowid_alias(
        "t",
        &[
            ("id", "INTEGER"),
            ("s", "TEXT"),
            ("n", "NUMERIC"),
            ("u", ""),
        ],
        0,
    );
    digest_parity_with_setup(
        &schema,
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT, n NUMERIC, u)",
            "INSERT INTO t VALUES (1, 'x', 1, 1)",
        ],
        &[
            "INSERT INTO t VALUES ('2', 2.0, '3.0e+5', -0.0)",
            "UPDATE t SET s = 0.1, n = '12' WHERE id = '1'",
            "INSERT INTO t VALUES (3, 1e17, 'abc', '5')",
            "DELETE FROM t WHERE id = 3.0",
            "DELETE FROM t WHERE id = 'nope'",
        ],
    );
}

#[test]
fn bit_parity_where_null_key_records_nothing() {
    let schema = SimpleTable::new("k", &[("id", "TEXT"), ("v", "")], &[0]);
    let setup = [
        "CREATE TABLE k (id TEXT PRIMARY KEY, v)",
        "INSERT INTO k VALUES ('a', 1)",
    ];
    digest_parity_with_setup(&schema, &setup, &["DELETE FROM k WHERE id = NULL"]);
    digest_parity_with_setup(&schema, &setup, &["UPDATE k SET v = 2 WHERE id = NULL"]);
}
