//! Tests for [`ParsedDiffSet::rename_tables`].
//!
//! A diffset captured against one schema sometimes has to apply against another
//! where the same tables carry different physical names (for example an RLS
//! translation that renames the storage table). `sqlite3changeset_apply` has no
//! table-name hook and silently skips a section whose name resolves to a view
//! or to nothing, so the rename must happen on the changeset bytes between
//! capture and apply. These tests prove the renamed bytes apply cleanly.
#![cfg(feature = "testing")]

use rusqlite::Connection;
use sqlite_diff_rs::testing::{apply_changeset, get_all_rows, session_changeset_and_patchset};
use sqlite_diff_rs::{ChangeSet, DiffOps, Insert, ParsedDiffSet, SimpleTable};

/// End-to-end: capture a real changeset against `orders`, rename it to
/// `orders_rls`, and apply the renamed bytes into a database whose physical
/// table is `orders_rls`.
#[test]
fn changeset_rename_applies_to_physical_table() {
    let (changeset, _patchset) = session_changeset_and_patchset(&[
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT)",
        "INSERT INTO orders (id, item) VALUES (1, 'apple')",
        "INSERT INTO orders (id, item) VALUES (2, 'pear')",
    ]);

    let mut parsed = ParsedDiffSet::parse(&changeset).unwrap();
    let renamed =
        parsed.rename_tables(|name| (name == "orders").then(|| String::from("orders_rls")));
    assert_eq!(renamed, 1);

    let bytes: Vec<u8> = parsed.into();

    let conn = Connection::open_in_memory().unwrap();
    conn.execute(
        "CREATE TABLE orders_rls (id INTEGER PRIMARY KEY, item TEXT)",
        [],
    )
    .unwrap();
    apply_changeset(&conn, &bytes).unwrap();

    let rows = get_all_rows(&conn, "orders_rls");
    assert_eq!(rows.len(), 2, "both rows should land in the physical table");
}

/// Patchset variant of the end-to-end test.
#[test]
fn patchset_rename_applies_to_physical_table() {
    let (_changeset, patchset) = session_changeset_and_patchset(&[
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT)",
        "INSERT INTO orders (id, item) VALUES (1, 'apple')",
        "INSERT INTO orders (id, item) VALUES (2, 'pear')",
    ]);

    let mut parsed = ParsedDiffSet::parse(&patchset).unwrap();
    assert!(parsed.is_patchset());
    let renamed =
        parsed.rename_tables(|name| (name == "orders").then(|| String::from("orders_rls")));
    assert_eq!(renamed, 1);

    let bytes: Vec<u8> = parsed.into();

    let conn = Connection::open_in_memory().unwrap();
    conn.execute(
        "CREATE TABLE orders_rls (id INTEGER PRIMARY KEY, item TEXT)",
        [],
    )
    .unwrap();
    apply_changeset(&conn, &bytes).unwrap();

    let rows = get_all_rows(&conn, "orders_rls");
    assert_eq!(rows.len(), 2, "both rows should land in the physical table");
}

/// An all-`None` callback renames nothing and re-encodes to identical bytes.
#[test]
fn all_none_callback_is_noop() {
    let (changeset, _) = session_changeset_and_patchset(&[
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT)",
        "INSERT INTO orders (id, item) VALUES (1, 'apple')",
    ]);

    let mut parsed = ParsedDiffSet::parse(&changeset).unwrap();
    let renamed = parsed.rename_tables(|_| None);
    assert_eq!(renamed, 0);

    let bytes: Vec<u8> = parsed.into();
    assert_eq!(bytes, changeset, "no-op rename must preserve the bytes");
}

/// The count reflects sections actually renamed, not callback invocations.
#[test]
fn count_reflects_renamed_sections_not_invocations() {
    let orders = SimpleTable::new("orders", &["id", "item"], &[0]);
    let payments = SimpleTable::new("payments", &["id", "amount"], &[0]);

    let bytes = ChangeSet::<SimpleTable, String, Vec<u8>>::new()
        .insert(
            Insert::from(orders)
                .set(0, 1i64)
                .unwrap()
                .set(1, "apple")
                .unwrap(),
        )
        .insert(
            Insert::from(payments)
                .set(0, 1i64)
                .unwrap()
                .set(1, 100i64)
                .unwrap(),
        )
        .build();

    let mut parsed = ParsedDiffSet::parse(&bytes).unwrap();
    let mut invocations = 0;
    let renamed = parsed.rename_tables(|name| {
        invocations += 1;
        (name == "orders").then(|| String::from("orders_rls"))
    });

    assert_eq!(invocations, 2, "callback runs once per section");
    assert_eq!(renamed, 1, "only one section was renamed");

    let names: Vec<&str> = parsed
        .table_schemas()
        .iter()
        .map(|s| s.name().as_str())
        .collect();
    assert!(names.contains(&"orders_rls"));
    assert!(names.contains(&"payments"));
    assert!(!names.contains(&"orders"));
}

/// Mapping two sections to the same name leaves them as two sections.
#[test]
fn colliding_names_stay_separate_sections() {
    let orders = SimpleTable::new("orders", &["id", "item"], &[0]);
    let payments = SimpleTable::new("payments", &["id", "item"], &[0]);

    let bytes = ChangeSet::<SimpleTable, String, Vec<u8>>::new()
        .insert(
            Insert::from(orders)
                .set(0, 1i64)
                .unwrap()
                .set(1, "apple")
                .unwrap(),
        )
        .insert(
            Insert::from(payments)
                .set(0, 2i64)
                .unwrap()
                .set(1, "pear")
                .unwrap(),
        )
        .build();

    let mut parsed = ParsedDiffSet::parse(&bytes).unwrap();
    let renamed = parsed.rename_tables(|_| Some(String::from("merged")));
    assert_eq!(renamed, 2);

    let names: Vec<&str> = parsed
        .table_schemas()
        .iter()
        .map(|s| s.name().as_str())
        .collect();
    assert_eq!(names, vec!["merged", "merged"]);
}

/// An empty diffset renames nothing.
#[test]
fn empty_diffset_renames_nothing() {
    let mut parsed = ParsedDiffSet::parse(&[]).unwrap();
    let renamed = parsed.rename_tables(|_| Some(String::from("whatever")));
    assert_eq!(renamed, 0);
}

/// The renamed rows carry through with their values intact, not just row count.
#[test]
fn renamed_rows_preserve_values() {
    let (changeset, _) = session_changeset_and_patchset(&[
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT)",
        "INSERT INTO orders (id, item) VALUES (7, 'plum')",
    ]);

    let mut parsed = ParsedDiffSet::parse(&changeset).unwrap();
    parsed.rename_tables(|name| (name == "orders").then(|| String::from("orders_rls")));
    let bytes: Vec<u8> = parsed.into();

    let conn = Connection::open_in_memory().unwrap();
    conn.execute(
        "CREATE TABLE orders_rls (id INTEGER PRIMARY KEY, item TEXT)",
        [],
    )
    .unwrap();
    apply_changeset(&conn, &bytes).unwrap();

    let id: i64 = conn
        .query_row("SELECT id FROM orders_rls", [], |r| r.get(0))
        .unwrap();
    let item: String = conn
        .query_row("SELECT item FROM orders_rls", [], |r| r.get(0))
        .unwrap();
    assert_eq!(id, 7);
    assert_eq!(item, "plum");
}
