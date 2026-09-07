//! Regression for the `DiffFormat` public bound.
//!
//! Before `DiffFormat` existed, the bound that makes a type parameter a diff
//! format (`Format`) was crate-private, so a downstream consumer could not
//! name it. Folding a batch of wire events had to be written once per format
//! even though `DiffSetBuilder::digest` is one method on one type. This test
//! is the downstream view: a single generic function, bound on
//! `F: DiffFormat<String, Vec<u8>>`, that serves both a changeset and a
//! patchset. It fails to compile if the bound is not nameable.

#![cfg(feature = "maxwell")]

extern crate alloc;

use alloc::vec::Vec;

use sqlite_diff_rs::maxwell::{Maxwell, Message, OpType, RowChange};
use sqlite_diff_rs::{
    ChangesetFormat, DiffFormat, DiffSetBuilder, Digestable, PatchsetFormat, TypeMap, WireAdapter,
    WireColumnTypes, WireSchema,
};

mod common;
use common::{TestUsersTable, test_schema};

fn data_map(id: i64, name: &str) -> serde_json::Map<String, serde_json::Value> {
    let mut map = serde_json::Map::new();
    map.insert(
        "id".to_string(),
        serde_json::Value::Number(serde_json::Number::from(id)),
    );
    map.insert(
        "name".to_string(),
        serde_json::Value::String(name.to_string()),
    );
    map.insert("active".to_string(), serde_json::Value::Bool(true));
    map
}

fn message(op_type: OpType, id: i64, name: &str) -> Message {
    let row = RowChange {
        database: "testdb".to_string(),
        table: "users".to_string(),
        data: data_map(id, name),
        old: None,
        ..Default::default()
    };
    match op_type {
        OpType::Insert => Message::Insert(row),
        OpType::Delete => Message::Delete(row),
        other => panic!("unhandled op type in test helper: {other:?}"),
    }
}

/// Fold a batch of wire events into either format with one function body.
///
/// This is the exact shape a consumer could not previously write: `F` is a
/// diff format, `E` is any digestible event, and the body is format-agnostic.
fn fold_events<F, E, Sch, A>(
    events: &[E],
    schema: &Sch,
    adapter: &A,
) -> Result<DiffSetBuilder<F, TestUsersTable, String, Vec<u8>>, E::Error>
where
    F: DiffFormat<String, Vec<u8>>,
    E: Digestable<F, TestUsersTable, String, Vec<u8>>,
    Sch: WireSchema<Table = TestUsersTable>,
    A: WireAdapter<E::Src, String, Vec<u8>>,
    TestUsersTable: WireColumnTypes,
{
    let mut builder = DiffSetBuilder::new();
    for event in events {
        builder = builder.digest(event, schema, adapter)?;
    }
    Ok(builder)
}

#[test]
fn one_function_folds_both_formats() {
    let schema = test_schema();
    let adapter: TypeMap<Maxwell, String, Vec<u8>> = TypeMap::defaults();
    let events = [
        message(OpType::Insert, 1, "Alice"),
        message(OpType::Delete, 2, "Bob"),
    ];

    // Same generic function, instantiated for each format.
    let changeset =
        fold_events::<ChangesetFormat, _, _, _>(&events, &schema, &adapter).expect("changeset");
    let patchset =
        fold_events::<PatchsetFormat, _, _, _>(&events, &schema, &adapter).expect("patchset");

    assert_eq!(changeset.len(), 2, "both events folded into the changeset");
    assert_eq!(patchset.len(), 2, "both events folded into the patchset");

    let changeset_bytes = changeset.build();
    let patchset_bytes = patchset.build();

    // A DELETE encodes every column in a changeset but only the PK in a
    // patchset, so the two builds genuinely differ: the one function really
    // produced two different formats, not the same bytes twice.
    assert_ne!(
        changeset_bytes, patchset_bytes,
        "changeset and patchset DELETE encodings must differ"
    );
    assert_eq!(changeset_bytes[0], b'T', "changeset marker");
    assert_eq!(patchset_bytes[0], b'P', "patchset marker");
}
