//! Stable table-section ordering for SQLite changesets and patchsets.
#![cfg(feature = "testing")]

use rusqlite::Connection;
use sqlite_diff_rs::ParsedDiffSet;
use sqlite_diff_rs::testing::{
    apply_changeset, get_all_rows, session_changeset_and_patchset_with_setup,
};

const TABLE_NAMES: [&str; 4] = ["orders", "audit", "products", "tags"];

fn setup_table(name: &str) -> String {
    format!(
        "CREATE TABLE {name} (
            value TEXT, id INTEGER, group_id INTEGER, data BLOB, measure,
            PRIMARY KEY (group_id, id)
        );
        INSERT INTO {name} VALUES ('old', 1, 7, X'00ff', 1.5);
        INSERT INTO {name} VALUES ('deleted', 2, 7, NULL, 2.5);"
    )
}

fn capture_table(name: &str) -> [Vec<u8>; 2] {
    let (changeset, patchset) = session_changeset_and_patchset_with_setup(
        &[&setup_table(name)],
        &[&format!(
            "INSERT INTO {name} VALUES ('new', 3, 7, X'fe00', -0.0);
            INSERT INTO {name} VALUES ('other', 4, 7, NULL, NULL);
            UPDATE {name} SET value = 'updated', measure = 3.5 WHERE id = 1;
            DELETE FROM {name} WHERE id = 2;"
        )],
    );
    [changeset, patchset]
}

#[test]
fn reordered_sections_preserve_bytes_and_applied_rows() {
    let sections = TABLE_NAMES.map(capture_table);
    for format in 0..2 {
        let input = sections
            .each_ref()
            .map(|section| section[format].as_slice())
            .concat();
        let mut parsed = ParsedDiffSet::parse(&input).unwrap();
        parsed.sort_tables_by_key(|schema| match schema.name().as_str() {
            "products" => 0,
            "orders" => 2,
            _ => 1,
        });
        let output = Vec::<u8>::from(parsed);
        let expected = [2, 1, 3, 0]
            .map(|index| sections[index][format].as_slice())
            .concat();
        assert_eq!(output, expected);

        let original = Connection::open_in_memory().unwrap();
        let reordered = Connection::open_in_memory().unwrap();
        for name in TABLE_NAMES {
            original.execute_batch(&setup_table(name)).unwrap();
            reordered.execute_batch(&setup_table(name)).unwrap();
        }
        apply_changeset(&original, &input).unwrap();
        apply_changeset(&reordered, &output).unwrap();
        for name in TABLE_NAMES {
            assert_eq!(
                get_all_rows(&reordered, name),
                get_all_rows(&original, name)
            );
        }
    }
}

#[test]
fn sorting_by_input_position_preserves_every_permutation() {
    let sections = TABLE_NAMES.map(capture_table);
    for a in 0..4 {
        for b in 0..4 {
            for c in 0..4 {
                for d in 0..4 {
                    let order = [a, b, c, d];
                    if (0..4).any(|i| order[i + 1..].contains(&order[i])) {
                        continue;
                    }
                    for (format, _) in sections[0].iter().enumerate() {
                        let input = order.map(|i| sections[i][format].as_slice()).concat();
                        let mut parsed = ParsedDiffSet::parse(&input).unwrap();
                        parsed.sort_tables_by_key(|schema| {
                            order
                                .iter()
                                .position(|&i| TABLE_NAMES[i] == schema.name())
                                .unwrap()
                        });
                        assert_eq!(Vec::<u8>::from(parsed), input);
                    }
                }
            }
        }
    }
}

#[test]
fn equal_keys_preserve_relative_order() {
    let sections: Vec<_> = (0..32)
        .map(|i| capture_table(&format!("table_{i}")))
        .collect();
    for format in 0..2 {
        let input = sections
            .iter()
            .flat_map(|section| &section[format])
            .copied()
            .collect::<Vec<_>>();
        let mut parsed = ParsedDiffSet::parse(&input).unwrap();
        parsed.sort_tables_by_key(|schema| {
            schema
                .name()
                .strip_prefix("table_")
                .unwrap()
                .parse::<usize>()
                .unwrap()
                % 3
        });
        let expected: Vec<_> = (0..3)
            .flat_map(|key| {
                sections
                    .iter()
                    .enumerate()
                    .filter(move |(i, _)| i % 3 == key)
            })
            .flat_map(|(_, section)| &section[format])
            .copied()
            .collect();
        assert_eq!(Vec::<u8>::from(parsed), expected);
    }
}

#[test]
fn sorting_empty_diffsets_preserves_empty_output() {
    for marker in *b"TP" {
        let mut parsed = ParsedDiffSet::parse(&[marker, 1, 1, b't', 0]).unwrap();
        parsed.sort_tables_by_key(|_| unreachable!("empty diffset has no sections"));
        assert_eq!(Vec::<u8>::from(parsed), Vec::<u8>::new());
    }
}
