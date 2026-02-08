//! Changeset format — `sqlite-diff-rs` ChangeSet builder.
//!
//! UUIDs are raw BLOB values. Unlike patchset, updates carry both old + new
//! values and deletes carry all column values (for conflict detection).

use sqlite_diff_rs::{ChangeDelete, ChangeSet, ChangeUpdate, TableSchema, Value};

use crate::common::{Format, TestMessage, messages_schema};
use crate::format_patchset; // reuse build_insert — INSERT is identical

// Type aliases for clarity
type Schema = TableSchema<String>;

pub struct Changeset;

fn build_update<'a>(schema: &'a Schema, m: &TestMessage) -> ChangeUpdate<&'a Schema, String, Vec<u8>> {
    ChangeUpdate::from(schema)
        .set(
            0,
            Value::Blob(m.id.as_bytes().to_vec()),
            Value::Blob(m.id.as_bytes().to_vec()),
        )
        .unwrap()
        .set(
            3,
            Value::Text(m.body.clone()),
            Value::Text(m.update_body.clone()),
        )
        .unwrap()
}

fn build_delete<'a>(schema: &'a Schema, m: &TestMessage) -> ChangeDelete<&'a Schema, String, Vec<u8>> {
    ChangeDelete::from(schema)
        .set(0, Value::Blob(m.id.as_bytes().to_vec()))
        .unwrap()
        .set(1, Value::Blob(m.sender.as_bytes().to_vec()))
        .unwrap()
        .set(2, Value::Blob(m.receiver.as_bytes().to_vec()))
        .unwrap()
        .set(3, Value::Text(m.body.clone()))
        .unwrap()
        .set(4, Value::Text(m.created_at.clone()))
        .unwrap()
}

impl Format for Changeset {
    fn name(&self) -> &'static str {
        "Changeset"
    }
    fn batch_mixed(
        &self,
        inserts: &[TestMessage],
        updates: &[TestMessage],
        deletes: &[TestMessage],
    ) -> Vec<u8> {
        let schema = messages_schema();
        let mut cset = ChangeSet::<&Schema, String, Vec<u8>>::new();
        for m in inserts {
            cset = cset.insert(format_patchset::build_insert(&schema, m));
        }
        for m in updates {
            cset = cset.update(build_update(&schema, m));
        }
        for m in deletes {
            cset = cset.delete(build_delete(&schema, m));
        }
        Vec::<u8>::from(cset)
    }
}
