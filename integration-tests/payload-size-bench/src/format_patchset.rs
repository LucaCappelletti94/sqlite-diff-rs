//! Patchset format — `sqlite-diff-rs` PatchSet builder.
//!
//! UUIDs are raw BLOB values. Updates carry only new values; deletes carry
//! only the primary key.

use sqlite_diff_rs::{Insert, PatchSet, PatchUpdate, TableSchema, Value};

use crate::common::{Format, TestMessage, messages_schema};

pub struct Patchset;

pub fn build_insert<'a>(schema: &'a TableSchema, m: &TestMessage) -> Insert<&'a TableSchema> {
    Insert::from(schema)
        .set(0, Value::Blob(m.id.as_bytes().to_vec()))
        .unwrap()
        .set(1, Value::Blob(m.sender.as_bytes().to_vec()))
        .unwrap()
        .set(2, Value::Blob(m.receiver.as_bytes().to_vec()))
        .unwrap()
        .set(3, Value::Text(m.body.to_string()))
        .unwrap()
        .set(4, Value::Text(m.created_at.to_string()))
        .unwrap()
}

fn build_update<'a>(schema: &'a TableSchema, m: &TestMessage) -> PatchUpdate<&'a TableSchema> {
    PatchUpdate::from(schema)
        .set(0, Value::Blob(m.id.as_bytes().to_vec()))
        .unwrap()
        .set(3, Value::Text(m.update_body.clone()))
        .unwrap()
}

impl Format for Patchset {
    fn name(&self) -> &'static str {
        "Patchset"
    }

    fn batch_mixed(
        &self,
        inserts: &[TestMessage],
        updates: &[TestMessage],
        deletes: &[TestMessage],
    ) -> Vec<u8> {
        let schema = messages_schema();
        let mut pset = PatchSet::<&TableSchema>::new();
        for m in inserts {
            pset = pset.insert(build_insert(&schema, m));
        }
        for m in updates {
            pset = pset.update(build_update(&schema, m));
        }
        for m in deletes {
            pset = pset.delete(&&schema, &[Value::Blob(m.id.as_bytes().to_vec())]);
        }
        Vec::<u8>::from(pset)
    }
}
