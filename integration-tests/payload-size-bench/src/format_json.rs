//! JSON format — `serde_json` with tagged `Op` enum.
//!
//! UUIDs are hex-encoded strings (JSON has no binary type).

use serde::Serialize;

use crate::common::{Format, TestMessage, hex};

// ---------------------------------------------------------------------------
// Serde structs (hex-string IDs)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct InsertMsg {
    id: String,
    sender_id: String,
    receiver_id: String,
    body: String,
    created_at: String,
}

#[derive(Serialize)]
struct UpdateMsg {
    id: String,
    body: String,
}

#[derive(Serialize)]
struct DeleteMsg {
    id: String,
}

#[derive(Serialize)]
#[serde(tag = "op")]
enum Op {
    #[serde(rename = "insert")]
    Insert(InsertMsg),
    #[serde(rename = "update")]
    Update(UpdateMsg),
    #[serde(rename = "delete")]
    Delete(DeleteMsg),
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

fn insert_op(m: &TestMessage) -> Op {
    Op::Insert(InsertMsg {
        id: hex(m.id.as_bytes()),
        sender_id: hex(m.sender.as_bytes()),
        receiver_id: hex(m.receiver.as_bytes()),
        body: m.body.to_string(),
        created_at: m.created_at.to_string(),
    })
}

fn update_op(m: &TestMessage) -> Op {
    Op::Update(UpdateMsg {
        id: hex(m.id.as_bytes()),
        body: m.update_body.clone(),
    })
}

fn delete_op(m: &TestMessage) -> Op {
    Op::Delete(DeleteMsg {
        id: hex(m.id.as_bytes()),
    })
}

// ---------------------------------------------------------------------------
// Format impl
// ---------------------------------------------------------------------------

pub struct Json;

impl Format for Json {
    fn name(&self) -> &'static str {
        "JSON"
    }

    fn batch_mixed(
        &self,
        inserts: &[TestMessage],
        updates: &[TestMessage],
        deletes: &[TestMessage],
    ) -> Vec<u8> {
        let mut ops: Vec<Op> = Vec::with_capacity(inserts.len() + updates.len() + deletes.len());
        for m in inserts {
            ops.push(insert_op(m));
        }
        for m in updates {
            ops.push(update_op(m));
        }
        for m in deletes {
            ops.push(delete_op(m));
        }
        serde_json::to_vec(&ops).unwrap()
    }
}
