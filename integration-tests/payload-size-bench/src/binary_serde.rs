//! Serde structs shared by CBOR and MessagePack — both support raw byte-string
//! IDs natively.

use serde::Serialize;

use crate::common::TestMessage;

// ---------------------------------------------------------------------------
// Custom serialization: Vec<u8> as byte-string (not array-of-integers)
// ---------------------------------------------------------------------------

pub mod as_bytes {
    use serde::Serializer;

    pub fn serialize<S: Serializer>(bytes: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(bytes)
    }
}

// ---------------------------------------------------------------------------
// Structs with raw byte IDs
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct InsertMsg {
    #[serde(with = "as_bytes")]
    pub id: Vec<u8>,
    #[serde(with = "as_bytes")]
    pub sender_id: Vec<u8>,
    #[serde(with = "as_bytes")]
    pub receiver_id: Vec<u8>,
    pub body: String,
    pub created_at: String,
}

#[derive(Serialize)]
pub struct UpdateMsg {
    #[serde(with = "as_bytes")]
    pub id: Vec<u8>,
    pub body: String,
}

#[derive(Serialize)]
pub struct DeleteMsg {
    #[serde(with = "as_bytes")]
    pub id: Vec<u8>,
}

#[derive(Serialize)]
#[serde(tag = "op")]
pub enum Op {
    #[serde(rename = "insert")]
    Insert(InsertMsg),
    #[serde(rename = "update")]
    Update(UpdateMsg),
    #[serde(rename = "delete")]
    Delete(DeleteMsg),
}

// ---------------------------------------------------------------------------
// Constructors from TestMessage
// ---------------------------------------------------------------------------

pub fn insert_op(m: &TestMessage) -> Op {
    Op::Insert(InsertMsg {
        id: m.id.as_bytes().to_vec(),
        sender_id: m.sender.as_bytes().to_vec(),
        receiver_id: m.receiver.as_bytes().to_vec(),
        body: m.body.to_string(),
        created_at: m.created_at.to_string(),
    })
}

pub fn update_op(m: &TestMessage) -> Op {
    Op::Update(UpdateMsg {
        id: m.id.as_bytes().to_vec(),
        body: m.update_body.clone(),
    })
}

pub fn delete_op(m: &TestMessage) -> Op {
    Op::Delete(DeleteMsg {
        id: m.id.as_bytes().to_vec(),
    })
}

/// Collect all operations into a single `Vec<Op>`.
///
/// Used by CBOR and MsgPack batch encoding.
pub fn collect_ops(
    inserts: &[TestMessage],
    updates: &[TestMessage],
    deletes: &[TestMessage],
) -> Vec<Op> {
    inserts
        .iter()
        .map(insert_op)
        .chain(updates.iter().map(update_op))
        .chain(deletes.iter().map(delete_op))
        .collect()
}
