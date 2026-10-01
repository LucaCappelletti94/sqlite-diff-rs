//! Protocol Buffers format using prost (pure Rust, no protoc needed).
//!
//! Uses prost derive macros to define messages directly in Rust.
//! Equivalent .proto:
//! ```proto
//! message Insert { bytes id = 1; bytes sender_id = 2; bytes receiver_id = 3; string body = 4; string created_at = 5; }
//! message Update { bytes id = 1; string body = 2; }
//! message Delete { bytes id = 1; }
//! message OpBatch { repeated Insert inserts = 1; repeated Update updates = 2; repeated Delete deletes = 3; }
//! ```

use prost::Message;

use crate::common::{Format, TestMessage};

// ---------------------------------------------------------------------------
// Protobuf message definitions (pure Rust, no .proto file needed)
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Message)]
pub struct Insert {
    #[prost(bytes = "vec", tag = "1")]
    pub id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub sender_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub receiver_id: Vec<u8>,
    #[prost(string, tag = "4")]
    pub body: String,
    #[prost(string, tag = "5")]
    pub created_at: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Update {
    #[prost(bytes = "vec", tag = "1")]
    pub id: Vec<u8>,
    #[prost(string, tag = "2")]
    pub body: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Delete {
    #[prost(bytes = "vec", tag = "1")]
    pub id: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct OpBatch {
    #[prost(message, repeated, tag = "1")]
    pub inserts: Vec<Insert>,
    #[prost(message, repeated, tag = "2")]
    pub updates: Vec<Update>,
    #[prost(message, repeated, tag = "3")]
    pub deletes: Vec<Delete>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_insert(m: &TestMessage) -> Insert {
    Insert {
        id: m.id.as_bytes().to_vec(),
        sender_id: m.sender.as_bytes().to_vec(),
        receiver_id: m.receiver.as_bytes().to_vec(),
        body: m.body.to_string(),
        created_at: m.created_at.to_string(),
    }
}

fn make_update(m: &TestMessage) -> Update {
    Update {
        id: m.id.as_bytes().to_vec(),
        body: m.update_body.clone(),
    }
}

fn make_delete(m: &TestMessage) -> Delete {
    Delete {
        id: m.id.as_bytes().to_vec(),
    }
}

// ---------------------------------------------------------------------------
// Format impl
// ---------------------------------------------------------------------------

pub struct Protobuf;

impl Format for Protobuf {
    fn name(&self) -> &'static str {
        "Protobuf"
    }

    fn batch_mixed(
        &self,
        inserts: &[TestMessage],
        updates: &[TestMessage],
        deletes: &[TestMessage],
    ) -> Vec<u8> {
        let batch = OpBatch {
            inserts: inserts.iter().map(make_insert).collect(),
            updates: updates.iter().map(make_update).collect(),
            deletes: deletes.iter().map(make_delete).collect(),
        };
        batch.encode_to_vec()
    }
}
