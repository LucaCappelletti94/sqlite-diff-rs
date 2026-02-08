//! MessagePack format — `rmp-serde` with native byte-string IDs.
//!
//! Reuses the same `binary_serde` structs as CBOR — both formats support
//! native byte-strings via `serde::Serializer::serialize_bytes`.

use crate::binary_serde;
use crate::common::{Format, TestMessage};

pub struct MsgPack;

impl Format for MsgPack {
    fn name(&self) -> &'static str {
        "MsgPack"
    }

    fn batch_mixed(
        &self,
        inserts: &[TestMessage],
        updates: &[TestMessage],
        deletes: &[TestMessage],
    ) -> Vec<u8> {
        rmp_serde::to_vec_named(&binary_serde::collect_ops(inserts, updates, deletes)).unwrap()
    }
}
