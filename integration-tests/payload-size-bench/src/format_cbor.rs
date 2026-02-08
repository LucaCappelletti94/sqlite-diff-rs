//! CBOR format — `ciborium` with native byte-string IDs.

use crate::binary_serde;
use crate::common::{Format, TestMessage};

fn cbor_vec(v: &impl serde::Serialize) -> Vec<u8> {
    let mut buf = Vec::new();
    ciborium::into_writer(v, &mut buf).unwrap();
    buf
}

pub struct Cbor;

impl Format for Cbor {
    fn name(&self) -> &'static str {
        "CBOR"
    }

    fn batch_mixed(
        &self,
        inserts: &[TestMessage],
        updates: &[TestMessage],
        deletes: &[TestMessage],
    ) -> Vec<u8> {
        cbor_vec(&binary_serde::collect_ops(inserts, updates, deletes))
    }
}
