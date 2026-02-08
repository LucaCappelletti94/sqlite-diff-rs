//! SQL format — plain-text INSERT/UPDATE/DELETE statements.
//!
//! UUIDs are encoded as hex blob literals `X'...'` (SQL has no raw binary literal).

use crate::common::{Format, TestMessage, hex};

pub struct Sql;

impl Format for Sql {
    fn name(&self) -> &'static str {
        "SQL"
    }

    fn batch_mixed(
        &self,
        inserts: &[TestMessage],
        updates: &[TestMessage],
        deletes: &[TestMessage],
    ) -> Vec<u8> {
        let mut parts: Vec<String> =
            Vec::with_capacity(inserts.len() + updates.len() + deletes.len());
        for m in inserts {
            parts.push(encode_insert(m));
        }
        for m in updates {
            parts.push(encode_update(m));
        }
        for m in deletes {
            parts.push(encode_delete(m));
        }
        parts.join("\n").into_bytes()
    }
}

fn encode_insert(m: &TestMessage) -> String {
    format!(
        "INSERT INTO messages (id, sender_id, receiver_id, body, created_at) VALUES (X'{}', X'{}', X'{}', '{}', '{}');",
        hex(m.id.as_bytes()),
        hex(m.sender.as_bytes()),
        hex(m.receiver.as_bytes()),
        m.body.replace('\'', "''"),
        m.created_at,
    )
}

fn encode_update(m: &TestMessage) -> String {
    format!(
        "UPDATE messages SET body = '{}' WHERE id = X'{}';",
        m.update_body.replace('\'', "''"),
        hex(m.id.as_bytes()),
    )
}

fn encode_delete(m: &TestMessage) -> String {
    format!(
        "DELETE FROM messages WHERE id = X'{}';",
        hex(m.id.as_bytes()),
    )
}
