//! Test utilities and helpers for pg_walstream integration tests.
//!
//! This crate provides utilities for testing pg_walstream event conversion
//! to sqlite-diff-rs changeset operations.
//!
//! Note: pg_walstream uses libpq directly for replication connections.
//! These tests focus on verifying the conversion from pg_walstream events
//! to sqlite-diff-rs changeset operations.

use pg_walstream::{ChangeEvent, EventType, Lsn, ReplicaIdentity};
use std::collections::HashMap;

/// Create a test Insert event.
pub fn make_insert_event(
    schema: &str,
    table: &str,
    data: HashMap<String, serde_json::Value>,
) -> EventType {
    EventType::Insert {
        schema: schema.into(),
        table: table.into(),
        relation_oid: 12345,
        data,
    }
}

/// Create a test Update event.
pub fn make_update_event(
    schema: &str,
    table: &str,
    old_data: Option<HashMap<String, serde_json::Value>>,
    new_data: HashMap<String, serde_json::Value>,
    key_columns: Vec<String>,
) -> EventType {
    EventType::Update {
        schema: schema.into(),
        table: table.into(),
        relation_oid: 12345,
        old_data,
        new_data,
        replica_identity: ReplicaIdentity::Default,
        key_columns,
    }
}

/// Create a test Delete event.
pub fn make_delete_event(
    schema: &str,
    table: &str,
    old_data: HashMap<String, serde_json::Value>,
    key_columns: Vec<String>,
) -> EventType {
    EventType::Delete {
        schema: schema.into(),
        table: table.into(),
        relation_oid: 12345,
        old_data,
        replica_identity: ReplicaIdentity::Default,
        key_columns,
    }
}

/// Wrap an EventType in a ChangeEvent with a given LSN.
pub fn wrap_in_change_event(event_type: EventType, lsn: u64) -> ChangeEvent {
    ChangeEvent {
        event_type,
        lsn: Lsn::from(lsn),
        metadata: None,
    }
}

/// Create a HashMap from key-value pairs for convenience.
#[macro_export]
macro_rules! hashmap {
    ($($key:expr => $value:expr),* $(,)?) => {{
        let mut map = std::collections::HashMap::new();
        $(map.insert($key.into(), $value);)*
        map
    }};
}
