//! Test utilities and helpers for pg_walstream integration tests.
//!
//! Verifies the conversion from pg_walstream events to sqlite-diff-rs
//! changeset operations. pg_walstream itself uses libpq directly for
//! replication connections, but these tests cover only the conversion path.

use pg_walstream::{ChangeEvent, ColumnValue, EventType, Lsn, ReplicaIdentity, RowData};
use std::collections::HashMap;
use std::sync::Arc;

/// Convert a `serde_json::Value` to a PostgreSQL-style wire-text `ColumnValue`
/// so the resulting event mirrors what `pg_walstream` produces from real WAL
/// data. Numbers become their decimal text form, booleans become `t`/`f`, and
/// nulls become [`ColumnValue::Null`].
fn json_to_column_value(v: &serde_json::Value) -> ColumnValue {
    match v {
        serde_json::Value::Null => ColumnValue::Null,
        serde_json::Value::Bool(b) => ColumnValue::text(if *b { "t" } else { "f" }),
        serde_json::Value::Number(n) => ColumnValue::text(&n.to_string()),
        serde_json::Value::String(s) => ColumnValue::text(s),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            ColumnValue::text(&v.to_string())
        }
    }
}

/// Build a `RowData` from a `HashMap<String, serde_json::Value>`.
fn to_row(map: HashMap<String, serde_json::Value>) -> RowData {
    let mut row = RowData::with_capacity(map.len());
    for (k, v) in map {
        row.push(Arc::from(k.as_str()), json_to_column_value(&v));
    }
    row
}

fn to_key_columns(cols: Vec<String>) -> Vec<Arc<str>> {
    cols.into_iter().map(|s| Arc::from(s.as_str())).collect()
}

/// Create a test Insert event.
pub fn make_insert_event(
    schema: &str,
    table: &str,
    data: HashMap<String, serde_json::Value>,
) -> EventType {
    EventType::Insert {
        schema: Arc::from(schema),
        table: Arc::from(table),
        relation_oid: 12345,
        data: to_row(data),
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
        schema: Arc::from(schema),
        table: Arc::from(table),
        relation_oid: 12345,
        old_data: old_data.map(to_row),
        new_data: to_row(new_data),
        replica_identity: ReplicaIdentity::Default,
        key_columns: to_key_columns(key_columns),
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
        schema: Arc::from(schema),
        table: Arc::from(table),
        relation_oid: 12345,
        old_data: to_row(old_data),
        replica_identity: ReplicaIdentity::Default,
        key_columns: to_key_columns(key_columns),
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
