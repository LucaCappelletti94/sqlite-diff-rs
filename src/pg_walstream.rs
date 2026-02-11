//! `pg_walstream` message conversion to `SQLite` changeset operations.
//!
//! This module provides conversions from [pg_walstream](https://github.com/isdaniel/pg-walstream)
//! logical replication events to `SQLite` changeset operations compatible with this crate's builders.
//!
//! `pg_walstream` is a `PostgreSQL` logical replication protocol client that parses the native
//! binary replication protocol, providing high-performance CDC (Change Data Capture) events.
//!
//! All conversions take ownership of the event and table to avoid cloning.
//!
//! # Example
//!
//! ```ignore
//! use sqlite_diff_rs::pg_walstream::EventType;
//! use sqlite_diff_rs::{Insert, SimpleTable};
//! use std::collections::HashMap;
//!
//! let table = SimpleTable::new("users", &["id", "name"], &[0]);
//!
//! let mut data = HashMap::new();
//! data.insert("id".to_string(), serde_json::json!(1));
//! data.insert("name".to_string(), serde_json::json!("Alice"));
//!
//! let event = EventType::Insert {
//!     schema: "public".to_string(),
//!     table: "users".to_string(),
//!     relation_oid: 12345,
//!     data,
//! };
//!
//! // Takes ownership of event and table (no cloning)
//! let insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
//! ```

use alloc::string::String;
use alloc::vec::Vec;

// Re-export key types from pg_walstream for convenience
pub use pg_walstream::{ChangeEvent, EventType, Lsn, ReplicaIdentity};

use crate::ChangesetFormat;
use crate::builders::{ChangeDelete, Insert, PatchDelete, Update};
use crate::encoding::Value;
use crate::schema::NamedColumns;

/// Errors during `pg_walstream` to changeset conversion.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConversionError {
    /// A column name from the event was not found in the table schema.
    #[error("Column '{0}' not found in table schema")]
    ColumnNotFound(String),

    /// The table name in the event doesn't match the expected schema.
    #[error("Table name mismatch: expected '{expected}', got '{actual}'")]
    TableMismatch {
        /// Expected table name from the schema.
        expected: String,
        /// Actual table name from the event.
        actual: String,
    },

    /// The event is missing required data.
    #[error("Missing data in event")]
    MissingData,

    /// A JSON value type is not supported for conversion.
    #[error("Unsupported JSON value type for column '{0}'")]
    UnsupportedType(String),

    /// The event type is not applicable for the requested conversion.
    #[error("Event type '{0}' cannot be converted to the requested operation")]
    InvalidEventType(String),

    /// Old data is required but not available (replica identity issue).
    #[error("Old data not available (check replica identity setting)")]
    MissingOldData,
}

/// Convert an owned `serde_json` Value to our Value type (zero-copy for strings).
fn json_to_value_owned(
    json: serde_json::Value,
    column_name: &str,
) -> Result<Value<String, Vec<u8>>, ConversionError> {
    match json {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Integer(i64::from(b))),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Real(f))
            } else {
                Err(ConversionError::UnsupportedType(column_name.into()))
            }
        }
        serde_json::Value::String(s) => Ok(Value::Text(s)),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Err(ConversionError::UnsupportedType(column_name.into()))
        }
    }
}

/// Convert a borrowed `serde_json` Value to our Value type.
fn json_to_value(
    json: &serde_json::Value,
    column_name: &str,
) -> Result<Value<String, Vec<u8>>, ConversionError> {
    match json {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Integer(i64::from(*b))),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Real(f))
            } else {
                Err(ConversionError::UnsupportedType(column_name.into()))
            }
        }
        serde_json::Value::String(s) => Ok(Value::Text(s.clone())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Err(ConversionError::UnsupportedType(column_name.into()))
        }
    }
}

/// Convert an `EventType::Insert` to an Insert operation (takes ownership).
impl<T: NamedColumns> TryFrom<(EventType, T)> for Insert<T, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((event, table): (EventType, T)) -> Result<Self, Self::Error> {
        let EventType::Insert {
            table: event_table,
            data,
            ..
        } = event
        else {
            return Err(ConversionError::InvalidEventType("not an Insert".into()));
        };

        // Verify table name matches
        if table.name() != event_table {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: event_table,
            });
        }

        let mut insert = Insert::from(table);

        // Map each column from the event data to the table schema
        for (name, value) in data {
            let col_idx = insert
                .as_ref()
                .column_index(&name)
                .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;

            let converted = json_to_value_owned(value, &name)?;
            insert = insert
                .set(col_idx, converted)
                .map_err(|_| ConversionError::ColumnNotFound(name))?;
        }

        Ok(insert)
    }
}

/// Convert an `EventType::Update` to an Update operation (changeset format, takes ownership).
impl<T: NamedColumns> TryFrom<(EventType, T)> for Update<T, ChangesetFormat, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((event, table): (EventType, T)) -> Result<Self, Self::Error> {
        let EventType::Update {
            table: event_table,
            old_data,
            new_data,
            ..
        } = event
        else {
            return Err(ConversionError::InvalidEventType("not an Update".into()));
        };

        // Verify table name matches
        if table.name() != event_table {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: event_table,
            });
        }

        let mut update: Update<T, ChangesetFormat, String, Vec<u8>> = Update::from(table);

        // Set values - if we have old_data, use set() with both old and new
        // If we only have new_data, use set_new()
        for (name, new_value) in new_data {
            let col_idx = update
                .as_ref()
                .column_index(&name)
                .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;

            // Check if we have old data for this column
            if let Some(ref old) = old_data
                && let Some(old_value) = old.get(&name)
            {
                let old_converted = json_to_value(old_value, &name)?;
                let new_converted = json_to_value_owned(new_value, &name)?;
                update = update
                    .set(col_idx, old_converted, new_converted)
                    .map_err(|_| ConversionError::ColumnNotFound(name))?;
                continue;
            }

            // No old data available, just set new
            let new_converted = json_to_value_owned(new_value, &name)?;
            update = update
                .set_new(col_idx, new_converted)
                .map_err(|_| ConversionError::ColumnNotFound(name))?;
        }

        Ok(update)
    }
}

/// Convert an `EventType::Delete` to a `ChangeDelete` operation (takes ownership).
impl<T: NamedColumns> TryFrom<(EventType, T)> for ChangeDelete<T, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((event, table): (EventType, T)) -> Result<Self, Self::Error> {
        let EventType::Delete {
            table: event_table,
            old_data,
            ..
        } = event
        else {
            return Err(ConversionError::InvalidEventType("not a Delete".into()));
        };

        // Verify table name matches
        if table.name() != event_table {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: event_table,
            });
        }

        let mut delete = ChangeDelete::from(table);

        // Set old values from old_data
        for (name, value) in old_data {
            let col_idx = delete
                .as_ref()
                .column_index(&name)
                .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;

            let converted = json_to_value_owned(value, &name)?;
            delete = delete
                .set(col_idx, converted)
                .map_err(|_| ConversionError::ColumnNotFound(name))?;
        }

        Ok(delete)
    }
}

/// Convert an `EventType::Delete` to a `PatchDelete` operation (takes ownership).
impl<T: NamedColumns> TryFrom<(EventType, T)> for PatchDelete<T, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((event, table): (EventType, T)) -> Result<Self, Self::Error> {
        let EventType::Delete {
            table: event_table,
            mut old_data,
            key_columns,
            ..
        } = event
        else {
            return Err(ConversionError::InvalidEventType("not a Delete".into()));
        };

        // Verify table name matches
        if table.name() != event_table {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: event_table,
            });
        }

        // Extract primary key values in schema order
        let num_pks = table.number_of_primary_keys();
        let mut pk_values: Vec<Option<Value<String, Vec<u8>>>> = alloc::vec![None; num_pks];

        // Use key_columns to identify which columns are PKs, get values from old_data
        for key_name in key_columns {
            let col_idx = table
                .column_index(&key_name)
                .ok_or_else(|| ConversionError::ColumnNotFound(key_name.clone()))?;

            if let Some(pk_idx) = table.primary_key_index(col_idx) {
                // Remove from old_data to take ownership
                let value = old_data
                    .remove(&key_name)
                    .ok_or(ConversionError::MissingData)?;
                let converted = json_to_value_owned(value, &key_name)?;
                pk_values[pk_idx] = Some(converted);
            }
        }

        // Verify all PKs are present and collect them
        let pk: Vec<Value<String, Vec<u8>>> = pk_values
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(ConversionError::MissingData)?;

        Ok(PatchDelete::new(table, pk))
    }
}

/// Convert a `ChangeEvent` to an Insert (if the event type is Insert, takes ownership).
impl<T: NamedColumns> TryFrom<(ChangeEvent, T)> for Insert<T, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((event, table): (ChangeEvent, T)) -> Result<Self, Self::Error> {
        (event.event_type, table).try_into()
    }
}

/// Convert a `ChangeEvent` to an Update (if the event type is Update, takes ownership).
impl<T: NamedColumns> TryFrom<(ChangeEvent, T)> for Update<T, ChangesetFormat, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((event, table): (ChangeEvent, T)) -> Result<Self, Self::Error> {
        (event.event_type, table).try_into()
    }
}

/// Convert a `ChangeEvent` to a `ChangeDelete` (if the event type is Delete, takes ownership).
impl<T: NamedColumns> TryFrom<(ChangeEvent, T)> for ChangeDelete<T, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((event, table): (ChangeEvent, T)) -> Result<Self, Self::Error> {
        (event.event_type, table).try_into()
    }
}

/// Convert a `ChangeEvent` to a `PatchDelete` (if the event type is Delete, takes ownership).
impl<T: NamedColumns> TryFrom<(ChangeEvent, T)> for PatchDelete<T, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((event, table): (ChangeEvent, T)) -> Result<Self, Self::Error> {
        (event.event_type, table).try_into()
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::*;
    use crate::SimpleTable;
    use alloc::vec;
    use std::collections::HashMap;

    fn make_insert_event(table: &str, data: HashMap<String, serde_json::Value>) -> EventType {
        EventType::Insert {
            schema: "public".into(),
            table: table.into(),
            relation_oid: 12345,
            data,
        }
    }

    fn make_update_event(
        table: &str,
        old_data: Option<HashMap<String, serde_json::Value>>,
        new_data: HashMap<String, serde_json::Value>,
        key_columns: Vec<String>,
    ) -> EventType {
        EventType::Update {
            schema: "public".into(),
            table: table.into(),
            relation_oid: 12345,
            old_data,
            new_data,
            replica_identity: ReplicaIdentity::Default,
            key_columns,
        }
    }

    fn make_delete_event(
        table: &str,
        old_data: HashMap<String, serde_json::Value>,
        key_columns: Vec<String>,
    ) -> EventType {
        EventType::Delete {
            schema: "public".into(),
            table: table.into(),
            relation_oid: 12345,
            old_data,
            replica_identity: ReplicaIdentity::Default,
            key_columns,
        }
    }

    #[test]
    fn test_insert_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let mut data = HashMap::new();
        data.insert("id".into(), serde_json::json!(1));
        data.insert("name".into(), serde_json::json!("Alice"));

        let event = make_insert_event("users", data);
        let insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();

        let values = insert.into_values();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Value::Integer(1));
        assert_eq!(values[1], Value::Text("Alice".into()));
    }

    #[test]
    fn test_update_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let mut old_data = HashMap::new();
        old_data.insert("id".into(), serde_json::json!(1));
        old_data.insert("name".into(), serde_json::json!("Alice"));

        let mut new_data = HashMap::new();
        new_data.insert("id".into(), serde_json::json!(1));
        new_data.insert("name".into(), serde_json::json!("Bob"));

        let event = make_update_event("users", Some(old_data), new_data, vec!["id".into()]);
        let _update: Update<_, ChangesetFormat, String, Vec<u8>> =
            (event, table).try_into().unwrap();
    }

    #[test]
    fn test_delete_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let mut old_data = HashMap::new();
        old_data.insert("id".into(), serde_json::json!(42));
        old_data.insert("name".into(), serde_json::json!("Alice"));

        let event = make_delete_event("users", old_data, vec!["id".into()]);
        let delete: ChangeDelete<_, String, Vec<u8>> = (event, table).try_into().unwrap();

        let values = delete.into_values();
        assert_eq!(values[0], Value::Integer(42));
    }

    #[test]
    fn test_patch_delete_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let mut old_data = HashMap::new();
        old_data.insert("id".into(), serde_json::json!(42));

        let event = make_delete_event("users", old_data, vec!["id".into()]);
        let _delete: PatchDelete<_, String, Vec<u8>> = (event, table).try_into().unwrap();
    }

    #[test]
    fn test_table_mismatch() {
        let table = SimpleTable::new("products", &["id", "name"], &[0]);

        let mut data = HashMap::new();
        data.insert("id".into(), serde_json::json!(1));

        let event = make_insert_event("users", data);
        let result: Result<Insert<_, String, Vec<u8>>, _> = (event, table).try_into();

        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_column_not_found() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let mut data = HashMap::new();
        data.insert("id".into(), serde_json::json!(1));
        data.insert("unknown".into(), serde_json::json!("test"));

        let event = make_insert_event("users", data);
        let result: Result<Insert<_, String, Vec<u8>>, _> = (event, table).try_into();

        assert!(matches!(result, Err(ConversionError::ColumnNotFound(_))));
    }

    #[test]
    fn test_invalid_event_type() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let mut data = HashMap::new();
        data.insert("id".into(), serde_json::json!(1));

        // Try to convert an Insert event to a Delete
        let event = make_insert_event("users", data);
        let result: Result<ChangeDelete<_, String, Vec<u8>>, _> = (event, table).try_into();

        assert!(matches!(result, Err(ConversionError::InvalidEventType(_))));
    }

    #[test]
    fn test_null_value() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let mut data = HashMap::new();
        data.insert("id".into(), serde_json::json!(1));
        data.insert("name".into(), serde_json::Value::Null);

        let event = make_insert_event("users", data);
        let insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();

        let values = insert.into_values();
        assert_eq!(values[1], Value::Null);
    }

    #[test]
    fn test_bool_value() {
        let table = SimpleTable::new("flags", &["id", "active"], &[0]);

        let mut data = HashMap::new();
        data.insert("id".into(), serde_json::json!(1));
        data.insert("active".into(), serde_json::json!(true));

        let event = make_insert_event("flags", data);
        let insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();

        let values = insert.into_values();
        assert_eq!(values[1], Value::Integer(1));
    }

    #[test]
    fn test_float_value() {
        let table = SimpleTable::new("prices", &["id", "amount"], &[0]);

        let mut data = HashMap::new();
        data.insert("id".into(), serde_json::json!(1));
        data.insert("amount".into(), serde_json::json!(99.99));

        let event = make_insert_event("prices", data);
        let insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();

        let values = insert.into_values();
        assert_eq!(values[1], Value::Real(99.99));
    }

    #[test]
    fn test_unsupported_array_type() {
        let table = SimpleTable::new("data", &["id", "tags"], &[0]);

        let mut data = HashMap::new();
        data.insert("id".into(), serde_json::json!(1));
        data.insert("tags".into(), serde_json::json!([1, 2, 3]));

        let event = make_insert_event("data", data);
        let result: Result<Insert<_, String, Vec<u8>>, _> = (event, table).try_into();

        assert!(matches!(result, Err(ConversionError::UnsupportedType(_))));
    }

    #[test]
    fn test_change_event_wrapper() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let mut data = HashMap::new();
        data.insert("id".into(), serde_json::json!(1));
        data.insert("name".into(), serde_json::json!("Alice"));

        let change_event = ChangeEvent {
            event_type: make_insert_event("users", data),
            lsn: Lsn::from(0x1234_5678),
            metadata: None,
        };

        let insert: Insert<_, String, Vec<u8>> = (change_event, table).try_into().unwrap();

        let values = insert.into_values();
        assert_eq!(values[0], Value::Integer(1));
    }
}
