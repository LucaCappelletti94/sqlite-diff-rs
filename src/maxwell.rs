//! Maxwell message parsing and conversion to `SQLite` changeset operations.
//!
//! This module provides types for deserializing [Maxwell](https://maxwells-daemon.io/)
//! Change Data Capture (CDC) events and converting them to `SQLite` changeset
//! operations compatible with this crate's builders.
//!
//! Maxwell is a `MySQL` CDC application that reads binlogs and outputs JSON
//! messages representing row-level changes.
//!
//! # Example
//!
//! ```
//! use sqlite_diff_rs::maxwell::{parse, Message, OpType};
//!
//! let json = r#"{"database":"mydb","table":"users","type":"insert","ts":1477053217,"data":{"id":1,"name":"Alice"}}"#;
//! let message = parse(json).unwrap();
//!
//! assert_eq!(message.op_type, OpType::Insert);
//! assert_eq!(message.table, "users");
//! ```

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use serde::{Deserialize, Serialize};

/// Maxwell operation type.
///
/// Represents the type of database operation captured by Maxwell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpType {
    /// INSERT operation.
    Insert,
    /// UPDATE operation.
    Update,
    /// DELETE operation.
    Delete,
}

/// Maxwell CDC message.
///
/// Represents a single row-level change captured from `MySQL` binlog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Database name.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Operation type (insert, update, delete).
    #[serde(rename = "type")]
    pub op_type: OpType,
    /// Unix timestamp (seconds) when the change occurred.
    #[serde(default)]
    pub ts: Option<i64>,
    /// Transaction ID.
    #[serde(default)]
    pub xid: Option<i64>,
    /// Whether this is the final row in the transaction.
    #[serde(default)]
    pub commit: Option<bool>,
    /// Binlog position (e.g., "master.000006:800911").
    #[serde(default)]
    pub position: Option<String>,
    /// `MySQL` server ID.
    #[serde(default)]
    pub server_id: Option<i64>,
    /// `MySQL` thread ID.
    #[serde(default)]
    pub thread_id: Option<i64>,
    /// Primary key values.
    #[serde(default)]
    pub primary_key: Option<Vec<serde_json::Value>>,
    /// Primary key column names.
    #[serde(default)]
    pub primary_key_columns: Option<Vec<String>>,
    /// Current row data (new values for insert/update, deleted values for delete).
    pub data: BTreeMap<String, serde_json::Value>,
    /// Previous values for changed columns (update only).
    #[serde(default)]
    pub old: Option<BTreeMap<String, serde_json::Value>>,
}

/// Parse a Maxwell message from JSON.
///
/// # Errors
///
/// Returns a [`serde_json::Error`] if the JSON is malformed.
///
/// # Example
///
/// ```
/// use sqlite_diff_rs::maxwell::{parse, OpType};
///
/// let json = r#"{"database":"test","table":"users","type":"insert","data":{"id":1}}"#;
/// let msg = parse(json).unwrap();
/// assert_eq!(msg.op_type, OpType::Insert);
/// ```
pub fn parse(json: &str) -> Result<Message, serde_json::Error> {
    serde_json::from_str(json)
}

/// Errors during Maxwell to changeset conversion.
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
        /// Actual table name from the Maxwell message.
        actual: String,
    },

    /// The event is missing required data for the operation.
    #[error("Missing {0} data for {1} operation")]
    MissingData(&'static str, &'static str),

    /// A JSON value type is not supported for conversion.
    #[error("Unsupported JSON value type for column '{0}'")]
    UnsupportedType(String),

    /// The operation type is not applicable for the requested conversion.
    #[error("Operation '{0}' cannot be converted to the requested type")]
    InvalidOperation(String),
}

use crate::ChangesetFormat;
use crate::builders::{ChangeDelete, Insert, PatchDelete, Update};
use crate::encoding::Value;
use crate::schema::NamedColumns;
use alloc::format;

/// Convert a `serde_json` Value to our Value type.
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

// ============================================================================
// TryFrom implementations for Message
// ============================================================================

impl<T: NamedColumns + Clone> TryFrom<(&Message, &T)> for Insert<T, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((message, table): (&Message, &T)) -> Result<Self, Self::Error> {
        // Verify operation type
        if message.op_type != OpType::Insert {
            return Err(ConversionError::InvalidOperation(format!(
                "{:?}",
                message.op_type
            )));
        }

        // Verify table name
        if table.name() != message.table {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: message.table.clone(),
            });
        }

        // Build insert from data
        let mut insert = Insert::from(table.clone());

        for (name, value) in &message.data {
            let col_idx = table
                .column_index(name)
                .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;

            let converted = json_to_value(value, name)?;
            insert = insert
                .set(col_idx, converted)
                .map_err(|_| ConversionError::ColumnNotFound(name.clone()))?;
        }

        Ok(insert)
    }
}

impl<T: NamedColumns + Clone> TryFrom<(&Message, &T)>
    for Update<T, ChangesetFormat, String, Vec<u8>>
{
    type Error = ConversionError;

    fn try_from((message, table): (&Message, &T)) -> Result<Self, Self::Error> {
        // Verify operation type
        if message.op_type != OpType::Update {
            return Err(ConversionError::InvalidOperation(format!(
                "{:?}",
                message.op_type
            )));
        }

        // Verify table name
        if table.name() != message.table {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: message.table.clone(),
            });
        }

        let mut update: Update<T, ChangesetFormat, String, Vec<u8>> = Update::from(table.clone());

        // Process all columns from data (new values)
        for (name, new_value) in &message.data {
            let col_idx = table
                .column_index(name)
                .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;

            let new_converted = json_to_value(new_value, name)?;

            // Check if we have old value for this column
            if let Some(ref old_map) = message.old
                && let Some(old_value) = old_map.get(name)
            {
                let old_converted = json_to_value(old_value, name)?;
                update = update
                    .set(col_idx, old_converted, new_converted)
                    .map_err(|_| ConversionError::ColumnNotFound(name.clone()))?;
                continue;
            }

            // No old value - column unchanged, use same value for old and new
            update = update
                .set(col_idx, new_converted.clone(), new_converted)
                .map_err(|_| ConversionError::ColumnNotFound(name.clone()))?;
        }

        Ok(update)
    }
}

impl<T: NamedColumns + Clone> TryFrom<(&Message, &T)> for ChangeDelete<T, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((message, table): (&Message, &T)) -> Result<Self, Self::Error> {
        // Verify operation type
        if message.op_type != OpType::Delete {
            return Err(ConversionError::InvalidOperation(format!(
                "{:?}",
                message.op_type
            )));
        }

        // Verify table name
        if table.name() != message.table {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: message.table.clone(),
            });
        }

        // Build delete from data (which contains the deleted row)
        let mut delete = ChangeDelete::from(table.clone());

        for (name, value) in &message.data {
            let col_idx = table
                .column_index(name)
                .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;

            let converted = json_to_value(value, name)?;
            delete = delete
                .set(col_idx, converted)
                .map_err(|_| ConversionError::ColumnNotFound(name.clone()))?;
        }

        Ok(delete)
    }
}

impl<T: NamedColumns + Clone> TryFrom<(&Message, &T)> for PatchDelete<T, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((message, table): (&Message, &T)) -> Result<Self, Self::Error> {
        // Verify operation type
        if message.op_type != OpType::Delete {
            return Err(ConversionError::InvalidOperation(format!(
                "{:?}",
                message.op_type
            )));
        }

        // Verify table name
        if table.name() != message.table {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: message.table.clone(),
            });
        }

        // Extract primary key values in schema order
        let num_pks = table.number_of_primary_keys();
        let mut pk_values: Vec<Option<Value<String, Vec<u8>>>> = alloc::vec![None; num_pks];

        for (name, value) in &message.data {
            let col_idx = table
                .column_index(name)
                .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;

            if let Some(pk_idx) = table.primary_key_index(col_idx) {
                let converted = json_to_value(value, name)?;
                pk_values[pk_idx] = Some(converted);
            }
        }

        // Verify all PKs are present
        let pk: Vec<Value<String, Vec<u8>>> = pk_values
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(ConversionError::MissingData("primary key", "DELETE"))?;

        Ok(PatchDelete::new(table.clone(), pk))
    }
}

// ============================================================================
// Arbitrary implementations for testing
// ============================================================================

#[cfg(feature = "testing")]
mod arbitrary_impl {
    use super::{Message, OpType};
    use alloc::collections::BTreeMap;
    use alloc::string::ToString;
    use arbitrary::{Arbitrary, Unstructured};

    impl<'a> Arbitrary<'a> for OpType {
        fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
            Ok(*u.choose(&[Self::Insert, Self::Update, Self::Delete])?)
        }
    }

    impl<'a> Arbitrary<'a> for Message {
        fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
            let op_type = OpType::arbitrary(u)?;

            // Generate 1-5 columns of data
            let num_cols: usize = u.int_in_range(1..=5)?;
            let mut data = BTreeMap::new();
            for i in 0..num_cols {
                let col_name = alloc::format!("col{i}");
                let value: i64 = u.arbitrary()?;
                data.insert(col_name, serde_json::Value::Number(value.into()));
            }

            // For updates, generate old values for some columns
            let old = if op_type == OpType::Update {
                let mut old_data = BTreeMap::new();
                let num_changed: usize = u.int_in_range(1..=num_cols)?;
                for i in 0..num_changed {
                    let col_name = alloc::format!("col{i}");
                    let value: i64 = u.arbitrary()?;
                    old_data.insert(col_name, serde_json::Value::Number(value.into()));
                }
                Some(old_data)
            } else {
                None
            };

            Ok(Self {
                database: "testdb".to_string(),
                table: "testtable".to_string(),
                op_type,
                ts: u.arbitrary()?,
                xid: u.arbitrary()?,
                commit: u.arbitrary()?,
                position: None,
                server_id: u.arbitrary()?,
                thread_id: u.arbitrary()?,
                primary_key: None,
                primary_key_columns: None,
                data,
                old,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SimpleTable;

    // ========================================================================
    // Real Maxwell JSON fixtures
    // Source: https://maxwells-daemon.io/ and Apache Flink documentation
    // ========================================================================

    const INSERT_JSON: &str = r#"{
        "database": "mydb",
        "table": "products",
        "type": "insert",
        "ts": 1477053217,
        "xid": 23396,
        "commit": true,
        "data": {
            "id": 111,
            "name": "scooter",
            "description": "Big 2-wheel scooter",
            "weight": 5.15
        }
    }"#;

    const UPDATE_JSON: &str = r#"{
        "database": "mydb",
        "table": "products",
        "type": "update",
        "ts": 1477053218,
        "xid": 23397,
        "data": {
            "id": 111,
            "name": "scooter",
            "description": "Big 2-wheel scooter",
            "weight": 5.18
        },
        "old": {
            "weight": 5.15
        }
    }"#;

    const DELETE_JSON: &str = r#"{
        "database": "mydb",
        "table": "products",
        "type": "delete",
        "ts": 1477053219,
        "xid": 23398,
        "data": {
            "id": 111,
            "name": "scooter",
            "description": "Big 2-wheel scooter",
            "weight": 5.18
        }
    }"#;

    const INSERT_WITH_POSITION_JSON: &str = r#"{
        "database": "test",
        "table": "users",
        "type": "insert",
        "ts": 1477053217,
        "xid": 23396,
        "commit": true,
        "position": "master.000006:800911",
        "server_id": 23042,
        "thread_id": 108,
        "primary_key": [1],
        "primary_key_columns": ["id"],
        "data": {
            "id": 1,
            "name": "Alice",
            "email": "alice@example.com"
        }
    }"#;

    fn products_table() -> SimpleTable {
        SimpleTable::new("products", &["id", "name", "description", "weight"], &[0])
    }

    fn users_table() -> SimpleTable {
        SimpleTable::new("users", &["id", "name", "email"], &[0])
    }

    // ========================================================================
    // Parsing tests
    // ========================================================================

    #[test]
    fn test_parse_insert() {
        let msg = parse(INSERT_JSON).unwrap();

        assert_eq!(msg.op_type, OpType::Insert);
        assert_eq!(msg.database, "mydb");
        assert_eq!(msg.table, "products");
        assert_eq!(msg.ts, Some(1_477_053_217));
        assert_eq!(msg.xid, Some(23396));
        assert_eq!(msg.commit, Some(true));
        assert_eq!(msg.data.len(), 4);
        assert_eq!(msg.data["id"], 111);
        assert_eq!(msg.data["name"], "scooter");
    }

    #[test]
    fn test_parse_update() {
        let msg = parse(UPDATE_JSON).unwrap();

        assert_eq!(msg.op_type, OpType::Update);
        assert!(msg.old.is_some());
        let old = msg.old.as_ref().unwrap();
        assert_eq!(old.len(), 1);
        assert_eq!(old["weight"], 5.15);
    }

    #[test]
    fn test_parse_delete() {
        let msg = parse(DELETE_JSON).unwrap();

        assert_eq!(msg.op_type, OpType::Delete);
        assert!(msg.old.is_none());
        assert_eq!(msg.data.len(), 4);
    }

    #[test]
    fn test_parse_with_position() {
        let msg = parse(INSERT_WITH_POSITION_JSON).unwrap();

        assert_eq!(msg.position.as_deref(), Some("master.000006:800911"));
        assert_eq!(msg.server_id, Some(23042));
        assert_eq!(msg.thread_id, Some(108));
        assert!(msg.primary_key.is_some());
        assert!(msg.primary_key_columns.is_some());
    }

    // ========================================================================
    // Conversion tests
    // ========================================================================

    #[test]
    fn test_convert_insert() {
        let table = products_table();
        let msg = parse(INSERT_JSON).unwrap();

        let insert: Insert<_, String, Vec<u8>> = (&msg, &table).try_into().unwrap();
        let values = insert.into_values();

        assert_eq!(values.len(), 4);
        assert_eq!(values[0], Value::Integer(111));
        assert_eq!(values[1], Value::Text("scooter".into()));
        assert_eq!(values[2], Value::Text("Big 2-wheel scooter".into()));
        assert_eq!(values[3], Value::Real(5.15));
    }

    #[test]
    fn test_convert_update() {
        let table = products_table();
        let msg = parse(UPDATE_JSON).unwrap();

        let update: Update<_, ChangesetFormat, String, Vec<u8>> =
            (&msg, &table).try_into().unwrap();

        let values = update.values();
        // weight changed from 5.15 to 5.18 (column index 3)
        assert_eq!(values[3].0, Some(Value::Real(5.15)));
        assert_eq!(values[3].1, Some(Value::Real(5.18)));
    }

    #[test]
    fn test_convert_delete() {
        let table = products_table();
        let msg = parse(DELETE_JSON).unwrap();

        let delete: ChangeDelete<_, String, Vec<u8>> = (&msg, &table).try_into().unwrap();
        let values = delete.into_values();

        assert_eq!(values.len(), 4);
        assert_eq!(values[0], Value::Integer(111));
    }

    #[test]
    fn test_convert_patch_delete() {
        let table = users_table();

        // Change to delete type for this test
        let delete_json =
            INSERT_WITH_POSITION_JSON.replace(r#""type": "insert""#, r#""type": "delete""#);
        let msg = parse(&delete_json).unwrap();

        let _delete: PatchDelete<_, String, Vec<u8>> = (&msg, &table).try_into().unwrap();
    }

    // ========================================================================
    // Error tests
    // ========================================================================

    #[test]
    fn test_table_mismatch() {
        let table = users_table(); // Different from products
        let msg = parse(INSERT_JSON).unwrap();

        let result: Result<Insert<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_invalid_operation() {
        let table = products_table();
        let msg = parse(DELETE_JSON).unwrap();

        // Try to convert delete to insert
        let result: Result<Insert<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::InvalidOperation(_))));
    }

    #[test]
    fn test_column_not_found() {
        let table = SimpleTable::new("products", &["id", "name"], &[0]); // Missing columns
        let msg = parse(INSERT_JSON).unwrap();

        let result: Result<Insert<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::ColumnNotFound(_))));
    }

    // ========================================================================
    // Value type tests
    // ========================================================================

    #[test]
    fn test_null_value() {
        let json = r#"{"database":"db","table":"t","type":"insert","data":{"id":1,"val":null}}"#;
        let table = SimpleTable::new("t", &["id", "val"], &[0]);
        let msg = parse(json).unwrap();

        let insert: Insert<_, String, Vec<u8>> = (&msg, &table).try_into().unwrap();
        let values = insert.into_values();

        assert_eq!(values[1], Value::Null);
    }

    #[test]
    fn test_bool_value() {
        let json = r#"{"database":"db","table":"t","type":"insert","data":{"id":1,"flag":true}}"#;
        let table = SimpleTable::new("t", &["id", "flag"], &[0]);
        let msg = parse(json).unwrap();

        let insert: Insert<_, String, Vec<u8>> = (&msg, &table).try_into().unwrap();
        let values = insert.into_values();

        assert_eq!(values[1], Value::Integer(1));
    }

    #[test]
    fn test_roundtrip_serialization() {
        let msg = parse(INSERT_JSON).unwrap();
        let serialized = serde_json::to_string(&msg).unwrap();
        let reparsed: Message = serde_json::from_str(&serialized).unwrap();

        assert_eq!(msg.op_type, reparsed.op_type);
        assert_eq!(msg.table, reparsed.table);
        assert_eq!(msg.data, reparsed.data);
    }
}
