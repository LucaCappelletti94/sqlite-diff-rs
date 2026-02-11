//! Debezium message parsing and conversion to `SQLite` changeset operations.
//!
//! This module provides types for deserializing [Debezium](https://debezium.io/)
//! Change Data Capture (CDC) events and converting them to `SQLite` changeset
//! operations compatible with this crate's builders.
//!
//! Debezium is a distributed platform for CDC that captures row-level changes
//! in databases. It produces events in a standardized JSON envelope format
//! that includes before/after states and metadata.
//!
//! # Example
//!
//! ```
//! use sqlite_diff_rs::debezium::{parse, Envelope, Op};
//!
//! let json = r#"{"before":null,"after":{"id":1,"name":"Alice"},"source":{"version":"2.3.0","connector":"postgresql","name":"my-connector","ts_ms":1234567890,"db":"mydb","schema":"public","table":"users"},"op":"c","ts_ms":1234567890}"#;
//! let envelope = parse::<serde_json::Value>(json).unwrap();
//!
//! assert_eq!(envelope.op, Op::Create);
//! assert!(envelope.after.is_some());
//! ```

use alloc::string::String;
use alloc::vec::Vec;
use serde::{Deserialize, Serialize};

/// Debezium operation type.
///
/// Represents the type of database operation captured by Debezium.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    /// Create (INSERT) operation.
    #[serde(rename = "c")]
    Create,
    /// Update operation.
    #[serde(rename = "u")]
    Update,
    /// Delete operation.
    #[serde(rename = "d")]
    Delete,
    /// Read (snapshot) operation.
    #[serde(rename = "r")]
    Read,
    /// Truncate operation.
    #[serde(rename = "t")]
    Truncate,
    /// Message operation (for custom messages).
    #[serde(rename = "m")]
    Message,
}

/// Debezium source metadata.
///
/// Contains information about the source database and connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    /// Debezium version.
    #[serde(default)]
    pub version: Option<String>,
    /// Connector type (e.g., "postgresql", "mysql").
    #[serde(default)]
    pub connector: Option<String>,
    /// Logical name of the connector.
    #[serde(default)]
    pub name: Option<String>,
    /// Timestamp in milliseconds when the event occurred in the source database.
    #[serde(default)]
    pub ts_ms: Option<i64>,
    /// Whether this is a snapshot event.
    #[serde(default)]
    pub snapshot: Option<String>,
    /// Database name.
    #[serde(default)]
    pub db: Option<String>,
    /// Schema name.
    #[serde(default)]
    pub schema: Option<String>,
    /// Table name.
    #[serde(default)]
    pub table: Option<String>,
    /// Transaction ID (`PostgreSQL`).
    #[serde(default, rename = "txId")]
    pub tx_id: Option<i64>,
    /// Log sequence number (`PostgreSQL`).
    #[serde(default)]
    pub lsn: Option<i64>,
    /// `XMin` value (`PostgreSQL`).
    #[serde(default)]
    pub xmin: Option<i64>,
}

/// Debezium transaction metadata.
///
/// Present when transaction metadata is enabled in the connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    /// Transaction ID.
    pub id: String,
    /// Total order of the event within the transaction.
    #[serde(default)]
    pub total_order: Option<i64>,
    /// Order of the event for this data collection within the transaction.
    #[serde(default)]
    pub data_collection_order: Option<i64>,
}

/// Debezium change event envelope.
///
/// The main structure for Debezium CDC events. The `T` type parameter
/// represents the row data type (typically a struct matching your table
/// schema, or `serde_json::Value` for dynamic access).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope<T> {
    /// Row state before the operation (None for INSERT).
    pub before: Option<T>,
    /// Row state after the operation (None for DELETE).
    pub after: Option<T>,
    /// Source metadata.
    pub source: Source,
    /// Operation type.
    pub op: Op,
    /// Timestamp in milliseconds when Debezium processed the event.
    #[serde(default)]
    pub ts_ms: Option<i64>,
    /// Transaction metadata (if enabled).
    #[serde(default)]
    pub transaction: Option<Transaction>,
}

/// Parse a Debezium envelope from JSON.
///
/// # Type Parameters
///
/// * `T` - The row data type. Use `serde_json::Value` for dynamic access,
///   or a custom struct for typed access.
///
/// # Errors
///
/// Returns a [`serde_json::Error`] if the JSON is malformed.
///
/// # Example
///
/// ```
/// use sqlite_diff_rs::debezium::{parse, Op};
/// use serde_json::Value;
///
/// let json = r#"{"before":null,"after":{"id":1},"source":{"table":"users"},"op":"c"}"#;
/// let envelope = parse::<Value>(json).unwrap();
/// assert_eq!(envelope.op, Op::Create);
/// ```
pub fn parse<T: for<'de> Deserialize<'de>>(json: &str) -> Result<Envelope<T>, serde_json::Error> {
    serde_json::from_str(json)
}

/// Errors during Debezium to changeset conversion.
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
        /// Actual table name from the Debezium event.
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

/// Extract column values from a JSON object.
fn extract_row_data(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> impl Iterator<Item = (&String, &serde_json::Value)> {
    obj.iter()
}

// ============================================================================
// Conversions for Envelope<serde_json::Value>
// ============================================================================

impl<T: NamedColumns + Clone> TryFrom<(&Envelope<serde_json::Value>, &T)>
    for Insert<T, String, Vec<u8>>
{
    type Error = ConversionError;

    fn try_from(
        (envelope, table): (&Envelope<serde_json::Value>, &T),
    ) -> Result<Self, Self::Error> {
        // Verify operation type
        if envelope.op != Op::Create && envelope.op != Op::Read {
            return Err(ConversionError::InvalidOperation(format!(
                "{:?}",
                envelope.op
            )));
        }

        // Verify table name if present
        if let Some(ref event_table) = envelope.source.table
            && table.name() != event_table
        {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: event_table.clone(),
            });
        }

        // Get the after data (required for insert)
        let after = envelope
            .after
            .as_ref()
            .and_then(|v| v.as_object())
            .ok_or(ConversionError::MissingData("after", "INSERT"))?;

        let mut insert = Insert::from(table.clone());

        for (name, value) in extract_row_data(after) {
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

impl<T: NamedColumns + Clone> TryFrom<(&Envelope<serde_json::Value>, &T)>
    for Update<T, ChangesetFormat, String, Vec<u8>>
{
    type Error = ConversionError;

    fn try_from(
        (envelope, table): (&Envelope<serde_json::Value>, &T),
    ) -> Result<Self, Self::Error> {
        // Verify operation type
        if envelope.op != Op::Update {
            return Err(ConversionError::InvalidOperation(format!(
                "{:?}",
                envelope.op
            )));
        }

        // Verify table name if present
        if let Some(ref event_table) = envelope.source.table
            && table.name() != event_table
        {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: event_table.clone(),
            });
        }

        // Get the after data (required for update)
        let after = envelope
            .after
            .as_ref()
            .and_then(|v| v.as_object())
            .ok_or(ConversionError::MissingData("after", "UPDATE"))?;

        // Get the before data (optional, depends on replica identity)
        let before = envelope.before.as_ref().and_then(|v| v.as_object());

        let mut update: Update<T, ChangesetFormat, String, Vec<u8>> = Update::from(table.clone());

        for (name, new_value) in extract_row_data(after) {
            let col_idx = table
                .column_index(name)
                .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;

            let new_converted = json_to_value(new_value, name)?;

            // Check if we have old data for this column
            if let Some(old_obj) = before
                && let Some(old_value) = old_obj.get(name)
            {
                let old_converted = json_to_value(old_value, name)?;
                update = update
                    .set(col_idx, old_converted, new_converted)
                    .map_err(|_| ConversionError::ColumnNotFound(name.clone()))?;
                continue;
            }

            // No old data available, just set new
            update = update
                .set_new(col_idx, new_converted)
                .map_err(|_| ConversionError::ColumnNotFound(name.clone()))?;
        }

        Ok(update)
    }
}

impl<T: NamedColumns + Clone> TryFrom<(&Envelope<serde_json::Value>, &T)>
    for ChangeDelete<T, String, Vec<u8>>
{
    type Error = ConversionError;

    fn try_from(
        (envelope, table): (&Envelope<serde_json::Value>, &T),
    ) -> Result<Self, Self::Error> {
        // Verify operation type
        if envelope.op != Op::Delete {
            return Err(ConversionError::InvalidOperation(format!(
                "{:?}",
                envelope.op
            )));
        }

        // Verify table name if present
        if let Some(ref event_table) = envelope.source.table
            && table.name() != event_table
        {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: event_table.clone(),
            });
        }

        // Get the before data (required for delete)
        let before = envelope
            .before
            .as_ref()
            .and_then(|v| v.as_object())
            .ok_or(ConversionError::MissingData("before", "DELETE"))?;

        let mut delete = ChangeDelete::from(table.clone());

        for (name, value) in extract_row_data(before) {
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

impl<T: NamedColumns + Clone> TryFrom<(&Envelope<serde_json::Value>, &T)>
    for PatchDelete<T, String, Vec<u8>>
{
    type Error = ConversionError;

    fn try_from(
        (envelope, table): (&Envelope<serde_json::Value>, &T),
    ) -> Result<Self, Self::Error> {
        // Verify operation type
        if envelope.op != Op::Delete {
            return Err(ConversionError::InvalidOperation(format!(
                "{:?}",
                envelope.op
            )));
        }

        // Verify table name if present
        if let Some(ref event_table) = envelope.source.table
            && table.name() != event_table
        {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: event_table.clone(),
            });
        }

        // Get the before data (required for delete)
        let before = envelope
            .before
            .as_ref()
            .and_then(|v| v.as_object())
            .ok_or(ConversionError::MissingData("before", "DELETE"))?;

        // Extract primary key values in schema order
        let num_pks = table.number_of_primary_keys();
        let mut pk_values: Vec<Option<Value<String, Vec<u8>>>> = alloc::vec![None; num_pks];

        for (name, value) in extract_row_data(before) {
            let col_idx = table
                .column_index(name)
                .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;

            if let Some(pk_idx) = table.primary_key_index(col_idx) {
                let converted = json_to_value(value, name)?;
                pk_values[pk_idx] = Some(converted);
            }
        }

        // Verify all PKs are present and collect them
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
    use super::{Envelope, Op, Source, Transaction};
    use arbitrary::{Arbitrary, Unstructured};

    impl<'a> Arbitrary<'a> for Op {
        fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
            Ok(*u.choose(&[
                Self::Create,
                Self::Update,
                Self::Delete,
                Self::Read,
                Self::Truncate,
                Self::Message,
            ])?)
        }
    }

    impl<'a> Arbitrary<'a> for Source {
        fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
            Ok(Self {
                version: u.arbitrary()?,
                connector: u.arbitrary()?,
                name: u.arbitrary()?,
                ts_ms: u.arbitrary()?,
                snapshot: u.arbitrary()?,
                db: u.arbitrary()?,
                schema: u.arbitrary()?,
                table: u.arbitrary()?,
                tx_id: u.arbitrary()?,
                lsn: u.arbitrary()?,
                xmin: u.arbitrary()?,
            })
        }
    }

    impl<'a> Arbitrary<'a> for Transaction {
        fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
            Ok(Self {
                id: u.arbitrary()?,
                total_order: u.arbitrary()?,
                data_collection_order: u.arbitrary()?,
            })
        }
    }

    impl<'a, T: Arbitrary<'a>> Arbitrary<'a> for Envelope<T> {
        fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
            Ok(Self {
                before: u.arbitrary()?,
                after: u.arbitrary()?,
                source: u.arbitrary()?,
                op: u.arbitrary()?,
                ts_ms: u.arbitrary()?,
                transaction: u.arbitrary()?,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SimpleTable;

    // ========================================================================
    // Real Debezium JSON fixtures from official PostgreSQL connector docs
    // Source: https://debezium.io/documentation/reference/stable/connectors/postgresql.html
    // ========================================================================

    /// Real Debezium CREATE (insert) event
    const CREATE_EVENT_JSON: &str = r#"{
        "before": null,
        "after": {
            "id": 1,
            "first_name": "Anne",
            "last_name": "Kretchmar",
            "email": "annek@noanswer.org"
        },
        "source": {
            "version": "2.7.4.Final",
            "connector": "postgresql",
            "name": "PostgreSQL_server",
            "ts_ms": 1559033904863,
            "db": "postgres",
            "schema": "public",
            "table": "customers",
            "txId": 555,
            "lsn": 24023128
        },
        "op": "c",
        "ts_ms": 1559033904863
    }"#;

    /// Real Debezium UPDATE event
    const UPDATE_EVENT_JSON: &str = r#"{
        "before": {
            "id": 1,
            "first_name": "Anne",
            "last_name": "Kretchmar",
            "email": "annek@noanswer.org"
        },
        "after": {
            "id": 1,
            "first_name": "Anne Marie",
            "last_name": "Kretchmar",
            "email": "annek@noanswer.org"
        },
        "source": {
            "version": "2.7.4.Final",
            "connector": "postgresql",
            "name": "PostgreSQL_server",
            "ts_ms": 1559033905123,
            "db": "postgres",
            "schema": "public",
            "table": "customers",
            "txId": 556,
            "lsn": 24023256
        },
        "op": "u",
        "ts_ms": 1559033905123
    }"#;

    /// Real Debezium DELETE event
    const DELETE_EVENT_JSON: &str = r#"{
        "before": {
            "id": 1,
            "first_name": "Anne Marie",
            "last_name": "Kretchmar",
            "email": "annek@noanswer.org"
        },
        "after": null,
        "source": {
            "version": "2.7.4.Final",
            "connector": "postgresql",
            "name": "PostgreSQL_server",
            "ts_ms": 1559033906456,
            "db": "postgres",
            "schema": "public",
            "table": "customers",
            "txId": 557,
            "lsn": 24023384
        },
        "op": "d",
        "ts_ms": 1559033906456
    }"#;

    /// Read/snapshot event (initial data load)
    const READ_EVENT_JSON: &str = r#"{
        "before": null,
        "after": {
            "id": 1001,
            "first_name": "Sally",
            "last_name": "Thomas",
            "email": "sally.thomas@acme.com"
        },
        "source": {
            "version": "2.7.4.Final",
            "connector": "postgresql",
            "name": "PostgreSQL_server",
            "ts_ms": 1559033900000,
            "snapshot": "true",
            "db": "postgres",
            "schema": "public",
            "table": "customers"
        },
        "op": "r",
        "ts_ms": 1559033900000
    }"#;

    /// Event with transaction metadata
    const CREATE_WITH_TRANSACTION_JSON: &str = r#"{
        "before": null,
        "after": {
            "id": 1,
            "first_name": "Anne",
            "last_name": "Kretchmar",
            "email": "annek@noanswer.org"
        },
        "source": {
            "version": "2.7.4.Final",
            "connector": "postgresql",
            "name": "PostgreSQL_server",
            "ts_ms": 1559033904863,
            "db": "postgres",
            "schema": "public",
            "table": "customers",
            "txId": 555,
            "lsn": 24023128
        },
        "op": "c",
        "ts_ms": 1559033904863,
        "transaction": {
            "id": "555:24023128",
            "total_order": 1,
            "data_collection_order": 1
        }
    }"#;

    fn customers_table() -> SimpleTable {
        SimpleTable::new(
            "customers",
            &["id", "first_name", "last_name", "email"],
            &[0],
        )
    }

    // ========================================================================
    // Tests using real Debezium JSON fixtures
    // ========================================================================

    #[test]
    fn test_parse_real_create_event() {
        let envelope = parse::<serde_json::Value>(CREATE_EVENT_JSON).unwrap();

        assert_eq!(envelope.op, Op::Create);
        assert!(envelope.before.is_none());
        assert!(envelope.after.is_some());
        assert_eq!(envelope.source.table.as_deref(), Some("customers"));
        assert_eq!(envelope.source.connector.as_deref(), Some("postgresql"));
        assert_eq!(envelope.source.version.as_deref(), Some("2.7.4.Final"));
        assert_eq!(envelope.source.db.as_deref(), Some("postgres"));
        assert_eq!(envelope.source.schema.as_deref(), Some("public"));
        assert_eq!(envelope.source.tx_id, Some(555));
        assert_eq!(envelope.source.lsn, Some(24_023_128));
        assert_eq!(envelope.ts_ms, Some(1_559_033_904_863));
    }

    #[test]
    fn test_parse_real_update_event() {
        let envelope = parse::<serde_json::Value>(UPDATE_EVENT_JSON).unwrap();

        assert_eq!(envelope.op, Op::Update);
        assert!(envelope.before.is_some());
        assert!(envelope.after.is_some());

        let before = envelope.before.as_ref().unwrap();
        let after = envelope.after.as_ref().unwrap();

        assert_eq!(before["first_name"], "Anne");
        assert_eq!(after["first_name"], "Anne Marie");
        // Other fields unchanged
        assert_eq!(before["last_name"], after["last_name"]);
        assert_eq!(before["email"], after["email"]);
    }

    #[test]
    fn test_parse_real_delete_event() {
        let envelope = parse::<serde_json::Value>(DELETE_EVENT_JSON).unwrap();

        assert_eq!(envelope.op, Op::Delete);
        assert!(envelope.before.is_some());
        assert!(envelope.after.is_none());

        let before = envelope.before.as_ref().unwrap();
        assert_eq!(before["first_name"], "Anne Marie");
    }

    #[test]
    fn test_parse_real_read_snapshot_event() {
        let envelope = parse::<serde_json::Value>(READ_EVENT_JSON).unwrap();

        assert_eq!(envelope.op, Op::Read);
        assert!(envelope.before.is_none());
        assert!(envelope.after.is_some());
        assert_eq!(envelope.source.snapshot.as_deref(), Some("true"));
    }

    #[test]
    fn test_parse_event_with_transaction() {
        let envelope = parse::<serde_json::Value>(CREATE_WITH_TRANSACTION_JSON).unwrap();

        assert_eq!(envelope.op, Op::Create);
        assert!(envelope.transaction.is_some());

        let tx = envelope.transaction.as_ref().unwrap();
        assert_eq!(tx.id, "555:24023128");
        assert_eq!(tx.total_order, Some(1));
        assert_eq!(tx.data_collection_order, Some(1));
    }

    #[test]
    fn test_convert_real_create_to_insert() {
        let table = customers_table();
        let envelope = parse::<serde_json::Value>(CREATE_EVENT_JSON).unwrap();

        let insert: Insert<_, String, Vec<u8>> = (&envelope, &table).try_into().unwrap();
        let values = insert.into_values();

        assert_eq!(values.len(), 4);
        assert_eq!(values[0], Value::Integer(1));
        assert_eq!(values[1], Value::Text("Anne".into()));
        assert_eq!(values[2], Value::Text("Kretchmar".into()));
        assert_eq!(values[3], Value::Text("annek@noanswer.org".into()));
    }

    #[test]
    fn test_convert_real_update_to_changeset() {
        let table = customers_table();
        let envelope = parse::<serde_json::Value>(UPDATE_EVENT_JSON).unwrap();

        let update: Update<_, ChangesetFormat, String, Vec<u8>> =
            (&envelope, &table).try_into().unwrap();

        // Verify the update was created successfully
        let values = update.values();

        // first_name changed from "Anne" to "Anne Marie" (column index 1)
        assert_eq!(values[1].0, Some(Value::Text("Anne".into())));
        assert_eq!(values[1].1, Some(Value::Text("Anne Marie".into())));
    }

    #[test]
    fn test_convert_real_delete_to_changeset() {
        let table = customers_table();
        let envelope = parse::<serde_json::Value>(DELETE_EVENT_JSON).unwrap();

        let delete: ChangeDelete<_, String, Vec<u8>> = (&envelope, &table).try_into().unwrap();
        let values = delete.into_values();

        assert_eq!(values.len(), 4);
        assert_eq!(values[0], Value::Integer(1));
        assert_eq!(values[1], Value::Text("Anne Marie".into()));
    }

    #[test]
    fn test_convert_real_read_to_insert() {
        let table = customers_table();
        let envelope = parse::<serde_json::Value>(READ_EVENT_JSON).unwrap();

        // Read events should convert to Insert (snapshot data)
        let insert: Insert<_, String, Vec<u8>> = (&envelope, &table).try_into().unwrap();
        let values = insert.into_values();

        assert_eq!(values[0], Value::Integer(1001));
        assert_eq!(values[1], Value::Text("Sally".into()));
        assert_eq!(values[2], Value::Text("Thomas".into()));
        assert_eq!(values[3], Value::Text("sally.thomas@acme.com".into()));
    }

    // ========================================================================
    // Original unit tests
    // ========================================================================

    #[test]
    fn test_parse_create_event() {
        let json = r#"{"before":null,"after":{"id":1,"name":"Alice"},"source":{"version":"2.3.0","connector":"postgresql","name":"test","ts_ms":1234567890,"db":"mydb","schema":"public","table":"users"},"op":"c","ts_ms":1234567890}"#;
        let envelope = parse::<serde_json::Value>(json).unwrap();

        assert_eq!(envelope.op, Op::Create);
        assert!(envelope.before.is_none());
        assert!(envelope.after.is_some());
        assert_eq!(envelope.source.table.as_deref(), Some("users"));
    }

    #[test]
    fn test_parse_update_event() {
        let json = r#"{"before":{"id":1,"name":"Alice"},"after":{"id":1,"name":"Alicia"},"source":{"table":"users"},"op":"u","ts_ms":1234567890}"#;
        let envelope = parse::<serde_json::Value>(json).unwrap();

        assert_eq!(envelope.op, Op::Update);
        assert!(envelope.before.is_some());
        assert!(envelope.after.is_some());
    }

    #[test]
    fn test_parse_delete_event() {
        let json = r#"{"before":{"id":1,"name":"Alice"},"after":null,"source":{"table":"users"},"op":"d","ts_ms":1234567890}"#;
        let envelope = parse::<serde_json::Value>(json).unwrap();

        assert_eq!(envelope.op, Op::Delete);
        assert!(envelope.before.is_some());
        assert!(envelope.after.is_none());
    }

    #[test]
    fn test_insert_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let json = r#"{"before":null,"after":{"id":1,"name":"Alice"},"source":{"table":"users"},"op":"c"}"#;
        let envelope = parse::<serde_json::Value>(json).unwrap();

        let insert: Insert<_, String, Vec<u8>> = (&envelope, &table).try_into().unwrap();
        let values = insert.into_values();

        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Value::Integer(1));
        assert_eq!(values[1], Value::Text("Alice".into()));
    }

    #[test]
    fn test_update_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let json = r#"{"before":{"id":1,"name":"Alice"},"after":{"id":1,"name":"Alicia"},"source":{"table":"users"},"op":"u"}"#;
        let envelope = parse::<serde_json::Value>(json).unwrap();

        let _update: Update<_, ChangesetFormat, String, Vec<u8>> =
            (&envelope, &table).try_into().unwrap();
    }

    #[test]
    fn test_delete_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let json = r#"{"before":{"id":42,"name":"Alice"},"after":null,"source":{"table":"users"},"op":"d"}"#;
        let envelope = parse::<serde_json::Value>(json).unwrap();

        let delete: ChangeDelete<_, String, Vec<u8>> = (&envelope, &table).try_into().unwrap();
        let values = delete.into_values();

        assert_eq!(values[0], Value::Integer(42));
    }

    #[test]
    fn test_patch_delete_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let json = r#"{"before":{"id":42,"name":"Alice"},"after":null,"source":{"table":"users"},"op":"d"}"#;
        let envelope = parse::<serde_json::Value>(json).unwrap();

        let _delete: PatchDelete<_, String, Vec<u8>> = (&envelope, &table).try_into().unwrap();
    }

    #[test]
    fn test_table_mismatch_error() {
        let table = SimpleTable::new("products", &["id", "name"], &[0]);

        let json = r#"{"before":null,"after":{"id":1},"source":{"table":"users"},"op":"c"}"#;
        let envelope = parse::<serde_json::Value>(json).unwrap();

        let result: Result<Insert<_, String, Vec<u8>>, _> = (&envelope, &table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_invalid_operation_error() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        // Try to convert a DELETE event to Insert
        let json = r#"{"before":{"id":1},"after":null,"source":{"table":"users"},"op":"d"}"#;
        let envelope = parse::<serde_json::Value>(json).unwrap();

        let result: Result<Insert<_, String, Vec<u8>>, _> = (&envelope, &table).try_into();
        assert!(matches!(result, Err(ConversionError::InvalidOperation(_))));
    }

    #[test]
    fn test_column_not_found_error() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let json = r#"{"before":null,"after":{"id":1,"unknown":"value"},"source":{"table":"users"},"op":"c"}"#;
        let envelope = parse::<serde_json::Value>(json).unwrap();

        let result: Result<Insert<_, String, Vec<u8>>, _> = (&envelope, &table).try_into();
        assert!(matches!(result, Err(ConversionError::ColumnNotFound(_))));
    }

    #[test]
    fn test_null_value() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let json =
            r#"{"before":null,"after":{"id":1,"name":null},"source":{"table":"users"},"op":"c"}"#;
        let envelope = parse::<serde_json::Value>(json).unwrap();

        let insert: Insert<_, String, Vec<u8>> = (&envelope, &table).try_into().unwrap();
        let values = insert.into_values();

        assert_eq!(values[1], Value::Null);
    }

    #[test]
    fn test_bool_value() {
        let table = SimpleTable::new("settings", &["id", "enabled"], &[0]);

        let json = r#"{"before":null,"after":{"id":1,"enabled":true},"source":{"table":"settings"},"op":"c"}"#;
        let envelope = parse::<serde_json::Value>(json).unwrap();

        let insert: Insert<_, String, Vec<u8>> = (&envelope, &table).try_into().unwrap();
        let values = insert.into_values();

        assert_eq!(values[1], Value::Integer(1));
    }

    #[test]
    fn test_float_value() {
        let table = SimpleTable::new("prices", &["id", "amount"], &[0]);

        let json = r#"{"before":null,"after":{"id":1,"amount":99.99},"source":{"table":"prices"},"op":"c"}"#;
        let envelope = parse::<serde_json::Value>(json).unwrap();

        let insert: Insert<_, String, Vec<u8>> = (&envelope, &table).try_into().unwrap();
        let values = insert.into_values();

        assert_eq!(values[1], Value::Real(99.99));
    }

    #[test]
    fn test_read_snapshot_event() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        // Snapshot events use "r" operation
        let json = r#"{"before":null,"after":{"id":1,"name":"Alice"},"source":{"table":"users","snapshot":"true"},"op":"r"}"#;
        let envelope = parse::<serde_json::Value>(json).unwrap();

        assert_eq!(envelope.op, Op::Read);

        // Read events should convert to Insert
        let insert: Insert<_, String, Vec<u8>> = (&envelope, &table).try_into().unwrap();
        let values = insert.into_values();

        assert_eq!(values[0], Value::Integer(1));
    }
}
