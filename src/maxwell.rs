//! Maxwell message parsing and conversion to `SQLite` changeset operations.
//!
//! [Maxwell](https://maxwells-daemon.io/) is a `MySQL` CDC application that
//! reads binlogs and emits row-level changes as JSON. This module deserializes
//! those events and converts them into changeset operations compatible with
//! this crate's builders.
//!
//! Maxwell events carry no trigger-origin marker, so converted ops default
//! to `indirect = false`. Override via the [`Indirect`](crate::Indirect) trait
//! if you know out-of-band that the event was trigger-induced.
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
use alloc::sync::Arc;
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
    /// Per-column MySQL type names emitted when the Maxwell daemon
    /// runs with `--include_types`. Absent otherwise.
    #[serde(default)]
    pub columns_types: Option<BTreeMap<String, String>>,
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

    /// A schema-aware decoder rejected a column payload. Populated by
    /// `DiffSetBuilder::digest_maxwell` when the user's registered
    /// decoder returns [`crate::wire::DecodeError`].
    #[error("Decoder failed: {0}")]
    Decode(#[from] crate::wire::DecodeError),
}

use crate::ChangesetFormat;
use crate::builders::{ChangeDelete, Insert, PatchDelete, Update};
use crate::encoding::Value;
use crate::schema::NamedColumns;
use alloc::format;

use crate::wire::{Sealed, WireSource};

use crate::builders::{DiffOps, DiffSetBuilder, PatchsetFormat};
use crate::wire::WireAdapter;
use core::fmt::Debug;
use core::hash::Hash;

/// Marker type for the `maxwell` source.
#[derive(Debug, Clone, Copy, Default)]
pub struct Maxwell;

impl Sealed for Maxwell {}

impl WireSource for Maxwell {
    type Payload<'a> = MaxwellColumn<'a>;
    type TypeKey = Arc<str>;

    fn type_key(payload: &Self::Payload<'_>) -> Self::TypeKey {
        // Maxwell's `--include_types` emits raw MySQL type expressions
        // with modifiers (e.g. `varchar(255)`, `decimal(10,2)`). Strip
        // the parenthesized suffix so base names dispatch through
        // `defaults()`. Two exceptions preserve their modifier: the
        // `tinyint(1)` bool convention and `bigint unsigned` (which
        // uses a suffix keyword, not parens).
        let name = payload.mysql_type.unwrap_or("");
        if name == "tinyint(1)" {
            return Arc::from(name);
        }
        let base = name
            .split_once('(')
            .map_or(name, |(head, _)| head)
            .trim_end();
        Arc::from(base)
    }

    fn column_name<'a>(payload: &'a Self::Payload<'_>) -> &'a str {
        payload.column_name
    }
}

/// Per-column payload for the `maxwell` source.
///
/// `mysql_type` is `Some` when the Maxwell daemon runs with
/// `--include_types`. When `None`, the empty-string key is used and
/// [`TypeMap`](crate::wire::TypeMap) lookups will fail with
/// [`DecodeError::NoDecoderForType`](crate::wire::DecodeError::NoDecoderForType)
/// unless the user registers a `""` mapping explicitly.
#[derive(Debug, Clone, Copy)]
pub struct MaxwellColumn<'a> {
    /// Column name.
    pub column_name: &'a str,
    /// MySQL type name (e.g. "int", "varchar", "datetime"). `None`
    /// when the daemon runs without `--include_types`.
    pub mysql_type: Option<&'a str>,
    /// Column value as a JSON value.
    pub value: &'a serde_json::Value,
}

impl MaxwellColumn<'_> {
    /// Ergonomic helper for calling a specific [`Decoder`] on this
    /// payload without fully-qualified syntax. Fixes the `Src` generic
    /// to [`Maxwell`] so the compiler can pick the impl.
    ///
    /// # Errors
    ///
    /// Propagates the decoder's [`DecodeError`](crate::wire::DecodeError).
    pub fn decoded_by<D, S, B>(
        self,
        decoder: &D,
    ) -> Result<crate::encoding::Value<S, B>, crate::wire::DecodeError>
    where
        D: crate::wire::Decoder<Maxwell, S, B>,
    {
        decoder.decode(self)
    }
}

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
// Schema-aware digest fns (0.2.0+).
// ============================================================================

impl<T, S, B> DiffSetBuilder<ChangesetFormat, T, S, B>
where
    T: NamedColumns + Clone,
    S: Clone + Debug + Hash + Eq + AsRef<str> + Default,
    B: Clone + Debug + Hash + Eq + AsRef<[u8]> + Default,
{
    /// Digest a Maxwell message into a changeset via the supplied `adapter`.
    ///
    /// # Errors
    ///
    /// Returns [`ConversionError::TableMismatch`] if `message.table` does not
    /// match `table.name()`, and [`ConversionError::ColumnNotFound`] for
    /// column names absent from the schema. Adapter failures surface as
    /// [`ConversionError::Decode`].
    pub fn digest_maxwell<A>(
        self,
        message: &Message,
        table: &T,
        adapter: &A,
    ) -> Result<Self, ConversionError>
    where
        A: WireAdapter<Maxwell, S, B>,
    {
        if table.name() != message.table {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: message.table.clone(),
            });
        }

        match message.op_type {
            OpType::Insert => {
                let insert = build_insert_from_maxwell(
                    &message.data,
                    message.columns_types.as_ref(),
                    table,
                    adapter,
                )?;
                Ok(DiffOps::insert(self, insert))
            }
            OpType::Update => {
                let update = build_changeset_update_from_maxwell(
                    &message.data,
                    message.old.as_ref(),
                    message.columns_types.as_ref(),
                    table,
                    adapter,
                )?;
                Ok(DiffOps::update(self, update))
            }
            OpType::Delete => {
                let delete = build_changeset_delete_from_maxwell(
                    &message.data,
                    message.columns_types.as_ref(),
                    table,
                    adapter,
                )?;
                Ok(DiffOps::delete(self, delete))
            }
        }
    }
}

impl<T, S, B> DiffSetBuilder<PatchsetFormat, T, S, B>
where
    T: NamedColumns + Clone,
    S: Clone + Debug + Hash + Eq + AsRef<str> + Default,
    B: Clone + Debug + Hash + Eq + AsRef<[u8]> + Default,
{
    /// Digest a Maxwell message into a patchset via the supplied `adapter`.
    ///
    /// # Errors
    ///
    /// See the changeset variant.
    pub fn digest_maxwell<A>(
        self,
        message: &Message,
        table: &T,
        adapter: &A,
    ) -> Result<Self, ConversionError>
    where
        A: WireAdapter<Maxwell, S, B>,
    {
        if table.name() != message.table {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: message.table.clone(),
            });
        }

        match message.op_type {
            OpType::Insert => {
                let insert = build_insert_from_maxwell(
                    &message.data,
                    message.columns_types.as_ref(),
                    table,
                    adapter,
                )?;
                Ok(DiffOps::insert(self, insert))
            }
            OpType::Update => {
                let update = build_patchset_update_from_maxwell(
                    &message.data,
                    message.columns_types.as_ref(),
                    table,
                    adapter,
                )?;
                Ok(DiffOps::update(self, update))
            }
            OpType::Delete => {
                let delete = build_patch_delete_from_maxwell(
                    &message.data,
                    message.columns_types.as_ref(),
                    table,
                    adapter,
                )?;
                Ok(DiffOps::delete(self, delete))
            }
        }
    }
}

fn build_insert_from_maxwell<T, S, B, A>(
    data: &BTreeMap<String, serde_json::Value>,
    columns_types: Option<&BTreeMap<String, String>>,
    table: &T,
    adapter: &A,
) -> Result<Insert<T, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<Maxwell, S, B>,
{
    let mut insert = Insert::from(table.clone());
    for (name, value) in data {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;
        let payload = MaxwellColumn {
            column_name: name.as_str(),
            mysql_type: columns_types.and_then(|m| m.get(name)).map(String::as_str),
            value,
        };
        let decoded = adapter.decode(payload)?;
        insert = insert
            .set(col_idx, decoded)
            .map_err(|_| ConversionError::ColumnNotFound(name.clone()))?;
    }
    Ok(insert)
}

fn build_changeset_update_from_maxwell<T, S, B, A>(
    data: &BTreeMap<String, serde_json::Value>,
    old: Option<&BTreeMap<String, serde_json::Value>>,
    columns_types: Option<&BTreeMap<String, String>>,
    table: &T,
    adapter: &A,
) -> Result<Update<T, ChangesetFormat, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + Debug + AsRef<str>,
    B: Clone + Debug + AsRef<[u8]>,
    A: WireAdapter<Maxwell, S, B>,
{
    let mut update: Update<T, ChangesetFormat, S, B> = Update::from(table.clone());
    for (name, new_value) in data {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;

        let new_payload = MaxwellColumn {
            column_name: name.as_str(),
            mysql_type: columns_types.and_then(|m| m.get(name)).map(String::as_str),
            value: new_value,
        };
        let new = adapter.decode(new_payload)?;

        if let Some(old_map) = old {
            if let Some(old_value) = old_map.get(name) {
                let old_payload = MaxwellColumn {
                    column_name: name.as_str(),
                    mysql_type: columns_types.and_then(|m| m.get(name)).map(String::as_str),
                    value: old_value,
                };
                let old = adapter.decode(old_payload)?;
                update = update
                    .set(col_idx, old, new)
                    .map_err(|_| ConversionError::ColumnNotFound(name.clone()))?;
                continue;
            }
        }

        update = update
            .set_new(col_idx, new)
            .map_err(|_| ConversionError::ColumnNotFound(name.clone()))?;
    }
    Ok(update)
}

fn build_patchset_update_from_maxwell<T, S, B, A>(
    data: &BTreeMap<String, serde_json::Value>,
    columns_types: Option<&BTreeMap<String, String>>,
    table: &T,
    adapter: &A,
) -> Result<Update<T, PatchsetFormat, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<Maxwell, S, B>,
{
    let mut update: Update<T, PatchsetFormat, S, B> = Update::from(table.clone());
    for (name, value) in data {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;
        let payload = MaxwellColumn {
            column_name: name.as_str(),
            mysql_type: columns_types.and_then(|m| m.get(name)).map(String::as_str),
            value,
        };
        let decoded = adapter.decode(payload)?;
        update = update
            .set(col_idx, decoded)
            .map_err(|_| ConversionError::ColumnNotFound(name.clone()))?;
    }
    Ok(update)
}

fn build_changeset_delete_from_maxwell<T, S, B, A>(
    data: &BTreeMap<String, serde_json::Value>,
    columns_types: Option<&BTreeMap<String, String>>,
    table: &T,
    adapter: &A,
) -> Result<ChangeDelete<T, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + Default + AsRef<str>,
    B: Clone + Default + AsRef<[u8]>,
    A: WireAdapter<Maxwell, S, B>,
{
    let mut delete = ChangeDelete::from(table.clone());
    for (name, value) in data {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;
        let payload = MaxwellColumn {
            column_name: name.as_str(),
            mysql_type: columns_types.and_then(|m| m.get(name)).map(String::as_str),
            value,
        };
        let decoded = adapter.decode(payload)?;
        delete = delete
            .set(col_idx, decoded)
            .map_err(|_| ConversionError::ColumnNotFound(name.clone()))?;
    }
    Ok(delete)
}

fn build_patch_delete_from_maxwell<T, S, B, A>(
    data: &BTreeMap<String, serde_json::Value>,
    columns_types: Option<&BTreeMap<String, String>>,
    table: &T,
    adapter: &A,
) -> Result<PatchDelete<T, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<Maxwell, S, B>,
{
    let num_pks = table.number_of_primary_keys();
    let mut pk_slots: Vec<Option<Value<S, B>>> = alloc::vec![None; num_pks];

    for (name, value) in data {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;
        if let Some(pk_idx) = table.primary_key_index(col_idx) {
            let payload = MaxwellColumn {
                column_name: name.as_str(),
                mysql_type: columns_types.and_then(|m| m.get(name)).map(String::as_str),
                value,
            };
            pk_slots[pk_idx] = Some(adapter.decode(payload)?);
        }
    }

    let pk = pk_slots
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or(ConversionError::MissingData("pk", "DELETE"))?;
    Ok(PatchDelete::new(table.clone(), pk))
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
                columns_types: None,
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
    fn test_cdc_default_indirect_false() {
        let table = products_table();
        let msg = parse(INSERT_JSON).unwrap();
        let insert: Insert<_, String, Vec<u8>> = (&msg, &table).try_into().unwrap();
        assert!(!insert.indirect);
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

    // ---- Additional branch coverage ----

    #[test]
    fn test_update_invalid_operation() {
        let table = products_table();
        let msg = parse(INSERT_JSON).unwrap();
        let result: Result<crate::ChangeUpdate<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::InvalidOperation(_))));
    }

    #[test]
    fn test_update_table_mismatch() {
        let table = users_table();
        let msg = parse(UPDATE_JSON).unwrap();
        let result: Result<crate::ChangeUpdate<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_changedelete_invalid_operation() {
        let table = products_table();
        let msg = parse(INSERT_JSON).unwrap();
        let result: Result<ChangeDelete<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::InvalidOperation(_))));
    }

    #[test]
    fn test_changedelete_table_mismatch() {
        let table = users_table();
        let msg = parse(DELETE_JSON).unwrap();
        let result: Result<ChangeDelete<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_patchdelete_invalid_operation() {
        let table = products_table();
        let msg = parse(INSERT_JSON).unwrap();
        let result: Result<PatchDelete<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::InvalidOperation(_))));
    }

    #[test]
    fn test_patchdelete_table_mismatch() {
        let table = users_table();
        let msg = parse(DELETE_JSON).unwrap();
        let result: Result<PatchDelete<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_update_no_old_values() {
        // An update without an "old" map: every column path uses
        // new value for both old and new (the no-change branch).
        let json = r#"{"database":"db","table":"t","type":"update","data":{"id":1,"v":2}}"#;
        let table = SimpleTable::new("t", &["id", "v"], &[0]);
        let msg = parse(json).unwrap();
        let update: crate::ChangeUpdate<_, String, Vec<u8>> = (&msg, &table).try_into().unwrap();
        let values = update.values();
        assert_eq!(values.len(), 2);
        // For columns with no "old" entry, old and new should match.
        assert_eq!(values[0].0, Some(Value::Integer(1)));
        assert_eq!(values[0].1, Some(Value::Integer(1)));
    }

    #[test]
    fn test_float_value() {
        let json = r#"{"database":"db","table":"t","type":"insert","data":{"id":1,"price":9.99}}"#;
        let table = SimpleTable::new("t", &["id", "price"], &[0]);
        let msg = parse(json).unwrap();
        let insert: Insert<_, String, Vec<u8>> = (&msg, &table).try_into().unwrap();
        let values = insert.into_values();
        assert_eq!(values[1], Value::Real(9.99));
    }
}
