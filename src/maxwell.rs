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

    /// Table named in the wire event is not in the schema.
    #[error("Table '{0}' not found in schema")]
    TableNotFound(String),

    /// The event is missing required data for the operation.
    #[error("Missing {0} data for {1} operation")]
    MissingData(&'static str, &'static str),

    /// A JSON value type is not supported for conversion.
    #[error("Unsupported JSON value type for column '{0}'")]
    UnsupportedType(String),

    /// The operation type is not applicable for the requested conversion.
    #[error("Operation '{0}' cannot be converted to the requested type")]
    InvalidOperation(String),

    /// User-registered decoder rejected a column payload.
    #[error("Decoder failed: {0}")]
    Decode(#[from] crate::wire::DecodeError),
}

use crate::ChangesetFormat;
use crate::builders::{ChangeDelete, Insert, PatchDelete, Update};
use crate::encoding::Value;
use crate::schema::NamedColumns;

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

use crate::wire::{Digestable, WireColumnTypes, WireSchema};

impl<T, S, B> Digestable<ChangesetFormat, T, S, B> for Message
where
    T: NamedColumns + WireColumnTypes<Maxwell>,
    S: Clone + Debug + Hash + Eq + AsRef<str> + Default,
    B: Clone + Debug + Hash + Eq + AsRef<[u8]> + Default,
{
    type Src = Maxwell;
    type Error = ConversionError;

    fn digest_into<Sch, A>(
        &self,
        builder: DiffSetBuilder<ChangesetFormat, T, S, B>,
        schema: &Sch,
        adapter: &A,
    ) -> Result<DiffSetBuilder<ChangesetFormat, T, S, B>, ConversionError>
    where
        Sch: WireSchema<Maxwell, Table = T>,
        A: WireAdapter<Maxwell, S, B>,
    {
        let table = resolve_table(schema, self.table.as_str())?;
        match self.op_type {
            OpType::Insert => {
                let insert = build_insert_from_maxwell(&self.data, table, adapter)?;
                Ok(DiffOps::insert(builder, insert))
            }
            OpType::Update => {
                let update = build_changeset_update_from_maxwell(
                    &self.data,
                    self.old.as_ref(),
                    table,
                    adapter,
                )?;
                Ok(DiffOps::update(builder, update))
            }
            OpType::Delete => {
                let delete = build_changeset_delete_from_maxwell(&self.data, table, adapter)?;
                Ok(DiffOps::delete(builder, delete))
            }
        }
    }
}

impl<T, S, B> Digestable<PatchsetFormat, T, S, B> for Message
where
    T: NamedColumns + WireColumnTypes<Maxwell>,
    S: Clone + Debug + Hash + Eq + AsRef<str> + Default,
    B: Clone + Debug + Hash + Eq + AsRef<[u8]> + Default,
{
    type Src = Maxwell;
    type Error = ConversionError;

    fn digest_into<Sch, A>(
        &self,
        builder: DiffSetBuilder<PatchsetFormat, T, S, B>,
        schema: &Sch,
        adapter: &A,
    ) -> Result<DiffSetBuilder<PatchsetFormat, T, S, B>, ConversionError>
    where
        Sch: WireSchema<Maxwell, Table = T>,
        A: WireAdapter<Maxwell, S, B>,
    {
        let table = resolve_table(schema, self.table.as_str())?;
        match self.op_type {
            OpType::Insert => {
                let insert = build_insert_from_maxwell(&self.data, table, adapter)?;
                Ok(DiffOps::insert(builder, insert))
            }
            OpType::Update => {
                let update = build_patchset_update_from_maxwell(&self.data, table, adapter)?;
                Ok(DiffOps::update(builder, update))
            }
            OpType::Delete => {
                let delete = build_patch_delete_from_maxwell(&self.data, table, adapter)?;
                Ok(DiffOps::delete(builder, delete))
            }
        }
    }
}

fn resolve_table<'a, Sch>(schema: &'a Sch, name: &str) -> Result<&'a Sch::Table, ConversionError>
where
    Sch: WireSchema<Maxwell>,
{
    schema
        .get(name)
        .ok_or_else(|| ConversionError::TableNotFound(name.into()))
}

fn build_insert_from_maxwell<T, S, B, A>(
    data: &BTreeMap<String, serde_json::Value>,
    table: &T,
    adapter: &A,
) -> Result<Insert<T, S, B>, ConversionError>
where
    T: NamedColumns + WireColumnTypes<Maxwell>,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<Maxwell, S, B>,
{
    let mut insert = Insert::from(table.clone());
    for (name, value) in data {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;
        let type_key = table.column_type_key(col_idx);
        let payload = MaxwellColumn {
            column_name: name.as_str(),
            mysql_type: Some(type_key.as_ref()),
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
    table: &T,
    adapter: &A,
) -> Result<Update<T, ChangesetFormat, S, B>, ConversionError>
where
    T: NamedColumns + WireColumnTypes<Maxwell>,
    S: Clone + Debug + AsRef<str>,
    B: Clone + Debug + AsRef<[u8]>,
    A: WireAdapter<Maxwell, S, B>,
{
    let mut update: Update<T, ChangesetFormat, S, B> = Update::from(table.clone());
    for (name, new_value) in data {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;
        let type_key = table.column_type_key(col_idx);

        let new_payload = MaxwellColumn {
            column_name: name.as_str(),
            mysql_type: Some(type_key.as_ref()),
            value: new_value,
        };
        let new = adapter.decode(new_payload)?;

        if let Some(old_map) = old
            && let Some(old_value) = old_map.get(name)
        {
            let old_payload = MaxwellColumn {
                column_name: name.as_str(),
                mysql_type: Some(type_key.as_ref()),
                value: old_value,
            };
            let old = adapter.decode(old_payload)?;
            update = update
                .set(col_idx, old, new)
                .map_err(|_| ConversionError::ColumnNotFound(name.clone()))?;
            continue;
        }

        update = update
            .set_new(col_idx, new)
            .map_err(|_| ConversionError::ColumnNotFound(name.clone()))?;
    }
    Ok(update)
}

fn build_patchset_update_from_maxwell<T, S, B, A>(
    data: &BTreeMap<String, serde_json::Value>,
    table: &T,
    adapter: &A,
) -> Result<Update<T, PatchsetFormat, S, B>, ConversionError>
where
    T: NamedColumns + WireColumnTypes<Maxwell>,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<Maxwell, S, B>,
{
    let mut update: Update<T, PatchsetFormat, S, B> = Update::from(table.clone());
    for (name, value) in data {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;
        let type_key = table.column_type_key(col_idx);
        let payload = MaxwellColumn {
            column_name: name.as_str(),
            mysql_type: Some(type_key.as_ref()),
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
    table: &T,
    adapter: &A,
) -> Result<ChangeDelete<T, S, B>, ConversionError>
where
    T: NamedColumns + WireColumnTypes<Maxwell>,
    S: Clone + Default + AsRef<str>,
    B: Clone + Default + AsRef<[u8]>,
    A: WireAdapter<Maxwell, S, B>,
{
    let mut delete = ChangeDelete::from(table.clone());
    for (name, value) in data {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;
        let type_key = table.column_type_key(col_idx);
        let payload = MaxwellColumn {
            column_name: name.as_str(),
            mysql_type: Some(type_key.as_ref()),
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
    table: &T,
    adapter: &A,
) -> Result<PatchDelete<T, S, B>, ConversionError>
where
    T: NamedColumns + WireColumnTypes<Maxwell>,
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
            let type_key = table.column_type_key(col_idx);
            let payload = MaxwellColumn {
                column_name: name.as_str(),
                mysql_type: Some(type_key.as_ref()),
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
