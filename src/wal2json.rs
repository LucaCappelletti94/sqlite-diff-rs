//! wal2json message parsing and conversion to `SQLite` changeset operations.
//!
//! [wal2json](https://github.com/eulerto/wal2json) is a `PostgreSQL` logical
//! replication output plugin. This module deserializes its messages and
//! converts them into changeset operations compatible with this crate's
//! builders.
//!
//! Two formats are supported: v1 emits one transaction-level JSON object with
//! a full `change` array, and v2 emits one JSON object per row tuple.
//!
//! Wal2json does not carry trigger-origin metadata, so converted ops default
//! to `indirect = false`. Override via the [`Indirect`](crate::Indirect) trait
//! if you know out-of-band that the event was trigger-induced.
//!
//! # Example
//!
//! ```
//! use sqlite_diff_rs::wal2json::{parse_v2, MessageV2, Action};
//!
//! let json = r#"{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"Alice"}]}"#;
//! let msg = parse_v2(json).unwrap();
//!
//! assert_eq!(msg.action, Action::I);
//! assert_eq!(msg.table.as_deref(), Some("users"));
//! ```

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use serde::{Deserialize, Serialize};

/// wal2json v2 action type.
///
/// Represents the type of database operation captured by wal2json.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    /// Begin transaction.
    B,
    /// Commit transaction.
    C,
    /// Insert operation.
    I,
    /// Update operation.
    U,
    /// Delete operation.
    D,
    /// Truncate operation.
    T,
    /// Message (user-defined).
    M,
}

/// A column in wal2json output.
///
/// Contains the column name, `PostgreSQL` type name, and the value as JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    /// Column name.
    pub name: String,
    /// `PostgreSQL` type name (e.g., "integer", "text", "boolean").
    #[serde(rename = "type")]
    pub type_name: String,
    /// The column value as a JSON value.
    pub value: serde_json::Value,
}

/// wal2json v2 message (one per tuple).
///
/// In v2 format, each database change produces a separate JSON object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageV2 {
    /// The action type (B, C, I, U, D, T, M).
    pub action: Action,
    /// Schema name (e.g., "public").
    #[serde(default)]
    pub schema: Option<String>,
    /// Table name.
    #[serde(default)]
    pub table: Option<String>,
    /// Column values for the new row (INSERT, UPDATE).
    #[serde(default)]
    pub columns: Option<Vec<Column>>,
    /// Identity columns for the old row (UPDATE, DELETE).
    #[serde(default)]
    pub identity: Option<Vec<Column>>,
}

/// Old key information for v1 updates/deletes.
///
/// Contains the primary key column names, types, and values that identify
/// the row being updated or deleted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OldKeys {
    /// Primary key column names.
    pub keynames: Vec<String>,
    /// Primary key column `PostgreSQL` types.
    pub keytypes: Vec<String>,
    /// Primary key column values.
    pub keyvalues: Vec<serde_json::Value>,
}

/// wal2json v1 change entry.
///
/// In v1 format, all changes for a transaction are grouped together.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeV1 {
    /// The kind of change: "insert", "update", or "delete".
    pub kind: String,
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Column names (in order).
    #[serde(default)]
    pub columnnames: Vec<String>,
    /// Column `PostgreSQL` types (in order).
    #[serde(default)]
    pub columntypes: Vec<String>,
    /// Column values (in order).
    #[serde(default)]
    pub columnvalues: Vec<serde_json::Value>,
    /// Old key information for UPDATE/DELETE operations.
    #[serde(default)]
    pub oldkeys: Option<OldKeys>,
}

/// wal2json v1 transaction wrapper.
///
/// Contains all changes that occurred within a single transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionV1 {
    /// The list of changes in this transaction.
    pub change: Vec<ChangeV1>,
}

/// Parse a wal2json v2 message from a JSON line.
///
/// # Errors
///
/// Returns a [`serde_json::Error`] if the JSON is malformed.
pub fn parse_v2(line: &str) -> Result<MessageV2, serde_json::Error> {
    serde_json::from_str(line)
}

/// Parse a wal2json v1 transaction from JSON.
///
/// # Errors
///
/// Returns a [`serde_json::Error`] if the JSON is malformed.
pub fn parse_v1(json: &str) -> Result<TransactionV1, serde_json::Error> {
    serde_json::from_str(json)
}

/// Errors during wal2json to changeset conversion.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConversionError {
    /// A column name from wal2json was not found in the table schema.
    #[error("Column '{0}' not found in table schema")]
    ColumnNotFound(String),

    /// The table name in the message doesn't match the expected schema.
    #[error("Table name mismatch: expected '{expected}', got '{actual}'")]
    TableMismatch {
        /// Expected table name from the schema.
        expected: String,
        /// Actual table name from the wal2json message.
        actual: String,
    },

    /// The message is missing required column data.
    #[error("Missing columns in message")]
    MissingColumns,

    /// A JSON value type is not supported for conversion.
    #[error("Unsupported JSON value type for column '{0}'")]
    UnsupportedType(String),

    /// A schema-aware decoder rejected a column payload. Populated by
    /// `DiffSetBuilder::digest_wal2json_v2` and
    /// `DiffSetBuilder::digest_wal2json_v1_change` when the user's
    /// registered decoder returns [`crate::wire::DecodeError`].
    #[error("Decoder failed: {0}")]
    Decode(#[from] crate::wire::DecodeError),
}

use crate::wire::{Sealed, WireSource};

/// Marker type for the `wal2json` source.
#[derive(Debug, Clone, Copy, Default)]
pub struct Wal2Json;

impl Sealed for Wal2Json {}

impl WireSource for Wal2Json {
    type Payload<'a> = Wal2JsonColumn<'a>;
    type TypeKey = Arc<str>;

    fn type_key(payload: &Self::Payload<'_>) -> Self::TypeKey {
        Arc::from(payload.pg_type_name)
    }

    fn column_name<'a>(payload: &'a Self::Payload<'_>) -> &'a str {
        payload.column_name
    }
}

/// Per-column payload for the `wal2json` source.
///
/// v2 populates from [`Column`] fields directly. v1 populates from the
/// parallel `columnnames`/`columntypes`/`columnvalues` arrays on
/// [`ChangeV1`].
#[derive(Debug, Clone, Copy)]
pub struct Wal2JsonColumn<'a> {
    /// Column name.
    pub column_name: &'a str,
    /// Postgres type name as emitted by wal2json (e.g. "integer",
    /// "text", "uuid", "timestamp with time zone").
    pub pg_type_name: &'a str,
    /// Column value as a JSON value.
    pub value: &'a serde_json::Value,
}

use crate::ChangesetFormat;
use crate::builders::{ChangeDelete, Insert, PatchDelete, Update};
use crate::encoding::Value;
use crate::schema::NamedColumns;

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

impl<T: NamedColumns + Clone> TryFrom<(&ChangeV1, &T)> for Insert<T, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((change, table): (&ChangeV1, &T)) -> Result<Self, Self::Error> {
        // Verify table name matches
        if table.name() != change.table {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: change.table.clone(),
            });
        }

        let mut insert = Insert::from(table.clone());

        // Map each column from the change to the table schema
        for (name, value) in change.columnnames.iter().zip(change.columnvalues.iter()) {
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

impl<T: NamedColumns + Clone> TryFrom<(&ChangeV1, &T)>
    for Update<T, ChangesetFormat, String, Vec<u8>>
{
    type Error = ConversionError;

    fn try_from((change, table): (&ChangeV1, &T)) -> Result<Self, Self::Error> {
        // Verify table name matches
        if table.name() != change.table {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: change.table.clone(),
            });
        }

        let mut update = Update::from(table.clone());

        // Set new values from columnvalues
        for (name, value) in change.columnnames.iter().zip(change.columnvalues.iter()) {
            let col_idx = table
                .column_index(name)
                .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;

            let converted = json_to_value(value, name)?;
            update = update
                .set_new(col_idx, converted)
                .map_err(|_| ConversionError::ColumnNotFound(name.clone()))?;
        }

        Ok(update)
    }
}

impl<T: NamedColumns + Clone> TryFrom<(&ChangeV1, &T)> for ChangeDelete<T, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((change, table): (&ChangeV1, &T)) -> Result<Self, Self::Error> {
        // Verify table name matches
        if table.name() != change.table {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: change.table.clone(),
            });
        }

        let mut delete = ChangeDelete::from(table.clone());

        // For deletes, we use the oldkeys to identify the row
        if let Some(ref oldkeys) = change.oldkeys {
            for (name, value) in oldkeys.keynames.iter().zip(oldkeys.keyvalues.iter()) {
                let col_idx = table
                    .column_index(name)
                    .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;

                let converted = json_to_value(value, name)?;
                delete = delete
                    .set(col_idx, converted)
                    .map_err(|_| ConversionError::ColumnNotFound(name.clone()))?;
            }
        } else {
            // If no oldkeys, try to use columnvalues (for full row logging)
            for (name, value) in change.columnnames.iter().zip(change.columnvalues.iter()) {
                let col_idx = table
                    .column_index(name)
                    .ok_or_else(|| ConversionError::ColumnNotFound(name.clone()))?;

                let converted = json_to_value(value, name)?;
                delete = delete
                    .set(col_idx, converted)
                    .map_err(|_| ConversionError::ColumnNotFound(name.clone()))?;
            }
        }

        Ok(delete)
    }
}

impl<T: NamedColumns + Clone> TryFrom<(&ChangeV1, &T)> for PatchDelete<T, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((change, table): (&ChangeV1, &T)) -> Result<Self, Self::Error> {
        // Verify table name matches
        if table.name() != change.table {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: change.table.clone(),
            });
        }

        let oldkeys = change
            .oldkeys
            .as_ref()
            .ok_or(ConversionError::MissingColumns)?;

        // Extract primary key values in schema order
        let num_pks = table.number_of_primary_keys();
        let mut pk_values: Vec<Option<Value<String, Vec<u8>>>> = alloc::vec![None; num_pks];

        for (name, value) in oldkeys.keynames.iter().zip(oldkeys.keyvalues.iter()) {
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
            .ok_or(ConversionError::MissingColumns)?;

        Ok(PatchDelete::new(table.clone(), pk))
    }
}

// V2 format conversions

impl<T: NamedColumns + Clone> TryFrom<(&MessageV2, &T)> for Insert<T, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((msg, table): (&MessageV2, &T)) -> Result<Self, Self::Error> {
        // Verify table name matches
        if let Some(ref msg_table) = msg.table
            && table.name() != msg_table
        {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: msg_table.clone(),
            });
        }

        let columns = msg
            .columns
            .as_ref()
            .ok_or(ConversionError::MissingColumns)?;

        let mut insert = Insert::from(table.clone());

        for col in columns {
            let col_idx = table
                .column_index(&col.name)
                .ok_or_else(|| ConversionError::ColumnNotFound(col.name.clone()))?;

            let converted = json_to_value(&col.value, &col.name)?;
            insert = insert
                .set(col_idx, converted)
                .map_err(|_| ConversionError::ColumnNotFound(col.name.clone()))?;
        }

        Ok(insert)
    }
}

impl<T: NamedColumns + Clone> TryFrom<(&MessageV2, &T)>
    for Update<T, ChangesetFormat, String, Vec<u8>>
{
    type Error = ConversionError;

    fn try_from((msg, table): (&MessageV2, &T)) -> Result<Self, Self::Error> {
        // Verify table name matches
        if let Some(ref msg_table) = msg.table
            && table.name() != msg_table
        {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: msg_table.clone(),
            });
        }

        let columns = msg
            .columns
            .as_ref()
            .ok_or(ConversionError::MissingColumns)?;

        let mut update = Update::from(table.clone());

        // Set new values from columns
        for col in columns {
            let col_idx = table
                .column_index(&col.name)
                .ok_or_else(|| ConversionError::ColumnNotFound(col.name.clone()))?;

            let converted = json_to_value(&col.value, &col.name)?;
            update = update
                .set_new(col_idx, converted)
                .map_err(|_| ConversionError::ColumnNotFound(col.name.clone()))?;
        }

        Ok(update)
    }
}

impl<T: NamedColumns + Clone> TryFrom<(&MessageV2, &T)> for ChangeDelete<T, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((msg, table): (&MessageV2, &T)) -> Result<Self, Self::Error> {
        // Verify table name matches
        if let Some(ref msg_table) = msg.table
            && table.name() != msg_table
        {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: msg_table.clone(),
            });
        }

        let identity = msg
            .identity
            .as_ref()
            .ok_or(ConversionError::MissingColumns)?;

        let mut delete = ChangeDelete::from(table.clone());

        for col in identity {
            let col_idx = table
                .column_index(&col.name)
                .ok_or_else(|| ConversionError::ColumnNotFound(col.name.clone()))?;

            let converted = json_to_value(&col.value, &col.name)?;
            delete = delete
                .set(col_idx, converted)
                .map_err(|_| ConversionError::ColumnNotFound(col.name.clone()))?;
        }

        Ok(delete)
    }
}

impl<T: NamedColumns + Clone> TryFrom<(&MessageV2, &T)> for PatchDelete<T, String, Vec<u8>> {
    type Error = ConversionError;

    fn try_from((msg, table): (&MessageV2, &T)) -> Result<Self, Self::Error> {
        // Verify table name matches
        if let Some(ref msg_table) = msg.table
            && table.name() != msg_table
        {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: msg_table.clone(),
            });
        }

        let identity = msg
            .identity
            .as_ref()
            .ok_or(ConversionError::MissingColumns)?;

        // Extract primary key values in schema order
        let num_pks = table.number_of_primary_keys();
        let mut pk_values: Vec<Option<Value<String, Vec<u8>>>> = alloc::vec![None; num_pks];

        for col in identity {
            let col_idx = table
                .column_index(&col.name)
                .ok_or_else(|| ConversionError::ColumnNotFound(col.name.clone()))?;

            if let Some(pk_idx) = table.primary_key_index(col_idx) {
                let converted = json_to_value(&col.value, &col.name)?;
                pk_values[pk_idx] = Some(converted);
            }
        }

        // Verify all PKs are present and collect them
        let pk: Vec<Value<String, Vec<u8>>> = pk_values
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(ConversionError::MissingColumns)?;

        Ok(PatchDelete::new(table.clone(), pk))
    }
}

// ============================================================================
// Schema-aware digest fns (0.2.0+).
//
// These are the intended entry points. Users construct a
// `wire::TypeMap<Wal2Json, S, B>` (or any other `WireAdapter`) and hand
// it to the digest fn, which iterates the wire message, populates
// per-column `Wal2JsonColumn` payloads, and dispatches through the
// adapter into `Value<S, B>` before feeding operations into the
// consolidation pipeline via `DiffOps`.
//
// The `TryFrom` impls above are the 0.1.x sniffer path; they remain in
// place during 0.2.0 development and are deleted in Phase 11 as the
// release step.
// ============================================================================

use crate::builders::{DiffOps, DiffSetBuilder, PatchsetFormat};
use crate::wire::WireAdapter;
use alloc::boxed::Box;
use core::fmt::Debug;
use core::hash::Hash;

#[inline]
fn wal2json_table_mismatch(expected: &str, actual_opt: Option<&str>) -> Option<ConversionError> {
    match actual_opt {
        Some(actual) if actual != expected => Some(ConversionError::TableMismatch {
            expected: expected.into(),
            actual: actual.into(),
        }),
        _ => None,
    }
}

impl<T, S, B> DiffSetBuilder<ChangesetFormat, T, S, B>
where
    T: NamedColumns + Clone,
    S: Clone + Debug + Hash + Eq + AsRef<str> + Default,
    B: Clone + Debug + Hash + Eq + AsRef<[u8]> + Default,
{
    /// Digest a wal2json v2 message (one row per JSON object) into a
    /// changeset via the supplied `adapter`.
    ///
    /// # Errors
    ///
    /// Returns [`ConversionError::TableMismatch`] if `msg.table` is set
    /// and does not match `table.name()`, [`ConversionError::MissingColumns`]
    /// if the operation is missing required column data, and
    /// [`ConversionError::ColumnNotFound`] for column names absent from
    /// the schema. Adapter failures surface as
    /// [`ConversionError::Decode`].
    pub fn digest_wal2json_v2<A>(
        self,
        msg: &MessageV2,
        table: &T,
        adapter: &A,
    ) -> Result<Self, ConversionError>
    where
        A: WireAdapter<Wal2Json, S, B>,
    {
        if let Some(err) = wal2json_table_mismatch(table.name(), msg.table.as_deref()) {
            return Err(err);
        }

        match msg.action {
            Action::I => {
                let columns = msg
                    .columns
                    .as_ref()
                    .ok_or(ConversionError::MissingColumns)?;
                let insert = build_insert_from_v2(columns, table, adapter)?;
                Ok(DiffOps::insert(self, insert))
            }
            Action::U => {
                let columns = msg
                    .columns
                    .as_ref()
                    .ok_or(ConversionError::MissingColumns)?;
                let update = build_changeset_update_from_v2(columns, table, adapter)?;
                Ok(DiffOps::update(self, update))
            }
            Action::D => {
                let identity = msg
                    .identity
                    .as_ref()
                    .ok_or(ConversionError::MissingColumns)?;
                let delete = build_changeset_delete_from_columns(identity, table, adapter)?;
                Ok(DiffOps::delete(self, delete))
            }
            Action::B | Action::C | Action::T | Action::M => Ok(self),
        }
    }

    /// Digest one wal2json v1 change (one row) into a changeset via the
    /// supplied `adapter`.
    ///
    /// # Errors
    ///
    /// See [`Self::digest_wal2json_v2`].
    pub fn digest_wal2json_v1_change<A>(
        self,
        change: &ChangeV1,
        table: &T,
        adapter: &A,
    ) -> Result<Self, ConversionError>
    where
        A: WireAdapter<Wal2Json, S, B>,
    {
        if let Some(err) = wal2json_table_mismatch(table.name(), Some(change.table.as_str())) {
            return Err(err);
        }

        match change.kind.as_str() {
            "insert" => {
                let insert = build_insert_from_v1(change, table, adapter)?;
                Ok(DiffOps::insert(self, insert))
            }
            "update" => {
                let update = build_changeset_update_from_v1(change, table, adapter)?;
                Ok(DiffOps::update(self, update))
            }
            "delete" => {
                let delete = build_changeset_delete_from_v1(change, table, adapter)?;
                Ok(DiffOps::delete(self, delete))
            }
            _ => Ok(self),
        }
    }
}

impl<T, S, B> DiffSetBuilder<PatchsetFormat, T, S, B>
where
    T: NamedColumns + Clone,
    S: Clone + Debug + Hash + Eq + AsRef<str> + Default,
    B: Clone + Debug + Hash + Eq + AsRef<[u8]> + Default,
{
    /// Patchset counterpart of
    /// [`digest_wal2json_v2`](DiffSetBuilder::digest_wal2json_v2).
    ///
    /// # Errors
    ///
    /// See the changeset variant.
    pub fn digest_wal2json_v2<A>(
        self,
        msg: &MessageV2,
        table: &T,
        adapter: &A,
    ) -> Result<Self, ConversionError>
    where
        A: WireAdapter<Wal2Json, S, B>,
    {
        if let Some(err) = wal2json_table_mismatch(table.name(), msg.table.as_deref()) {
            return Err(err);
        }

        match msg.action {
            Action::I => {
                let columns = msg
                    .columns
                    .as_ref()
                    .ok_or(ConversionError::MissingColumns)?;
                let insert = build_insert_from_v2(columns, table, adapter)?;
                Ok(DiffOps::insert(self, insert))
            }
            Action::U => {
                let columns = msg
                    .columns
                    .as_ref()
                    .ok_or(ConversionError::MissingColumns)?;
                let update = build_patchset_update_from_v2(columns, table, adapter)?;
                Ok(DiffOps::update(self, update))
            }
            Action::D => {
                let identity = msg
                    .identity
                    .as_ref()
                    .ok_or(ConversionError::MissingColumns)?;
                let delete = build_patch_delete_from_columns(identity, table, adapter)?;
                Ok(DiffOps::delete(self, delete))
            }
            Action::B | Action::C | Action::T | Action::M => Ok(self),
        }
    }

    /// Patchset counterpart of
    /// [`digest_wal2json_v1_change`](DiffSetBuilder::digest_wal2json_v1_change).
    ///
    /// # Errors
    ///
    /// See the changeset variant.
    pub fn digest_wal2json_v1_change<A>(
        self,
        change: &ChangeV1,
        table: &T,
        adapter: &A,
    ) -> Result<Self, ConversionError>
    where
        A: WireAdapter<Wal2Json, S, B>,
    {
        if let Some(err) = wal2json_table_mismatch(table.name(), Some(change.table.as_str())) {
            return Err(err);
        }

        match change.kind.as_str() {
            "insert" => {
                let insert = build_insert_from_v1(change, table, adapter)?;
                Ok(DiffOps::insert(self, insert))
            }
            "update" => {
                let update = build_patchset_update_from_v1(change, table, adapter)?;
                Ok(DiffOps::update(self, update))
            }
            "delete" => {
                let delete = build_patch_delete_from_v1(change, table, adapter)?;
                Ok(DiffOps::delete(self, delete))
            }
            _ => Ok(self),
        }
    }
}

// -- v2 helpers ---------------------------------------------------------------

fn build_insert_from_v2<T, S, B, A>(
    columns: &[Column],
    table: &T,
    adapter: &A,
) -> Result<Insert<T, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let mut insert = Insert::from(table.clone());
    for col in columns {
        let col_idx = table
            .column_index(&col.name)
            .ok_or_else(|| ConversionError::ColumnNotFound(col.name.clone()))?;
        let payload = Wal2JsonColumn {
            column_name: col.name.as_str(),
            pg_type_name: col.type_name.as_str(),
            value: &col.value,
        };
        let value = adapter.decode(payload)?;
        insert = insert
            .set(col_idx, value)
            .map_err(|_| ConversionError::ColumnNotFound(col.name.clone()))?;
    }
    Ok(insert)
}

fn build_changeset_update_from_v2<T, S, B, A>(
    columns: &[Column],
    table: &T,
    adapter: &A,
) -> Result<Update<T, ChangesetFormat, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + Debug + AsRef<str>,
    B: Clone + Debug + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let mut update: Update<T, ChangesetFormat, S, B> = Update::from(table.clone());
    for col in columns {
        let col_idx = table
            .column_index(&col.name)
            .ok_or_else(|| ConversionError::ColumnNotFound(col.name.clone()))?;
        let payload = Wal2JsonColumn {
            column_name: col.name.as_str(),
            pg_type_name: col.type_name.as_str(),
            value: &col.value,
        };
        let new = adapter.decode(payload)?;
        update = update
            .set_new(col_idx, new)
            .map_err(|_| ConversionError::ColumnNotFound(col.name.clone()))?;
    }
    Ok(update)
}

fn build_patchset_update_from_v2<T, S, B, A>(
    columns: &[Column],
    table: &T,
    adapter: &A,
) -> Result<Update<T, PatchsetFormat, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let mut update: Update<T, PatchsetFormat, S, B> = Update::from(table.clone());
    for col in columns {
        let col_idx = table
            .column_index(&col.name)
            .ok_or_else(|| ConversionError::ColumnNotFound(col.name.clone()))?;
        let payload = Wal2JsonColumn {
            column_name: col.name.as_str(),
            pg_type_name: col.type_name.as_str(),
            value: &col.value,
        };
        let new = adapter.decode(payload)?;
        update = update
            .set(col_idx, new)
            .map_err(|_| ConversionError::ColumnNotFound(col.name.clone()))?;
    }
    Ok(update)
}

fn build_changeset_delete_from_columns<T, S, B, A>(
    identity: &[Column],
    table: &T,
    adapter: &A,
) -> Result<ChangeDelete<T, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + Default + AsRef<str>,
    B: Clone + Default + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let mut delete = ChangeDelete::from(table.clone());
    for col in identity {
        let col_idx = table
            .column_index(&col.name)
            .ok_or_else(|| ConversionError::ColumnNotFound(col.name.clone()))?;
        let payload = Wal2JsonColumn {
            column_name: col.name.as_str(),
            pg_type_name: col.type_name.as_str(),
            value: &col.value,
        };
        let value = adapter.decode(payload)?;
        delete = delete
            .set(col_idx, value)
            .map_err(|_| ConversionError::ColumnNotFound(col.name.clone()))?;
    }
    Ok(delete)
}

fn build_patch_delete_from_columns<T, S, B, A>(
    identity: &[Column],
    table: &T,
    adapter: &A,
) -> Result<PatchDelete<T, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let num_pks = table.number_of_primary_keys();
    let mut pk_slots: Vec<Option<Value<S, B>>> = alloc::vec![None; num_pks];

    for col in identity {
        let col_idx = table
            .column_index(&col.name)
            .ok_or_else(|| ConversionError::ColumnNotFound(col.name.clone()))?;
        if let Some(pk_idx) = table.primary_key_index(col_idx) {
            let payload = Wal2JsonColumn {
                column_name: col.name.as_str(),
                pg_type_name: col.type_name.as_str(),
                value: &col.value,
            };
            pk_slots[pk_idx] = Some(adapter.decode(payload)?);
        }
    }

    let pk = pk_slots
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or(ConversionError::MissingColumns)?;
    Ok(PatchDelete::new(table.clone(), pk))
}

// -- v1 helpers ---------------------------------------------------------------

fn iter_v1_columns(
    change: &ChangeV1,
) -> impl Iterator<Item = (&str, &str, &serde_json::Value)> + '_ {
    change
        .columnnames
        .iter()
        .zip(change.columntypes.iter())
        .zip(change.columnvalues.iter())
        .map(|((n, t), v)| (n.as_str(), t.as_str(), v))
}

fn iter_v1_oldkeys(
    oldkeys: &OldKeys,
) -> impl Iterator<Item = (&str, &str, &serde_json::Value)> + '_ {
    oldkeys
        .keynames
        .iter()
        .zip(oldkeys.keytypes.iter())
        .zip(oldkeys.keyvalues.iter())
        .map(|((n, t), v)| (n.as_str(), t.as_str(), v))
}

fn build_insert_from_v1<T, S, B, A>(
    change: &ChangeV1,
    table: &T,
    adapter: &A,
) -> Result<Insert<T, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let mut insert = Insert::from(table.clone());
    for (name, type_name, value) in iter_v1_columns(change) {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.into()))?;
        let payload = Wal2JsonColumn {
            column_name: name,
            pg_type_name: type_name,
            value,
        };
        let decoded = adapter.decode(payload)?;
        insert = insert
            .set(col_idx, decoded)
            .map_err(|_| ConversionError::ColumnNotFound(name.into()))?;
    }
    Ok(insert)
}

fn build_changeset_update_from_v1<T, S, B, A>(
    change: &ChangeV1,
    table: &T,
    adapter: &A,
) -> Result<Update<T, ChangesetFormat, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + Debug + AsRef<str>,
    B: Clone + Debug + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let mut update: Update<T, ChangesetFormat, S, B> = Update::from(table.clone());
    for (name, type_name, value) in iter_v1_columns(change) {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.into()))?;
        let payload = Wal2JsonColumn {
            column_name: name,
            pg_type_name: type_name,
            value,
        };
        let new = adapter.decode(payload)?;
        update = update
            .set_new(col_idx, new)
            .map_err(|_| ConversionError::ColumnNotFound(name.into()))?;
    }
    Ok(update)
}

fn build_patchset_update_from_v1<T, S, B, A>(
    change: &ChangeV1,
    table: &T,
    adapter: &A,
) -> Result<Update<T, PatchsetFormat, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let mut update: Update<T, PatchsetFormat, S, B> = Update::from(table.clone());
    for (name, type_name, value) in iter_v1_columns(change) {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.into()))?;
        let payload = Wal2JsonColumn {
            column_name: name,
            pg_type_name: type_name,
            value,
        };
        let new = adapter.decode(payload)?;
        update = update
            .set(col_idx, new)
            .map_err(|_| ConversionError::ColumnNotFound(name.into()))?;
    }
    Ok(update)
}

fn build_changeset_delete_from_v1<T, S, B, A>(
    change: &ChangeV1,
    table: &T,
    adapter: &A,
) -> Result<ChangeDelete<T, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + Default + AsRef<str>,
    B: Clone + Default + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let mut delete = ChangeDelete::from(table.clone());
    let key_iter: Box<dyn Iterator<Item = (&str, &str, &serde_json::Value)>> =
        if let Some(oldkeys) = &change.oldkeys {
            Box::new(iter_v1_oldkeys(oldkeys))
        } else {
            Box::new(iter_v1_columns(change))
        };
    for (name, type_name, value) in key_iter {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.into()))?;
        let payload = Wal2JsonColumn {
            column_name: name,
            pg_type_name: type_name,
            value,
        };
        let decoded = adapter.decode(payload)?;
        delete = delete
            .set(col_idx, decoded)
            .map_err(|_| ConversionError::ColumnNotFound(name.into()))?;
    }
    Ok(delete)
}

fn build_patch_delete_from_v1<T, S, B, A>(
    change: &ChangeV1,
    table: &T,
    adapter: &A,
) -> Result<PatchDelete<T, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let oldkeys = change
        .oldkeys
        .as_ref()
        .ok_or(ConversionError::MissingColumns)?;

    let num_pks = table.number_of_primary_keys();
    let mut pk_slots: Vec<Option<Value<S, B>>> = alloc::vec![None; num_pks];

    for (name, type_name, value) in iter_v1_oldkeys(oldkeys) {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.into()))?;
        if let Some(pk_idx) = table.primary_key_index(col_idx) {
            let payload = Wal2JsonColumn {
                column_name: name,
                pg_type_name: type_name,
                value,
            };
            pk_slots[pk_idx] = Some(adapter.decode(payload)?);
        }
    }

    let pk = pk_slots
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or(ConversionError::MissingColumns)?;
    Ok(PatchDelete::new(table.clone(), pk))
}

// Arbitrary implementations for testing
#[cfg(feature = "testing")]
mod arbitrary_impl {
    use super::{Action, ChangeV1, Column, MessageV2, OldKeys, String, TransactionV1, Vec};
    use alloc::string::ToString;
    use arbitrary::{Arbitrary, Unstructured};

    impl<'a> Arbitrary<'a> for Action {
        fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
            Ok(*u.choose(&[
                Self::B,
                Self::C,
                Self::I,
                Self::U,
                Self::D,
                Self::T,
                Self::M,
            ])?)
        }
    }

    impl<'a> Arbitrary<'a> for Column {
        fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
            let name: String = u.arbitrary()?;
            let type_name = (*u.choose(&["integer", "text", "boolean", "real"])?).to_string();

            // Generate a simple JSON value (not nested objects/arrays)
            let value = match u.int_in_range(0..=3)? {
                0 => serde_json::Value::Null,
                1 => serde_json::Value::Bool(u.arbitrary()?),
                2 => serde_json::Value::Number(serde_json::Number::from(
                    u.int_in_range::<i64>(-1000..=1000)?,
                )),
                _ => serde_json::Value::String(u.arbitrary()?),
            };

            Ok(Self {
                name,
                type_name,
                value,
            })
        }
    }

    impl<'a> Arbitrary<'a> for MessageV2 {
        fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
            Ok(Self {
                action: u.arbitrary()?,
                schema: u.arbitrary()?,
                table: u.arbitrary()?,
                columns: u.arbitrary()?,
                identity: u.arbitrary()?,
            })
        }
    }

    impl<'a> Arbitrary<'a> for OldKeys {
        fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
            let len = u.int_in_range(1..=5)?;
            let keynames: Vec<String> =
                (0..len).map(|_| u.arbitrary()).collect::<Result<_, _>>()?;
            let keytypes: Vec<String> = (0..len)
                .map(|_| {
                    u.choose(&["integer", "text", "boolean"])
                        .map(|s| (*s).to_string())
                })
                .collect::<Result<_, _>>()?;
            let keyvalues: Vec<serde_json::Value> = (0..len)
                .map(|_| {
                    Ok(match u.int_in_range(0..=2)? {
                        0 => serde_json::Value::Null,
                        1 => serde_json::Value::Number(serde_json::Number::from(
                            u.int_in_range::<i64>(-1000..=1000)?,
                        )),
                        _ => serde_json::Value::String(u.arbitrary()?),
                    })
                })
                .collect::<Result<_, arbitrary::Error>>()?;

            Ok(Self {
                keynames,
                keytypes,
                keyvalues,
            })
        }
    }

    impl<'a> Arbitrary<'a> for ChangeV1 {
        fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
            let kind = (*u.choose(&["insert", "update", "delete"])?).to_string();
            let schema: String = u.arbitrary()?;
            let table: String = u.arbitrary()?;

            let len = u.int_in_range(1..=5)?;
            let columnnames: Vec<String> =
                (0..len).map(|_| u.arbitrary()).collect::<Result<_, _>>()?;
            let columntypes: Vec<String> = (0..len)
                .map(|_| {
                    u.choose(&["integer", "text", "boolean", "real"])
                        .map(|s| (*s).to_string())
                })
                .collect::<Result<_, _>>()?;
            let columnvalues: Vec<serde_json::Value> = (0..len)
                .map(|_| {
                    Ok(match u.int_in_range(0..=3)? {
                        0 => serde_json::Value::Null,
                        1 => serde_json::Value::Bool(u.arbitrary()?),
                        2 => serde_json::Value::Number(serde_json::Number::from(
                            u.int_in_range::<i64>(-1000..=1000)?,
                        )),
                        _ => serde_json::Value::String(u.arbitrary()?),
                    })
                })
                .collect::<Result<_, arbitrary::Error>>()?;

            let oldkeys = if kind == "insert" {
                None
            } else {
                u.arbitrary()?
            };

            Ok(Self {
                kind,
                schema,
                table,
                columnnames,
                columntypes,
                columnvalues,
                oldkeys,
            })
        }
    }

    impl<'a> Arbitrary<'a> for TransactionV1 {
        fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
            Ok(Self {
                change: u.arbitrary()?,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DynTable, SimpleTable};

    #[test]
    fn test_parse_v2_insert() {
        let json = r#"{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"Alice"}]}"#;
        let msg = parse_v2(json).unwrap();

        assert_eq!(msg.action, Action::I);
        assert_eq!(msg.schema.as_deref(), Some("public"));
        assert_eq!(msg.table.as_deref(), Some("users"));
        assert!(msg.columns.is_some());

        let columns = msg.columns.as_ref().unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[1].name, "name");
    }

    #[test]
    fn test_parse_v2_update() {
        let json = r#"{"action":"U","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"Bob"}],"identity":[{"name":"id","type":"integer","value":1}]}"#;
        let msg = parse_v2(json).unwrap();

        assert_eq!(msg.action, Action::U);
        assert!(msg.identity.is_some());
    }

    #[test]
    fn test_parse_v2_delete() {
        let json = r#"{"action":"D","schema":"public","table":"users","identity":[{"name":"id","type":"integer","value":1}]}"#;
        let msg = parse_v2(json).unwrap();

        assert_eq!(msg.action, Action::D);
        assert!(msg.identity.is_some());
        assert!(msg.columns.is_none());
    }

    #[test]
    fn test_parse_v1_transaction() {
        let json = r#"{"change":[{"kind":"insert","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","text"],"columnvalues":[1,"Alice"]}]}"#;
        let tx = parse_v1(json).unwrap();

        assert_eq!(tx.change.len(), 1);
        assert_eq!(tx.change[0].kind, "insert");
        assert_eq!(tx.change[0].table, "users");
    }

    #[test]
    fn test_v1_insert_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let json = r#"{"change":[{"kind":"insert","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","text"],"columnvalues":[1,"Alice"]}]}"#;
        let tx = parse_v1(json).unwrap();

        let insert: Insert<_, String, Vec<u8>> = (&tx.change[0], &table).try_into().unwrap();

        // Verify the insert was created correctly
        let values = insert.into_values();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Value::Integer(1));
        assert_eq!(values[1], Value::Text("Alice".into()));
    }

    #[test]
    fn test_v2_insert_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let json = r#"{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"Alice"}]}"#;
        let msg = parse_v2(json).unwrap();

        let insert: Insert<_, String, Vec<u8>> = (&msg, &table).try_into().unwrap();

        let values = insert.into_values();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Value::Integer(1));
        assert_eq!(values[1], Value::Text("Alice".into()));
    }

    #[test]
    fn test_cdc_default_indirect_false() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let json = r#"{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"Alice"}]}"#;
        let msg = parse_v2(json).unwrap();

        let insert: Insert<_, String, Vec<u8>> = (&msg, &table).try_into().unwrap();
        assert!(!insert.indirect);
    }

    #[test]
    fn test_column_not_found_error() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let json = r#"{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"unknown","type":"text","value":"test"}]}"#;
        let msg = parse_v2(json).unwrap();

        let result: Result<Insert<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::ColumnNotFound(_))));
    }

    #[test]
    fn test_table_mismatch_error() {
        let table = SimpleTable::new("products", &["id", "name"], &[0]);

        let json = r#"{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1}]}"#;
        let msg = parse_v2(json).unwrap();

        let result: Result<Insert<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_json_null_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let json = r#"{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":null}]}"#;
        let msg = parse_v2(json).unwrap();

        let insert: Insert<_, String, Vec<u8>> = (&msg, &table).try_into().unwrap();

        let values = insert.into_values();
        assert_eq!(values[1], Value::Null);
    }

    #[test]
    fn test_json_bool_conversion() {
        let table = SimpleTable::new("flags", &["id", "active"], &[0]);

        let json = r#"{"action":"I","schema":"public","table":"flags","columns":[{"name":"id","type":"integer","value":1},{"name":"active","type":"boolean","value":true}]}"#;
        let msg = parse_v2(json).unwrap();

        let insert: Insert<_, String, Vec<u8>> = (&msg, &table).try_into().unwrap();

        let values = insert.into_values();
        // Boolean true converts to integer 1
        assert_eq!(values[1], Value::Integer(1));
    }

    #[test]
    fn test_json_float_conversion() {
        let table = SimpleTable::new("prices", &["id", "amount"], &[0]);

        let json = r#"{"action":"I","schema":"public","table":"prices","columns":[{"name":"id","type":"integer","value":1},{"name":"amount","type":"real","value":99.99}]}"#;
        let msg = parse_v2(json).unwrap();

        let insert: Insert<_, String, Vec<u8>> = (&msg, &table).try_into().unwrap();

        let values = insert.into_values();
        assert_eq!(values[1], Value::Real(99.99));
    }

    #[test]
    fn test_v1_delete_with_oldkeys() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let json = r#"{"change":[{"kind":"delete","schema":"public","table":"users","columnnames":[],"columntypes":[],"columnvalues":[],"oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[42]}}]}"#;
        let tx = parse_v1(json).unwrap();

        let delete: ChangeDelete<_, String, Vec<u8>> = (&tx.change[0], &table).try_into().unwrap();

        // The delete should have the PK value set
        let values = delete.into_values();
        assert_eq!(values[0], Value::Integer(42));
    }

    #[test]
    fn test_v2_delete_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);

        let json = r#"{"action":"D","schema":"public","table":"users","identity":[{"name":"id","type":"integer","value":42}]}"#;
        let msg = parse_v2(json).unwrap();

        let delete: ChangeDelete<_, String, Vec<u8>> = (&msg, &table).try_into().unwrap();

        let values = delete.into_values();
        assert_eq!(values[0], Value::Integer(42));
    }

    #[test]
    fn test_unsupported_type_error() {
        let table = SimpleTable::new("data", &["id", "payload"], &[0]);

        // JSON array is not supported
        let json = r#"{"action":"I","schema":"public","table":"data","columns":[{"name":"id","type":"integer","value":1},{"name":"payload","type":"json","value":[1,2,3]}]}"#;
        let msg = parse_v2(json).unwrap();

        let result: Result<Insert<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::UnsupportedType(_))));
    }

    // ---- Additional branch coverage ----

    #[test]
    fn test_v1_update_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let json = r#"{"change":[{"kind":"update","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","text"],"columnvalues":[1,"Bob"]}]}"#;
        let tx = parse_v1(json).unwrap();
        let update: crate::ChangeUpdate<_, String, Vec<u8>> =
            (&tx.change[0], &table).try_into().unwrap();
        let values = update.values();
        assert_eq!(values.len(), 2);
        assert_eq!(values[1].1, Some(Value::Text("Bob".into())));
    }

    #[test]
    fn test_v1_table_mismatch() {
        let table = SimpleTable::new("orders", &["id", "name"], &[0]);
        let json = r#"{"change":[{"kind":"insert","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","text"],"columnvalues":[1,"Alice"]}]}"#;
        let tx = parse_v1(json).unwrap();
        let result: Result<Insert<_, String, Vec<u8>>, _> = (&tx.change[0], &table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_v1_patch_delete_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let json = r#"{"change":[{"kind":"delete","schema":"public","table":"users","columnnames":[],"columntypes":[],"columnvalues":[],"oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[42]}}]}"#;
        let tx = parse_v1(json).unwrap();
        let delete: PatchDelete<_, String, Vec<u8>> = (&tx.change[0], &table).try_into().unwrap();
        assert_eq!(delete.as_ref().name(), "users");
    }

    #[test]
    fn test_v1_patch_delete_missing_oldkeys() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let json = r#"{"change":[{"kind":"delete","schema":"public","table":"users","columnnames":[],"columntypes":[],"columnvalues":[]}]}"#;
        let tx = parse_v1(json).unwrap();
        let result: Result<PatchDelete<_, String, Vec<u8>>, _> = (&tx.change[0], &table).try_into();
        assert!(matches!(result, Err(ConversionError::MissingColumns)));
    }

    #[test]
    fn test_v1_delete_no_oldkeys_uses_columnvalues() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let json = r#"{"change":[{"kind":"delete","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","text"],"columnvalues":[7,"Eve"]}]}"#;
        let tx = parse_v1(json).unwrap();
        let delete: ChangeDelete<_, String, Vec<u8>> = (&tx.change[0], &table).try_into().unwrap();
        let values = delete.into_values();
        assert_eq!(values[0], Value::Integer(7));
        assert_eq!(values[1], Value::Text("Eve".into()));
    }

    #[test]
    fn test_v2_update_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let json = r#"{"action":"U","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"Bob"}],"identity":[{"name":"id","type":"integer","value":1}]}"#;
        let msg = parse_v2(json).unwrap();
        let update: crate::ChangeUpdate<_, String, Vec<u8>> = (&msg, &table).try_into().unwrap();
        let values = update.values();
        assert_eq!(values.len(), 2);
        assert_eq!(values[1].1, Some(Value::Text("Bob".into())));
    }

    #[test]
    fn test_v2_patch_delete_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let json = r#"{"action":"D","schema":"public","table":"users","identity":[{"name":"id","type":"integer","value":42}]}"#;
        let msg = parse_v2(json).unwrap();
        let delete: PatchDelete<_, String, Vec<u8>> = (&msg, &table).try_into().unwrap();
        assert_eq!(delete.as_ref().name(), "users");
    }

    #[test]
    fn test_v2_insert_missing_columns() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let json = r#"{"action":"I","schema":"public","table":"users"}"#;
        let msg = parse_v2(json).unwrap();
        let result: Result<Insert<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::MissingColumns)));
    }

    #[test]
    fn test_v2_delete_missing_identity() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let json = r#"{"action":"D","schema":"public","table":"users"}"#;
        let msg = parse_v2(json).unwrap();
        let result: Result<ChangeDelete<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::MissingColumns)));
    }

    // ---- TableMismatch symmetry across all Update/Delete variants ----

    #[test]
    fn test_v1_update_table_mismatch() {
        let table = SimpleTable::new("orders", &["id", "name"], &[0]);
        let json = r#"{"change":[{"kind":"update","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","text"],"columnvalues":[1,"Bob"]}]}"#;
        let tx = parse_v1(json).unwrap();
        let result: Result<crate::ChangeUpdate<_, String, Vec<u8>>, _> =
            (&tx.change[0], &table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_v1_changedelete_table_mismatch() {
        let table = SimpleTable::new("orders", &["id", "name"], &[0]);
        let json = r#"{"change":[{"kind":"delete","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","text"],"columnvalues":[1,"Eve"]}]}"#;
        let tx = parse_v1(json).unwrap();
        let result: Result<ChangeDelete<_, String, Vec<u8>>, _> =
            (&tx.change[0], &table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_v1_patchdelete_table_mismatch() {
        let table = SimpleTable::new("orders", &["id", "name"], &[0]);
        let json = r#"{"change":[{"kind":"delete","schema":"public","table":"users","columnnames":[],"columntypes":[],"columnvalues":[],"oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[42]}}]}"#;
        let tx = parse_v1(json).unwrap();
        let result: Result<PatchDelete<_, String, Vec<u8>>, _> = (&tx.change[0], &table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_v2_update_table_mismatch() {
        let table = SimpleTable::new("orders", &["id", "name"], &[0]);
        let json = r#"{"action":"U","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"Bob"}],"identity":[{"name":"id","type":"integer","value":1}]}"#;
        let msg = parse_v2(json).unwrap();
        let result: Result<crate::ChangeUpdate<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_v2_changedelete_table_mismatch() {
        let table = SimpleTable::new("orders", &["id", "name"], &[0]);
        let json = r#"{"action":"D","schema":"public","table":"users","identity":[{"name":"id","type":"integer","value":42}]}"#;
        let msg = parse_v2(json).unwrap();
        let result: Result<ChangeDelete<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_v2_patchdelete_table_mismatch() {
        let table = SimpleTable::new("orders", &["id", "name"], &[0]);
        let json = r#"{"action":"D","schema":"public","table":"users","identity":[{"name":"id","type":"integer","value":42}]}"#;
        let msg = parse_v2(json).unwrap();
        let result: Result<PatchDelete<_, String, Vec<u8>>, _> = (&msg, &table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }
}
