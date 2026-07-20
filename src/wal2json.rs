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
    /// `PostgreSQL` LSN in `hi/lo` hex notation (for example `0/16B2270`),
    /// present when wal2json runs with `include-lsn=true`. `None` otherwise.
    /// Kept as a raw string so this module stays free of any Postgres-specific
    /// numeric LSN type. The consumer decides how to interpret it.
    #[serde(default)]
    pub lsn: Option<String>,
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

    /// Table named in the wire message is not in the schema.
    #[error("Table '{0}' not found in schema")]
    TableNotFound(String),

    /// The message is missing required column data.
    #[error("Missing columns in message")]
    MissingColumns,

    /// A JSON value type is not supported for conversion.
    #[error("Unsupported JSON value type for column '{0}'")]
    UnsupportedType(String),

    /// User-registered decoder rejected a column payload.
    #[error("Decoder failed: {0}")]
    Decode(#[from] crate::wire::DecodeError),
}

use crate::wire::{Sealed, WireSource, WireType};

/// Marker type for the `wal2json` source.
#[derive(Debug, Clone, Copy, Default)]
pub struct Wal2Json;

impl Sealed for Wal2Json {}

impl WireSource for Wal2Json {
    type Payload<'a> = Wal2JsonColumn<'a>;

    fn wire_type(payload: &Self::Payload<'_>) -> WireType {
        payload.wire_type
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
    /// Semantic column type driving decoder dispatch.
    pub wire_type: WireType,
    /// Column value as a JSON value.
    pub value: &'a serde_json::Value,
}

impl Wal2JsonColumn<'_> {
    /// Ergonomic helper for calling a specific [`Decoder`](crate::wire::Decoder) on this
    /// payload without fully-qualified syntax. Fixes the `Src` generic
    /// to [`Wal2Json`] so the compiler can pick the impl.
    ///
    /// # Errors
    ///
    /// Propagates the decoder's [`DecodeError`](crate::wire::DecodeError).
    pub fn decoded_by<D, S, B>(self, decoder: &D) -> Result<Value<S, B>, crate::wire::DecodeError>
    where
        D: crate::wire::Decoder<Wal2Json, S, B>,
    {
        decoder.decode(self)
    }
}

use crate::builders::{
    ChangeDelete, ChangesetFormat, DiffOps, DiffSetBuilder, Insert, PatchDelete, PatchsetFormat,
    Update,
};
use crate::encoding::Value;
use crate::schema::NamedColumns;
use crate::wire::{Digestable, WireAdapter, WireColumnTypes, WireSchema};
use alloc::boxed::Box;
use core::fmt::Debug;
use core::hash::Hash;

fn resolve_table<'a, Sch>(schema: &'a Sch, name: &str) -> Result<&'a Sch::Table, ConversionError>
where
    Sch: WireSchema,
{
    schema
        .get(name)
        .ok_or_else(|| ConversionError::TableNotFound(name.into()))
}

impl<T, S, B> Digestable<ChangesetFormat, T, S, B> for MessageV2
where
    T: NamedColumns + WireColumnTypes,
    S: Clone + Debug + Hash + Eq + AsRef<str> + Default,
    B: Clone + Debug + Hash + Eq + AsRef<[u8]> + Default,
{
    type Src = Wal2Json;
    type Error = ConversionError;

    fn digest_into<Sch, A>(
        &self,
        builder: DiffSetBuilder<ChangesetFormat, T, S, B>,
        schema: &Sch,
        adapter: &A,
    ) -> Result<DiffSetBuilder<ChangesetFormat, T, S, B>, ConversionError>
    where
        Sch: WireSchema<Table = T>,
        A: WireAdapter<Wal2Json, S, B>,
    {
        let Some(table_name) = self.table.as_deref() else {
            return Ok(builder);
        };
        match self.action {
            Action::I => {
                let table = resolve_table(schema, table_name)?;
                let columns = self
                    .columns
                    .as_ref()
                    .ok_or(ConversionError::MissingColumns)?;
                let insert = build_insert_from_v2(columns, table, adapter)?;
                Ok(DiffOps::insert(builder, insert))
            }
            Action::U => {
                let table = resolve_table(schema, table_name)?;
                let columns = self
                    .columns
                    .as_ref()
                    .ok_or(ConversionError::MissingColumns)?;
                let update = build_changeset_update_from_v2(
                    columns,
                    self.identity.as_deref(),
                    table,
                    adapter,
                )?;
                Ok(DiffOps::update(builder, update))
            }
            Action::D => {
                let table = resolve_table(schema, table_name)?;
                let identity = self
                    .identity
                    .as_ref()
                    .ok_or(ConversionError::MissingColumns)?;
                let delete = build_changeset_delete_from_columns(identity, table, adapter)?;
                Ok(DiffOps::delete(builder, delete))
            }
            Action::B | Action::C | Action::T | Action::M => Ok(builder),
        }
    }
}

impl<T, S, B> Digestable<PatchsetFormat, T, S, B> for MessageV2
where
    T: NamedColumns + WireColumnTypes,
    S: Clone + Debug + Hash + Eq + AsRef<str> + Default,
    B: Clone + Debug + Hash + Eq + AsRef<[u8]> + Default,
{
    type Src = Wal2Json;
    type Error = ConversionError;

    fn digest_into<Sch, A>(
        &self,
        builder: DiffSetBuilder<PatchsetFormat, T, S, B>,
        schema: &Sch,
        adapter: &A,
    ) -> Result<DiffSetBuilder<PatchsetFormat, T, S, B>, ConversionError>
    where
        Sch: WireSchema<Table = T>,
        A: WireAdapter<Wal2Json, S, B>,
    {
        let Some(table_name) = self.table.as_deref() else {
            return Ok(builder);
        };
        match self.action {
            Action::I => {
                let table = resolve_table(schema, table_name)?;
                let columns = self
                    .columns
                    .as_ref()
                    .ok_or(ConversionError::MissingColumns)?;
                let insert = build_insert_from_v2(columns, table, adapter)?;
                Ok(DiffOps::insert(builder, insert))
            }
            Action::U => {
                let table = resolve_table(schema, table_name)?;
                let columns = self
                    .columns
                    .as_ref()
                    .ok_or(ConversionError::MissingColumns)?;
                let update = build_patchset_update_from_v2(columns, table, adapter)?;
                Ok(DiffOps::update(builder, update))
            }
            Action::D => {
                let table = resolve_table(schema, table_name)?;
                let identity = self
                    .identity
                    .as_ref()
                    .ok_or(ConversionError::MissingColumns)?;
                let delete = build_patch_delete_from_columns(identity, table, adapter)?;
                Ok(DiffOps::delete(builder, delete))
            }
            Action::B | Action::C | Action::T | Action::M => Ok(builder),
        }
    }
}

impl<T, S, B> Digestable<ChangesetFormat, T, S, B> for ChangeV1
where
    T: NamedColumns + WireColumnTypes,
    S: Clone + Debug + Hash + Eq + AsRef<str> + Default,
    B: Clone + Debug + Hash + Eq + AsRef<[u8]> + Default,
{
    type Src = Wal2Json;
    type Error = ConversionError;

    fn digest_into<Sch, A>(
        &self,
        builder: DiffSetBuilder<ChangesetFormat, T, S, B>,
        schema: &Sch,
        adapter: &A,
    ) -> Result<DiffSetBuilder<ChangesetFormat, T, S, B>, ConversionError>
    where
        Sch: WireSchema<Table = T>,
        A: WireAdapter<Wal2Json, S, B>,
    {
        let table = resolve_table(schema, self.table.as_str())?;
        match self.kind.as_str() {
            "insert" => {
                let insert = build_insert_from_v1(self, table, adapter)?;
                Ok(DiffOps::insert(builder, insert))
            }
            "update" => {
                let update = build_changeset_update_from_v1(self, table, adapter)?;
                Ok(DiffOps::update(builder, update))
            }
            "delete" => {
                let delete = build_changeset_delete_from_v1(self, table, adapter)?;
                Ok(DiffOps::delete(builder, delete))
            }
            _ => Ok(builder),
        }
    }
}

impl<T, S, B> Digestable<PatchsetFormat, T, S, B> for ChangeV1
where
    T: NamedColumns + WireColumnTypes,
    S: Clone + Debug + Hash + Eq + AsRef<str> + Default,
    B: Clone + Debug + Hash + Eq + AsRef<[u8]> + Default,
{
    type Src = Wal2Json;
    type Error = ConversionError;

    fn digest_into<Sch, A>(
        &self,
        builder: DiffSetBuilder<PatchsetFormat, T, S, B>,
        schema: &Sch,
        adapter: &A,
    ) -> Result<DiffSetBuilder<PatchsetFormat, T, S, B>, ConversionError>
    where
        Sch: WireSchema<Table = T>,
        A: WireAdapter<Wal2Json, S, B>,
    {
        let table = resolve_table(schema, self.table.as_str())?;
        match self.kind.as_str() {
            "insert" => {
                let insert = build_insert_from_v1(self, table, adapter)?;
                Ok(DiffOps::insert(builder, insert))
            }
            "update" => {
                let update = build_patchset_update_from_v1(self, table, adapter)?;
                Ok(DiffOps::update(builder, update))
            }
            "delete" => {
                let delete = build_patch_delete_from_v1(self, table, adapter)?;
                Ok(DiffOps::delete(builder, delete))
            }
            _ => Ok(builder),
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
    T: NamedColumns + WireColumnTypes,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let mut insert = Insert::from(table.clone());
    for col in columns {
        let col_idx = table
            .column_index(&col.name)
            .ok_or_else(|| ConversionError::ColumnNotFound(col.name.clone()))?;
        let wire_type = table.column_type(col_idx);
        let payload = Wal2JsonColumn {
            column_name: col.name.as_str(),
            wire_type,
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
    identity: Option<&[Column]>,
    table: &T,
    adapter: &A,
) -> Result<Update<T, ChangesetFormat, S, B>, ConversionError>
where
    T: NamedColumns + WireColumnTypes,
    S: Clone + Debug + AsRef<str>,
    B: Clone + Debug + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let mut update: Update<T, ChangesetFormat, S, B> = Update::from(table.clone());
    for col in columns {
        let col_idx = table
            .column_index(&col.name)
            .ok_or_else(|| ConversionError::ColumnNotFound(col.name.clone()))?;
        let wire_type = table.column_type(col_idx);
        let new = adapter.decode(Wal2JsonColumn {
            column_name: col.name.as_str(),
            wire_type,
            value: &col.value,
        })?;

        // Pair the new value with its old-row value from the identity image
        // when present, so a primary-key change keeps the old key for the
        // WHERE clause. Non-key columns absent from the identity fall back to
        // set_new.
        if let Some(old_col) = identity.and_then(|id| id.iter().find(|c| c.name == col.name)) {
            let old = adapter.decode(Wal2JsonColumn {
                column_name: col.name.as_str(),
                wire_type,
                value: &old_col.value,
            })?;
            update = update
                .set(col_idx, old, new)
                .map_err(|_| ConversionError::ColumnNotFound(col.name.clone()))?;
        } else {
            update = update
                .set_new(col_idx, new)
                .map_err(|_| ConversionError::ColumnNotFound(col.name.clone()))?;
        }
    }
    Ok(update)
}

fn build_patchset_update_from_v2<T, S, B, A>(
    columns: &[Column],
    table: &T,
    adapter: &A,
) -> Result<Update<T, PatchsetFormat, S, B>, ConversionError>
where
    T: NamedColumns + WireColumnTypes,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let mut update: Update<T, PatchsetFormat, S, B> = Update::from(table.clone());
    for col in columns {
        let col_idx = table
            .column_index(&col.name)
            .ok_or_else(|| ConversionError::ColumnNotFound(col.name.clone()))?;
        let wire_type = table.column_type(col_idx);
        let payload = Wal2JsonColumn {
            column_name: col.name.as_str(),
            wire_type,
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
    T: NamedColumns + WireColumnTypes,
    S: Clone + Default + AsRef<str>,
    B: Clone + Default + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let mut delete = ChangeDelete::from(table.clone());
    for col in identity {
        let col_idx = table
            .column_index(&col.name)
            .ok_or_else(|| ConversionError::ColumnNotFound(col.name.clone()))?;
        let wire_type = table.column_type(col_idx);
        let payload = Wal2JsonColumn {
            column_name: col.name.as_str(),
            wire_type,
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
    T: NamedColumns + WireColumnTypes,
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
            let wire_type = table.column_type(col_idx);
            let payload = Wal2JsonColumn {
                column_name: col.name.as_str(),
                wire_type,
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

fn iter_v1_columns(change: &ChangeV1) -> impl Iterator<Item = (&str, &serde_json::Value)> + '_ {
    change
        .columnnames
        .iter()
        .zip(change.columnvalues.iter())
        .map(|(n, v)| (n.as_str(), v))
}

fn iter_v1_oldkeys(oldkeys: &OldKeys) -> impl Iterator<Item = (&str, &serde_json::Value)> + '_ {
    oldkeys
        .keynames
        .iter()
        .zip(oldkeys.keyvalues.iter())
        .map(|(n, v)| (n.as_str(), v))
}

fn build_insert_from_v1<T, S, B, A>(
    change: &ChangeV1,
    table: &T,
    adapter: &A,
) -> Result<Insert<T, S, B>, ConversionError>
where
    T: NamedColumns + WireColumnTypes,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let mut insert = Insert::from(table.clone());
    for (name, value) in iter_v1_columns(change) {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.into()))?;
        let wire_type = table.column_type(col_idx);
        let payload = Wal2JsonColumn {
            column_name: name,
            wire_type,
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
    T: NamedColumns + WireColumnTypes,
    S: Clone + Debug + AsRef<str>,
    B: Clone + Debug + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let mut update: Update<T, ChangesetFormat, S, B> = Update::from(table.clone());
    for (name, value) in iter_v1_columns(change) {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.into()))?;
        let wire_type = table.column_type(col_idx);
        let new = adapter.decode(Wal2JsonColumn {
            column_name: name,
            wire_type,
            value,
        })?;

        // Pair with the old value from oldkeys when the column is present
        // there (always at least the primary key), else fall back to set_new.
        let old_value = change.oldkeys.as_ref().and_then(|ok| {
            iter_v1_oldkeys(ok)
                .find(|(n, _)| *n == name)
                .map(|(_, v)| v)
        });
        if let Some(old_value) = old_value {
            let old = adapter.decode(Wal2JsonColumn {
                column_name: name,
                wire_type,
                value: old_value,
            })?;
            update = update
                .set(col_idx, old, new)
                .map_err(|_| ConversionError::ColumnNotFound(name.into()))?;
        } else {
            update = update
                .set_new(col_idx, new)
                .map_err(|_| ConversionError::ColumnNotFound(name.into()))?;
        }
    }
    Ok(update)
}

fn build_patchset_update_from_v1<T, S, B, A>(
    change: &ChangeV1,
    table: &T,
    adapter: &A,
) -> Result<Update<T, PatchsetFormat, S, B>, ConversionError>
where
    T: NamedColumns + WireColumnTypes,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let mut update: Update<T, PatchsetFormat, S, B> = Update::from(table.clone());
    for (name, value) in iter_v1_columns(change) {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.into()))?;
        let wire_type = table.column_type(col_idx);
        let payload = Wal2JsonColumn {
            column_name: name,
            wire_type,
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
    T: NamedColumns + WireColumnTypes,
    S: Clone + Default + AsRef<str>,
    B: Clone + Default + AsRef<[u8]>,
    A: WireAdapter<Wal2Json, S, B>,
{
    let mut delete = ChangeDelete::from(table.clone());
    let key_iter: Box<dyn Iterator<Item = (&str, &serde_json::Value)>> =
        if let Some(oldkeys) = &change.oldkeys {
            Box::new(iter_v1_oldkeys(oldkeys))
        } else {
            Box::new(iter_v1_columns(change))
        };
    for (name, value) in key_iter {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.into()))?;
        let wire_type = table.column_type(col_idx);
        let payload = Wal2JsonColumn {
            column_name: name,
            wire_type,
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
    T: NamedColumns + WireColumnTypes,
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

    for (name, value) in iter_v1_oldkeys(oldkeys) {
        let col_idx = table
            .column_index(name)
            .ok_or_else(|| ConversionError::ColumnNotFound(name.into()))?;
        if let Some(pk_idx) = table.primary_key_index(col_idx) {
            let wire_type = table.column_type(col_idx);
            let payload = Wal2JsonColumn {
                column_name: name,
                wire_type,
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
                lsn: u.arbitrary()?,
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
