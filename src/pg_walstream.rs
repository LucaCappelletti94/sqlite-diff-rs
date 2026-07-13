//! `pg_walstream` message conversion to `SQLite` changeset operations.
//!
//! [pg_walstream](https://github.com/isdaniel/pg-walstream) is a `PostgreSQL`
//! logical replication client that parses the native binary replication
//! protocol into high-performance CDC events. This module converts those
//! events into changeset operations compatible with this crate's builders.
//!
//! `pg_walstream` 0.6 delivers column values as raw PostgreSQL wire-format
//! text bytes via [`ColumnValue`]. The conversions here parse those text
//! bytes into our [`Value`] enum: integers parse as `Value::Integer`, floats
//! as `Value::Real`, the booleans `t` and `f` as `Value::Integer(1)` and
//! `Value::Integer(0)`, and everything else falls back to `Value::Text`.
//! Binary columns become `Value::Blob`, and SQL `NULL` becomes `Value::Null`.
//!
//! `pg_walstream` events carry no trigger-origin marker, so converted ops
//! default to `indirect = false`. Override via the [`Indirect`](crate::Indirect)
//! trait if you know out-of-band that the event was trigger-induced.
//!
//! # Example
//!
//! ```ignore
//! use std::sync::Arc;
//! use sqlite_diff_rs::pg_walstream::EventType;
//! use sqlite_diff_rs::{Insert, SimpleTable};
//! use pg_walstream::{ColumnValue, RowData};
//!
//! let table = SimpleTable::new("users", &["id", "name"], &[0]);
//!
//! let mut data = RowData::new();
//! data.push(Arc::from("id"), ColumnValue::text("1"));
//! data.push(Arc::from("name"), ColumnValue::text("Alice"));
//!
//! let event = EventType::Insert {
//!     schema: Arc::from("public"),
//!     table: Arc::from("users"),
//!     relation_oid: 12345,
//!     data,
//! };
//!
//! let insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
//! ```

use alloc::string::{String, ToString};
use alloc::vec::Vec;

// Re-export key types from pg_walstream for convenience
pub use pg_walstream::Oid;
pub use pg_walstream::{ChangeEvent, ColumnValue, EventType, Lsn, ReplicaIdentity, RowData};

use crate::ChangesetFormat;
use crate::builders::{
    ChangeDelete, DiffOps, DiffSetBuilder, Insert, PatchDelete, PatchsetFormat, Update,
};
use crate::encoding::Value;
use crate::schema::NamedColumns;
use crate::wire::{Sealed, WireAdapter, WireSource};
use core::fmt::Debug;
use core::hash::Hash;

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

    /// A column value could not be decoded into a supported `Value`.
    #[error("Unsupported value for column '{0}'")]
    UnsupportedType(String),

    /// The event type is not applicable for the requested conversion.
    #[error("Event type '{0}' cannot be converted to the requested operation")]
    InvalidEventType(String),

    /// Old data is required but not available (replica identity issue).
    #[error("Old data not available (check replica identity setting)")]
    MissingOldData,

    /// A schema-aware decoder rejected a column payload. Populated by
    /// `DiffSetBuilder::digest_pg_walstream` when the user's registered
    /// decoder returns [`crate::wire::DecodeError`].
    #[error("Decoder failed: {0}")]
    Decode(#[from] crate::wire::DecodeError),
}

/// Marker type for the `pg_walstream` source. Passed as the `Src`
/// generic parameter to `TypeMap`, `WireAdapter`, and `Decoder`.
#[derive(Debug, Clone, Copy, Default)]
pub struct PgWalstream;

impl Sealed for PgWalstream {}

impl WireSource for PgWalstream {
    type Payload<'a> = PgWalstreamColumn<'a>;
    type TypeKey = Oid;

    fn type_key(payload: &Self::Payload<'_>) -> Self::TypeKey {
        payload.oid
    }

    fn column_name<'a>(payload: &'a Self::Payload<'_>) -> &'a str {
        payload.column_name
    }
}

/// Per-column payload for the `pg_walstream` source.
///
/// The format wrapper populates this once per column before invoking
/// [`WireAdapter::decode`](crate::wire::WireAdapter::decode).
#[derive(Debug, Clone, Copy)]
pub struct PgWalstreamColumn<'a> {
    /// Column name resolved from the relation cache.
    pub column_name: &'a str,
    /// Postgres type OID (from `ColumnInfo::type_id`).
    pub oid: Oid,
    /// Postgres type modifier (from `ColumnInfo::type_modifier`).
    pub type_modifier: i32,
    /// Raw wire payload.
    pub data: &'a ColumnValue,
}

impl PgWalstreamColumn<'_> {
    /// Ergonomic helper for calling a specific [`Decoder`] on this
    /// payload without fully-qualified syntax. Fixes the `Src` generic
    /// to [`PgWalstream`] so the compiler can pick the impl.
    ///
    /// # Errors
    ///
    /// Propagates the decoder's [`DecodeError`](crate::wire::DecodeError).
    pub fn decoded_by<D, S, B>(
        self,
        decoder: &D,
    ) -> Result<crate::encoding::Value<S, B>, crate::wire::DecodeError>
    where
        D: crate::wire::Decoder<PgWalstream, S, B>,
    {
        decoder.decode(self)
    }
}

/// Convert a `ColumnValue` to our `Value` type.
///
/// Text values from PostgreSQL are best-effort coerced into integers, floats,
/// or booleans. Anything else is preserved as `Value::Text`. Binary columns
/// pass through as `Value::Blob`. Non-UTF-8 text bytes are also stored as a
/// blob to avoid lossy decoding.
fn column_value_to_value(value: &ColumnValue) -> Value<String, Vec<u8>> {
    match value {
        ColumnValue::Null => Value::Null,
        ColumnValue::Binary(b) => Value::Blob(b.to_vec()),
        ColumnValue::Text(b) => {
            let Some(s) = value.as_str() else {
                // Non-UTF-8 text bytes: surface as a blob rather than an error.
                return Value::Blob(b.to_vec());
            };
            // Postgres booleans arrive as 't' or 'f' over the wire.
            if s == "t" {
                return Value::Integer(1);
            }
            if s == "f" {
                return Value::Integer(0);
            }
            if let Ok(i) = s.parse::<i64>() {
                return Value::Integer(i);
            }
            if let Ok(f) = s.parse::<f64>() {
                return Value::Real(f);
            }
            Value::Text(s.to_string())
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

        if table.name() != event_table.as_ref() {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: event_table.as_ref().into(),
            });
        }

        let mut insert = Insert::from(table);

        for (name, value) in data.iter() {
            let col_idx = insert
                .as_ref()
                .column_index(name.as_ref())
                .ok_or_else(|| ConversionError::ColumnNotFound(name.as_ref().into()))?;

            let converted = column_value_to_value(value);
            insert = insert
                .set(col_idx, converted)
                .map_err(|_| ConversionError::ColumnNotFound(name.as_ref().into()))?;
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

        if table.name() != event_table.as_ref() {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: event_table.as_ref().into(),
            });
        }

        let mut update: Update<T, ChangesetFormat, String, Vec<u8>> = Update::from(table);

        for (name, new_value) in new_data.iter() {
            let col_idx = update
                .as_ref()
                .column_index(name.as_ref())
                .ok_or_else(|| ConversionError::ColumnNotFound(name.as_ref().into()))?;

            let new_converted = column_value_to_value(new_value);

            if let Some(ref old) = old_data
                && let Some(old_value) = old.get(name.as_ref())
            {
                let old_converted = column_value_to_value(old_value);
                update = update
                    .set(col_idx, old_converted, new_converted)
                    .map_err(|_| ConversionError::ColumnNotFound(name.as_ref().into()))?;
                continue;
            }

            update = update
                .set_new(col_idx, new_converted)
                .map_err(|_| ConversionError::ColumnNotFound(name.as_ref().into()))?;
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

        if table.name() != event_table.as_ref() {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: event_table.as_ref().into(),
            });
        }

        let mut delete = ChangeDelete::from(table);

        for (name, value) in old_data.iter() {
            let col_idx = delete
                .as_ref()
                .column_index(name.as_ref())
                .ok_or_else(|| ConversionError::ColumnNotFound(name.as_ref().into()))?;

            let converted = column_value_to_value(value);
            delete = delete
                .set(col_idx, converted)
                .map_err(|_| ConversionError::ColumnNotFound(name.as_ref().into()))?;
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
            old_data,
            key_columns,
            ..
        } = event
        else {
            return Err(ConversionError::InvalidEventType("not a Delete".into()));
        };

        if table.name() != event_table.as_ref() {
            return Err(ConversionError::TableMismatch {
                expected: table.name().into(),
                actual: event_table.as_ref().into(),
            });
        }

        let num_pks = table.number_of_primary_keys();
        let mut pk_values: Vec<Option<Value<String, Vec<u8>>>> = alloc::vec![None; num_pks];

        for key_name in &key_columns {
            let col_idx = table
                .column_index(key_name.as_ref())
                .ok_or_else(|| ConversionError::ColumnNotFound(key_name.as_ref().into()))?;

            if let Some(pk_idx) = table.primary_key_index(col_idx) {
                let value = old_data
                    .get(key_name.as_ref())
                    .ok_or(ConversionError::MissingData)?;
                let converted = column_value_to_value(value);
                pk_values[pk_idx] = Some(converted);
            }
        }

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

// ============================================================================
// Schema-aware digest fns (0.2.0+).
// ============================================================================

fn pg_walstream_table_mismatch(expected: &str, actual: &str) -> Option<ConversionError> {
    if actual == expected {
        None
    } else {
        Some(ConversionError::TableMismatch {
            expected: expected.into(),
            actual: actual.into(),
        })
    }
}

impl<T, S, B> DiffSetBuilder<ChangesetFormat, T, S, B>
where
    T: NamedColumns + Clone,
    S: Clone + Debug + Hash + Eq + AsRef<str> + Default,
    B: Clone + Debug + Hash + Eq + AsRef<[u8]> + Default,
{
    /// Digest a `pg_walstream` event into a changeset via the supplied
    /// `adapter`.
    ///
    /// The `relation` argument provides column type metadata (OID and type
    /// modifier) for each column in the table. The `adapter` decodes raw
    /// `ColumnValue` payloads into [`Value`] instances using that metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ConversionError::TableMismatch`] if the event table name
    /// does not match `table.name()`, [`ConversionError::MissingData`] if
    /// the event is missing required column data, and
    /// [`ConversionError::ColumnNotFound`] for column names absent from
    /// the schema. Adapter failures surface as [`ConversionError::Decode`].
    pub fn digest_pg_walstream<A>(
        self,
        event: &EventType,
        relation: &pg_walstream::RelationInfo,
        table: &T,
        adapter: &A,
    ) -> Result<Self, ConversionError>
    where
        A: WireAdapter<PgWalstream, S, B>,
    {
        match event {
            EventType::Insert {
                table: event_table,
                data,
                ..
            } => {
                if let Some(err) = pg_walstream_table_mismatch(table.name(), event_table.as_ref()) {
                    return Err(err);
                }
                let insert = build_insert_from_pg(data, relation, table, adapter)?;
                Ok(DiffOps::insert(self, insert))
            }
            EventType::Update {
                table: event_table,
                old_data,
                new_data,
                ..
            } => {
                if let Some(err) = pg_walstream_table_mismatch(table.name(), event_table.as_ref()) {
                    return Err(err);
                }
                let update = build_changeset_update_from_pg(
                    old_data.as_ref(),
                    new_data,
                    relation,
                    table,
                    adapter,
                )?;
                Ok(DiffOps::update(self, update))
            }
            EventType::Delete {
                table: event_table,
                old_data,
                ..
            } => {
                if let Some(err) = pg_walstream_table_mismatch(table.name(), event_table.as_ref()) {
                    return Err(err);
                }
                let delete = build_changeset_delete_from_pg(old_data, relation, table, adapter)?;
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
    /// [`digest_pg_walstream`](DiffSetBuilder::digest_pg_walstream).
    ///
    /// # Errors
    ///
    /// See the changeset variant.
    pub fn digest_pg_walstream<A>(
        self,
        event: &EventType,
        relation: &pg_walstream::RelationInfo,
        table: &T,
        adapter: &A,
    ) -> Result<Self, ConversionError>
    where
        A: WireAdapter<PgWalstream, S, B>,
    {
        match event {
            EventType::Insert {
                table: event_table,
                data,
                ..
            } => {
                if let Some(err) = pg_walstream_table_mismatch(table.name(), event_table.as_ref()) {
                    return Err(err);
                }
                let insert = build_insert_from_pg(data, relation, table, adapter)?;
                Ok(DiffOps::insert(self, insert))
            }
            EventType::Update {
                table: event_table,
                new_data,
                ..
            } => {
                if let Some(err) = pg_walstream_table_mismatch(table.name(), event_table.as_ref()) {
                    return Err(err);
                }
                let update = build_patchset_update_from_pg(new_data, relation, table, adapter)?;
                Ok(DiffOps::update(self, update))
            }
            EventType::Delete {
                table: event_table,
                old_data,
                ..
            } => {
                if let Some(err) = pg_walstream_table_mismatch(table.name(), event_table.as_ref()) {
                    return Err(err);
                }
                let delete = build_patch_delete_from_pg(old_data, relation, table, adapter)?;
                Ok(DiffOps::delete(self, delete))
            }
            _ => Ok(self),
        }
    }
}

fn lookup_column_info<'a>(
    relation: &'a pg_walstream::RelationInfo,
    name: &str,
) -> Result<&'a pg_walstream::ColumnInfo, ConversionError> {
    relation
        .get_column_by_name(name)
        .ok_or_else(|| ConversionError::ColumnNotFound(name.into()))
}

fn build_insert_from_pg<T, S, B, A>(
    data: &RowData,
    relation: &pg_walstream::RelationInfo,
    table: &T,
    adapter: &A,
) -> Result<Insert<T, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<PgWalstream, S, B>,
{
    let mut insert = Insert::from(table.clone());
    for (name, value) in data.iter() {
        let col_idx = table
            .column_index(name.as_ref())
            .ok_or_else(|| ConversionError::ColumnNotFound(name.as_ref().into()))?;
        let col_info = lookup_column_info(relation, name.as_ref())?;
        let payload = PgWalstreamColumn {
            column_name: name.as_ref(),
            oid: col_info.type_id,
            type_modifier: col_info.type_modifier,
            data: value,
        };
        let decoded = adapter.decode(payload)?;
        insert = insert
            .set(col_idx, decoded)
            .map_err(|_| ConversionError::ColumnNotFound(name.as_ref().into()))?;
    }
    Ok(insert)
}

fn build_changeset_update_from_pg<T, S, B, A>(
    old_data: Option<&RowData>,
    new_data: &RowData,
    relation: &pg_walstream::RelationInfo,
    table: &T,
    adapter: &A,
) -> Result<Update<T, ChangesetFormat, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + Debug + AsRef<str>,
    B: Clone + Debug + AsRef<[u8]>,
    A: WireAdapter<PgWalstream, S, B>,
{
    let mut update: Update<T, ChangesetFormat, S, B> = Update::from(table.clone());
    for (name, new_value) in new_data.iter() {
        let col_idx = table
            .column_index(name.as_ref())
            .ok_or_else(|| ConversionError::ColumnNotFound(name.as_ref().into()))?;
        let col_info = lookup_column_info(relation, name.as_ref())?;
        let new_payload = PgWalstreamColumn {
            column_name: name.as_ref(),
            oid: col_info.type_id,
            type_modifier: col_info.type_modifier,
            data: new_value,
        };
        let new_decoded = adapter.decode(new_payload)?;

        if let Some(old) = old_data
            && let Some(old_value) = old.get(name.as_ref())
        {
            let old_col_info = lookup_column_info(relation, name.as_ref())?;
            let old_payload = PgWalstreamColumn {
                column_name: name.as_ref(),
                oid: old_col_info.type_id,
                type_modifier: old_col_info.type_modifier,
                data: old_value,
            };
            let old_decoded = adapter.decode(old_payload)?;
            update = update
                .set(col_idx, old_decoded, new_decoded)
                .map_err(|_| ConversionError::ColumnNotFound(name.as_ref().into()))?;
            continue;
        }

        update = update
            .set_new(col_idx, new_decoded)
            .map_err(|_| ConversionError::ColumnNotFound(name.as_ref().into()))?;
    }
    Ok(update)
}

fn build_patchset_update_from_pg<T, S, B, A>(
    new_data: &RowData,
    relation: &pg_walstream::RelationInfo,
    table: &T,
    adapter: &A,
) -> Result<Update<T, PatchsetFormat, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<PgWalstream, S, B>,
{
    let mut update: Update<T, PatchsetFormat, S, B> = Update::from(table.clone());
    for (name, value) in new_data.iter() {
        let col_idx = table
            .column_index(name.as_ref())
            .ok_or_else(|| ConversionError::ColumnNotFound(name.as_ref().into()))?;
        let col_info = lookup_column_info(relation, name.as_ref())?;
        let payload = PgWalstreamColumn {
            column_name: name.as_ref(),
            oid: col_info.type_id,
            type_modifier: col_info.type_modifier,
            data: value,
        };
        let decoded = adapter.decode(payload)?;
        update = update
            .set(col_idx, decoded)
            .map_err(|_| ConversionError::ColumnNotFound(name.as_ref().into()))?;
    }
    Ok(update)
}

fn build_changeset_delete_from_pg<T, S, B, A>(
    old_data: &RowData,
    relation: &pg_walstream::RelationInfo,
    table: &T,
    adapter: &A,
) -> Result<ChangeDelete<T, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + Default + AsRef<str>,
    B: Clone + Default + AsRef<[u8]>,
    A: WireAdapter<PgWalstream, S, B>,
{
    let mut delete = ChangeDelete::from(table.clone());
    for (name, value) in old_data.iter() {
        let col_idx = table
            .column_index(name.as_ref())
            .ok_or_else(|| ConversionError::ColumnNotFound(name.as_ref().into()))?;
        let col_info = lookup_column_info(relation, name.as_ref())?;
        let payload = PgWalstreamColumn {
            column_name: name.as_ref(),
            oid: col_info.type_id,
            type_modifier: col_info.type_modifier,
            data: value,
        };
        let decoded = adapter.decode(payload)?;
        delete = delete
            .set(col_idx, decoded)
            .map_err(|_| ConversionError::ColumnNotFound(name.as_ref().into()))?;
    }
    Ok(delete)
}

fn build_patch_delete_from_pg<T, S, B, A>(
    old_data: &RowData,
    relation: &pg_walstream::RelationInfo,
    table: &T,
    adapter: &A,
) -> Result<PatchDelete<T, S, B>, ConversionError>
where
    T: NamedColumns + Clone,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<PgWalstream, S, B>,
{
    let num_pks = table.number_of_primary_keys();
    let mut pk_slots: Vec<Option<Value<S, B>>> = alloc::vec![None; num_pks];

    for (name, value) in old_data.iter() {
        let col_idx = table
            .column_index(name.as_ref())
            .ok_or_else(|| ConversionError::ColumnNotFound(name.as_ref().into()))?;
        if let Some(pk_idx) = table.primary_key_index(col_idx) {
            let col_info = lookup_column_info(relation, name.as_ref())?;
            let payload = PgWalstreamColumn {
                column_name: name.as_ref(),
                oid: col_info.type_id,
                type_modifier: col_info.type_modifier,
                data: value,
            };
            pk_slots[pk_idx] = Some(adapter.decode(payload)?);
        }
    }

    let pk: Vec<Value<S, B>> = pk_slots
        .iter()
        .cloned()
        .collect::<Option<Vec<_>>>()
        .ok_or(ConversionError::MissingData)?;

    Ok(PatchDelete::new(table.clone(), pk))
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::*;
    use crate::SimpleTable;
    use alloc::sync::Arc;
    use alloc::vec;

    /// Build a `RowData` from `(name, ColumnValue)` pairs.
    fn row_data(pairs: &[(&str, ColumnValue)]) -> RowData {
        let mut data = RowData::with_capacity(pairs.len());
        for (name, value) in pairs {
            data.push(Arc::from(*name), value.clone());
        }
        data
    }

    fn make_insert_event(table: &str, data: RowData) -> EventType {
        EventType::Insert {
            schema: Arc::from("public"),
            table: Arc::from(table),
            relation_oid: 12345,
            data,
        }
    }

    fn make_update_event(
        table: &str,
        old_data: Option<RowData>,
        new_data: RowData,
        key_columns: Vec<&str>,
    ) -> EventType {
        EventType::Update {
            schema: Arc::from("public"),
            table: Arc::from(table),
            relation_oid: 12345,
            old_data,
            new_data,
            replica_identity: ReplicaIdentity::Default,
            key_columns: key_columns.into_iter().map(Arc::from).collect(),
        }
    }

    fn make_delete_event(table: &str, old_data: RowData, key_columns: Vec<&str>) -> EventType {
        EventType::Delete {
            schema: Arc::from("public"),
            table: Arc::from(table),
            relation_oid: 12345,
            old_data,
            replica_identity: ReplicaIdentity::Default,
            key_columns: key_columns.into_iter().map(Arc::from).collect(),
        }
    }

    #[test]
    fn test_insert_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let data = row_data(&[
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("Alice")),
        ]);
        let event = make_insert_event("users", data);
        let insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
        let values = insert.into_values();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Value::Integer(1));
        assert_eq!(values[1], Value::Text("Alice".into()));
    }

    #[test]
    fn test_cdc_default_indirect_false() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let data = row_data(&[
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("Alice")),
        ]);
        let event = make_insert_event("users", data);
        let insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
        assert!(!insert.indirect);
    }

    #[test]
    fn test_update_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let old_data = row_data(&[
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("Alice")),
        ]);
        let new_data = row_data(&[
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("Bob")),
        ]);
        let event = make_update_event("users", Some(old_data), new_data, vec!["id"]);
        let _update: Update<_, ChangesetFormat, String, Vec<u8>> =
            (event, table).try_into().unwrap();
    }

    #[test]
    fn test_delete_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let old_data = row_data(&[
            ("id", ColumnValue::text("42")),
            ("name", ColumnValue::text("Alice")),
        ]);
        let event = make_delete_event("users", old_data, vec!["id"]);
        let delete: ChangeDelete<_, String, Vec<u8>> = (event, table).try_into().unwrap();
        let values = delete.into_values();
        assert_eq!(values[0], Value::Integer(42));
    }

    #[test]
    fn test_patch_delete_conversion() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let old_data = row_data(&[("id", ColumnValue::text("42"))]);
        let event = make_delete_event("users", old_data, vec!["id"]);
        let _delete: PatchDelete<_, String, Vec<u8>> = (event, table).try_into().unwrap();
    }

    #[test]
    fn test_table_mismatch() {
        let table = SimpleTable::new("products", &["id", "name"], &[0]);
        let data = row_data(&[("id", ColumnValue::text("1"))]);
        let event = make_insert_event("users", data);
        let result: Result<Insert<_, String, Vec<u8>>, _> = (event, table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_column_not_found() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let data = row_data(&[
            ("id", ColumnValue::text("1")),
            ("unknown", ColumnValue::text("test")),
        ]);
        let event = make_insert_event("users", data);
        let result: Result<Insert<_, String, Vec<u8>>, _> = (event, table).try_into();
        assert!(matches!(result, Err(ConversionError::ColumnNotFound(_))));
    }

    #[test]
    fn test_invalid_event_type() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let data = row_data(&[("id", ColumnValue::text("1"))]);
        let event = make_insert_event("users", data);
        let result: Result<ChangeDelete<_, String, Vec<u8>>, _> = (event, table).try_into();
        assert!(matches!(result, Err(ConversionError::InvalidEventType(_))));
    }

    #[test]
    fn test_null_value() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let data = row_data(&[("id", ColumnValue::text("1")), ("name", ColumnValue::Null)]);
        let event = make_insert_event("users", data);
        let insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
        let values = insert.into_values();
        assert_eq!(values[1], Value::Null);
    }

    #[test]
    fn test_bool_value_true() {
        let table = SimpleTable::new("flags", &["id", "active"], &[0]);
        let data = row_data(&[
            ("id", ColumnValue::text("1")),
            ("active", ColumnValue::text("t")),
        ]);
        let event = make_insert_event("flags", data);
        let insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
        let values = insert.into_values();
        assert_eq!(values[1], Value::Integer(1));
    }

    #[test]
    fn test_bool_value_false() {
        let table = SimpleTable::new("flags", &["id", "active"], &[0]);
        let data = row_data(&[
            ("id", ColumnValue::text("1")),
            ("active", ColumnValue::text("f")),
        ]);
        let event = make_insert_event("flags", data);
        let insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
        let values = insert.into_values();
        assert_eq!(values[1], Value::Integer(0));
    }

    #[test]
    fn test_float_value() {
        let table = SimpleTable::new("prices", &["id", "amount"], &[0]);
        let data = row_data(&[
            ("id", ColumnValue::text("1")),
            ("amount", ColumnValue::text("99.99")),
        ]);
        let event = make_insert_event("prices", data);
        let insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
        let values = insert.into_values();
        assert_eq!(values[1], Value::Real(99.99));
    }

    #[test]
    fn test_binary_value_becomes_blob() {
        let table = SimpleTable::new("data", &["id", "payload"], &[0]);
        let bin = ColumnValue::binary_bytes(bytes::Bytes::copy_from_slice(&[1u8, 2, 3, 4]));
        let data = row_data(&[("id", ColumnValue::text("1")), ("payload", bin)]);
        let event = make_insert_event("data", data);
        let insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
        let values = insert.into_values();
        assert_eq!(values[1], Value::Blob(vec![1, 2, 3, 4]));
    }

    #[test]
    fn test_text_value_falls_back_to_text() {
        let table = SimpleTable::new("logs", &["id", "msg"], &[0]);
        let data = row_data(&[
            ("id", ColumnValue::text("1")),
            ("msg", ColumnValue::text("hello world")),
        ]);
        let event = make_insert_event("logs", data);
        let insert: Insert<_, String, Vec<u8>> = (event, table).try_into().unwrap();
        let values = insert.into_values();
        assert_eq!(values[1], Value::Text("hello world".into()));
    }

    #[test]
    fn test_change_event_wrapper() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        let data = row_data(&[
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("Alice")),
        ]);
        let change_event = ChangeEvent {
            event_type: make_insert_event("users", data),
            lsn: Lsn::from(0x1234_5678),
            metadata: None,
        };
        let insert: Insert<_, String, Vec<u8>> = (change_event, table).try_into().unwrap();
        let values = insert.into_values();
        assert_eq!(values[0], Value::Integer(1));
    }

    // ---- Additional branch coverage ----

    #[test]
    fn test_update_table_mismatch() {
        let table = SimpleTable::new("orders", &["id", "name"], &[0]);
        let new_data = row_data(&[("id", ColumnValue::text("1"))]);
        let event = make_update_event("users", None, new_data, vec!["id"]);
        let result: Result<Update<_, ChangesetFormat, String, Vec<u8>>, _> =
            (event, table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_update_invalid_event_type() {
        let table = SimpleTable::new("users", &["id"], &[0]);
        let data = row_data(&[("id", ColumnValue::text("1"))]);
        let event = make_insert_event("users", data);
        let result: Result<Update<_, ChangesetFormat, String, Vec<u8>>, _> =
            (event, table).try_into();
        assert!(matches!(result, Err(ConversionError::InvalidEventType(_))));
    }

    #[test]
    fn test_update_without_old_data_uses_set_new() {
        let table = SimpleTable::new("t", &["id", "v"], &[0]);
        let new_data = row_data(&[
            ("id", ColumnValue::text("1")),
            ("v", ColumnValue::text("2")),
        ]);
        let event = make_update_event("t", None, new_data, vec!["id"]);
        let update: Update<_, ChangesetFormat, String, Vec<u8>> =
            (event, table).try_into().unwrap();
        let values = update.values();
        assert!(values.iter().any(|v| v.0.is_none() && v.1.is_some()));
    }

    #[test]
    fn test_delete_table_mismatch() {
        let table = SimpleTable::new("orders", &["id"], &[0]);
        let old_data = row_data(&[("id", ColumnValue::text("1"))]);
        let event = make_delete_event("users", old_data, vec!["id"]);
        let result: Result<ChangeDelete<_, String, Vec<u8>>, _> = (event, table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_delete_invalid_event_type() {
        let table = SimpleTable::new("users", &["id"], &[0]);
        let data = row_data(&[("id", ColumnValue::text("1"))]);
        let event = make_insert_event("users", data);
        let result: Result<ChangeDelete<_, String, Vec<u8>>, _> = (event, table).try_into();
        assert!(matches!(result, Err(ConversionError::InvalidEventType(_))));
    }

    #[test]
    fn test_patch_delete_invalid_event_type() {
        let table = SimpleTable::new("users", &["id"], &[0]);
        let data = row_data(&[("id", ColumnValue::text("1"))]);
        let event = make_insert_event("users", data);
        let result: Result<PatchDelete<_, String, Vec<u8>>, _> = (event, table).try_into();
        assert!(matches!(result, Err(ConversionError::InvalidEventType(_))));
    }

    #[test]
    fn test_patch_delete_table_mismatch() {
        let table = SimpleTable::new("orders", &["id"], &[0]);
        let old_data = row_data(&[("id", ColumnValue::text("1"))]);
        let event = make_delete_event("users", old_data, vec!["id"]);
        let result: Result<PatchDelete<_, String, Vec<u8>>, _> = (event, table).try_into();
        assert!(matches!(result, Err(ConversionError::TableMismatch { .. })));
    }

    #[test]
    fn test_patch_delete_missing_key_value() {
        let table = SimpleTable::new("users", &["id", "name"], &[0]);
        // key_columns says `id` is the PK but old_data does not include it.
        let old_data = row_data(&[("name", ColumnValue::text("Alice"))]);
        let event = make_delete_event("users", old_data, vec!["id"]);
        let result: Result<PatchDelete<_, String, Vec<u8>>, _> = (event, table).try_into();
        assert!(matches!(result, Err(ConversionError::MissingData)));
    }

    #[test]
    fn test_change_event_delete_wrapper() {
        let table = SimpleTable::new("users", &["id"], &[0]);
        let old_data = row_data(&[("id", ColumnValue::text("7"))]);
        let change_event = ChangeEvent {
            event_type: make_delete_event("users", old_data, vec!["id"]),
            lsn: Lsn::from(0),
            metadata: None,
        };
        let delete: ChangeDelete<_, String, Vec<u8>> = (change_event, table).try_into().unwrap();
        let values = delete.into_values();
        assert_eq!(values[0], Value::Integer(7));
    }
}
