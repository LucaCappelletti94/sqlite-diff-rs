//! `pg_walstream` message conversion to `SQLite` changeset operations.
//!
//! [pg_walstream](https://github.com/isdaniel/pg-walstream) parses PostgreSQL
//! logical replication into `EventType` values. This module implements
//! [`Digestable`] on those events so callers fold them
//! into a builder via `DiffSetBuilder::digest(&event, &schema, &adapter)`.

use alloc::string::String;
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
use crate::wire::{Sealed, WireAdapter, WireSource, WireType};
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

    /// Table named in the wire event is not in the schema.
    #[error("Table '{0}' not found in schema")]
    TableNotFound(String),

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

    /// User-registered decoder rejected a column payload.
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

    fn wire_type(payload: &Self::Payload<'_>) -> WireType {
        payload.wire_type
    }

    fn column_name<'a>(payload: &'a Self::Payload<'_>) -> &'a str {
        payload.column_name
    }
}

/// Per-column payload for the `pg_walstream` source.
///
/// The format wrapper populates this once per column before invoking
/// [`WireAdapter::decode`].
#[derive(Debug, Clone, Copy)]
pub struct PgWalstreamColumn<'a> {
    /// Column name resolved from the relation cache.
    pub column_name: &'a str,
    /// Semantic column type driving decoder dispatch.
    pub wire_type: WireType,
    /// Raw wire payload.
    pub data: &'a ColumnValue,
}

impl PgWalstreamColumn<'_> {
    /// Ergonomic helper for calling a specific [`Decoder`](crate::wire::Decoder) on this
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

// Schema-aware digest impls.

use crate::wire::{Digestable, WireColumnTypes, WireSchema};

impl<T, S, B> Digestable<ChangesetFormat, T, S, B> for EventType
where
    T: NamedColumns + WireColumnTypes,
    S: Clone + Debug + Hash + Eq + AsRef<str> + Default,
    B: Clone + Debug + Hash + Eq + AsRef<[u8]> + Default,
{
    type Src = PgWalstream;
    type Error = ConversionError;

    fn digest_into<Sch, A>(
        &self,
        builder: DiffSetBuilder<ChangesetFormat, T, S, B>,
        schema: &Sch,
        adapter: &A,
    ) -> Result<DiffSetBuilder<ChangesetFormat, T, S, B>, ConversionError>
    where
        Sch: WireSchema<Table = T>,
        A: WireAdapter<PgWalstream, S, B>,
    {
        match self {
            EventType::Insert {
                table: name, data, ..
            } => {
                let table = resolve_table(schema, name.as_ref())?;
                let insert = build_insert_from_pg(data, table, adapter)?;
                Ok(DiffOps::insert(builder, insert))
            }
            EventType::Update {
                table: name,
                old_data,
                new_data,
                ..
            } => {
                let table = resolve_table(schema, name.as_ref())?;
                let update =
                    build_changeset_update_from_pg(old_data.as_ref(), new_data, table, adapter)?;
                Ok(DiffOps::update(builder, update))
            }
            EventType::Delete {
                table: name,
                old_data,
                ..
            } => {
                let table = resolve_table(schema, name.as_ref())?;
                let delete = build_changeset_delete_from_pg(old_data, table, adapter)?;
                Ok(DiffOps::delete(builder, delete))
            }
            _ => Ok(builder),
        }
    }
}

impl<T, S, B> Digestable<PatchsetFormat, T, S, B> for EventType
where
    T: NamedColumns + WireColumnTypes,
    S: Clone + Debug + Hash + Eq + AsRef<str> + Default,
    B: Clone + Debug + Hash + Eq + AsRef<[u8]> + Default,
{
    type Src = PgWalstream;
    type Error = ConversionError;

    fn digest_into<Sch, A>(
        &self,
        builder: DiffSetBuilder<PatchsetFormat, T, S, B>,
        schema: &Sch,
        adapter: &A,
    ) -> Result<DiffSetBuilder<PatchsetFormat, T, S, B>, ConversionError>
    where
        Sch: WireSchema<Table = T>,
        A: WireAdapter<PgWalstream, S, B>,
    {
        match self {
            EventType::Insert {
                table: name, data, ..
            } => {
                let table = resolve_table(schema, name.as_ref())?;
                let insert = build_insert_from_pg(data, table, adapter)?;
                Ok(DiffOps::insert(builder, insert))
            }
            EventType::Update {
                table: name,
                new_data,
                ..
            } => {
                let table = resolve_table(schema, name.as_ref())?;
                let update = build_patchset_update_from_pg(new_data, table, adapter)?;
                Ok(DiffOps::update(builder, update))
            }
            EventType::Delete {
                table: name,
                old_data,
                ..
            } => {
                let table = resolve_table(schema, name.as_ref())?;
                let delete = build_patch_delete_from_pg(old_data, table, adapter)?;
                Ok(DiffOps::delete(builder, delete))
            }
            _ => Ok(builder),
        }
    }
}

fn resolve_table<'a, Sch>(schema: &'a Sch, name: &str) -> Result<&'a Sch::Table, ConversionError>
where
    Sch: WireSchema,
{
    schema
        .get(name)
        .ok_or_else(|| ConversionError::TableNotFound(name.into()))
}

fn build_insert_from_pg<T, S, B, A>(
    data: &RowData,
    table: &T,
    adapter: &A,
) -> Result<Insert<T, S, B>, ConversionError>
where
    T: NamedColumns + WireColumnTypes,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<PgWalstream, S, B>,
{
    let mut insert = Insert::from(table.clone());
    for (name, value) in data.iter() {
        let col_idx = table
            .column_index(name.as_ref())
            .ok_or_else(|| ConversionError::ColumnNotFound(name.as_ref().into()))?;
        let payload = PgWalstreamColumn {
            column_name: name.as_ref(),
            wire_type: table.column_type(col_idx),
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
    table: &T,
    adapter: &A,
) -> Result<Update<T, ChangesetFormat, S, B>, ConversionError>
where
    T: NamedColumns + WireColumnTypes,
    S: Clone + Debug + AsRef<str>,
    B: Clone + Debug + AsRef<[u8]>,
    A: WireAdapter<PgWalstream, S, B>,
{
    let mut update: Update<T, ChangesetFormat, S, B> = Update::from(table.clone());
    for (name, new_value) in new_data.iter() {
        let col_idx = table
            .column_index(name.as_ref())
            .ok_or_else(|| ConversionError::ColumnNotFound(name.as_ref().into()))?;
        let wire_type = table.column_type(col_idx);
        let new_payload = PgWalstreamColumn {
            column_name: name.as_ref(),
            wire_type,
            data: new_value,
        };
        let new_decoded = adapter.decode(new_payload)?;

        if let Some(old) = old_data
            && let Some(old_value) = old.get(name.as_ref())
        {
            let old_payload = PgWalstreamColumn {
                column_name: name.as_ref(),
                wire_type,
                data: old_value,
            };
            let old_decoded = adapter.decode(old_payload)?;
            update = update
                .set(col_idx, old_decoded, new_decoded)
                .map_err(|_| ConversionError::ColumnNotFound(name.as_ref().into()))?;
            continue;
        }

        // No old value on the wire. Under REPLICA IDENTITY DEFAULT a non-key
        // update sends no old tuple, so the key did not change and its old
        // value equals the new value. Keep it for primary-key columns so the
        // WHERE predicate can be built; other columns stay set_new.
        update = if table.primary_key_index(col_idx).is_some() {
            update
                .set(col_idx, new_decoded.clone(), new_decoded)
                .map_err(|_| ConversionError::ColumnNotFound(name.as_ref().into()))?
        } else {
            update
                .set_new(col_idx, new_decoded)
                .map_err(|_| ConversionError::ColumnNotFound(name.as_ref().into()))?
        };
    }
    Ok(update)
}

fn build_patchset_update_from_pg<T, S, B, A>(
    new_data: &RowData,
    table: &T,
    adapter: &A,
) -> Result<Update<T, PatchsetFormat, S, B>, ConversionError>
where
    T: NamedColumns + WireColumnTypes,
    S: Clone + AsRef<str>,
    B: Clone + AsRef<[u8]>,
    A: WireAdapter<PgWalstream, S, B>,
{
    let mut update: Update<T, PatchsetFormat, S, B> = Update::from(table.clone());
    for (name, value) in new_data.iter() {
        let col_idx = table
            .column_index(name.as_ref())
            .ok_or_else(|| ConversionError::ColumnNotFound(name.as_ref().into()))?;
        let payload = PgWalstreamColumn {
            column_name: name.as_ref(),
            wire_type: table.column_type(col_idx),
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
    table: &T,
    adapter: &A,
) -> Result<ChangeDelete<T, S, B>, ConversionError>
where
    T: NamedColumns + WireColumnTypes,
    S: Clone + Default + AsRef<str>,
    B: Clone + Default + AsRef<[u8]>,
    A: WireAdapter<PgWalstream, S, B>,
{
    let mut delete = ChangeDelete::from(table.clone());
    for (name, value) in old_data.iter() {
        let col_idx = table
            .column_index(name.as_ref())
            .ok_or_else(|| ConversionError::ColumnNotFound(name.as_ref().into()))?;
        let payload = PgWalstreamColumn {
            column_name: name.as_ref(),
            wire_type: table.column_type(col_idx),
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
    table: &T,
    adapter: &A,
) -> Result<PatchDelete<T, S, B>, ConversionError>
where
    T: NamedColumns + WireColumnTypes,
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
            let payload = PgWalstreamColumn {
                column_name: name.as_ref(),
                wire_type: table.column_type(col_idx),
                data: value,
            };
            pk_slots[pk_idx] = Some(adapter.decode(payload)?);
        }
    }

    let pk: Vec<Value<S, B>> = pk_slots
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or(ConversionError::MissingData)?;

    Ok(PatchDelete::new(table.clone(), pk))
}
