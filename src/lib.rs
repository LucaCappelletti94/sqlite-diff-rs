#![doc = include_str!("../README.md")]
#![no_std]
#![deny(clippy::mod_module_files)]
#![allow(private_bounds, private_interfaces)]

extern crate alloc;

pub mod builders;
#[cfg(any(test, feature = "testing"))]
pub mod differential_testing;
pub(crate) mod encoding;
pub mod errors;
#[cfg(feature = "maxwell")]
pub mod maxwell;
pub mod parser;
#[cfg(feature = "pg-walstream")]
pub mod pg_walstream;
#[cfg(feature = "pg-walstream")]
pub mod pg_walstream_reverse;
pub mod schema;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
#[cfg(feature = "wal2json")]
pub mod wal2json;
pub mod wire;

// Re-export main types
#[cfg(feature = "diesel")]
pub use builders::{
    Adapter, ApplyOps, Binder, BoundChangesetOp, BoundOp, BoundPatchsetOp, DefaultBinder,
};
pub use builders::{
    ChangeDelete, ChangeSet, ChangesetFormat, ChangesetOp, ChangesetUpdatePair, ColumnNames,
    DiffOps, DiffSet, DiffSetBuilder, Indirect, Insert, PatchDelete, PatchSet, PatchsetFormat,
    PatchsetOp, PatchsetUpdateEntry, Reverse, Update,
};
pub use encoding::Value;
pub use parser::{FormatMarker, ParseError, ParsedDiffSet, TableSchema};
pub use schema::{DynTable, IndexableValues, NamedColumns, SchemaWithPK, SimpleTable};
pub use wire::{
    BoolDecoder, DateVerbatimDecoder, DecimalTextDecoder, DecodeError, Decoder, Digestable,
    Int64OverflowToTextDecoder, IntDecoder, IntervalVerbatimDecoder, JsonCanonicalDecoder,
    JsonVerbatimDecoder, MySqlBinaryDecoder, NullDecoder, PgByteaBinaryDecoder,
    PgByteaTextModeDecoder, RealDecoder, TextDecoder, TimeVerbatimDecoder,
    TimestampTzVerbatimDecoder, TimestampVerbatimDecoder, TypeMap, TypeMapDefaults,
    UuidBlob16Decoder, UuidText36Decoder, WireAdapter, WireColumnTypes, WireSchema, WireSource,
    WireType,
};

// Type aliases for common use cases
/// Type alias for `Update<T, ChangesetFormat, S, B>`.
///
/// Changeset updates store both old and new values for each column.
pub type ChangeUpdate<T, S, B> = Update<T, ChangesetFormat, S, B>;

/// Type alias for `Update<T, PatchsetFormat, S, B>`.
///
/// Patchset updates only store new values (PK values in new, non-PK as Undefined or new value).
pub type PatchUpdate<T, S, B> = Update<T, PatchsetFormat, S, B>;

// Re-export errors
pub use errors::Error;
