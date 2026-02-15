#![doc = include_str!("../README.md")]
#![no_std]
#![deny(clippy::mod_module_files)]
#![allow(private_bounds, private_interfaces)]

extern crate alloc;

pub mod builders;
#[cfg(feature = "debezium")]
pub mod debezium;
#[cfg(any(test, feature = "testing"))]
pub mod differential_testing;
pub(crate) mod encoding;
pub mod errors;
#[cfg(feature = "maxwell")]
pub mod maxwell;
pub mod parser;
#[cfg(feature = "pg-walstream")]
pub mod pg_walstream;
pub mod schema;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
#[cfg(feature = "wal2json")]
pub mod wal2json;

// Re-export main types
pub use builders::{
    ChangeDelete, ChangeSet, ChangesetFormat, ColumnNames, DiffOps, DiffSet, DiffSetBuilder,
    Insert, PatchDelete, PatchSet, PatchsetFormat, Reverse, Update,
};
pub use encoding::Value;
pub use parser::{FormatMarker, ParseError, ParsedDiffSet, TableSchema};
pub(crate) use schema::IndexableValues;
pub use schema::{DynTable, NamedColumns, SchemaWithPK, SimpleTable};

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
