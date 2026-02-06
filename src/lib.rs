#![doc = include_str!("../README.md")]
#![no_std]
#![deny(clippy::mod_module_files)]
#![allow(private_bounds, private_interfaces)]

extern crate alloc;

pub mod builders;
#[cfg(all(feature = "sqlparser", feature = "rusqlite"))]
pub mod differential_testing;
pub(crate) mod encoding;
pub mod errors;
pub mod parser;
pub mod schema;

// Re-export main types
pub use builders::{
    ChangeDelete, ChangeSet, ChangesetFormat, DiffSetBuilder, Insert, PatchSet, PatchsetFormat,
    Reverse, Update,
};
pub use encoding::Value;
pub use parser::{FormatMarker, ParseError, ParsedDiffSet, ParsedTableSchema};
pub use schema::{DynTable, SchemaWithPK};

// Type aliases for common use cases
/// Type alias for `Update<T, ChangesetFormat>`.
///
/// Changeset updates store both old and new values for each column.
pub type ChangeUpdate<T> = Update<T, ChangesetFormat>;

/// Type alias for `Update<T, PatchsetFormat>`.
///
/// Patchset updates only store new values (PK values in new, non-PK as Undefined or new value).
pub type PatchUpdate<T> = Update<T, PatchsetFormat>;

// Re-export errors
pub use errors::Error;
#[cfg(feature = "sqlparser")]
pub use errors::{
    DeleteConversionError, DiffSetParseError, InsertConversionError, UpdateConversionError,
    ValueConversionError,
};
