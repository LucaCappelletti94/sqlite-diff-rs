#![doc = include_str!("../README.md")]
#![no_std]
#![deny(clippy::mod_module_files)]

extern crate alloc;

pub mod builders;
#[cfg(all(feature = "sqlparser", feature = "rusqlite"))]
pub mod differential_testing;
pub mod encoding;
pub mod errors;
pub mod parser;
pub mod schema;

// Re-export main types
pub use builders::{
    ChangeDelete, ChangeSet, ChangesetFormat, DiffSetBuilder, Format, Insert, PatchDelete,
    PatchSet, PatchsetFormat, Reverse, Update,
};
pub use parser::{FormatMarker, ParseError, ParsedDiffSet, ParsedTableSchema};
pub use schema::{DynTable, SchemaWithPK};

// Re-export errors
pub use errors::Error;
#[cfg(feature = "sqlparser")]
pub use errors::{
    DeleteConversionError, DiffSetParseError, InsertConversionError, UpdateConversionError,
    ValueConversionError,
};
