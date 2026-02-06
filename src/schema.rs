//! Schema traits for compile-time and runtime table definitions.
mod dyn_table;

pub use dyn_table::{DynTable, SchemaWithPK};

#[cfg(feature = "sqlparser")]
pub(crate) mod sqlparser;
