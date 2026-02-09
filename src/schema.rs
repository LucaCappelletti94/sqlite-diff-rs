//! Schema traits for compile-time and runtime table definitions.
mod dyn_table;
mod simple_table;

pub(crate) use dyn_table::IndexableValues;
pub use dyn_table::{DynTable, SchemaWithPK};
pub use simple_table::{NamedColumns, SimpleTable};
