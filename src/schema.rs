//! Schema traits for compile-time and runtime table definitions.
mod dyn_table;
mod simple_table;

pub use dyn_table::{DynTable, SchemaWithPK};
pub use simple_table::SimpleTable;
