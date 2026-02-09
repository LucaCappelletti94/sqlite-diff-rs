//! Submodule defining the errors used across the crate.

/// Errors that can occur during diffing and patching operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The provided index is out of bounds for the number of columns in the table.
    #[error("Column index {0} out of bounds for table with {1} columns")]
    ColumnIndexOutOfBounds(usize, usize),
}
