//! Submodule defining the errors used across the crate.

/// Errors that can occur during diffing and patching operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The provided index is out of bounds for the number of columns in the table.
    #[error("Column index {0} out of bounds for table with {1} columns")]
    ColumnIndexOutOfBounds(usize, usize),
    /// The primary key values are missing or invalid for the specified table.
    #[error("Missing or invalid primary key values for table")]
    MissingPrimaryKey,
    /// The column is not part of the primary key.
    #[error("Column index {0} is not part of the primary key")]
    ColumnIsNotPrimaryKey(usize),
}

// =============================================================================
// sqlparser integration errors (feature-gated)
// =============================================================================

/// Errors that can occur when converting sqlparser AST expressions to our Value type.
#[cfg(feature = "sqlparser")]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValueConversionError {
    /// The expression type is not supported for conversion.
    #[error("Unsupported expression: {0}")]
    UnsupportedExpression(alloc::string::String),
    /// Invalid number format.
    #[error("Invalid number: {0}")]
    InvalidNumber(alloc::string::String),
    /// Invalid hex string format.
    #[error("Invalid hex string: {0}")]
    InvalidHexString(alloc::string::String),
}

/// Errors that can occur when converting a sqlparser INSERT to our Insert builder.
#[cfg(feature = "sqlparser")]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InsertConversionError {
    /// Table name in INSERT doesn't match the schema.
    #[error("Table name mismatch: expected '{expected}', got '{got}'")]
    TableNameMismatch {
        /// The expected table name.
        expected: alloc::string::String,
        /// The actual table name found.
        got: alloc::string::String,
    },
    /// INSERT has no source (VALUES clause).
    #[error("INSERT has no source")]
    NoSource,
    /// INSERT source is not a VALUES clause.
    #[error("INSERT source is not a VALUES clause")]
    NotValuesSource,
    /// INSERT has wrong number of values.
    #[error("Wrong value count: expected {expected}, got {got}")]
    WrongValueCount {
        /// The expected number of values.
        expected: usize,
        /// The actual number of values provided.
        got: usize,
    },
    /// INSERT has multiple rows (not supported).
    #[error("INSERT has multiple rows (not supported)")]
    MultipleRows,
    /// Column name in INSERT doesn't match schema.
    #[error("Column mismatch: {column}")]
    ColumnMismatch {
        /// The column name that doesn't match.
        column: alloc::string::String,
    },
    /// Error converting a value expression.
    #[error("Value conversion error: {0}")]
    ValueConversion(#[from] ValueConversionError),
}

/// Errors that can occur when converting a sqlparser UPDATE to our Update builder.
#[cfg(feature = "sqlparser")]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UpdateConversionError {
    /// Table name in UPDATE doesn't match the schema.
    #[error("Table name mismatch: expected '{expected}', got '{got}'")]
    TableNameMismatch {
        /// The expected table name.
        expected: alloc::string::String,
        /// The actual table name found.
        got: alloc::string::String,
    },
    /// UPDATE has no WHERE clause to identify the row.
    #[error("UPDATE has no WHERE clause")]
    NoWhereClause,
    /// Cannot extract PK values from WHERE clause.
    #[error("Cannot extract PK from WHERE: {0}")]
    CannotExtractPK(alloc::string::String),
    /// Column name in UPDATE doesn't match schema.
    #[error("Column mismatch: {column}")]
    ColumnMismatch {
        /// The column name that doesn't match.
        column: alloc::string::String,
    },
    /// Error converting a value expression.
    #[error("Value conversion error: {0}")]
    ValueConversion(#[from] ValueConversionError),
    /// Missing PK column in WHERE clause.
    #[error("Missing PK column in WHERE: {column}")]
    MissingPKColumn {
        /// The missing primary key column.
        column: alloc::string::String,
    },
}

/// Errors that can occur when converting a sqlparser DELETE to our Delete builder.
#[cfg(feature = "sqlparser")]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeleteConversionError {
    /// Table name in DELETE doesn't match the schema.
    #[error("Table name mismatch: expected '{expected}', got '{got}'")]
    TableNameMismatch {
        /// The expected table name.
        expected: alloc::string::String,
        /// The actual table name found.
        got: alloc::string::String,
    },
    /// DELETE has no FROM clause.
    #[error("DELETE has no FROM clause")]
    NoFromClause,
    /// DELETE has no WHERE clause to identify the row.
    #[error("DELETE has no WHERE clause")]
    NoWhereClause,
    /// Cannot extract PK values from WHERE clause.
    #[error("Cannot extract PK from WHERE: {0}")]
    CannotExtractPK(alloc::string::String),
    /// Column name in DELETE doesn't match schema.
    #[error("Column mismatch: {column}")]
    ColumnMismatch {
        /// The column name that doesn't match.
        column: alloc::string::String,
    },
    /// Error converting a value expression.
    #[error("Value conversion error: {0}")]
    ValueConversion(#[from] ValueConversionError),
    /// Missing PK column in WHERE clause.
    #[error("Missing PK column in WHERE: {column}")]
    MissingPKColumn {
        /// The missing primary key column.
        column: alloc::string::String,
    },
}

/// Errors that can occur when parsing SQL into a DiffSetBuilder.
#[cfg(feature = "sqlparser")]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DiffSetParseError {
    /// SQL parsing failed.
    #[error("SQL parse error: {0}")]
    SqlParser(#[from] sqlparser::parser::ParserError),
    /// INSERT statement conversion failed.
    #[error("INSERT conversion error: {0}")]
    Insert(#[from] InsertConversionError),
    /// UPDATE statement conversion failed.
    #[error("UPDATE conversion error: {0}")]
    Update(#[from] UpdateConversionError),
    /// DELETE statement conversion failed.
    #[error("DELETE conversion error: {0}")]
    Delete(#[from] DeleteConversionError),
    /// Referenced table not found (CREATE TABLE must come before DML statements).
    #[error("Table not found: '{0}' (CREATE TABLE must appear before operations referencing it)")]
    TableNotFound(alloc::string::String),
    /// Unsupported SQL statement type.
    #[error("Unsupported statement: {0}")]
    UnsupportedStatement(alloc::string::String),
}
