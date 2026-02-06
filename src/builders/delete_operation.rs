//! Submodule defining a builder for a delete operation.

use alloc::vec;
use alloc::vec::Vec;

use crate::{DynTable, encoding::Value};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Represents a delete operation in changeset format.
///
/// Stores the full old-row values for all columns.
pub struct ChangeDelete<T: DynTable> {
    table: T,
    /// Old values for the deleted row.
    values: Vec<Value>,
}

impl<T: DynTable> AsRef<T> for ChangeDelete<T> {
    fn as_ref(&self) -> &T {
        &self.table
    }
}

impl<T: DynTable> From<T> for ChangeDelete<T> {
    fn from(table: T) -> Self {
        let num_cols = table.number_of_columns();
        Self {
            table,
            values: vec![Value::Null; num_cols],
        }
    }
}

impl<T: DynTable> ChangeDelete<T> {
    /// Create a delete operation with the given values.
    pub(crate) fn from_values(table: T, values: Vec<Value>) -> Self {
        Self { table, values }
    }

    /// Sets the value for a specific column by index.
    ///
    /// # Arguments
    ///
    /// * `col_idx` - The index of the column to set.
    /// * `value` - The value to set for the column.
    ///
    /// # Errors
    ///
    /// * `ColumnIndexOutOfBounds` - If the provided column index is out of bounds for the table schema.
    ///
    pub fn set(
        mut self,
        col_idx: usize,
        value: impl Into<Value>,
    ) -> Result<Self, crate::errors::Error> {
        if col_idx >= self.values.len() {
            return Err(crate::errors::Error::ColumnIndexOutOfBounds(
                col_idx,
                self.values.len(),
            ));
        }
        self.values[col_idx] = value.into();
        Ok(self)
    }

    /// Sets a column to NULL.
    ///
    /// This is a convenience method equivalent to `.set(col_idx, Value::Null)`.
    ///
    /// # Errors
    ///
    /// * `ColumnIndexOutOfBounds` - If the provided column index is out of bounds for the table schema.
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_diff_rs::ChangeDelete;
    /// use sqlparser::ast::CreateTable;
    /// use sqlparser::dialect::SQLiteDialect;
    /// use sqlparser::parser::Parser;
    ///
    /// let dialect = SQLiteDialect {};
    /// let sql = "CREATE TABLE items (id INTEGER PRIMARY KEY, description TEXT)";
    /// let statements = Parser::parse_sql(&dialect, sql).unwrap();
    /// let schema = match &statements[0] {
    ///     sqlparser::ast::Statement::CreateTable(ct) => ct.clone(),
    ///     _ => panic!(),
    /// };
    ///
    /// // DELETE FROM items WHERE id = 1 AND description IS NULL
    /// let delete = ChangeDelete::from(schema)
    ///     .set(0, 1i64).unwrap()
    ///     .set_null(1).unwrap();
    /// ```
    pub fn set_null(self, col_idx: usize) -> Result<Self, crate::errors::Error> {
        self.set(col_idx, Value::Null)
    }

    /// Returns a reference to the values.
    pub(crate) fn values(&self) -> &[Value] {
        &self.values
    }

    /// Consumes self and returns the values.
    pub(crate) fn into_values(self) -> Vec<Value> {
        self.values
    }
}

// =============================================================================
// sqlparser integration
// =============================================================================

#[cfg(feature = "sqlparser")]
mod sqlparser_impl {
    use alloc::boxed::Box;
    use alloc::vec;
    use core::fmt::{self, Display};

    use sqlparser::ast::{
        self, CreateTable, Expr, FromTable, Ident, TableFactor, TableWithJoins,
        helpers::attached_token::AttachedToken,
    };

    use super::ChangeDelete;
    use crate::builders::ast_helpers::{extract_where_conditions, make_table_factor};
    use crate::errors::DeleteConversionError;
    use crate::schema::{DynTable, sqlparser::get_pk_indices};

    impl<'a> ChangeDelete<&'a CreateTable> {
        /// Try to create a ChangeDelete from a sqlparser DELETE statement and a table schema.
        ///
        /// Note: A DELETE statement only contains PK values in the WHERE clause.
        /// For a full changeset, we would need the old values for all columns,
        /// which are not present in the DELETE statement itself.
        /// Non-PK columns are set to Null (unknown).
        ///
        /// # Errors
        ///
        /// Returns `DeleteConversionError` if the DELETE statement cannot be converted.
        pub(crate) fn try_from_ast(
            delete: &ast::Delete,
            schema: &'a CreateTable,
        ) -> Result<Self, DeleteConversionError> {
            // Extract table name from FROM clause
            let delete_table_name = match &delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables
                    .first()
                    .and_then(|t| match &t.relation {
                        TableFactor::Table { name, .. } => name.0.last().map(|part| match part {
                            ast::ObjectNamePart::Identifier(ident) => ident.value.as_str(),
                            ast::ObjectNamePart::Function(func) => func.name.value.as_str(),
                        }),
                        _ => None,
                    })
                    .unwrap_or(""),
            };

            let schema_name = schema.name();
            if delete_table_name != schema_name {
                return Err(DeleteConversionError::TableNameMismatch {
                    expected: schema_name.into(),
                    got: delete_table_name.into(),
                });
            }

            // Extract PK values from WHERE clause
            let where_clause = delete
                .selection
                .as_ref()
                .ok_or(DeleteConversionError::NoWhereClause)?;
            let where_conditions =
                extract_where_conditions(where_clause, DeleteConversionError::CannotExtractPK)?;

            // Build the ChangeDelete
            let pk_indices = get_pk_indices(schema);
            let mut result = ChangeDelete::from(schema);

            // Set values from WHERE clause
            for (col_name, value) in where_conditions {
                let col_idx = result
                    .as_ref()
                    .columns
                    .iter()
                    .position(|c| c.name.value == col_name)
                    .ok_or_else(|| DeleteConversionError::ColumnMismatch {
                        column: col_name.clone(),
                    })?;

                result = result
                    .set(col_idx, value)
                    .map_err(|_| DeleteConversionError::ColumnMismatch { column: col_name })?;
            }

            // Verify all PK columns are set
            for &pk_idx in &pk_indices {
                let col_name = &result.as_ref().columns[pk_idx].name.value;
                if result.values()[pk_idx].is_null() {
                    return Err(DeleteConversionError::MissingPKColumn {
                        column: col_name.clone(),
                    });
                }
            }

            Ok(result)
        }
    }

    impl From<&ChangeDelete<CreateTable>> for ast::Delete {
        fn from(delete: &ChangeDelete<CreateTable>) -> Self {
            let table = delete.as_ref();
            let values = delete.values();
            let pk_indices = get_pk_indices(table);

            // Build WHERE clause from PK columns
            let selection = pk_indices
                .iter()
                .map(|&pk_idx| {
                    let value = &values[pk_idx];
                    Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new(
                            &table.columns[pk_idx].name.value,
                        ))),
                        op: ast::BinaryOperator::Eq,
                        right: Box::new(value.into()),
                    }
                })
                .reduce(|acc, expr| Expr::BinaryOp {
                    left: Box::new(acc),
                    op: ast::BinaryOperator::And,
                    right: Box::new(expr),
                });

            ast::Delete {
                delete_token: AttachedToken::empty(),
                optimizer_hint: None,
                tables: vec![],
                from: FromTable::WithFromKeyword(vec![TableWithJoins {
                    relation: make_table_factor(table.name()),
                    joins: vec![],
                }]),
                using: None,
                selection,
                returning: None,
                order_by: vec![],
                limit: None,
            }
        }
    }

    impl Display for ChangeDelete<CreateTable> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let stmt: ast::Delete = self.into();
            write!(f, "{stmt}")
        }
    }
}
