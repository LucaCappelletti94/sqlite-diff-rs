//! Submodule defining a builder for an delete operation.

use alloc::vec;
use alloc::vec::Vec;
use core::ops::Add;

use crate::{
    DynTable, SchemaWithPK,
    builders::{
        ChangesetFormat, Insert, PatchsetFormat, Update, format::Format, operation::Reverse,
    },
    encoding::Value,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Represents a delete operation in changeset format.
pub struct ChangeDelete<T: DynTable> {
    table: T,
    /// Old values for the deleted row.
    values: Vec<Value>,
}

impl<T: DynTable> Reverse for ChangeDelete<T> {
    type Output = crate::builders::Insert<T>;

    fn reverse(self) -> Self::Output {
        let mut insert = crate::builders::Insert::from(self.table);
        for (idx, value) in self.values.into_iter().enumerate() {
            // Skip Undefined values
            if !value.is_undefined() {
                insert = insert.set(idx, value).unwrap();
            }
        }
        insert
    }
}

impl<T: DynTable> AsRef<T> for ChangeDelete<T> {
    fn as_ref(&self) -> &T {
        &self.table
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Represents a delete operation in patchset format.
pub struct PatchDelete<T: DynTable> {
    table: T,
}

impl<T: DynTable> AsRef<T> for PatchDelete<T> {
    fn as_ref(&self) -> &T {
        &self.table
    }
}

impl<T: DynTable> Reverse for PatchDelete<T> {
    type Output = crate::builders::Insert<T>;

    fn reverse(self) -> Self::Output {
        crate::builders::Insert::from(self.table)
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

impl<T: SchemaWithPK> From<T> for PatchDelete<T> {
    fn from(table: T) -> Self {
        // Count PK columns
        let num_cols = table.number_of_columns();
        let mut pk_flags = vec![0u8; num_cols];
        table.write_pk_flags(&mut pk_flags);

        Self { table }
    }
}

impl<T: DynTable> ChangeDelete<T> {
    /// Create a delete operation with the given values.
    pub fn from_values(table: T, values: Vec<Value>) -> Self {
        Self { table, values }
    }

    /// Replaces the old values with the provided ones.
    pub(super) fn replace_values(mut self, values: Vec<Value>) -> Self {
        assert_eq!(values.len(), self.values.len());
        self.values = values;
        self
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

    /// Returns a reference to the values.
    pub fn values(&self) -> &[Value] {
        &self.values
    }
}

// ============================================================================
// DELETE + INSERT combinations
// ============================================================================

/// DELETE + INSERT (changeset): If values differ → UPDATE, if same → cancel out
impl<T: DynTable + Clone> Add<Insert<T>> for ChangeDelete<T> {
    type Output = Option<Update<T, ChangesetFormat>>;

    fn add(self, rhs: Insert<T>) -> Self::Output {
        assert_eq!(&self.table, rhs.as_ref());
        if self.values == *rhs.values() {
            None // Same values - cancel out
        } else {
            // Different values - becomes UPDATE from original to new
            let mut update: Update<T, ChangesetFormat> = Update::from(self.table);
            for (idx, (old, new)) in self.values.into_iter().zip(rhs.into_values()).enumerate() {
                // Skip if either value is Undefined
                if !old.is_undefined() && !new.is_undefined() {
                    update = update.set(idx, old, new).unwrap();
                }
            }
            Some(update)
        }
    }
}

/// DELETE + INSERT (patchset): Always becomes UPDATE (can't compare old values)
impl<T: DynTable + Clone> Add<Insert<T>> for PatchDelete<T> {
    type Output = Update<T, PatchsetFormat>;

    fn add(self, rhs: Insert<T>) -> Self::Output {
        assert_eq!(&self.table, rhs.as_ref());
        let mut update: Update<T, PatchsetFormat> = Update::from(self.table);
        for (idx, new) in rhs.into_values().into_iter().enumerate() {
            // Skip Undefined values - they represent unchanged columns
            if !new.is_undefined() {
                update = update.set(idx, new).unwrap();
            }
        }
        update
    }
}

// ============================================================================
// DELETE + UPDATE combinations
// ============================================================================

/// DELETE + UPDATE: Ignore the new update, keep DELETE
impl<T: DynTable, F: Format> Add<Update<T, F>> for ChangeDelete<T> {
    type Output = Self;

    fn add(self, rhs: Update<T, F>) -> Self::Output {
        assert_eq!(&self.table, rhs.as_ref());
        self
    }
}

impl<T: DynTable, F: Format> Add<Update<T, F>> for PatchDelete<T> {
    type Output = Self;

    fn add(self, rhs: Update<T, F>) -> Self::Output {
        assert_eq!(&self.table, rhs.as_ref());
        self
    }
}

// ============================================================================
// DELETE + DELETE combinations
// ============================================================================

/// DELETE + DELETE: Ignore the new delete, keep first
impl<T: DynTable> Add<ChangeDelete<T>> for ChangeDelete<T> {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        assert_eq!(&self.table, rhs.as_ref());
        self
    }
}

impl<T: DynTable> Add<PatchDelete<T>> for PatchDelete<T> {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        assert_eq!(&self.table, rhs.as_ref());
        self
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
        pub fn try_from_ast(
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
