//! Submodule defining a builder for an update operation.

use alloc::vec;
use alloc::vec::Vec;

use crate::{
    DynTable,
    builders::{ChangesetFormat, PatchsetFormat, format::Format},
    encoding::Value,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Builder for an update operation, parameterized by the format type `F`.
pub struct Update<T: DynTable, F: Format> {
    /// The table being updated.
    table: T,
    /// Values for the updated row, stored as pairs of (old, new) values.
    values: Vec<(F::Old, Value)>,
}

impl<T: DynTable, F: Format> From<Update<T, F>> for Vec<(F::Old, Value)> {
    fn from(update: Update<T, F>) -> Self {
        update.values
    }
}

impl<T: DynTable, F: Format> AsRef<T> for Update<T, F> {
    fn as_ref(&self) -> &T {
        &self.table
    }
}

impl<T: DynTable, F: Format> Update<T, F> {
    /// Returns a reference to the (old, new) value pairs.
    pub fn values(&self) -> &[(F::Old, Value)] {
        &self.values
    }
}

impl<T: DynTable, F: Format> From<T> for Update<T, F> {
    fn from(table: T) -> Self {
        let num_cols = table.number_of_columns();
        Self {
            table,
            values: vec![(F::Old::default(), Value::default()); num_cols],
        }
    }
}

impl<T: DynTable> Update<T, ChangesetFormat> {
    /// Create an update operation with the given old and new values.
    pub fn from_values(table: T, old_values: Vec<Value>, new_values: Vec<Value>) -> Self {
        let values = old_values.into_iter().zip(new_values).collect();
        Self { table, values }
    }

    /// Sets the value for a specific column by index.
    ///
    /// # Arguments
    ///
    /// * `col_idx` - The index of the column to set.
    /// * `old` - The old value for the column.
    /// * `new` - The new value for the column.
    ///
    /// # Errors
    ///
    /// * `ColumnIndexOutOfBounds` - If the provided column index is out of bounds for the table schema.
    ///
    pub fn set(
        mut self,
        col_idx: usize,
        old: impl Into<Value>,
        new: impl Into<Value>,
    ) -> Result<Self, crate::errors::Error> {
        if col_idx >= self.values.len() {
            return Err(crate::errors::Error::ColumnIndexOutOfBounds(
                col_idx,
                self.values.len(),
            ));
        }
        let old_value = old.into();
        let new_value = new.into();

        if old_value.is_undefined() || new_value.is_undefined() {
            return Err(crate::errors::Error::UndefinedValueProvided);
        }

        self.values[col_idx] = (old_value, new_value);
        Ok(self)
    }

    /// Sets only the new value for a column, leaving old as Undefined.
    ///
    /// This is useful when the old value is not known (e.g., when parsing SQL
    /// UPDATE statements where only the new value is specified).
    ///
    /// # Arguments
    ///
    /// * `col_idx` - The index of the column to set.
    /// * `new` - The new value for the column.
    ///
    /// # Errors
    ///
    /// * `ColumnIndexOutOfBounds` - If the column index is out of bounds.
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_diff_rs::{Update, ChangesetFormat};
    /// use sqlparser::ast::CreateTable;
    /// use sqlparser::dialect::SQLiteDialect;
    /// use sqlparser::parser::Parser;
    ///
    /// let dialect = SQLiteDialect {};
    /// let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
    /// let statements = Parser::parse_sql(&dialect, sql).unwrap();
    /// let schema = match &statements[0] {
    ///     sqlparser::ast::Statement::CreateTable(ct) => ct.clone(),
    ///     _ => panic!(),
    /// };
    ///
    /// // UPDATE users SET name = 'Bob' WHERE id = 1
    /// // We know id=1 (PK, unchanged) and name='Bob' (new), but not the old name
    /// let update = Update::<_, ChangesetFormat>::from(schema)
    ///     .set(0, 1i64, 1i64).unwrap()      // PK: old=1, new=1 (unchanged)
    ///     .set_new(1, "Bob").unwrap();      // name: old=Undefined, new="Bob"
    /// ```
    pub fn set_new(
        mut self,
        col_idx: usize,
        new: impl Into<Value>,
    ) -> Result<Self, crate::errors::Error> {
        if col_idx >= self.values.len() {
            return Err(crate::errors::Error::ColumnIndexOutOfBounds(
                col_idx,
                self.values.len(),
            ));
        }
        let new_value = new.into();

        if new_value.is_undefined() {
            return Err(crate::errors::Error::UndefinedValueProvided);
        }

        self.values[col_idx] = (Value::Undefined, new_value);
        Ok(self)
    }

    /// Sets a column to NULL for both old and new values.
    ///
    /// # Errors
    ///
    /// * `ColumnIndexOutOfBounds` - If the column index is out of bounds for the table schema.
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_diff_rs::{Update, ChangesetFormat};
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
    /// // UPDATE items SET description = NULL WHERE id = 1 AND description = NULL
    /// let update = Update::<_, ChangesetFormat>::from(schema)
    ///     .set(0, 1i64, 1i64).unwrap()
    ///     .set_null(1).unwrap();
    /// ```
    pub fn set_null(self, col_idx: usize) -> Result<Self, crate::errors::Error> {
        self.set(col_idx, Value::Null, Value::Null)
    }
}

impl<T: DynTable> Update<T, PatchsetFormat> {
    /// Create an update operation with the given new values (patchset ignores old).
    pub fn from_new_values(table: T, new_values: Vec<Value>) -> Self {
        let values = new_values.into_iter().map(|new| ((), new)).collect();
        Self { table, values }
    }

    /// Returns the new values as a slice.
    ///
    /// This is useful for extracting the primary key values for patchset operations,
    /// where the PK values are stored in the new values.
    pub fn new_values(&self) -> Vec<Value> {
        self.values.iter().map(|((), new)| new.clone()).collect()
    }

    /// Sets the value for a specific column by index.
    ///
    /// # Implementation Note
    ///
    /// In the patchset format, the old value is not stored for updates,
    /// so we set it to the default value of `()`. Only the new value is stored.
    ///
    /// # Arguments
    ///
    /// * `col_idx` - The index of the column to set.
    /// * `new` - The new value for the column.
    ///
    /// # Errors
    ///
    /// * `ColumnIndexOutOfBounds` - If the provided column index is out of bounds for the table schema.
    ///
    pub fn set(
        mut self,
        col_idx: usize,
        new: impl Into<Value>,
    ) -> Result<Self, crate::errors::Error> {
        if col_idx >= self.values.len() {
            return Err(crate::errors::Error::ColumnIndexOutOfBounds(
                col_idx,
                self.values.len(),
            ));
        }

        let new_value = new.into();
        if new_value.is_undefined() {
            return Err(crate::errors::Error::UndefinedValueProvided);
        }

        self.values[col_idx] = ((), new_value);
        Ok(self)
    }

    /// Sets a column to NULL.
    ///
    /// # Errors
    ///
    /// * `ColumnIndexOutOfBounds` - If the column index is out of bounds for the table schema.
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_diff_rs::{Update, PatchsetFormat};
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
    /// // UPDATE items SET description = NULL WHERE id = 1
    /// let update = Update::<_, PatchsetFormat>::from(schema)
    ///     .set(0, 1i64).unwrap()
    ///     .set_null(1).unwrap();
    /// ```
    pub fn set_null(self, col_idx: usize) -> Result<Self, crate::errors::Error> {
        self.set(col_idx, Value::Null)
    }
}

// =============================================================================
// sqlparser integration
// =============================================================================

#[cfg(feature = "sqlparser")]
mod sqlparser_impl {
    use alloc::boxed::Box;
    use alloc::vec;
    use alloc::vec::Vec;
    use core::fmt::{self, Display};

    use sqlparser::ast::{
        self, Assignment, AssignmentTarget, CreateTable, Expr, Ident, TableFactor, TableWithJoins,
        helpers::attached_token::AttachedToken,
    };

    use super::Update;
    use crate::builders::ChangesetFormat;
    use crate::builders::PatchsetFormat;
    use crate::builders::ast_helpers::{
        extract_where_conditions, make_object_name, make_table_factor,
    };
    use crate::encoding::Value;
    use crate::errors::UpdateConversionError;
    use crate::schema::{DynTable, sqlparser::get_pk_indices};

    impl<'a> Update<&'a CreateTable, ChangesetFormat> {
        /// Try to create an Update from a sqlparser UPDATE statement and a table schema.
        ///
        /// Note: This creates a changeset-style Update where we need both old and new values.
        /// The old values for PK columns come from the WHERE clause.
        /// For non-PK columns not in the SET clause, old = new = Undefined (unchanged).
        /// For non-PK columns in the SET clause, old is set to Undefined (unknown from SQL alone).
        ///
        /// # Errors
        ///
        /// Returns `UpdateConversionError` if the UPDATE statement cannot be converted.
        pub fn try_from_ast(
            update: &ast::Update,
            schema: &'a CreateTable,
        ) -> Result<Self, UpdateConversionError> {
            // Validate table name
            let update_table_name = match &update.table.relation {
                TableFactor::Table { name, .. } => name.0.last().map_or("", |part| match part {
                    ast::ObjectNamePart::Identifier(ident) => ident.value.as_str(),
                    ast::ObjectNamePart::Function(func) => func.name.value.as_str(),
                }),
                _ => "",
            };

            let schema_name = schema.name();
            if update_table_name != schema_name {
                return Err(UpdateConversionError::TableNameMismatch {
                    expected: schema_name.into(),
                    got: update_table_name.into(),
                });
            }

            // Extract PK values from WHERE clause
            let where_clause = update
                .selection
                .as_ref()
                .ok_or(UpdateConversionError::NoWhereClause)?;
            let where_conditions =
                extract_where_conditions(where_clause, UpdateConversionError::CannotExtractPK)?;

            // Get schema info
            let pk_indices = get_pk_indices(schema);
            let num_cols = schema.number_of_columns();

            // Initialize old and new values as Undefined
            let mut old_values = vec![Value::Undefined; num_cols];
            let mut new_values = vec![Value::Undefined; num_cols];

            // Set PK columns from WHERE clause (old = new for PKs)
            for &pk_idx in &pk_indices {
                let col_name = &schema.columns[pk_idx].name.value;
                let pk_value = where_conditions
                    .iter()
                    .find(|(name, _)| name == col_name)
                    .map(|(_, v)| v.clone())
                    .ok_or_else(|| UpdateConversionError::MissingPKColumn {
                        column: col_name.clone(),
                    })?;
                old_values[pk_idx] = pk_value.clone();
                new_values[pk_idx] = pk_value;
            }

            // Apply SET assignments (only affects new_values, old remains Undefined)
            for assignment in &update.assignments {
                let col_name = match &assignment.target {
                    AssignmentTarget::ColumnName(name) => name
                        .0
                        .last()
                        .map(|part| match part {
                            ast::ObjectNamePart::Identifier(ident) => ident.value.clone(),
                            ast::ObjectNamePart::Function(func) => func.name.value.clone(),
                        })
                        .unwrap_or_default(),
                    AssignmentTarget::Tuple(_) => continue,
                };

                let col_idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name.value == col_name)
                    .ok_or_else(|| UpdateConversionError::ColumnMismatch {
                        column: col_name.clone(),
                    })?;

                let new_value = Value::try_from(&assignment.value)?;
                new_values[col_idx] = new_value;
            }

            Ok(Self::from_values(schema, old_values, new_values))
        }
    }

    impl<'a> Update<&'a CreateTable, PatchsetFormat> {
        /// Try to create a patchset Update from a sqlparser UPDATE statement and a table schema.
        ///
        /// In patchset format only new values are stored:
        /// - PK columns get their values from the WHERE clause.
        /// - SET columns get their new values from the assignments.
        /// - All other columns remain Undefined.
        ///
        /// # Errors
        ///
        /// Returns `UpdateConversionError` if the UPDATE statement cannot be converted.
        pub fn try_from_ast(
            update: &ast::Update,
            schema: &'a CreateTable,
        ) -> Result<Self, UpdateConversionError> {
            // Validate table name
            let update_table_name = match &update.table.relation {
                TableFactor::Table { name, .. } => name.0.last().map_or("", |part| match part {
                    ast::ObjectNamePart::Identifier(ident) => ident.value.as_str(),
                    ast::ObjectNamePart::Function(func) => func.name.value.as_str(),
                }),
                _ => "",
            };

            let schema_name = schema.name();
            if update_table_name != schema_name {
                return Err(UpdateConversionError::TableNameMismatch {
                    expected: schema_name.into(),
                    got: update_table_name.into(),
                });
            }

            // Extract PK values from WHERE clause
            let where_clause = update
                .selection
                .as_ref()
                .ok_or(UpdateConversionError::NoWhereClause)?;
            let where_conditions =
                extract_where_conditions(where_clause, UpdateConversionError::CannotExtractPK)?;

            // Get schema info
            let pk_indices = get_pk_indices(schema);
            let num_cols = schema.number_of_columns();

            // Initialize new values as Undefined
            let mut new_values = vec![Value::Undefined; num_cols];

            // Set PK columns from WHERE clause
            for &pk_idx in &pk_indices {
                let col_name = &schema.columns[pk_idx].name.value;
                let pk_value = where_conditions
                    .iter()
                    .find(|(name, _)| name == col_name)
                    .map(|(_, v)| v.clone())
                    .ok_or_else(|| UpdateConversionError::MissingPKColumn {
                        column: col_name.clone(),
                    })?;
                new_values[pk_idx] = pk_value;
            }

            // Apply SET assignments
            for assignment in &update.assignments {
                let col_name = match &assignment.target {
                    AssignmentTarget::ColumnName(name) => name
                        .0
                        .last()
                        .map(|part| match part {
                            ast::ObjectNamePart::Identifier(ident) => ident.value.clone(),
                            ast::ObjectNamePart::Function(func) => func.name.value.clone(),
                        })
                        .unwrap_or_default(),
                    AssignmentTarget::Tuple(_) => continue,
                };

                let col_idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name.value == col_name)
                    .ok_or_else(|| UpdateConversionError::ColumnMismatch {
                        column: col_name.clone(),
                    })?;

                let new_value = Value::try_from(&assignment.value)?;
                new_values[col_idx] = new_value;
            }

            Ok(Self::from_new_values(schema, new_values))
        }
    }

    impl From<&Update<CreateTable, ChangesetFormat>> for ast::Update {
        fn from(update: &Update<CreateTable, ChangesetFormat>) -> Self {
            let table = update.as_ref();
            let values = update.values();
            let pk_indices = get_pk_indices(table);

            // Build assignments for changed non-PK columns
            let assignments: Vec<Assignment> = values
                .iter()
                .enumerate()
                .filter(|(i, (old, new))| !pk_indices.contains(i) && old != new)
                .map(|(i, (_, new))| Assignment {
                    target: AssignmentTarget::ColumnName(make_object_name(
                        &table.columns[i].name.value,
                    )),
                    value: new.into(),
                })
                .collect();

            // Build WHERE clause from PK columns
            let selection = pk_indices
                .iter()
                .map(|&pk_idx| {
                    let (old, _) = &values[pk_idx];
                    Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new(
                            &table.columns[pk_idx].name.value,
                        ))),
                        op: ast::BinaryOperator::Eq,
                        right: Box::new(old.into()),
                    }
                })
                .reduce(|acc, expr| Expr::BinaryOp {
                    left: Box::new(acc),
                    op: ast::BinaryOperator::And,
                    right: Box::new(expr),
                });

            ast::Update {
                update_token: AttachedToken::empty(),
                optimizer_hint: None,
                table: TableWithJoins {
                    relation: make_table_factor(table.name()),
                    joins: vec![],
                },
                assignments,
                from: None,
                selection,
                returning: None,
                or: None,
                limit: None,
            }
        }
    }

    impl Display for Update<CreateTable, ChangesetFormat> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let stmt: ast::Update = self.into();
            write!(f, "{stmt}")
        }
    }
}
