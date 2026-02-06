//! Submodule defining a builder for an insert operation.

use alloc::vec;
use alloc::vec::Vec;
use core::ops::Add;

use crate::{
    DynTable,
    builders::{Update, format::Format, operation::Reverse},
    encoding::Value,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Builder for an insert operation.
pub struct Insert<T: DynTable> {
    /// The table being inserted into.
    table: T,
    /// Values for the inserted row.
    values: Vec<Value>,
}

/// When we add an Insert to another Insert, we just keep the first one.
impl<T: DynTable> Add<Insert<T>> for Insert<T> {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        assert_eq!(&self.table, rhs.as_ref());
        self
    }
}

/// When we add an Update to an Insert, we apply the updates to the insert values.
impl<T: DynTable, F: Format> Add<Update<T, F>> for Insert<T> {
    type Output = Self;

    fn add(self, rhs: Update<T, F>) -> Self::Output {
        assert_eq!(&self.table, rhs.as_ref());
        let mut result = self;
        let values: Vec<(F::Old, Value)> = rhs.into();
        for (idx, (_old, new)) in values.into_iter().enumerate() {
            // Only update if new value is defined (not Undefined)
            // This preserves the original INSERT value for columns not modified by UPDATE
            if !matches!(new, Value::Undefined) {
                result.values[idx] = new;
            }
        }
        result
    }
}

/// When we add a delete to an Insert, they cancel each other out.
impl<T: DynTable> Add<crate::builders::ChangeDelete<T>> for Insert<T> {
    type Output = Option<Self>;
    fn add(self, _rhs: crate::builders::ChangeDelete<T>) -> Self::Output {
        None
    }
}
impl<T: DynTable> Add<crate::builders::PatchDelete<T>> for Insert<T> {
    type Output = Option<Self>;
    fn add(self, _rhs: crate::builders::PatchDelete<T>) -> Self::Output {
        None
    }
}

impl<T: DynTable> From<T> for Insert<T> {
    fn from(table: T) -> Self {
        let num_cols = table.number_of_columns();
        Self {
            table,
            values: vec![Value::default(); num_cols],
        }
    }
}

impl<T: DynTable> AsRef<T> for Insert<T> {
    fn as_ref(&self) -> &T {
        &self.table
    }
}

impl<T: DynTable> Insert<T> {
    /// Create an insert operation with the given values.
    pub fn from_values(table: T, values: Vec<Value>) -> Self {
        Self { table, values }
    }

    /// Returns a reference to the values.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Consumes self and returns the values.
    pub fn into_values(self) -> Vec<Value> {
        self.values
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

        let value = value.into();
        if value.is_undefined() {
            return Err(crate::errors::Error::UndefinedValueProvided);
        }

        self.values[col_idx] = value;
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
    /// use sqlite_diff_rs::Insert;
    /// use sqlparser::ast::CreateTable;
    /// use sqlparser::dialect::SQLiteDialect;
    /// use sqlparser::parser::Parser;
    ///
    /// let dialect = SQLiteDialect {};
    /// let sql = "CREATE TABLE items (id INTEGER PRIMARY KEY, description TEXT, price REAL)";
    /// let statements = Parser::parse_sql(&dialect, sql).unwrap();
    /// let schema = match &statements[0] {
    ///     sqlparser::ast::Statement::CreateTable(ct) => ct.clone(),
    ///     _ => panic!(),
    /// };
    ///
    /// // INSERT INTO items (id, description, price) VALUES (1, NULL, 9.99)
    /// let insert = Insert::from(schema)
    ///     .set(0, 1i64).unwrap()
    ///     .set_null(1).unwrap()    // description = NULL
    ///     .set(2, 9.99f64).unwrap();
    /// ```
    pub fn set_null(self, col_idx: usize) -> Result<Self, crate::errors::Error> {
        self.set(col_idx, Value::Null)
    }
}

impl<T: DynTable> Reverse for Insert<T> {
    type Output = crate::builders::ChangeDelete<T>;

    fn reverse(self) -> Self::Output {
        let mut delete = crate::builders::ChangeDelete::from(self.table);
        for (idx, value) in self.values.into_iter().enumerate() {
            // Skip Undefined values
            if !value.is_undefined() {
                delete = delete.set(idx, value).unwrap();
            }
        }
        delete
    }
}

// =============================================================================
// sqlparser integration
// =============================================================================

#[cfg(feature = "sqlparser")]
mod sqlparser_impl {
    use alloc::boxed::Box;
    use alloc::string::String;
    use alloc::vec;
    use alloc::vec::Vec;
    use core::fmt::{self, Display};

    use sqlparser::ast::{
        self, CreateTable, Expr, Ident, Query, SetExpr, TableObject, Values,
        helpers::attached_token::AttachedToken,
    };

    use super::Insert;
    use crate::builders::ast_helpers::make_object_name;
    use crate::encoding::Value;
    use crate::errors::InsertConversionError;
    use crate::schema::DynTable;

    impl<'a> Insert<&'a CreateTable> {
        /// Try to create an Insert from a sqlparser INSERT statement and a table schema.
        ///
        /// # Errors
        ///
        /// Returns `InsertConversionError` if the INSERT statement cannot be converted.
        pub fn try_from_ast(
            insert: &ast::Insert,
            schema: &'a CreateTable,
        ) -> Result<Self, InsertConversionError> {
            // Validate table name
            let insert_table_name = match &insert.table {
                TableObject::TableName(name) => name.0.last().map_or("", |part| match part {
                    ast::ObjectNamePart::Identifier(ident) => ident.value.as_str(),
                    ast::ObjectNamePart::Function(func) => func.name.value.as_str(),
                }),
                TableObject::TableFunction(_) => "",
            };

            let schema_name = schema.name();
            if insert_table_name != schema_name {
                return Err(InsertConversionError::TableNameMismatch {
                    expected: schema_name.into(),
                    got: insert_table_name.into(),
                });
            }

            // Get the VALUES from the source
            let source = insert
                .source
                .as_ref()
                .ok_or(InsertConversionError::NoSource)?;
            let rows = match source.body.as_ref() {
                SetExpr::Values(values) => &values.rows,
                _ => return Err(InsertConversionError::NotValuesSource),
            };

            // We only support single-row inserts
            if rows.len() != 1 {
                return Err(InsertConversionError::MultipleRows);
            }
            let row = &rows[0];

            // Build the Insert
            let num_cols = schema.number_of_columns();
            let mut result = Insert::from(schema);

            // If columns are specified, match by name; otherwise assume positional
            if insert.columns.is_empty() {
                // Positional: values must match column count
                if row.len() != num_cols {
                    return Err(InsertConversionError::WrongValueCount {
                        expected: num_cols,
                        got: row.len(),
                    });
                }
                for (idx, expr) in row.iter().enumerate() {
                    let value = Value::try_from(expr)?;
                    result = result.set(idx, value).map_err(|_| {
                        InsertConversionError::WrongValueCount {
                            expected: num_cols,
                            got: idx + 1,
                        }
                    })?;
                }
            } else {
                // Named columns: match by name
                if insert.columns.len() != row.len() {
                    return Err(InsertConversionError::WrongValueCount {
                        expected: insert.columns.len(),
                        got: row.len(),
                    });
                }

                // Resolve column indices upfront to avoid holding a borrow across set()
                let col_indices: Vec<(usize, String)> = insert
                    .columns
                    .iter()
                    .map(|col_ident| {
                        let col_name = &col_ident.value;
                        let col_idx = result
                            .as_ref()
                            .columns
                            .iter()
                            .position(|c| &c.name.value == col_name)
                            .ok_or_else(|| InsertConversionError::ColumnMismatch {
                                column: col_name.clone(),
                            })?;
                        Ok((col_idx, col_name.clone()))
                    })
                    .collect::<Result<_, InsertConversionError>>()?;

                for ((col_idx, col_name), expr) in col_indices.into_iter().zip(row.iter()) {
                    let value = Value::try_from(expr)?;
                    result = result
                        .set(col_idx, value)
                        .map_err(|_| InsertConversionError::ColumnMismatch { column: col_name })?;
                }
            }

            Ok(result)
        }
    }

    impl From<&Insert<CreateTable>> for ast::Insert {
        fn from(insert: &Insert<CreateTable>) -> Self {
            let table = insert.as_ref();
            let values = insert.values();

            // Convert column names to Idents
            let columns: Vec<Ident> = table
                .columns
                .iter()
                .map(|c| Ident::new(&c.name.value))
                .collect();

            // Convert values to expressions
            let row: Vec<Expr> = values.iter().map(Expr::from).collect();

            ast::Insert {
                insert_token: AttachedToken::empty(),
                optimizer_hint: None,
                or: None,
                ignore: false,
                into: true,
                table: TableObject::TableName(make_object_name(table.name())),
                table_alias: None,
                columns,
                overwrite: false,
                source: Some(Box::new(Query {
                    with: None,
                    body: Box::new(SetExpr::Values(Values {
                        explicit_row: false,
                        value_keyword: false,
                        rows: vec![row],
                    })),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
                })),
                assignments: vec![],
                partitioned: None,
                after_columns: vec![],
                has_table_keyword: false,
                on: None,
                returning: None,
                replace_into: false,
                priority: None,
                insert_alias: None,
                settings: None,
                format_clause: None,
            }
        }
    }

    impl Display for Insert<CreateTable> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let stmt: ast::Insert = self.into();
            write!(f, "{stmt}")
        }
    }
}
