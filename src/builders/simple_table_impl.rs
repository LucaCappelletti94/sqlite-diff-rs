//! SimpleTable implementations for Insert, Update, Delete, and DiffSetBuilder.
//!
//! This module provides SQL parsing and formatting capabilities for SimpleTable-based
//! operations, replacing the sqlparser-based implementations.

use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use core::fmt::{self, Display};
use core::str::FromStr;

use hashbrown::HashMap;

use super::change::DiffSetBuilder;
use super::delete_operation::ChangeDelete;
use super::format::{ChangesetFormat, PatchsetFormat};
use super::insert_operation::Insert;
use super::operation::Operation;
use super::update_operation::Update;
use crate::encoding::{MaybeValue, Value};
use crate::errors::{
    DeleteConversionError, DiffSetParseError, InsertConversionError, UpdateConversionError,
};
use crate::schema::SimpleTable;
use crate::sql::{
    ColumnDef, CreateTable, DeleteStatement, InsertStatement, Parser, SqlValue, Statement,
    UpdateStatement,
};
use crate::{DynTable, SchemaWithPK};

// =============================================================================
// Insert<SimpleTable, ...> implementations
// =============================================================================

impl Insert<SimpleTable, String, Vec<u8>> {
    /// Try to create an Insert from a parsed INSERT statement and a table schema.
    ///
    /// # Errors
    ///
    /// Returns `InsertConversionError` if the INSERT statement cannot be converted.
    pub fn try_from_sql(
        insert: &InsertStatement,
        schema: &SimpleTable,
    ) -> Result<Self, InsertConversionError> {
        // Validate table name
        if insert.table != schema.name() {
            return Err(InsertConversionError::TableNameMismatch {
                expected: schema.name().into(),
                got: insert.table.clone(),
            });
        }

        let num_cols = schema.number_of_columns();
        let mut result = Insert::from(schema.clone());

        if insert.columns.is_empty() {
            // Positional: values must match column count
            if insert.values.len() != num_cols {
                return Err(InsertConversionError::WrongValueCount {
                    expected: num_cols,
                    got: insert.values.len(),
                });
            }
            for (idx, sql_val) in insert.values.iter().enumerate() {
                let value: Value<String, Vec<u8>> = sql_val.into();
                result = result.set(idx, value).map_err(|_| {
                    InsertConversionError::WrongValueCount {
                        expected: num_cols,
                        got: idx + 1,
                    }
                })?;
            }
        } else {
            // Named columns: match by name
            if insert.columns.len() != insert.values.len() {
                return Err(InsertConversionError::WrongValueCount {
                    expected: insert.columns.len(),
                    got: insert.values.len(),
                });
            }

            for (col_name, sql_val) in insert.columns.iter().zip(insert.values.iter()) {
                let col_idx = schema.column_index(col_name).ok_or_else(|| {
                    InsertConversionError::ColumnMismatch {
                        column: col_name.clone(),
                    }
                })?;
                let value: Value<String, Vec<u8>> = sql_val.into();
                result = result.set(col_idx, value).map_err(|_| {
                    InsertConversionError::ColumnMismatch {
                        column: col_name.clone(),
                    }
                })?;
            }
        }

        Ok(result)
    }
}

impl<S: AsRef<str>, B: AsRef<[u8]>> Display for Insert<SimpleTable, S, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let table = self.as_ref();
        let values = self.values();

        write!(f, "INSERT INTO {} (", table.name())?;

        for (i, col_name) in table.column_names().iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{col_name}")?;
        }

        write!(f, ") VALUES (")?;

        for (i, val) in values.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            let sql_val: SqlValue = val.into();
            write!(f, "{sql_val}")?;
        }

        write!(f, ")")
    }
}

// =============================================================================
// Update<SimpleTable, ChangesetFormat, ...> implementations
// =============================================================================

impl Update<SimpleTable, ChangesetFormat, String, Vec<u8>> {
    /// Try to create an Update from a parsed UPDATE statement and a table schema.
    ///
    /// Note: This creates an Update with old values set to the WHERE clause values
    /// (for PK columns) and NULL for other columns. This is a limitation of parsing
    /// UPDATE statements which don't contain the original row values.
    ///
    /// # Errors
    ///
    /// Returns `UpdateConversionError` if the UPDATE statement cannot be converted.
    pub fn try_from_sql(
        update: &UpdateStatement,
        schema: &SimpleTable,
    ) -> Result<Self, UpdateConversionError> {
        // Validate table name
        if update.table != schema.name() {
            return Err(UpdateConversionError::TableNameMismatch {
                expected: schema.name().into(),
                got: update.table.clone(),
            });
        }

        let num_cols = schema.number_of_columns();
        let pk_indices = schema.pk_indices();

        // Initialize values: (old, new) where both start as None (undefined)
        let mut values: Vec<(MaybeValue<String, Vec<u8>>, MaybeValue<String, Vec<u8>>)> =
            vec![(None, None); num_cols];

        // Extract PK values from WHERE clause
        for (col_name, sql_val) in &update.where_clause {
            let col_idx = schema.column_index(col_name).ok_or_else(|| {
                UpdateConversionError::ColumnMismatch {
                    column: col_name.clone(),
                }
            })?;
            let value: Value<String, Vec<u8>> = sql_val.into();
            values[col_idx].0 = Some(value.clone());
            // PK columns: old = new (unchanged)
            if pk_indices.contains(&col_idx) {
                values[col_idx].1 = Some(value);
            }
        }

        // Process SET assignments
        for (col_name, sql_val) in &update.assignments {
            let col_idx = schema.column_index(col_name).ok_or_else(|| {
                UpdateConversionError::ColumnMismatch {
                    column: col_name.clone(),
                }
            })?;
            let value: Value<String, Vec<u8>> = sql_val.into();
            values[col_idx].1 = Some(value);
        }

        // Verify all PK columns have values
        for &pk_idx in &pk_indices {
            if values[pk_idx].0.is_none() {
                return Err(UpdateConversionError::MissingPKColumn {
                    column: schema.column_name(pk_idx).unwrap_or("?").into(),
                });
            }
        }

        Ok(Self {
            table: schema.clone(),
            values,
        })
    }
}

impl<S: Clone + PartialEq + core::fmt::Debug + AsRef<str>, B: Clone + PartialEq + core::fmt::Debug + AsRef<[u8]>> Display
    for Update<SimpleTable, ChangesetFormat, S, B>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let table = self.as_ref();
        let values = self.values();
        let pk_indices = table.pk_indices();

        write!(f, "UPDATE {} SET ", table.name())?;

        // Build SET clause for changed non-PK columns
        let mut first = true;
        for (i, (old, new)) in values.iter().enumerate() {
            if pk_indices.contains(&i) || old == new {
                continue;
            }
            if !first {
                write!(f, ", ")?;
            }
            let col_name = table.column_name(i).unwrap_or("?");
            let sql_val: SqlValue = match new {
                Some(v) => SqlValue::from(v),
                None => SqlValue::Null,
            };
            write!(f, "{col_name} = {sql_val}")?;
            first = false;
        }

        // Build WHERE clause from PK columns
        write!(f, " WHERE ")?;
        let mut first_pk = true;
        for &pk_idx in &pk_indices {
            if !first_pk {
                write!(f, " AND ")?;
            }
            first_pk = false;
            let col_name = table.column_name(pk_idx).unwrap_or("?");
            let (old, _) = &values[pk_idx];
            let sql_val: SqlValue = match old {
                Some(v) => SqlValue::from(v),
                None => SqlValue::Null,
            };
            write!(f, "{col_name} = {sql_val}")?;
        }

        Ok(())
    }
}

// =============================================================================
// Update<SimpleTable, PatchsetFormat, ...> implementations
// =============================================================================

impl Update<SimpleTable, PatchsetFormat, String, Vec<u8>> {
    /// Try to create an Update from a parsed UPDATE statement and a table schema.
    ///
    /// # Errors
    ///
    /// Returns `UpdateConversionError` if the UPDATE statement cannot be converted.
    pub fn try_from_sql(
        update: &UpdateStatement,
        schema: &SimpleTable,
    ) -> Result<Self, UpdateConversionError> {
        // Validate table name
        if update.table != schema.name() {
            return Err(UpdateConversionError::TableNameMismatch {
                expected: schema.name().into(),
                got: update.table.clone(),
            });
        }

        let num_cols = schema.number_of_columns();
        let pk_indices = schema.pk_indices();

        // Initialize: ((), new) where new is None initially
        let mut new_values: Vec<Option<Value<String, Vec<u8>>>> = vec![None; num_cols];

        // Extract PK values from WHERE clause
        for (col_name, sql_val) in &update.where_clause {
            let col_idx = schema.column_index(col_name).ok_or_else(|| {
                UpdateConversionError::ColumnMismatch {
                    column: col_name.clone(),
                }
            })?;
            if pk_indices.contains(&col_idx) {
                new_values[col_idx] = Some(sql_val.into());
            }
        }

        // Process SET assignments
        for (col_name, sql_val) in &update.assignments {
            let col_idx = schema.column_index(col_name).ok_or_else(|| {
                UpdateConversionError::ColumnMismatch {
                    column: col_name.clone(),
                }
            })?;
            new_values[col_idx] = Some(sql_val.into());
        }

        // Verify all PK columns have values
        for &pk_idx in &pk_indices {
            if new_values[pk_idx].is_none() {
                return Err(UpdateConversionError::MissingPKColumn {
                    column: schema.column_name(pk_idx).unwrap_or("?").into(),
                });
            }
        }

        Ok(Self {
            table: schema.clone(),
            values: new_values.into_iter().map(|new| ((), new)).collect(),
        })
    }
}

// =============================================================================
// ChangeDelete<SimpleTable, ...> implementations
// =============================================================================

impl ChangeDelete<SimpleTable, String, Vec<u8>> {
    /// Try to create a ChangeDelete from a parsed DELETE statement and a table schema.
    ///
    /// # Errors
    ///
    /// Returns `DeleteConversionError` if the DELETE statement cannot be converted.
    pub fn try_from_sql(
        delete: &DeleteStatement,
        schema: &SimpleTable,
    ) -> Result<Self, DeleteConversionError> {
        // Validate table name
        if delete.table != schema.name() {
            return Err(DeleteConversionError::TableNameMismatch {
                expected: schema.name().into(),
                got: delete.table.clone(),
            });
        }

        let pk_indices = schema.pk_indices();

        let mut result = ChangeDelete::from(schema.clone());

        // Extract values from WHERE clause
        for (col_name, sql_val) in &delete.where_clause {
            let col_idx = schema.column_index(col_name).ok_or_else(|| {
                DeleteConversionError::ColumnMismatch {
                    column: col_name.clone(),
                }
            })?;
            let value: Value<String, Vec<u8>> = sql_val.into();
            result = result.set(col_idx, value).map_err(|_| {
                DeleteConversionError::ColumnMismatch {
                    column: col_name.clone(),
                }
            })?;
        }

        // Verify all PK columns are set
        for &pk_idx in &pk_indices {
            if matches!(result.values()[pk_idx], Value::Null) {
                return Err(DeleteConversionError::MissingPKColumn {
                    column: schema.column_name(pk_idx).unwrap_or("?").into(),
                });
            }
        }

        Ok(result)
    }
}

impl<S: AsRef<str>, B: AsRef<[u8]>> Display for ChangeDelete<SimpleTable, S, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let table = self.as_ref();
        let values = self.values();
        let pk_indices = table.pk_indices();

        write!(f, "DELETE FROM {} WHERE ", table.name())?;

        let mut first = true;
        for &pk_idx in &pk_indices {
            if !first {
                write!(f, " AND ")?;
            }
            first = false;
            let col_name = table.column_name(pk_idx).unwrap_or("?");
            let sql_val: SqlValue = (&values[pk_idx]).into();
            write!(f, "{col_name} = {sql_val}")?;
        }

        Ok(())
    }
}

// =============================================================================
// DiffSetBuilder<ChangesetFormat, SimpleTable, ...> implementations
// =============================================================================

impl DiffSetBuilder<ChangesetFormat, SimpleTable, String, Vec<u8>> {
    /// Try to create a ChangeSet DiffSetBuilder from a slice of SQL statements.
    ///
    /// The statements must include CREATE TABLE statements before any DML
    /// (INSERT/UPDATE/DELETE) statements that reference those tables.
    ///
    /// # Errors
    ///
    /// Returns `DiffSetParseError` if parsing fails.
    pub fn try_from_statements(statements: &[Statement]) -> Result<Self, DiffSetParseError> {
        let mut builder = Self::new();
        let mut schemas: HashMap<String, SimpleTable> = HashMap::new();

        for stmt in statements {
            match stmt {
                Statement::CreateTable(create) => {
                    let table = SimpleTable::from(create.clone());
                    schemas.insert(table.name().to_string(), table);
                }
                Statement::Insert(insert) => {
                    let schema = schemas.get(&insert.table).ok_or_else(|| {
                        DiffSetParseError::TableNotFound(insert.table.clone())
                    })?;
                    let insert_op = Insert::try_from_sql(insert, schema)?;
                    let values: Vec<MaybeValue<String, Vec<u8>>> =
                        insert_op.into_values().into_iter().map(Some).collect();
                    builder = builder.insert_raw(schema, values);
                }
                Statement::Update(update) => {
                    let schema = schemas.get(&update.table).ok_or_else(|| {
                        DiffSetParseError::TableNotFound(update.table.clone())
                    })?;
                    let update_op =
                        Update::<SimpleTable, ChangesetFormat, String, Vec<u8>>::try_from_sql(
                            update, schema,
                        )?;
                    let (old_values, new_values): (Vec<_>, Vec<_>) = update_op
                        .values()
                        .iter()
                        .map(|(old, new)| (old.clone(), new.clone()))
                        .unzip();
                    builder = builder.update_raw(schema, old_values, new_values);
                }
                Statement::Delete(delete) => {
                    let schema = schemas.get(&delete.table).ok_or_else(|| {
                        DiffSetParseError::TableNotFound(delete.table.clone())
                    })?;
                    let delete_op = ChangeDelete::try_from_sql(delete, schema)?;
                    let values: Vec<MaybeValue<String, Vec<u8>>> =
                        delete_op.into_values().into_iter().map(Some).collect();
                    builder = builder.delete_raw(schema, values);
                }
            }
        }

        Ok(builder)
    }
}

impl TryFrom<&[Statement]> for DiffSetBuilder<ChangesetFormat, SimpleTable, String, Vec<u8>> {
    type Error = DiffSetParseError;

    fn try_from(statements: &[Statement]) -> Result<Self, Self::Error> {
        Self::try_from_statements(statements)
    }
}

impl TryFrom<Vec<Statement>> for DiffSetBuilder<ChangesetFormat, SimpleTable, String, Vec<u8>> {
    type Error = DiffSetParseError;

    fn try_from(statements: Vec<Statement>) -> Result<Self, Self::Error> {
        Self::try_from_statements(&statements)
    }
}

impl FromStr for DiffSetBuilder<ChangesetFormat, SimpleTable, String, Vec<u8>> {
    type Err = DiffSetParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parser = Parser::new(s);
        let statements = parser.parse_all()?;
        Self::try_from_statements(&statements)
    }
}

impl TryFrom<&str> for DiffSetBuilder<ChangesetFormat, SimpleTable, String, Vec<u8>> {
    type Error = DiffSetParseError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl TryFrom<String> for DiffSetBuilder<ChangesetFormat, SimpleTable, String, Vec<u8>> {
    type Error = DiffSetParseError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl Display for DiffSetBuilder<ChangesetFormat, SimpleTable, String, Vec<u8>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // First, emit CREATE TABLE statements for all tables with operations
        for (table, rows) in &self.tables {
            if !rows.is_empty() {
                writeln!(f, "{};", table.to_create_table_sql())?;
            }
        }

        // Then, emit operations for each table
        for (table, rows) in &self.tables {
            if rows.is_empty() {
                continue;
            }

            for (_pk, op) in rows {
                match op {
                    Operation::Insert(values) => {
                        let mut insert = Insert::from(table.clone());
                        for (i, val) in values.iter().enumerate() {
                            insert = insert.set(i, val.clone()).unwrap();
                        }
                        writeln!(f, "{insert};")?;
                    }
                    Operation::Delete(values) => {
                        let mut delete = ChangeDelete::from(table.clone());
                        for (i, val) in values.iter().enumerate() {
                            delete = delete.set(i, val.clone()).unwrap();
                        }
                        writeln!(f, "{delete};")?;
                    }
                    Operation::Update(values) => {
                        let mut update =
                            Update::<SimpleTable, ChangesetFormat, String, Vec<u8>>::from(
                                table.clone(),
                            );
                        for (i, (old, new)) in values.iter().enumerate() {
                            update = update.set(i, old.clone(), new.clone()).unwrap();
                        }
                        writeln!(f, "{update};")?;
                    }
                }
            }
        }

        Ok(())
    }
}

impl From<&DiffSetBuilder<ChangesetFormat, SimpleTable, String, Vec<u8>>> for Vec<Statement> {
    fn from(builder: &DiffSetBuilder<ChangesetFormat, SimpleTable, String, Vec<u8>>) -> Self {
        let mut statements = Vec::new();

        // First, emit CREATE TABLE statements for all tables in order
        for (table, rows) in &builder.tables {
            if !rows.is_empty() {
                let pk_indices = table.pk_indices();
                let use_column_pk = pk_indices.len() == 1;
                
                let ct = CreateTable {
                    name: table.name().to_string(),
                    columns: table
                        .column_names()
                        .iter()
                        .enumerate()
                        .map(|(i, name)| ColumnDef {
                            name: (*name).to_string(),
                            type_name: None,
                            is_primary_key: use_column_pk && pk_indices.contains(&i),
                        })
                        .collect(),
                    table_pk_columns: if use_column_pk {
                        vec![]
                    } else {
                        pk_indices.clone()
                    },
                };
                statements.push(Statement::CreateTable(ct));
            }
        }

        // Then, emit operations for each table
        for (table, rows) in &builder.tables {
            if rows.is_empty() {
                continue;
            }

            for (_pk, op) in rows {
                match op {
                    Operation::Insert(values) => {
                        let insert_stmt = InsertStatement {
                            table: table.name().to_string(),
                            columns: table.column_names().iter().map(|s| (*s).to_string()).collect(),
                            values: values.iter().map(|v| v.into()).collect(),
                        };
                        statements.push(Statement::Insert(insert_stmt));
                    }
                    Operation::Delete(values) => {
                        let pk_indices = table.pk_indices();
                        let where_clause: Vec<(String, SqlValue)> = pk_indices
                            .iter()
                            .map(|&i| {
                                (
                                    table.column_name(i).unwrap_or("?").to_string(),
                                    (&values[i]).into(),
                                )
                            })
                            .collect();
                        let delete_stmt = DeleteStatement {
                            table: table.name().to_string(),
                            where_clause,
                        };
                        statements.push(Statement::Delete(delete_stmt));
                    }
                    Operation::Update(values) => {
                        let pk_indices = table.pk_indices();
                        let assignments: Vec<(String, SqlValue)> = values
                            .iter()
                            .enumerate()
                            .filter(|(i, (old, new))| !pk_indices.contains(i) && old != new)
                            .map(|(i, (_, new))| {
                                let sql_val = match new {
                                    Some(v) => SqlValue::from(v),
                                    None => SqlValue::Null,
                                };
                                (
                                    table.column_name(i).unwrap_or("?").to_string(),
                                    sql_val,
                                )
                            })
                            .collect();
                        let where_clause: Vec<(String, SqlValue)> = pk_indices
                            .iter()
                            .map(|&i| {
                                let (old, _) = &values[i];
                                let sql_val = match old {
                                    Some(v) => SqlValue::from(v),
                                    None => SqlValue::Null,
                                };
                                (
                                    table.column_name(i).unwrap_or("?").to_string(),
                                    sql_val,
                                )
                            })
                            .collect();
                        let update_stmt = UpdateStatement {
                            table: table.name().to_string(),
                            assignments,
                            where_clause,
                        };
                        statements.push(Statement::Update(update_stmt));
                    }
                }
            }
        }

        statements
    }
}

impl From<DiffSetBuilder<ChangesetFormat, SimpleTable, String, Vec<u8>>> for Vec<Statement> {
    fn from(builder: DiffSetBuilder<ChangesetFormat, SimpleTable, String, Vec<u8>>) -> Self {
        (&builder).into()
    }
}

// =============================================================================
// DiffSetBuilder<PatchsetFormat, SimpleTable, ...> implementations
// =============================================================================

impl DiffSetBuilder<PatchsetFormat, SimpleTable, String, Vec<u8>> {
    /// Try to create a PatchSet DiffSetBuilder from a slice of SQL statements.
    ///
    /// # Errors
    ///
    /// Returns `DiffSetParseError` if parsing fails.
    pub fn try_from_statements(statements: &[Statement]) -> Result<Self, DiffSetParseError> {
        let mut builder = Self::new();
        let mut schemas: HashMap<String, SimpleTable> = HashMap::new();

        for stmt in statements {
            match stmt {
                Statement::CreateTable(create) => {
                    let table = SimpleTable::from(create.clone());
                    schemas.insert(table.name().to_string(), table);
                }
                Statement::Insert(insert) => {
                    let schema = schemas.get(&insert.table).ok_or_else(|| {
                        DiffSetParseError::TableNotFound(insert.table.clone())
                    })?;
                    let insert_op = Insert::try_from_sql(insert, schema)?;
                    let values: Vec<MaybeValue<String, Vec<u8>>> =
                        insert_op.into_values().into_iter().map(Some).collect();
                    builder = builder.insert_raw(schema, values);
                }
                Statement::Update(update) => {
                    let schema = schemas.get(&update.table).ok_or_else(|| {
                        DiffSetParseError::TableNotFound(update.table.clone())
                    })?;
                    let update_op =
                        Update::<SimpleTable, PatchsetFormat, String, Vec<u8>>::try_from_sql(
                            update, schema,
                        )?;
                    let new_values: Vec<_> = update_op
                        .values()
                        .iter()
                        .map(|((), new)| new.clone())
                        .collect();
                    builder = builder.update_raw(schema, new_values);
                }
                Statement::Delete(delete) => {
                    let schema = schemas.get(&delete.table).ok_or_else(|| {
                        DiffSetParseError::TableNotFound(delete.table.clone())
                    })?;
                    let delete_op = ChangeDelete::try_from_sql(delete, schema)?;
                    let pk = schema.extract_pk(delete_op.values());
                    builder = builder.delete(schema, &pk);
                }
            }
        }

        Ok(builder)
    }
}

impl TryFrom<&[Statement]> for DiffSetBuilder<PatchsetFormat, SimpleTable, String, Vec<u8>> {
    type Error = DiffSetParseError;

    fn try_from(statements: &[Statement]) -> Result<Self, Self::Error> {
        Self::try_from_statements(statements)
    }
}

impl TryFrom<Vec<Statement>> for DiffSetBuilder<PatchsetFormat, SimpleTable, String, Vec<u8>> {
    type Error = DiffSetParseError;

    fn try_from(statements: Vec<Statement>) -> Result<Self, Self::Error> {
        Self::try_from_statements(&statements)
    }
}

impl FromStr for DiffSetBuilder<PatchsetFormat, SimpleTable, String, Vec<u8>> {
    type Err = DiffSetParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parser = Parser::new(s);
        let statements = parser.parse_all()?;
        Self::try_from_statements(&statements)
    }
}

impl TryFrom<&str> for DiffSetBuilder<PatchsetFormat, SimpleTable, String, Vec<u8>> {
    type Error = DiffSetParseError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl TryFrom<String> for DiffSetBuilder<PatchsetFormat, SimpleTable, String, Vec<u8>> {
    type Error = DiffSetParseError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChangeSet, PatchSet};

    #[test]
    fn test_parse_simple_insert() {
        let sql = "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO users (id, name) VALUES (1, 'Alice');
        ";

        let builder: ChangeSet<SimpleTable, String, Vec<u8>> =
            sql.parse().expect("Failed to parse SQL");
        assert_eq!(builder.len(), 1);
        assert!(!builder.is_empty());
    }

    #[test]
    fn test_parse_simple_delete() {
        let sql = "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            DELETE FROM users WHERE id = 1;
        ";

        let builder: ChangeSet<SimpleTable, String, Vec<u8>> =
            sql.parse().expect("Failed to parse SQL");
        assert_eq!(builder.len(), 1);
    }

    #[test]
    fn test_parse_simple_update() {
        let sql = "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            UPDATE users SET name = 'Bob' WHERE id = 1;
        ";

        let builder: ChangeSet<SimpleTable, String, Vec<u8>> =
            sql.parse().expect("Failed to parse SQL");
        assert_eq!(builder.len(), 1);
    }

    #[test]
    fn test_display_roundtrip() {
        let sql = "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO users (id, name) VALUES (1, 'Alice');
        ";

        let builder: ChangeSet<SimpleTable, String, Vec<u8>> =
            sql.parse().expect("Failed to parse SQL");
        let output = builder.to_string();

        // Parse the output back
        let reparsed: ChangeSet<SimpleTable, String, Vec<u8>> =
            output.parse().expect("Failed to re-parse SQL");

        assert_eq!(builder.len(), reparsed.len());
    }

    #[test]
    fn test_patchset_parsing() {
        let sql = "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO users (id, name) VALUES (1, 'Alice');
        ";

        let builder: PatchSet<SimpleTable, String, Vec<u8>> =
            sql.parse().expect("Failed to parse SQL");
        assert_eq!(builder.len(), 1);
    }

    #[test]
    fn test_into_vec_statement() {
        let sql = "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO users (id, name) VALUES (1, 'Alice');
        ";

        let builder: ChangeSet<SimpleTable, String, Vec<u8>> =
            sql.parse().expect("Failed to parse SQL");
        let statements: Vec<Statement> = builder.into();

        // Should have 1 CREATE TABLE + 1 INSERT
        assert_eq!(statements.len(), 2);
        assert!(matches!(&statements[0], Statement::CreateTable(_)));
        assert!(matches!(&statements[1], Statement::Insert(_)));
    }
}
