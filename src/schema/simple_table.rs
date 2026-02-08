//! Simple table schema for SQL-based operations.
//!
//! This module provides [`SimpleTable`], a schema type that can be created from
//! SQL `CREATE TABLE` statements and used for generating SQL INSERT, UPDATE,
//! and DELETE statements.

use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use core::hash::{Hash, Hasher};

use crate::encoding::Value;
use crate::parser::TableSchema;
use crate::sql::{CreateTable, FormatSql};

use super::{DynTable, SchemaWithPK};

/// A simple table schema with column names for SQL generation.
///
/// This type wraps [`TableSchema`] and adds column names, allowing it to be
/// used for both binary encoding/decoding and SQL statement generation.
///
/// # Example
///
/// ```rust,ignore
/// use sqlite_diff_rs::schema::SimpleTable;
/// use sqlite_diff_rs::sql::Parser;
///
/// let mut parser = Parser::new("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
/// let stmts = parser.parse_all().unwrap();
/// if let Statement::CreateTable(ct) = &stmts[0] {
///     let table = SimpleTable::from(ct.clone());
///     assert_eq!(table.name(), "users");
///     assert_eq!(table.column_names(), vec!["id", "name"]);
/// }
/// ```
#[derive(Debug, Clone, Eq)]
pub struct SimpleTable {
    /// The underlying table schema (for binary encoding).
    schema: TableSchema<String>,
    /// Column names in order.
    columns: Vec<String>,
}

impl SimpleTable {
    /// Create a new simple table schema.
    ///
    /// # Arguments
    ///
    /// * `name` - The table name.
    /// * `columns` - The column names in order.
    /// * `pk_indices` - Indices of primary key columns (in PK order).
    ///
    /// # Panics
    ///
    /// Panics if any `pk_indices` value is out of bounds.
    #[must_use]
    pub fn new(name: impl Into<String>, columns: Vec<String>, pk_indices: Vec<usize>) -> Self {
        let name = name.into();
        let column_count = columns.len();

        // Convert pk_indices to pk_flags
        let mut pk_flags = vec![0u8; column_count];
        for (pk_ordinal, &col_idx) in pk_indices.iter().enumerate() {
            assert!(col_idx < column_count, "PK index out of bounds");
            pk_flags[col_idx] = u8::try_from(pk_ordinal + 1).expect("Too many PK columns");
        }

        Self {
            schema: TableSchema::new(name, column_count, pk_flags),
            columns,
        }
    }

    /// Get the column names.
    #[must_use]
    pub fn column_names(&self) -> &[String] {
        &self.columns
    }

    /// Get a column name by index.
    #[must_use]
    pub fn column_name(&self, index: usize) -> Option<&str> {
        self.columns.get(index).map(String::as_str)
    }

    /// Get the column index by name.
    #[must_use]
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c == name)
    }

    /// Get the indices of primary key columns, in PK order.
    #[must_use]
    pub fn pk_indices(&self) -> Vec<usize> {
        self.schema.pk_indices()
    }

    /// Get the inner TableSchema.
    #[must_use]
    pub fn inner(&self) -> &TableSchema<String> {
        &self.schema
    }

    /// Format as a CREATE TABLE SQL statement.
    #[must_use]
    pub fn to_create_table_sql(&self) -> String {
        let pk_indices = self.pk_indices();
        let use_column_pk = pk_indices.len() == 1;

        let ct = CreateTable {
            name: self.schema.name().to_string(),
            columns: self
                .columns
                .iter()
                .enumerate()
                .map(|(i, name)| crate::sql::ColumnDef {
                    name: name.clone(),
                    type_name: None,
                    is_primary_key: use_column_pk && pk_indices.contains(&i),
                })
                .collect(),
            table_pk_columns: if use_column_pk { vec![] } else { pk_indices },
        };

        ct.format_sql()
    }
}

impl PartialEq for SimpleTable {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema && self.columns == other.columns
    }
}

impl Hash for SimpleTable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.columns.hash(state);
    }
}

impl DynTable for SimpleTable {
    #[inline]
    fn name(&self) -> &str {
        self.schema.name()
    }

    #[inline]
    fn number_of_columns(&self) -> usize {
        self.schema.number_of_columns()
    }

    #[inline]
    fn write_pk_flags(&self, buf: &mut [u8]) {
        self.schema.write_pk_flags(buf);
    }
}

impl SchemaWithPK for SimpleTable {
    fn extract_pk<S, B>(&self, values: &[Value<S, B>]) -> Vec<Value<S, B>>
    where
        S: Clone + AsRef<str>,
        B: Clone + AsRef<[u8]>,
    {
        self.schema.extract_pk(values)
    }
}

impl From<CreateTable> for SimpleTable {
    fn from(ct: CreateTable) -> Self {
        let pk_indices = ct.pk_indices();
        let columns: Vec<String> = ct.columns.into_iter().map(|c| c.name).collect();
        Self::new(ct.name, columns, pk_indices)
    }
}

impl From<&CreateTable> for SimpleTable {
    fn from(ct: &CreateTable) -> Self {
        let pk_indices = ct.pk_indices();
        let columns: Vec<String> = ct.columns.iter().map(|c| c.name.clone()).collect();
        Self::new(ct.name.clone(), columns, pk_indices)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{Parser, Statement};

    fn parse_simple_table(sql: &str) -> SimpleTable {
        let mut parser = Parser::new(sql);
        let stmts = parser.parse_all().unwrap();
        match &stmts[0] {
            Statement::CreateTable(ct) => SimpleTable::from(ct.clone()),
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_simple_table_from_sql() {
        let table = parse_simple_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        assert_eq!(table.name(), "users");
        assert_eq!(table.column_names(), &["id", "name"]);
        assert_eq!(table.pk_indices(), vec![0]);
    }

    #[test]
    fn test_simple_table_composite_pk() {
        let table = parse_simple_table("CREATE TABLE t (a INT, b INT, c TEXT, PRIMARY KEY (a, b))");
        assert_eq!(table.name(), "t");
        assert_eq!(table.column_names(), &["a", "b", "c"]);
        assert_eq!(table.pk_indices(), vec![0, 1]);
    }

    #[test]
    fn test_simple_table_column_lookup() {
        let table = parse_simple_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        assert_eq!(table.column_index("id"), Some(0));
        assert_eq!(table.column_index("name"), Some(1));
        assert_eq!(table.column_index("unknown"), None);
        assert_eq!(table.column_name(0), Some("id"));
        assert_eq!(table.column_name(1), Some("name"));
        assert_eq!(table.column_name(2), None);
    }

    #[test]
    fn test_simple_table_manual_construction() {
        let table = SimpleTable::new(
            "users",
            vec!["id".into(), "name".into()],
            vec![0],
        );
        assert_eq!(table.name(), "users");
        assert_eq!(table.number_of_columns(), 2);
        assert_eq!(table.pk_indices(), vec![0]);
    }

    #[test]
    fn test_dyn_table_impl() {
        let table = parse_simple_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        assert_eq!(table.number_of_columns(), 2);
        let mut buf = vec![0u8; 2];
        table.write_pk_flags(&mut buf);
        assert_eq!(buf, vec![1, 0]); // id is first PK column, name is not PK
    }

    #[test]
    fn test_schema_with_pk_impl() {
        let table = parse_simple_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        let values: Vec<Value<String, Vec<u8>>> = vec![Value::Integer(1), Value::Text("Alice".to_string())];
        let pk = table.extract_pk(&values);
        assert_eq!(pk, vec![Value::<String, Vec<u8>>::Integer(1)]);
    }

    #[test]
    fn test_to_create_table_sql() {
        let table = SimpleTable::new(
            "users",
            vec!["id".into(), "name".into()],
            vec![0],
        );
        let sql = table.to_create_table_sql();
        assert_eq!(sql, "CREATE TABLE users (id PRIMARY KEY, name)");
    }
}
