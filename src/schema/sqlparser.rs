//! SQLParser integration for runtime table schemas.
//!
//! This module provides implementations of [`DynTable`] and [`SchemaWithPK`]
//! directly for sqlparser's `CreateTable` statement, allowing you to define tables
//! using SQL DDL and use them with the changeset builder.
//!
//! # Example
//!
//! ```rust,ignore
//! use sqlparser::dialect::SQLiteDialect;
//! use sqlparser::parser::Parser;
//! use sqlite_diff_rs::{DiffSetBuilder, Insert, ChangesetFormat};
//!
//! let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
//! let dialect = SQLiteDialect {};
//! let statements = Parser::parse_sql(&dialect, sql).unwrap();
//!
//! if let sqlparser::ast::Statement::CreateTable(create) = &statements[0] {
//!     let insert = Insert::from(create.clone())
//!         .set(0, 1i64).unwrap()
//!         .set(1, "alice").unwrap();
//!     
//!     // Display outputs: INSERT INTO users (id, name) VALUES (1, 'alice')
//!     println!("{}", insert);
//! }
//! ```

use alloc::vec::Vec;

use sqlparser::ast::{ColumnDef, ColumnOption, CreateTable, Expr, ObjectNamePart, TableConstraint};

use super::DynTable;
use super::SchemaWithPK;
use crate::encoding::Value;

// =============================================================================
// DynTable implementation for CreateTable
// =============================================================================

impl DynTable for CreateTable {
    fn name(&self) -> &str {
        self.name.0.last().map_or("", |part| match part {
            ObjectNamePart::Identifier(ident) => ident.value.as_str(),
            ObjectNamePart::Function(func) => func.name.value.as_str(),
        })
    }

    fn number_of_columns(&self) -> usize {
        self.columns.len()
    }

    fn write_pk_flags(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.columns.len());
        buf.fill(0);

        // Get all PK column indices in order
        let pk_indices = get_pk_indices(self);

        // Write 1-based ordinal position for each PK column
        for (pk_ordinal, col_idx) in pk_indices.iter().enumerate() {
            buf[*col_idx] = u8::try_from(pk_ordinal + 1).expect("Too many PK columns to fit in u8");
        }
    }
}

// =============================================================================
// SchemaWithPK implementation for CreateTable
// =============================================================================

impl SchemaWithPK for CreateTable {
    fn extract_pk(&self, values: &[Value]) -> alloc::vec::Vec<Value> {
        get_pk_indices(self)
            .into_iter()
            .map(|idx| values[idx].clone())
            .collect()
    }
}

// =============================================================================
// Helper functions
// =============================================================================

pub(crate) fn get_pk_indices(create: &CreateTable) -> alloc::vec::Vec<usize> {
    let mut pk_indices = Vec::new();

    for (idx, col) in create.columns.iter().enumerate() {
        if is_primary_key_column(col) {
            pk_indices.push(idx);
        }
    }

    for constraint in &create.constraints {
        if let TableConstraint::PrimaryKey(pk_constraint) = constraint {
            for index_col in &pk_constraint.columns {
                if let Expr::Identifier(ident) = &index_col.column.expr {
                    let pk_name = &ident.value;
                    if let Some(idx) = create.columns.iter().position(|c| &c.name.value == pk_name)
                        && !pk_indices.contains(&idx)
                    {
                        pk_indices.push(idx);
                    }
                }
            }
        }
    }

    pk_indices.sort_unstable();
    pk_indices
}

fn is_primary_key_column(col: &ColumnDef) -> bool {
    col.options
        .iter()
        .any(|opt| matches!(opt.option, ColumnOption::PrimaryKey(_)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;
    use alloc::vec;
    use sqlparser::dialect::SQLiteDialect;
    use sqlparser::parser::Parser;

    fn parse_create_table(sql: &str) -> CreateTable {
        let dialect = SQLiteDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        match statements.into_iter().next().unwrap() {
            sqlparser::ast::Statement::CreateTable(create) => create,
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_simple_table() {
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
        let create = parse_create_table(sql);

        assert_eq!(create.name(), "users");
        assert_eq!(create.number_of_columns(), 2);
        let column_names: Vec<_> = create
            .columns
            .iter()
            .map(|c| c.name.value.as_str())
            .collect();
        assert_eq!(column_names, vec!["id", "name"]);
        assert_eq!(get_pk_indices(&create), vec![0]);
    }

    #[test]
    fn test_composite_pk_inline() {
        let sql = "CREATE TABLE order_items (order_id INTEGER PRIMARY KEY, item_id INTEGER PRIMARY KEY, quantity INTEGER)";
        let create = parse_create_table(sql);

        assert_eq!(get_pk_indices(&create), vec![0, 1]);
    }

    #[test]
    fn test_composite_pk_constraint() {
        let sql = "CREATE TABLE order_items (order_id INTEGER, item_id INTEGER, quantity INTEGER, PRIMARY KEY (order_id, item_id))";
        let create = parse_create_table(sql);

        assert_eq!(get_pk_indices(&create), vec![0, 1]);
    }

    #[test]
    fn test_pk_flags() {
        let sql = "CREATE TABLE t (a INT, b INT PRIMARY KEY, c INT)";
        let create = parse_create_table(sql);

        let mut flags = [0u8; 3];
        create.write_pk_flags(&mut flags);
        assert_eq!(flags, [0, 1, 0]);
    }

    #[test]
    fn test_extract_pk() {
        let sql = "CREATE TABLE t (a INT PRIMARY KEY, b TEXT, c INT PRIMARY KEY)";
        let create = parse_create_table(sql);

        let values = vec![
            Value::Integer(1),
            Value::Text("hello".to_string()),
            Value::Integer(2),
        ];
        let pk = create.extract_pk(&values);
        assert_eq!(pk, vec![Value::Integer(1), Value::Integer(2)]);
    }

    #[test]
    fn test_no_pk() {
        let sql = "CREATE TABLE t (a INT, b TEXT)";
        let create = parse_create_table(sql);
        assert!(get_pk_indices(&create).is_empty());
    }
}
