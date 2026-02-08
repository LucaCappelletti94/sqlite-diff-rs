//! SQL output formatting.

use alloc::string::String;
use core::fmt::{self, Display, Write};

use super::parser::SqlValue;

impl Display for SqlValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlValue::Null => write!(f, "NULL"),
            SqlValue::Integer(v) => write!(f, "{v}"),
            SqlValue::Real(v) => {
                if v.is_nan() {
                    write!(f, "NULL")
                } else if v.is_infinite() {
                    if v.is_sign_positive() {
                        write!(f, "9e999")
                    } else {
                        write!(f, "-9e999")
                    }
                } else {
                    // Ensure we always have a decimal point so the value is recognized as REAL
                    let s = alloc::format!("{v}");
                    if s.contains('.') || s.contains('e') || s.contains('E') {
                        write!(f, "{s}")
                    } else {
                        write!(f, "{s}.0")
                    }
                }
            }
            SqlValue::Text(s) => {
                // Escape single quotes by doubling them
                write!(f, "'")?;
                for c in s.chars() {
                    if c == '\'' {
                        write!(f, "''")?;
                    } else {
                        write!(f, "{c}")?;
                    }
                }
                write!(f, "'")
            }
            SqlValue::Blob(b) => {
                write!(f, "X'")?;
                for byte in b {
                    write!(f, "{byte:02X}")?;
                }
                write!(f, "'")
            }
        }
    }
}

/// Trait for types that can be formatted as SQL statements.
pub trait FormatSql {
    /// Format this value as a SQL string.
    fn format_sql(&self) -> String;
}

impl FormatSql for super::parser::CreateTable {
    fn format_sql(&self) -> String {
        let mut sql = String::new();
        write!(sql, "CREATE TABLE {} (", self.name).unwrap();

        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&col.name);
            if let Some(ref type_name) = col.type_name {
                write!(sql, " {type_name}").unwrap();
            }
            if col.is_primary_key {
                sql.push_str(" PRIMARY KEY");
            }
        }

        // Table-level PRIMARY KEY constraint
        if !self.table_pk_columns.is_empty() {
            sql.push_str(", PRIMARY KEY (");
            for (i, &idx) in self.table_pk_columns.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&self.columns[idx].name);
            }
            sql.push(')');
        }

        sql.push(')');
        sql
    }
}

impl FormatSql for super::parser::InsertStatement {
    fn format_sql(&self) -> String {
        let mut sql = String::new();
        write!(sql, "INSERT INTO {}", self.table).unwrap();

        if !self.columns.is_empty() {
            sql.push_str(" (");
            for (i, col) in self.columns.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(col);
            }
            sql.push(')');
        }

        sql.push_str(" VALUES (");
        for (i, val) in self.values.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            write!(sql, "{val}").unwrap();
        }
        sql.push(')');

        sql
    }
}

impl FormatSql for super::parser::UpdateStatement {
    fn format_sql(&self) -> String {
        let mut sql = String::new();
        write!(sql, "UPDATE {} SET ", self.table).unwrap();

        for (i, (col, val)) in self.assignments.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            write!(sql, "{col} = {val}").unwrap();
        }

        sql.push_str(" WHERE ");
        for (i, (col, val)) in self.where_clause.iter().enumerate() {
            if i > 0 {
                sql.push_str(" AND ");
            }
            write!(sql, "{col} = {val}").unwrap();
        }

        sql
    }
}

impl FormatSql for super::parser::DeleteStatement {
    fn format_sql(&self) -> String {
        let mut sql = String::new();
        write!(sql, "DELETE FROM {} WHERE ", self.table).unwrap();

        for (i, (col, val)) in self.where_clause.iter().enumerate() {
            if i > 0 {
                sql.push_str(" AND ");
            }
            write!(sql, "{col} = {val}").unwrap();
        }

        sql
    }
}

impl FormatSql for super::parser::Statement {
    fn format_sql(&self) -> String {
        match self {
            super::parser::Statement::CreateTable(ct) => ct.format_sql(),
            super::parser::Statement::Insert(ins) => ins.format_sql(),
            super::parser::Statement::Update(upd) => upd.format_sql(),
            super::parser::Statement::Delete(del) => del.format_sql(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;
    use alloc::vec;
    use crate::sql::parser::{ColumnDef, CreateTable, DeleteStatement, InsertStatement, UpdateStatement};

    #[test]
    fn test_format_sql_value() {
        assert_eq!(SqlValue::Null.to_string(), "NULL");
        assert_eq!(SqlValue::Integer(42).to_string(), "42");
        assert_eq!(SqlValue::Integer(-100).to_string(), "-100");
        assert_eq!(SqlValue::Real(3.14).to_string(), "3.14");
        assert_eq!(SqlValue::Text("hello".into()).to_string(), "'hello'");
        assert_eq!(SqlValue::Text("it's".into()).to_string(), "'it''s'");
        assert_eq!(
            SqlValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]).to_string(),
            "X'DEADBEEF'"
        );
    }

    #[test]
    fn test_format_create_table() {
        let ct = CreateTable {
            name: "users".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    type_name: Some("INTEGER".into()),
                    is_primary_key: true,
                },
                ColumnDef {
                    name: "name".into(),
                    type_name: Some("TEXT".into()),
                    is_primary_key: false,
                },
            ],
            table_pk_columns: vec![],
        };
        assert_eq!(
            ct.format_sql(),
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
        );
    }

    #[test]
    fn test_format_insert() {
        let ins = InsertStatement {
            table: "users".into(),
            columns: vec!["id".into(), "name".into()],
            values: vec![SqlValue::Integer(1), SqlValue::Text("Alice".into())],
        };
        assert_eq!(
            ins.format_sql(),
            "INSERT INTO users (id, name) VALUES (1, 'Alice')"
        );
    }

    #[test]
    fn test_format_update() {
        let upd = UpdateStatement {
            table: "users".into(),
            assignments: vec![("name".into(), SqlValue::Text("Bob".into()))],
            where_clause: vec![("id".into(), SqlValue::Integer(1))],
        };
        assert_eq!(
            upd.format_sql(),
            "UPDATE users SET name = 'Bob' WHERE id = 1"
        );
    }

    #[test]
    fn test_format_delete() {
        let del = DeleteStatement {
            table: "users".into(),
            where_clause: vec![("id".into(), SqlValue::Integer(1))],
        };
        assert_eq!(del.format_sql(), "DELETE FROM users WHERE id = 1");
    }
}
