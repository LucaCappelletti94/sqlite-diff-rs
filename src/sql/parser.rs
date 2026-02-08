//! SQL parser for changeset/patchset operations.

use alloc::string::String;
use alloc::vec::Vec;

use super::lexer::{Lexer, LexerError, Token, TokenKind};

/// A SQL value literal.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    /// NULL value.
    Null,
    /// Integer value.
    Integer(i64),
    /// Real/float value.
    Real(f64),
    /// Text/string value.
    Text(String),
    /// Blob value.
    Blob(Vec<u8>),
}

/// A column definition in CREATE TABLE.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Column type (optional, for documentation only).
    pub type_name: Option<String>,
    /// Whether this column is marked as PRIMARY KEY.
    pub is_primary_key: bool,
}

/// A CREATE TABLE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    /// Table name.
    pub name: String,
    /// Column definitions.
    pub columns: Vec<ColumnDef>,
    /// Indices of primary key columns (from table-level PRIMARY KEY constraint).
    /// If empty, check individual column `is_primary_key` flags.
    pub table_pk_columns: Vec<usize>,
}

impl CreateTable {
    /// Get the indices of primary key columns, in PK order.
    #[must_use]
    pub fn pk_indices(&self) -> Vec<usize> {
        if !self.table_pk_columns.is_empty() {
            return self.table_pk_columns.clone();
        }

        // Fall back to column-level PRIMARY KEY
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, col)| col.is_primary_key)
            .map(|(i, _)| i)
            .collect()
    }

    /// Get the column names.
    #[must_use]
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

/// An INSERT statement.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    /// Table name.
    pub table: String,
    /// Column names (empty if positional insert).
    pub columns: Vec<String>,
    /// Values to insert.
    pub values: Vec<SqlValue>,
}

/// An UPDATE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    /// Table name.
    pub table: String,
    /// SET assignments: (column, value) pairs.
    pub assignments: Vec<(String, SqlValue)>,
    /// WHERE conditions: (column, value) pairs (joined by AND).
    pub where_clause: Vec<(String, SqlValue)>,
}

/// A DELETE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    /// Table name.
    pub table: String,
    /// WHERE conditions: (column, value) pairs (joined by AND).
    pub where_clause: Vec<(String, SqlValue)>,
}

/// A parsed SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// CREATE TABLE statement.
    CreateTable(CreateTable),
    /// INSERT statement.
    Insert(InsertStatement),
    /// UPDATE statement.
    Update(UpdateStatement),
    /// DELETE statement.
    Delete(DeleteStatement),
}

/// SQL parser errors.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ParseError {
    /// Lexer error.
    #[error("Lexer error: {0}")]
    Lexer(#[from] LexerError),
    /// Unexpected token.
    #[error("Unexpected token {found:?} at position {pos}, expected {expected}")]
    UnexpectedToken {
        /// What was expected.
        expected: String,
        /// What was found.
        found: TokenKind,
        /// Position in input.
        pos: usize,
    },
    /// Unexpected end of input.
    #[error("Unexpected end of input, expected {expected}")]
    UnexpectedEof {
        /// What was expected.
        expected: String,
    },
    /// Empty column list.
    #[error("Empty column list in CREATE TABLE")]
    EmptyColumnList,
    /// Duplicate column name.
    #[error("Duplicate column name: {0}")]
    DuplicateColumn(String),
    /// Unknown column in PRIMARY KEY constraint.
    #[error("Unknown column '{column}' in PRIMARY KEY constraint")]
    UnknownPKColumn {
        /// The unknown column name.
        column: String,
    },
    /// Missing VALUES clause in INSERT.
    #[error("Missing VALUES clause in INSERT")]
    MissingValues,
    /// Missing WHERE clause in UPDATE/DELETE.
    #[error("Missing WHERE clause in {statement}")]
    MissingWhere {
        /// The statement type.
        statement: String,
    },
}

/// SQL parser.
pub struct Parser<'a> {
    lexer: Lexer<'a>,
}

impl<'a> Parser<'a> {
    /// Create a new parser for the given input.
    #[must_use]
    pub fn new(input: &'a str) -> Self {
        Self {
            lexer: Lexer::new(input),
        }
    }

    /// Parse all statements from the input.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse_all(&mut self) -> Result<Vec<Statement>, ParseError> {
        let mut statements = Vec::new();

        loop {
            let token = self.lexer.peek()?;
            if token.kind == TokenKind::Eof {
                break;
            }

            statements.push(self.parse_statement()?);

            // Skip optional semicolon
            if self.lexer.peek()?.kind == TokenKind::Semicolon {
                self.lexer.next()?;
            }
        }

        Ok(statements)
    }

    /// Parse a single statement.
    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        let token = self.lexer.peek()?;
        match &token.kind {
            TokenKind::Create => self.parse_create_table(),
            TokenKind::Insert => self.parse_insert(),
            TokenKind::Update => self.parse_update(),
            TokenKind::Delete => self.parse_delete(),
            other => Err(ParseError::UnexpectedToken {
                expected: "CREATE, INSERT, UPDATE, or DELETE".into(),
                found: other.clone(),
                pos: token.pos,
            }),
        }
    }

    /// Parse a CREATE TABLE statement.
    fn parse_create_table(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::Create)?;
        self.expect(TokenKind::Table)?;

        let name = self.expect_identifier()?;
        self.expect(TokenKind::LParen)?;

        let mut columns = Vec::new();
        let mut table_pk_columns = Vec::new();

        loop {
            // Check for table-level PRIMARY KEY constraint
            if self.lexer.peek()?.kind == TokenKind::Primary {
                self.lexer.next()?;
                self.expect(TokenKind::Key)?;
                self.expect(TokenKind::LParen)?;

                // Parse column names in PK
                loop {
                    let col_name = self.expect_identifier()?;
                    let col_idx = columns
                        .iter()
                        .position(|c: &ColumnDef| c.name == col_name)
                        .ok_or_else(|| ParseError::UnknownPKColumn {
                            column: col_name.clone(),
                        })?;
                    table_pk_columns.push(col_idx);

                    if self.lexer.peek()?.kind != TokenKind::Comma {
                        break;
                    }
                    self.lexer.next()?;
                }

                self.expect(TokenKind::RParen)?;
            } else {
                // Parse column definition
                let col_def = self.parse_column_def()?;

                // Check for duplicate
                if columns.iter().any(|c: &ColumnDef| c.name == col_def.name) {
                    return Err(ParseError::DuplicateColumn(col_def.name));
                }

                columns.push(col_def);
            }

            if self.lexer.peek()?.kind != TokenKind::Comma {
                break;
            }
            self.lexer.next()?;
        }

        self.expect(TokenKind::RParen)?;

        if columns.is_empty() {
            return Err(ParseError::EmptyColumnList);
        }

        Ok(Statement::CreateTable(CreateTable {
            name,
            columns,
            table_pk_columns,
        }))
    }

    /// Parse a column definition.
    fn parse_column_def(&mut self) -> Result<ColumnDef, ParseError> {
        let name = self.expect_identifier()?;

        // Optional type name
        let mut type_name = None;
        let mut is_primary_key = false;

        loop {
            let token = self.lexer.peek()?;
            match &token.kind {
                TokenKind::Integer | TokenKind::Int => {
                    self.lexer.next()?;
                    type_name = Some("INTEGER".into());
                }
                TokenKind::Real => {
                    self.lexer.next()?;
                    type_name = Some("REAL".into());
                }
                TokenKind::Text => {
                    self.lexer.next()?;
                    type_name = Some("TEXT".into());
                }
                TokenKind::Blob => {
                    self.lexer.next()?;
                    type_name = Some("BLOB".into());
                }
                TokenKind::Identifier(s) => {
                    // Accept any identifier as type name
                    type_name = Some(s.clone());
                    self.lexer.next()?;
                }
                TokenKind::Primary => {
                    self.lexer.next()?;
                    self.expect(TokenKind::Key)?;
                    is_primary_key = true;
                }
                TokenKind::Not => {
                    // Skip NOT NULL
                    self.lexer.next()?;
                    self.expect(TokenKind::Null)?;
                }
                _ => break,
            }
        }

        Ok(ColumnDef {
            name,
            type_name,
            is_primary_key,
        })
    }

    /// Parse an INSERT statement.
    fn parse_insert(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::Insert)?;
        self.expect(TokenKind::Into)?;

        let table = self.expect_identifier()?;

        // Optional column list
        let mut columns = Vec::new();
        if self.lexer.peek()?.kind == TokenKind::LParen {
            self.lexer.next()?;

            loop {
                columns.push(self.expect_identifier()?);
                if self.lexer.peek()?.kind != TokenKind::Comma {
                    break;
                }
                self.lexer.next()?;
            }

            self.expect(TokenKind::RParen)?;
        }

        self.expect(TokenKind::Values)?;
        self.expect(TokenKind::LParen)?;

        let mut values = Vec::new();
        loop {
            values.push(self.parse_value()?);
            if self.lexer.peek()?.kind != TokenKind::Comma {
                break;
            }
            self.lexer.next()?;
        }

        self.expect(TokenKind::RParen)?;

        Ok(Statement::Insert(InsertStatement {
            table,
            columns,
            values,
        }))
    }

    /// Parse an UPDATE statement.
    fn parse_update(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::Update)?;

        let table = self.expect_identifier()?;
        self.expect(TokenKind::Set)?;

        // Parse SET assignments
        let mut assignments = Vec::new();
        loop {
            let col = self.expect_identifier()?;
            self.expect(TokenKind::Equals)?;
            let val = self.parse_value()?;
            assignments.push((col, val));

            if self.lexer.peek()?.kind != TokenKind::Comma {
                break;
            }
            self.lexer.next()?;
        }

        // WHERE clause is required
        if self.lexer.peek()?.kind != TokenKind::Where {
            return Err(ParseError::MissingWhere {
                statement: "UPDATE".into(),
            });
        }
        let where_clause = self.parse_where()?;

        Ok(Statement::Update(UpdateStatement {
            table,
            assignments,
            where_clause,
        }))
    }

    /// Parse a DELETE statement.
    fn parse_delete(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::Delete)?;
        self.expect(TokenKind::From)?;

        let table = self.expect_identifier()?;

        // WHERE clause is required
        if self.lexer.peek()?.kind != TokenKind::Where {
            return Err(ParseError::MissingWhere {
                statement: "DELETE".into(),
            });
        }
        let where_clause = self.parse_where()?;

        Ok(Statement::Delete(DeleteStatement {
            table,
            where_clause,
        }))
    }

    /// Parse a WHERE clause.
    fn parse_where(&mut self) -> Result<Vec<(String, SqlValue)>, ParseError> {
        self.expect(TokenKind::Where)?;

        let mut conditions = Vec::new();
        loop {
            let col = self.expect_identifier()?;
            self.expect(TokenKind::Equals)?;
            let val = self.parse_value()?;
            conditions.push((col, val));

            if self.lexer.peek()?.kind != TokenKind::And {
                break;
            }
            self.lexer.next()?;
        }

        Ok(conditions)
    }

    /// Parse a value literal.
    fn parse_value(&mut self) -> Result<SqlValue, ParseError> {
        let token = self.lexer.next()?;
        match token.kind {
            TokenKind::Null => Ok(SqlValue::Null),
            TokenKind::IntegerLiteral(v) => Ok(SqlValue::Integer(v)),
            TokenKind::RealLiteral(v) => Ok(SqlValue::Real(v)),
            TokenKind::StringLiteral(s) => Ok(SqlValue::Text(s)),
            TokenKind::BlobLiteral(b) => Ok(SqlValue::Blob(b)),
            TokenKind::Minus => {
                // Negative number
                let next = self.lexer.next()?;
                match next.kind {
                    TokenKind::IntegerLiteral(v) => Ok(SqlValue::Integer(-v)),
                    TokenKind::RealLiteral(v) => Ok(SqlValue::Real(-v)),
                    other => Err(ParseError::UnexpectedToken {
                        expected: "number after minus".into(),
                        found: other,
                        pos: next.pos,
                    }),
                }
            }
            other => Err(ParseError::UnexpectedToken {
                expected: "value (NULL, number, string, or blob)".into(),
                found: other,
                pos: token.pos,
            }),
        }
    }

    /// Expect a specific token kind.
    fn expect(&mut self, expected: TokenKind) -> Result<Token, ParseError> {
        let token = self.lexer.next()?;
        if core::mem::discriminant(&token.kind) == core::mem::discriminant(&expected) {
            Ok(token)
        } else {
            Err(ParseError::UnexpectedToken {
                expected: alloc::format!("{expected:?}"),
                found: token.kind,
                pos: token.pos,
            })
        }
    }

    /// Expect an identifier and return its name.
    fn expect_identifier(&mut self) -> Result<String, ParseError> {
        let token = self.lexer.next()?;
        match token.kind {
            TokenKind::Identifier(name) => Ok(name),
            // Also accept keywords as identifiers (common in SQL)
            TokenKind::Create => Ok("CREATE".into()),
            TokenKind::Table => Ok("TABLE".into()),
            TokenKind::Insert => Ok("INSERT".into()),
            TokenKind::Into => Ok("INTO".into()),
            TokenKind::Values => Ok("VALUES".into()),
            TokenKind::Update => Ok("UPDATE".into()),
            TokenKind::Set => Ok("SET".into()),
            TokenKind::Delete => Ok("DELETE".into()),
            TokenKind::From => Ok("FROM".into()),
            TokenKind::Where => Ok("WHERE".into()),
            TokenKind::And => Ok("AND".into()),
            TokenKind::Primary => Ok("PRIMARY".into()),
            TokenKind::Key => Ok("KEY".into()),
            TokenKind::Null => Ok("NULL".into()),
            TokenKind::Integer => Ok("INTEGER".into()),
            TokenKind::Int => Ok("INT".into()),
            TokenKind::Real => Ok("REAL".into()),
            TokenKind::Text => Ok("TEXT".into()),
            TokenKind::Blob => Ok("BLOB".into()),
            TokenKind::Not => Ok("NOT".into()),
            other => Err(ParseError::UnexpectedToken {
                expected: "identifier".into(),
                found: other,
                pos: token.pos,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_parse_create_table_simple() {
        let mut parser = Parser::new("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        let stmts = parser.parse_all().unwrap();
        assert_eq!(stmts.len(), 1);

        if let Statement::CreateTable(ct) = &stmts[0] {
            assert_eq!(ct.name, "users");
            assert_eq!(ct.columns.len(), 2);
            assert_eq!(ct.columns[0].name, "id");
            assert!(ct.columns[0].is_primary_key);
            assert_eq!(ct.columns[1].name, "name");
            assert!(!ct.columns[1].is_primary_key);
            assert_eq!(ct.pk_indices(), vec![0]);
        } else {
            panic!("Expected CreateTable");
        }
    }

    #[test]
    fn test_parse_create_table_composite_pk() {
        let mut parser =
            Parser::new("CREATE TABLE t (a INT, b INT, c TEXT, PRIMARY KEY (a, b))");
        let stmts = parser.parse_all().unwrap();
        assert_eq!(stmts.len(), 1);

        if let Statement::CreateTable(ct) = &stmts[0] {
            assert_eq!(ct.name, "t");
            assert_eq!(ct.columns.len(), 3);
            assert_eq!(ct.pk_indices(), vec![0, 1]);
        } else {
            panic!("Expected CreateTable");
        }
    }

    #[test]
    fn test_parse_insert() {
        let mut parser = Parser::new("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        let stmts = parser.parse_all().unwrap();
        assert_eq!(stmts.len(), 1);

        if let Statement::Insert(ins) = &stmts[0] {
            assert_eq!(ins.table, "users");
            assert_eq!(ins.columns, vec!["id", "name"]);
            assert_eq!(ins.values.len(), 2);
            assert_eq!(ins.values[0], SqlValue::Integer(1));
            assert_eq!(ins.values[1], SqlValue::Text("Alice".into()));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_parse_insert_positional() {
        let mut parser = Parser::new("INSERT INTO users VALUES (1, 'Alice')");
        let stmts = parser.parse_all().unwrap();
        assert_eq!(stmts.len(), 1);

        if let Statement::Insert(ins) = &stmts[0] {
            assert_eq!(ins.table, "users");
            assert!(ins.columns.is_empty());
            assert_eq!(ins.values.len(), 2);
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_parse_update() {
        let mut parser = Parser::new("UPDATE users SET name = 'Bob' WHERE id = 1");
        let stmts = parser.parse_all().unwrap();
        assert_eq!(stmts.len(), 1);

        if let Statement::Update(upd) = &stmts[0] {
            assert_eq!(upd.table, "users");
            assert_eq!(upd.assignments.len(), 1);
            assert_eq!(upd.assignments[0].0, "name");
            assert_eq!(upd.assignments[0].1, SqlValue::Text("Bob".into()));
            assert_eq!(upd.where_clause.len(), 1);
            assert_eq!(upd.where_clause[0].0, "id");
            assert_eq!(upd.where_clause[0].1, SqlValue::Integer(1));
        } else {
            panic!("Expected Update");
        }
    }

    #[test]
    fn test_parse_delete() {
        let mut parser = Parser::new("DELETE FROM users WHERE id = 1 AND status = 'active'");
        let stmts = parser.parse_all().unwrap();
        assert_eq!(stmts.len(), 1);

        if let Statement::Delete(del) = &stmts[0] {
            assert_eq!(del.table, "users");
            assert_eq!(del.where_clause.len(), 2);
        } else {
            panic!("Expected Delete");
        }
    }

    #[test]
    fn test_parse_multiple_statements() {
        let sql = "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO users (id, name) VALUES (1, 'Alice');
            UPDATE users SET name = 'Alicia' WHERE id = 1;
            DELETE FROM users WHERE id = 1;
        ";
        let mut parser = Parser::new(sql);
        let stmts = parser.parse_all().unwrap();
        assert_eq!(stmts.len(), 4);
    }

    #[test]
    fn test_parse_blob_value() {
        let mut parser = Parser::new("INSERT INTO t (data) VALUES (X'DEADBEEF')");
        let stmts = parser.parse_all().unwrap();

        if let Statement::Insert(ins) = &stmts[0] {
            assert_eq!(ins.values[0], SqlValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_parse_null_value() {
        let mut parser = Parser::new("INSERT INTO t (v) VALUES (NULL)");
        let stmts = parser.parse_all().unwrap();

        if let Statement::Insert(ins) = &stmts[0] {
            assert_eq!(ins.values[0], SqlValue::Null);
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_parse_negative_numbers() {
        let mut parser = Parser::new("INSERT INTO t (a, b) VALUES (-42, -3.14)");
        let stmts = parser.parse_all().unwrap();

        if let Statement::Insert(ins) = &stmts[0] {
            assert_eq!(ins.values[0], SqlValue::Integer(-42));
            assert_eq!(ins.values[1], SqlValue::Real(-3.14));
        } else {
            panic!("Expected Insert");
        }
    }
}
