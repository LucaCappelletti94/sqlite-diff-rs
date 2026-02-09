//! SQL parser for changeset/patchset operations.

use core::hash::Hash;

use crate::{
    DiffSetBuilder, PatchsetFormat, SchemaWithPK, Value, builders::operation::Operation,
    schema::NamedColumns,
};
use alloc::borrow::Cow;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use super::lexer::{Lexer, LexerError, Token, TokenKind};

/// SQL parser errors.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ParseError<'a> {
    /// Lexer error.
    #[error("Lexer error: {0}")]
    Lexer(#[from] LexerError),
    /// Unexpected token.
    #[error("Unexpected token {found:?} at position {pos}, expected {expected}")]
    UnexpectedToken {
        /// What was expected.
        expected: &'static str,
        /// What was found.
        found: TokenKind<'a>,
        /// Position in input.
        pos: usize,
    },
    /// Unexpected end of input.
    #[error("Unexpected end of input, expected {expected}")]
    UnexpectedEof {
        /// What was expected.
        expected: &'static str,
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
    /// Unknown table name in statement.
    #[error("Unknown table name: {0}")]
    UnknownTable(&'a str),
    /// Unknown column name in statement.
    #[error("Unknown column name: {0}")]
    UnknownColumn(&'a str),
    /// Missing WHERE clause in UPDATE or DELETE.
    #[error("Missing WHERE clause in {statement}")]
    MissingWhere {
        /// The statement type.
        statement: &'static str,
    },
    /// Where constraint on non-PK column.
    #[error("WHERE clause on non-primary key column '{column}'")]
    WhereNonPKColumn {
        /// The column name.
        column: &'a str,
    },
}

/// SQL parser.
pub(crate) struct Parser<'input, 'builder, T: SchemaWithPK, S> {
    lexer: Lexer<'input>,
    builder: &'builder mut DiffSetBuilder<PatchsetFormat, T, S, Vec<u8>>,
}

impl<'input, 'builder, T: NamedColumns, S: Clone + Hash + Eq + AsRef<str> + for<'a> From<&'a str>>
    Parser<'input, 'builder, T, S>
{
    /// Create a new parser for the given input.
    #[must_use]
    pub(crate) fn new(
        input: &'input str,
        builder: &'builder mut DiffSetBuilder<PatchsetFormat, T, S, Vec<u8>>,
    ) -> Self {
        Self {
            lexer: Lexer::new(input),
            builder,
        }
    }

    /// Parse all statements from the input.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub(crate) fn digest_all(&mut self) -> Result<(), ParseError<'input>> {
        loop {
            // Skip any semicolons (leading, trailing, between statements)
            while self.lexer.peek()?.kind == TokenKind::Semicolon {
                self.lexer.next()?;
            }

            if self.lexer.peek()?.kind == TokenKind::Eof {
                break;
            }

            self.digest_statement()?;
        }

        Ok(())
    }

    /// Parse a single statement.
    fn digest_statement(&mut self) -> Result<(), ParseError<'input>> {
        let token = self.lexer.peek()?;
        match &token.kind {
            TokenKind::Insert => self.digest_insert(),
            TokenKind::Update => self.digest_update(),
            TokenKind::Delete => self.digest_delete(),
            other => Err(ParseError::UnexpectedToken {
                expected: "INSERT, UPDATE, or DELETE",
                found: other.clone(),
                pos: token.pos,
            }),
        }
    }

    /// Parse an INSERT statement.
    fn digest_insert(&mut self) -> Result<(), ParseError<'input>> {
        self.expect(TokenKind::Insert)?;
        self.expect(TokenKind::Into)?;

        let table = self.expect_table()?;

        // There cannot ever possibly be more than i32::MAX columns
        // and since they are unsigned, we use u16 for column identifiers.
        // By default, there can be up to 2000 columns in SQLite, so this is more than enough.
        // If a user employes SQLITE_MAX_COLUMN to set a higher limit, they still cannot exceed
        // i16::MAX columns, so this is safe.
        let mut column_identifiers: Vec<u16> = Vec::new();
        // Optional column list
        if self.lexer.peek()?.kind == TokenKind::LParen {
            self.lexer.next()?;

            loop {
                column_identifiers.push(self.expect_column(&table)?.0);
                if self.lexer.peek()?.kind != TokenKind::Comma {
                    break;
                }
                self.lexer.next()?;
            }

            self.expect(TokenKind::RParen)?;
        }

        self.expect(TokenKind::Values)?;
        self.expect(TokenKind::LParen)?;

        let mut values = vec![Value::Null; table.number_of_columns()];
        let mut pks = vec![Value::Null; table.number_of_primary_keys()];

        if column_identifiers.is_empty() {
            // No explicit column list: values map to all columns in order
            for col_idx in 0..table.number_of_columns() {
                if col_idx > 0 {
                    self.expect(TokenKind::Comma)?;
                }
                values[col_idx] = self.parse_value()?;
                if let Some(pk_idx) = table.primary_key_index(col_idx) {
                    pks[pk_idx] = values[col_idx].clone();
                }
            }
        } else {
            for column_index in column_identifiers {
                values[usize::from(column_index)] = self.parse_value()?;
                if let Some(primary_key_index) = table.primary_key_index(usize::from(column_index)) {
                    pks[primary_key_index] = values[usize::from(column_index)].clone();
                };
                if self.lexer.peek()?.kind != TokenKind::Comma {
                    break;
                }
                self.lexer.next()?;
            }
        }

        self.expect(TokenKind::RParen)?;

        self.builder
            .add_operation(&table, pks, Operation::Insert(values));

        Ok(())
    }

    /// Parse an UPDATE statement.
    fn digest_update(&mut self) -> Result<(), ParseError<'input>> {
        self.expect(TokenKind::Update)?;

        let table = self.expect_table()?;
        self.expect(TokenKind::Set)?;

        let mut new_values = vec![((), None); table.number_of_columns()];

        // Parse SET assignments
        loop {
            let (col_idx, _) = self.expect_column(&table)?;
            self.expect(TokenKind::Equals)?;
            let val = self.parse_value()?;
            new_values[usize::from(col_idx)] = ((), Some(val));

            if self.lexer.peek()?.kind != TokenKind::Comma {
                break;
            }
            self.lexer.next()?;
        }

        // WHERE clause is required
        if self.lexer.peek()?.kind != TokenKind::Where {
            return Err(ParseError::MissingWhere {
                statement: "UPDATE",
            });
        }

        let mut pk = vec![Value::Null; table.number_of_primary_keys()];

        self.digest_where(&table, |col_idx, col_name, val| {
            if let Some(primary_key_index) = table.primary_key_index(usize::from(col_idx)) {
                pk[primary_key_index] = val.clone();
                Ok(())
            } else {
                Err(ParseError::WhereNonPKColumn { column: col_name })
            }
        })?;

        self.builder
            .add_operation(&table, pk, Operation::Update(new_values));

        Ok(())
    }

    /// Parse a DELETE statement.
    fn digest_delete(&mut self) -> Result<(), ParseError<'input>> {
        self.expect(TokenKind::Delete)?;
        self.expect(TokenKind::From)?;

        let table = self.expect_table()?;
        let mut pks = vec![Value::Null; table.number_of_primary_keys()];

        // WHERE clause is required
        if self.lexer.peek()?.kind != TokenKind::Where {
            return Err(ParseError::MissingWhere {
                statement: "DELETE",
            });
        }
        self.digest_where(&table, |col_idx, col_name, val| {
            if let Some(primary_key_index) = table.primary_key_index(usize::from(col_idx)) {
                pks[primary_key_index] = val.clone();
                Ok(())
            } else {
                Err(ParseError::WhereNonPKColumn { column: col_name })
            }
        })?;

        self.builder
            .add_operation(&table, pks, Operation::Delete(()));

        Ok(())
    }

    /// Parse a WHERE clause.
    fn digest_where<D>(
        &mut self,
        table: &T,
        mut digestor: D,
    ) -> Result<(), ParseError<'input>>
    where
        D: FnMut(u16, &'input str, Value<S, Vec<u8>>) -> Result<(), ParseError<'input>>,
    {
        self.expect(TokenKind::Where)?;

        loop {
            let (col_idx, col_name) = self.expect_column(table)?;
            self.expect(TokenKind::Equals)?;
            let val = self.parse_value()?;
            digestor(col_idx, col_name, val)?;

            if self.lexer.peek()?.kind != TokenKind::And {
                break;
            }
            self.lexer.next()?;
        }

        Ok(())
    }

    /// Parse a value literal.
    fn parse_value(&mut self) -> Result<Value<S, Vec<u8>>, ParseError<'input>> {
        let token = self.lexer.next()?;
        match token.kind {
            TokenKind::Null => Ok(Value::Null),
            TokenKind::IntegerLiteral(v) => Ok(Value::Integer(v)),
            TokenKind::RealLiteral(v) => Ok(Value::Real(v)),
            TokenKind::StringLiteral(s) => {
                let text: S = match s {
                    Cow::Borrowed(b) => S::from(b),
                    Cow::Owned(o) => S::from(o.as_str()),
                };
                Ok(Value::Text(text))
            }
            TokenKind::BlobLiteral(b) => Ok(Value::Blob(b)),
            TokenKind::Minus => {
                // Negative number
                let next = self.lexer.next()?;
                match next.kind {
                    TokenKind::IntegerLiteral(v) => Ok(Value::Integer(-v)),
                    TokenKind::RealLiteral(v) => {
                        // Check if the negated float is exactly representable as i64.
                        // This handles the i64::MIN case: 9223372036854775808 overflows
                        // i64 in the lexer (positive), but -9223372036854775808 is valid.
                        let neg = -v;
                        #[allow(clippy::cast_possible_truncation)]
                        if neg >= i64::MIN as f64
                            && neg <= i64::MAX as f64
                            && neg == (neg as i64 as f64)
                        {
                            Ok(Value::Integer(neg as i64))
                        } else {
                            Ok(Value::Real(neg))
                        }
                    }
                    other => Err(ParseError::UnexpectedToken {
                        expected: "number after minus",
                        found: other,
                        pos: next.pos,
                    }),
                }
            }
            other => Err(ParseError::UnexpectedToken {
                expected: "value (NULL, number, string, or blob)",
                found: other,
                pos: token.pos,
            }),
        }
    }

    /// Expect a specific token kind.
    fn expect(&mut self, expected: TokenKind<'input>) -> Result<Token<'input>, ParseError<'input>> {
        let token = self.lexer.next()?;
        if core::mem::discriminant(&token.kind) == core::mem::discriminant(&expected) {
            Ok(token)
        } else {
            Err(ParseError::UnexpectedToken {
                expected: expected.static_name(),
                found: token.kind,
                pos: token.pos,
            })
        }
    }

    /// Expects a column identifier and returns the corresponding column index in the table schema.
    fn expect_column(
        &mut self,
        table: &T,
    ) -> Result<(u16, &'input str), ParseError<'input>> {
        let column_name = self.expect_identifier()?;
        table
            .column_index(column_name)
            .map(|idx| (idx as u16, column_name))
            .ok_or(ParseError::UnknownColumn(column_name))
    }

    /// Expects a table existing in the builder's schema and returns a clone.
    fn expect_table(&mut self) -> Result<T, ParseError<'input>> {
        let table_name = self.expect_identifier()?;
        self.builder
            .table(table_name)
            .cloned()
            .ok_or(ParseError::UnknownTable(table_name))
    }

    /// Expect an identifier and return its name.
    fn expect_identifier(&mut self) -> Result<&'input str, ParseError<'input>> {
        let token = self.lexer.next()?;
        match token.kind {
            TokenKind::Identifier(name) => Ok(name),
            // Also accept keywords as identifiers (common in SQL)
            TokenKind::Insert => Ok("INSERT"),
            TokenKind::Into => Ok("INTO"),
            TokenKind::Values => Ok("VALUES"),
            TokenKind::Update => Ok("UPDATE"),
            TokenKind::Set => Ok("SET"),
            TokenKind::Delete => Ok("DELETE"),
            TokenKind::From => Ok("FROM"),
            TokenKind::Where => Ok("WHERE"),
            TokenKind::And => Ok("AND"),
            TokenKind::Primary => Ok("PRIMARY"),
            TokenKind::Key => Ok("KEY"),
            TokenKind::Null => Ok("NULL"),
            TokenKind::Integer => Ok("INTEGER"),
            TokenKind::Int => Ok("INT"),
            TokenKind::Real => Ok("REAL"),
            TokenKind::Text => Ok("TEXT"),
            TokenKind::Blob => Ok("BLOB"),
            TokenKind::Not => Ok("NOT"),
            other => Err(ParseError::UnexpectedToken {
                expected: "identifier",
                found: other,
                pos: token.pos,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::string::String;
    use alloc::vec::Vec;

    use crate::schema::SimpleTable;
    use crate::{DiffSetBuilder, PatchsetFormat};

    fn make_builder(
        tables: &[SimpleTable],
    ) -> DiffSetBuilder<PatchsetFormat, SimpleTable, String, Vec<u8>> {
        let mut builder = DiffSetBuilder::default();
        for t in tables {
            builder.add_table(t);
        }
        builder
    }

    #[test]
    fn test_digest_insert() {
        let users = SimpleTable::new("users", &["id", "name"], &[0]);
        let mut builder = make_builder(&[users]);
        builder
            .digest_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .unwrap();
        assert_eq!(builder.len(), 1);
        assert!(!builder.build().is_empty());
    }

    #[test]
    fn test_digest_insert_positional() {
        let users = SimpleTable::new("users", &["id", "name"], &[0]);
        let mut builder = make_builder(&[users]);
        builder
            .digest_sql("INSERT INTO users VALUES (1, 'Alice')")
            .unwrap();
        assert_eq!(builder.len(), 1);
    }

    #[test]
    fn test_digest_update() {
        let users = SimpleTable::new("users", &["id", "name"], &[0]);
        let mut builder = make_builder(&[users]);
        builder
            .digest_sql("UPDATE users SET name = 'Bob' WHERE id = 1")
            .unwrap();
        assert_eq!(builder.len(), 1);
        assert!(!builder.build().is_empty());
    }

    #[test]
    fn test_digest_delete() {
        let users = SimpleTable::new("users", &["id", "name"], &[0]);
        let mut builder = make_builder(&[users]);
        builder
            .digest_sql("DELETE FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(builder.len(), 1);
        assert!(!builder.build().is_empty());
    }

    #[test]
    fn test_digest_delete_rejects_non_pk_in_where() {
        let users = SimpleTable::new("users", &["id", "name", "status"], &[0]);
        let mut builder = make_builder(&[users]);
        let result =
            builder.digest_sql("DELETE FROM users WHERE id = 1 AND status = 'active'");
        assert!(result.is_err());
    }

    #[test]
    fn test_digest_multiple_dml() {
        let users = SimpleTable::new("users", &["id", "name"], &[0]);
        let mut builder = make_builder(&[users]);
        builder
            .digest_sql(
                "INSERT INTO users (id, name) VALUES (1, 'Alice');\
                 INSERT INTO users (id, name) VALUES (2, 'Bob');\
                 DELETE FROM users WHERE id = 1;",
            )
            .unwrap();
        // INSERT(1) + INSERT(2) + DELETE(1) → only INSERT(2) survives
        assert_eq!(builder.len(), 1);
        assert!(!builder.build().is_empty());
    }

    #[test]
    fn test_digest_create_table_rejected() {
        let mut builder: DiffSetBuilder<PatchsetFormat, SimpleTable, String, Vec<u8>> =
            DiffSetBuilder::default();
        let result =
            builder.digest_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        assert!(result.is_err());
    }

    #[test]
    fn test_digest_blob_value() {
        let t = SimpleTable::new("t", &["data"], &[0]);
        let mut builder = make_builder(&[t]);
        builder
            .digest_sql("INSERT INTO t (data) VALUES (X'DEADBEEF')")
            .unwrap();
        assert_eq!(builder.len(), 1);
    }

    #[test]
    fn test_digest_null_value() {
        let t = SimpleTable::new("t", &["id", "v"], &[0]);
        let mut builder = make_builder(&[t]);
        builder
            .digest_sql("INSERT INTO t (id, v) VALUES (1, NULL)")
            .unwrap();
        assert_eq!(builder.len(), 1);
    }

    #[test]
    fn test_digest_negative_numbers() {
        let t = SimpleTable::new("t", &["a", "b"], &[0]);
        let mut builder = make_builder(&[t]);
        builder
            .digest_sql("INSERT INTO t (a, b) VALUES (-42, -3.14)")
            .unwrap();
        assert_eq!(builder.len(), 1);
    }
}
