//! SQL lexer for tokenizing input.

use alloc::borrow::Cow;
use alloc::string::String;
use alloc::vec::Vec;

/// A token produced by the lexer.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Token<'input> {
    /// The kind of token.
    pub kind: TokenKind<'input>,
    /// The position in the input where this token starts.
    pub pos: usize,
}

/// The different kinds of tokens.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum TokenKind<'input> {
    /// INSERT keyword
    Insert,
    /// INTO keyword
    Into,
    /// VALUES keyword
    Values,
    /// UPDATE keyword
    Update,
    /// SET keyword
    Set,
    /// DELETE keyword
    Delete,
    /// FROM keyword
    From,
    /// WHERE keyword
    Where,
    /// AND keyword
    And,
    /// PRIMARY keyword
    Primary,
    /// KEY keyword
    Key,
    /// NULL keyword
    Null,
    /// INTEGER keyword
    Integer,
    /// INT keyword
    Int,
    /// REAL keyword
    Real,
    /// TEXT keyword
    Text,
    /// BLOB keyword
    Blob,
    /// NOT keyword
    Not,

    // Literals
    /// Integer literal
    IntegerLiteral(i64),
    /// Real/float literal
    RealLiteral(f64),
    /// String literal (single or double quoted)
    StringLiteral(Cow<'input, str>),
    /// Blob literal (X'...')
    BlobLiteral(Vec<u8>),

    // Identifiers
    /// An identifier (table name, column name, etc.)
    Identifier(&'input str),

    // Symbols
    /// Left parenthesis
    LParen,
    /// Right parenthesis
    RParen,
    /// Comma
    Comma,
    /// Semicolon
    Semicolon,
    /// Equals sign
    Equals,
    /// Minus sign
    Minus,

    // Special
    /// End of input
    Eof,
}

impl<'input> TokenKind<'input> {
    /// Returns a `'static` descriptive name for this token kind.
    ///
    /// Unlike `AsRef<str>`, this never borrows from the input and is
    /// safe to store in error types with `'static` lifetime.
    pub(super) fn static_name(&self) -> &'static str {
        match self {
            TokenKind::Insert => "INSERT",
            TokenKind::Into => "INTO",
            TokenKind::Values => "VALUES",
            TokenKind::Update => "UPDATE",
            TokenKind::Set => "SET",
            TokenKind::Delete => "DELETE",
            TokenKind::From => "FROM",
            TokenKind::Where => "WHERE",
            TokenKind::And => "AND",
            TokenKind::Primary => "PRIMARY",
            TokenKind::Key => "KEY",
            TokenKind::Null => "NULL",
            TokenKind::Integer => "INTEGER",
            TokenKind::Int => "INT",
            TokenKind::Real => "REAL",
            TokenKind::Text => "TEXT",
            TokenKind::Blob => "BLOB",
            TokenKind::Not => "NOT",
            TokenKind::IntegerLiteral(_) => "<integer>",
            TokenKind::RealLiteral(_) => "<real>",
            TokenKind::StringLiteral(_) => "<string>",
            TokenKind::BlobLiteral(_) => "<blob>",
            TokenKind::Identifier(_) => "<identifier>",
            TokenKind::LParen => "(",
            TokenKind::RParen => ")",
            TokenKind::Comma => ",",
            TokenKind::Semicolon => ";",
            TokenKind::Equals => "=",
            TokenKind::Minus => "-",
            TokenKind::Eof => "<eof>",
        }
    }
}

impl<'input> AsRef<str> for TokenKind<'input> {
    fn as_ref(&self) -> &str {
        match self {
            TokenKind::Insert => "INSERT",
            TokenKind::Into => "INTO",
            TokenKind::Values => "VALUES",
            TokenKind::Update => "UPDATE",
            TokenKind::Set => "SET",
            TokenKind::Delete => "DELETE",
            TokenKind::From => "FROM",
            TokenKind::Where => "WHERE",
            TokenKind::And => "AND",
            TokenKind::Primary => "PRIMARY",
            TokenKind::Key => "KEY",
            TokenKind::Null => "NULL",
            TokenKind::Integer => "INTEGER",
            TokenKind::Int => "INT",
            TokenKind::Real => "REAL",
            TokenKind::Text => "TEXT",
            TokenKind::Blob => "BLOB",
            TokenKind::Not => "NOT",
            TokenKind::IntegerLiteral(_) => "<integer>",
            TokenKind::RealLiteral(_) => "<real>",
            TokenKind::StringLiteral(s) => s.as_ref(),
            TokenKind::BlobLiteral(_) => "<blob>",
            TokenKind::Identifier(s) => s,
            TokenKind::LParen => "(",
            TokenKind::RParen => ")",
            TokenKind::Comma => ",",
            TokenKind::Semicolon => ";",
            TokenKind::Equals => "=",
            TokenKind::Minus => "-",
            TokenKind::Eof => "<eof>",
        }
    }
}

/// SQL lexer that produces tokens from input.
pub struct Lexer<'input> {
    pub(super) input: &'input str,
    pos: usize,
    peeked: Option<Token<'input>>,
}

impl<'input> Lexer<'input> {
    /// Create a new lexer for the given input.
    #[must_use]
    pub(super) fn new(input: &'input str) -> Self {
        Self {
            input,
            pos: 0,
            peeked: None,
        }
    }

    /// Peek at the next token without consuming it.
    pub fn peek<'b>(&'b mut self) -> Result<&'b Token<'input>, LexerError>
    {
        if self.peeked.is_none() {
            self.peeked = Some(self.next_token()?);
        }
        Ok(self.peeked.as_ref().unwrap())
    }

    /// Consume and return the next token.
    pub fn next(&mut self) -> Result<Token<'input>, LexerError> {
        if let Some(token) = self.peeked.take() {
            return Ok(token);
        }
        self.next_token()
    }

    /// Skip whitespace and comments.
    fn skip_whitespace(&mut self) {
        let bytes = self.input.as_bytes();
        while self.pos < bytes.len() {
            let b = bytes[self.pos];
            if b.is_ascii_whitespace() {
                self.pos += 1;
            } else if b == b'-' && self.pos + 1 < bytes.len() && bytes[self.pos + 1] == b'-' {
                // Line comment
                self.pos += 2;
                while self.pos < bytes.len() && bytes[self.pos] != b'\n' {
                    self.pos += 1;
                }
            } else if b == b'/' && self.pos + 1 < bytes.len() && bytes[self.pos + 1] == b'*' {
                // Block comment
                self.pos += 2;
                while self.pos + 1 < bytes.len()
                    && !(bytes[self.pos] == b'*' && bytes[self.pos + 1] == b'/')
                {
                    self.pos += 1;
                }
                if self.pos + 1 < bytes.len() {
                    self.pos += 2;
                }
            } else {
                break;
            }
        }
    }

    fn next_token(&mut self) -> Result<Token<'input>, LexerError> {
        self.skip_whitespace();

        let start_pos = self.pos;
        let bytes = self.input.as_bytes();

        if self.pos >= bytes.len() {
            return Ok(Token {
                kind: TokenKind::Eof,
                pos: start_pos,
            });
        }

        let b = bytes[self.pos];

        // Single-character symbols
        let kind = match b {
            b'(' => {
                self.pos += 1;
                TokenKind::LParen
            }
            b')' => {
                self.pos += 1;
                TokenKind::RParen
            }
            b',' => {
                self.pos += 1;
                TokenKind::Comma
            }
            b';' => {
                self.pos += 1;
                TokenKind::Semicolon
            }
            b'=' => {
                self.pos += 1;
                TokenKind::Equals
            }
            b'-' => {
                self.pos += 1;
                TokenKind::Minus
            }
            b'\'' | b'"' => return self.read_string(start_pos),
            b'X' | b'x' if self.pos + 1 < bytes.len() && bytes[self.pos + 1] == b'\'' => {
                return self.read_blob(start_pos);
            }
            _ if b.is_ascii_digit() => return self.read_number(start_pos),
            _ if is_ident_start(b) => return self.read_identifier(start_pos),
            _ => {
                return Err(LexerError::UnexpectedChar {
                    char: b as char,
                    pos: start_pos,
                });
            }
        };

        Ok(Token {
            kind,
            pos: start_pos,
        })
    }

    fn read_string(&mut self, start_pos: usize) -> Result<Token<'input>, LexerError> {
        let bytes = self.input.as_bytes();
        let quote = bytes[self.pos];
        self.pos += 1;

        let start = self.pos;
        let mut has_escape = false;
        while self.pos < bytes.len() {
            let b = bytes[self.pos];
            if b == quote {
                // Check for escaped quote (doubled)
                if self.pos + 1 < bytes.len() && bytes[self.pos + 1] == quote {
                    has_escape = true;
                    self.pos += 2;
                } else {
                    let raw = &self.input[start..self.pos];
                    self.pos += 1;
                    let value = if has_escape {
                        let q = quote as char;
                        let doubled = alloc::format!("{q}{q}");
                        Cow::Owned(raw.replace(&doubled, &alloc::format!("{q}")))
                    } else {
                        Cow::Borrowed(raw)
                    };
                    return Ok(Token {
                        kind: TokenKind::StringLiteral(value),
                        pos: start_pos,
                    });
                }
            } else {
                self.pos += 1;
            }
        }

        Err(LexerError::UnterminatedString { pos: start_pos })
    }

    fn read_blob(&mut self, start_pos: usize) -> Result<Token<'input>, LexerError> {
        let bytes = self.input.as_bytes();
        self.pos += 2; // Skip X'

        let hex_start = self.pos;
        while self.pos < bytes.len() && bytes[self.pos] != b'\'' {
            let b = bytes[self.pos];
            if !b.is_ascii_hexdigit() {
                return Err(LexerError::InvalidHexDigit {
                    char: b as char,
                    pos: self.pos,
                });
            }
            self.pos += 1;
        }

        if self.pos >= bytes.len() {
            return Err(LexerError::UnterminatedBlob { pos: start_pos });
        }

        let hex_str = &self.input[hex_start..self.pos];
        self.pos += 1; // Skip closing quote

        // Pad with leading zero if odd length
        let padded = if hex_str.len() % 2 == 1 {
            alloc::format!("0{hex_str}")
        } else {
            hex_str.to_string()
        };

        let blob: Result<Vec<u8>, _> = (0..padded.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&padded[i..i + 2], 16))
            .collect();

        match blob {
            Ok(b) => Ok(Token {
                kind: TokenKind::BlobLiteral(b),
                pos: start_pos,
            }),
            Err(_) => Err(LexerError::InvalidHexString { pos: start_pos }),
        }
    }

    fn read_number(&mut self, start_pos: usize) -> Result<Token<'input>, LexerError> {
        let bytes = self.input.as_bytes();
        let num_start = self.pos;

        // Read integer part
        while self.pos < bytes.len() && bytes[self.pos].is_ascii_digit() {
            self.pos += 1;
        }

        // Check for decimal point
        let mut is_real = false;
        if self.pos < bytes.len() && bytes[self.pos] == b'.' {
            is_real = true;
            self.pos += 1;
            while self.pos < bytes.len() && bytes[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }

        // Check for exponent
        if self.pos < bytes.len() && (bytes[self.pos] == b'e' || bytes[self.pos] == b'E') {
            is_real = true;
            self.pos += 1;
            if self.pos < bytes.len() && (bytes[self.pos] == b'+' || bytes[self.pos] == b'-') {
                self.pos += 1;
            }
            while self.pos < bytes.len() && bytes[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }

        let num_str = &self.input[num_start..self.pos];

        if is_real {
            match num_str.parse::<f64>() {
                Ok(v) => Ok(Token {
                    kind: TokenKind::RealLiteral(v),
                    pos: start_pos,
                }),
                Err(_) => Err(LexerError::InvalidNumber {
                    value: num_str.into(),
                    pos: start_pos,
                }),
            }
        } else {
            match num_str.parse::<i64>() {
                Ok(v) => Ok(Token {
                    kind: TokenKind::IntegerLiteral(v),
                    pos: start_pos,
                }),
                Err(_) => {
                    // Try as f64 if too large for i64
                    match num_str.parse::<f64>() {
                        Ok(v) => Ok(Token {
                            kind: TokenKind::RealLiteral(v),
                            pos: start_pos,
                        }),
                        Err(_) => Err(LexerError::InvalidNumber {
                            value: num_str.into(),
                            pos: start_pos,
                        }),
                    }
                }
            }
        }
    }

    fn read_identifier(&mut self, start_pos: usize) -> Result<Token<'input>, LexerError> {
        let bytes = self.input.as_bytes();
        let ident_start = self.pos;

        while self.pos < bytes.len() && is_ident_cont(bytes[self.pos]) {
            self.pos += 1;
        }

        let ident = &self.input[ident_start..self.pos];
        let kind = match ident.to_uppercase().as_str() {
            "INSERT" => TokenKind::Insert,
            "INTO" => TokenKind::Into,
            "VALUES" => TokenKind::Values,
            "UPDATE" => TokenKind::Update,
            "SET" => TokenKind::Set,
            "DELETE" => TokenKind::Delete,
            "FROM" => TokenKind::From,
            "WHERE" => TokenKind::Where,
            "AND" => TokenKind::And,
            "PRIMARY" => TokenKind::Primary,
            "KEY" => TokenKind::Key,
            "NULL" => TokenKind::Null,
            "INTEGER" => TokenKind::Integer,
            "INT" => TokenKind::Int,
            "REAL" => TokenKind::Real,
            "TEXT" => TokenKind::Text,
            "BLOB" => TokenKind::Blob,
            "NOT" => TokenKind::Not,
            _ => TokenKind::Identifier(ident),
        };

        Ok(Token {
            kind,
            pos: start_pos,
        })
    }
}

/// Check if a byte can start an identifier.
fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

/// Check if a byte can continue an identifier.
fn is_ident_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

use alloc::string::ToString;

/// Errors that can occur during lexing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LexerError {
    /// Unexpected character in input.
    #[error("Unexpected character '{char}' at position {pos}")]
    UnexpectedChar {
        /// The unexpected character.
        char: char,
        /// Position in input.
        pos: usize,
    },
    /// Unterminated string literal.
    #[error("Unterminated string literal starting at position {pos}")]
    UnterminatedString {
        /// Position where string started.
        pos: usize,
    },
    /// Unterminated blob literal.
    #[error("Unterminated blob literal starting at position {pos}")]
    UnterminatedBlob {
        /// Position where blob started.
        pos: usize,
    },
    /// Invalid hex digit in blob.
    #[error("Invalid hex digit '{char}' at position {pos}")]
    InvalidHexDigit {
        /// The invalid character.
        char: char,
        /// Position in input.
        pos: usize,
    },
    /// Invalid hex string.
    #[error("Invalid hex string at position {pos}")]
    InvalidHexString {
        /// Position where hex string started.
        pos: usize,
    },
    /// Invalid number format.
    #[error("Invalid number '{value}' at position {pos}")]
    InvalidNumber {
        /// The invalid number string.
        value: String,
        /// Position in input.
        pos: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_identifiers() {
        let mut lexer = Lexer::new("users my_table _private");
        assert_eq!(
            lexer.next().unwrap().kind,
            TokenKind::Identifier("users".into())
        );
        assert_eq!(
            lexer.next().unwrap().kind,
            TokenKind::Identifier("my_table".into())
        );
        assert_eq!(
            lexer.next().unwrap().kind,
            TokenKind::Identifier("_private".into())
        );
    }

    #[test]
    fn test_numbers() {
        let mut lexer = Lexer::new("42 -100 3.14 1e10");
        assert_eq!(lexer.next().unwrap().kind, TokenKind::IntegerLiteral(42));
        assert_eq!(lexer.next().unwrap().kind, TokenKind::Minus);
        assert_eq!(lexer.next().unwrap().kind, TokenKind::IntegerLiteral(100));
        assert_eq!(lexer.next().unwrap().kind, TokenKind::RealLiteral(3.14));
        assert_eq!(lexer.next().unwrap().kind, TokenKind::RealLiteral(1e10));
    }

    #[test]
    fn test_strings() {
        let mut lexer = Lexer::new("'hello' \"world\" 'it''s'");
        assert_eq!(
            lexer.next().unwrap().kind,
            TokenKind::StringLiteral("hello".into())
        );
        assert_eq!(
            lexer.next().unwrap().kind,
            TokenKind::StringLiteral("world".into())
        );
        assert_eq!(
            lexer.next().unwrap().kind,
            TokenKind::StringLiteral("it's".into())
        );
    }

    #[test]
    fn test_blob() {
        let mut lexer = Lexer::new("X'DEADBEEF'");
        assert_eq!(
            lexer.next().unwrap().kind,
            TokenKind::BlobLiteral(vec![0xDE, 0xAD, 0xBE, 0xEF])
        );
    }

    #[test]
    fn test_symbols() {
        let mut lexer = Lexer::new("(),;=");
        assert_eq!(lexer.next().unwrap().kind, TokenKind::LParen);
        assert_eq!(lexer.next().unwrap().kind, TokenKind::RParen);
        assert_eq!(lexer.next().unwrap().kind, TokenKind::Comma);
        assert_eq!(lexer.next().unwrap().kind, TokenKind::Semicolon);
        assert_eq!(lexer.next().unwrap().kind, TokenKind::Equals);
    }
}
