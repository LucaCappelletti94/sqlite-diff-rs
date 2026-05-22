//! Simplified SQL parser for changeset/patchset operations.
//!
//! A lightweight parser that handles only `INSERT`, `UPDATE`, and `DELETE`
//! statements, which is all the round-trip path needs. A full SQL parser like
//! `sqlparser` would be overkill.

mod lexer;
mod parser;

pub use parser::ParseError;
pub(crate) use parser::Parser;
