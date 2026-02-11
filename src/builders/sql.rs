//! Simplified SQL parser for changeset/patchset operations.
//!
//! This module provides a lightweight SQL parser that only handles the minimal
//! syntax needed for changeset/patchset operations:
//! - `INSERT` statements
//! - `UPDATE` statements
//! - `DELETE` statements
//!
//! This is intentionally limited compared to a full SQL parser like `sqlparser`,
//! as it only needs to handle the specific syntax used in changeset/patchset
//! round-trip operations.

mod lexer;
mod parser;

pub use parser::ParseError;
pub(crate) use parser::Parser;
