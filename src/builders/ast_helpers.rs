//! Shared AST helper functions for sqlparser integration.
//!
//! This module provides common utilities used across operation builders
//! for constructing and parsing sqlparser AST nodes.

use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use sqlparser::ast::{self, Expr, Ident, ObjectName, TableFactor};

use crate::encoding::Value;
use crate::errors::ValueConversionError;

/// Create an [`ObjectName`] from a table name string.
///
/// This creates a simple unqualified table name suitable for SQL statements.
#[must_use]
pub fn make_object_name(name: &str) -> ObjectName {
    ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(name))])
}

/// Create a simple [`TableFactor`] for FROM/UPDATE clauses.
///
/// This creates a basic table reference with no alias, hints, or other options.
#[must_use]
pub fn make_table_factor(name: &str) -> TableFactor {
    TableFactor::Table {
        name: make_object_name(name),
        alias: None,
        args: None,
        with_hints: vec![],
        version: None,
        with_ordinality: false,
        partitions: vec![],
        json_path: None,
        sample: None,
        index_hints: vec![],
    }
}

/// Extract column = value pairs from a WHERE clause.
///
/// Handles simple cases like `col1 = val1 AND col2 = val2`.
///
/// # Arguments
///
/// * `expr` - The WHERE clause expression to parse.
/// * `make_err` - A closure to create the appropriate error type for failures.
///
/// # Errors
///
/// Returns an error created by `make_err` if:
/// - The left side of `=` is not an identifier
/// - A value expression cannot be converted
/// - The WHERE clause uses unsupported operators or structures
pub fn extract_where_conditions<E>(
    expr: &Expr,
    make_err: impl Fn(String) -> E + Copy,
) -> Result<Vec<(String, Value)>, E>
where
    E: From<ValueConversionError>,
{
    match expr {
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            let col_name = match left.as_ref() {
                Expr::Identifier(ident) => ident.value.clone(),
                _ => {
                    return Err(make_err("Left side of = is not an identifier".into()));
                }
            };
            let value = Value::try_from(right.as_ref())?;
            Ok(vec![(col_name, value)])
        }
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            let mut conditions = extract_where_conditions(left, make_err)?;
            conditions.extend(extract_where_conditions(right, make_err)?);
            Ok(conditions)
        }
        _ => Err(make_err(alloc::format!(
            "Unsupported WHERE expression: {expr:?}"
        ))),
    }
}

/// Extract the table name from a sqlparser [`ObjectName`].
///
/// Returns an empty string if the name has no parts.
#[must_use]
pub fn extract_table_name(name: &ObjectName) -> &str {
    name.0.last().map_or("", |part| match part {
        ast::ObjectNamePart::Identifier(ident) => ident.value.as_str(),
        ast::ObjectNamePart::Function(func) => func.name.value.as_str(),
    })
}
