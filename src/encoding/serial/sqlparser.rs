//! sqlparser AST conversion for Value.

use alloc::string::ToString;
use alloc::vec::Vec;

use sqlparser::ast::{self, Expr};
use sqlparser::tokenizer::Span;

use crate::encoding::Value;
use crate::errors::ValueConversionError;

impl TryFrom<&Expr> for Value {
    type Error = ValueConversionError;

    fn try_from(expr: &Expr) -> Result<Self, Self::Error> {
        match expr {
            Expr::Value(val_with_span) => match &val_with_span.value {
                ast::Value::Null => Ok(Value::Null),
                ast::Value::Number(s, _) => {
                    // Try parsing as integer first, then as float
                    if let Ok(i) = s.parse::<i64>() {
                        Ok(Value::Integer(i))
                    } else if let Ok(f) = s.parse::<f64>() {
                        Ok(Value::Real(f))
                    } else {
                        Err(ValueConversionError::InvalidNumber(s.clone()))
                    }
                }
                ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                    Ok(Value::Text(s.clone()))
                }
                ast::Value::HexStringLiteral(hex) => {
                    // Parse hex string like "DEADBEEF" to bytes
                    let bytes: Result<Vec<u8>, _> = (0..hex.len())
                        .step_by(2)
                        .map(|i| {
                            u8::from_str_radix(&hex[i..i.min(hex.len()) + 2.min(hex.len() - i)], 16)
                        })
                        .collect();
                    match bytes {
                        Ok(b) => Ok(Value::Blob(b)),
                        Err(_) => Err(ValueConversionError::InvalidHexString(hex.clone())),
                    }
                }
                other => Err(ValueConversionError::UnsupportedExpression(alloc::format!(
                    "{other:?}"
                ))),
            },
            Expr::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr,
            } => {
                // Handle negative numbers like -123
                if let Expr::Value(val_with_span) = expr.as_ref()
                    && let ast::Value::Number(s, _) = &val_with_span.value
                {
                    let neg_s = alloc::format!("-{s}");
                    if let Ok(i) = neg_s.parse::<i64>() {
                        return Ok(Value::Integer(i));
                    } else if let Ok(f) = neg_s.parse::<f64>() {
                        return Ok(Value::Real(f));
                    }
                }
                Err(ValueConversionError::UnsupportedExpression(alloc::format!(
                    "{expr:?}"
                )))
            }
            other => Err(ValueConversionError::UnsupportedExpression(alloc::format!(
                "{other:?}"
            ))),
        }
    }
}

impl TryFrom<Expr> for Value {
    type Error = ValueConversionError;

    fn try_from(expr: Expr) -> Result<Self, Self::Error> {
        Self::try_from(&expr)
    }
}

impl From<&Value> for Expr {
    fn from(value: &Value) -> Self {
        let sql_value = match value {
            Value::Integer(v) => ast::Value::Number(v.to_string(), false),
            Value::Real(v) => {
                if v.is_nan() {
                    ast::Value::Null
                } else if v.is_infinite() {
                    if v.is_sign_positive() {
                        ast::Value::Number("9e999".to_string(), false)
                    } else {
                        ast::Value::Number("-9e999".to_string(), false)
                    }
                } else {
                    ast::Value::Number(v.to_string(), false)
                }
            }
            Value::Text(s) => ast::Value::SingleQuotedString(s.clone()),
            Value::Blob(b) => {
                let mut hex = alloc::string::String::new();
                for byte in b {
                    use core::fmt::Write;
                    write!(hex, "{byte:02X}").unwrap();
                }
                ast::Value::HexStringLiteral(hex)
            }
            Value::Null | Value::Undefined => ast::Value::Null,
        };
        Expr::Value(ast::ValueWithSpan {
            value: sql_value,
            span: Span::empty(),
        })
    }
}
