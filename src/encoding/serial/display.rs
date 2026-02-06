//! Display implementation for Value as SQL literals.

use super::Value;

impl core::fmt::Display for Value {
    /// Format a Value as a SQL literal.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Value::Integer(v) => write!(f, "{v}"),
            Value::Real(v) => {
                if v.is_nan() {
                    write!(f, "NULL")
                } else if v.is_infinite() {
                    if v.is_sign_positive() {
                        write!(f, "9e999") // SQLite's way of representing +infinity
                    } else {
                        write!(f, "-9e999")
                    }
                } else {
                    write!(f, "{v}")
                }
            }
            Value::Text(s) => {
                // Escape single quotes by doubling them
                write!(f, "'")?;
                for c in s.chars() {
                    if c == '\'' {
                        write!(f, "''")?;
                    } else {
                        core::fmt::Write::write_char(f, c)?;
                    }
                }
                write!(f, "'")
            }
            Value::Blob(b) => {
                write!(f, "X'")?;
                for byte in b {
                    write!(f, "{byte:02X}")?;
                }
                write!(f, "'")
            }
            Value::Null => write!(f, "NULL"),
        }
    }
}
