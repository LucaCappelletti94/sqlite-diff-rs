//! Display implementation for Value as SQL literals.

use super::Value;

impl<S: AsRef<str>, B: AsRef<[u8]>> core::fmt::Display for Value<S, B> {
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
                for c in s.as_ref().chars() {
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
                for byte in b.as_ref() {
                    write!(f, "{byte:02X}")?;
                }
                write!(f, "'")
            }
            Value::Null => write!(f, "NULL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Value;
    use alloc::format;
    use alloc::string::String;
    use alloc::vec;
    use alloc::vec::Vec;

    type TestValue = Value<String, Vec<u8>>;

    #[test]
    fn test_display_null() {
        let v: TestValue = Value::Null;
        assert_eq!(format!("{v}"), "NULL");
    }

    #[test]
    fn test_display_integer() {
        let v: TestValue = Value::Integer(42);
        assert_eq!(format!("{v}"), "42");

        let v: TestValue = Value::Integer(-1);
        assert_eq!(format!("{v}"), "-1");

        let v: TestValue = Value::Integer(i64::MAX);
        assert_eq!(format!("{v}"), "9223372036854775807");

        let v: TestValue = Value::Integer(i64::MIN);
        assert_eq!(format!("{v}"), "-9223372036854775808");
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_display_real_finite() {
        let v: TestValue = Value::Real(3.14);
        assert_eq!(format!("{v}"), "3.14");

        let v: TestValue = Value::Real(-0.0);
        let s = format!("{v}");
        assert!(s == "-0" || s == "0", "got {s}");
    }

    #[test]
    fn test_display_real_nan_becomes_null() {
        let v: TestValue = Value::Real(f64::NAN);
        assert_eq!(format!("{v}"), "NULL");
    }

    #[test]
    fn test_display_real_positive_infinity() {
        let v: TestValue = Value::Real(f64::INFINITY);
        assert_eq!(format!("{v}"), "9e999");
    }

    #[test]
    fn test_display_real_negative_infinity() {
        let v: TestValue = Value::Real(f64::NEG_INFINITY);
        assert_eq!(format!("{v}"), "-9e999");
    }

    #[test]
    fn test_display_text_plain() {
        let v: TestValue = Value::Text("hello".into());
        assert_eq!(format!("{v}"), "'hello'");
    }

    #[test]
    fn test_display_text_empty() {
        let v: TestValue = Value::Text(String::new());
        assert_eq!(format!("{v}"), "''");
    }

    #[test]
    fn test_display_text_escapes_single_quote() {
        let v: TestValue = Value::Text("it's".into());
        assert_eq!(format!("{v}"), "'it''s'");
    }

    #[test]
    fn test_display_text_escapes_multiple_quotes() {
        let v: TestValue = Value::Text("''".into());
        assert_eq!(format!("{v}"), "''''''");
    }

    #[test]
    fn test_display_blob_uppercase_hex() {
        let v: TestValue = Value::Blob(vec![0x01, 0xAB, 0xFF]);
        assert_eq!(format!("{v}"), "X'01ABFF'");
    }

    #[test]
    fn test_display_blob_empty() {
        let v: TestValue = Value::Blob(Vec::new());
        assert_eq!(format!("{v}"), "X''");
    }
}
