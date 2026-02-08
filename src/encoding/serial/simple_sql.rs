//! Conversions between Value and SqlValue.

use alloc::string::String;
use alloc::vec::Vec;

use crate::encoding::Value;
use crate::sql::SqlValue;

impl From<SqlValue> for Value<String, Vec<u8>> {
    fn from(v: SqlValue) -> Self {
        match v {
            SqlValue::Null => Value::Null,
            SqlValue::Integer(i) => Value::Integer(i),
            SqlValue::Real(r) => Value::Real(r),
            SqlValue::Text(s) => Value::Text(s),
            SqlValue::Blob(b) => Value::Blob(b),
        }
    }
}

impl From<&SqlValue> for Value<String, Vec<u8>> {
    fn from(v: &SqlValue) -> Self {
        match v {
            SqlValue::Null => Value::Null,
            SqlValue::Integer(i) => Value::Integer(*i),
            SqlValue::Real(r) => Value::Real(*r),
            SqlValue::Text(s) => Value::Text(s.clone()),
            SqlValue::Blob(b) => Value::Blob(b.clone()),
        }
    }
}

impl<S: AsRef<str>, B: AsRef<[u8]>> From<&Value<S, B>> for SqlValue {
    fn from(v: &Value<S, B>) -> Self {
        match v {
            Value::Null => SqlValue::Null,
            Value::Integer(i) => SqlValue::Integer(*i),
            Value::Real(r) => SqlValue::Real(*r),
            Value::Text(s) => SqlValue::Text(s.as_ref().into()),
            Value::Blob(b) => SqlValue::Blob(b.as_ref().to_vec()),
        }
    }
}

impl<S: AsRef<str>, B: AsRef<[u8]>> From<Value<S, B>> for SqlValue {
    fn from(v: Value<S, B>) -> Self {
        (&v).into()
    }
}
