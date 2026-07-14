//! JSON canonicalization helper for
//! [`JsonCanonicalDecoder`](crate::wire::JsonCanonicalDecoder).
//!
//! Recursively rebuilds a `serde_json::Value` with `BTreeMap`-ordered
//! keys (lexicographic on unicode scalars) and emits compact JSON via
//! `serde_json::to_string`. Arrays keep their positional order.

use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};

/// Recursively rebuild `value` with objects re-keyed lexicographically.
pub(crate) fn canonicalize(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let sorted: BTreeMap<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), canonicalize(v)))
                .collect();
            let mut out = serde_json::Map::new();
            for (k, v) in sorted {
                out.insert(k, v);
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(canonicalize).collect())
        }
        _ => value.clone(),
    }
}

/// Canonicalize `value` and emit compact JSON text.
///
/// # Errors
///
/// Returns the `serde_json::Error` from `to_string` if the value
/// cannot be serialized (extremely unlikely for a value that just
/// deserialized).
pub(crate) fn canonicalize_to_string(value: &serde_json::Value) -> Result<String, String> {
    let canon = canonicalize(value);
    serde_json::to_string(&canon).map_err(|e| e.to_string())
}

/// Parse `s` as JSON and canonicalize it. When `s` is not valid JSON,
/// falls back to returning it verbatim.
pub(crate) fn canonicalize_string(s: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(value) => match canonicalize_to_string(&value) {
            Ok(canon) => canon,
            Err(_) => s.to_string(),
        },
        Err(_) => s.to_string(),
    }
}

/// Serialize `value` verbatim (compact) without canonicalization.
///
/// # Errors
///
/// Returns the `serde_json::Error` message if serialization fails.
pub(crate) fn serialize_verbatim(value: &serde_json::Value) -> Result<String, String> {
    serde_json::to_string(value).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_sorts_top_level() {
        let src: serde_json::Value = serde_json::from_str("{\"z\":1,\"a\":2}").unwrap();
        assert_eq!(canonicalize_to_string(&src).unwrap(), "{\"a\":2,\"z\":1}");
    }

    #[test]
    fn canonical_recurses() {
        let src: serde_json::Value =
            serde_json::from_str("{\"z\":{\"b\":1,\"a\":2},\"a\":[3,4]}").unwrap();
        assert_eq!(
            canonicalize_to_string(&src).unwrap(),
            "{\"a\":[3,4],\"z\":{\"a\":2,\"b\":1}}"
        );
    }

    #[test]
    fn canonicalize_string_parses_and_reorders() {
        assert_eq!(
            canonicalize_string("{\"z\":1,\"a\":2}"),
            "{\"a\":2,\"z\":1}"
        );
    }

    #[test]
    fn canonicalize_string_falls_back_verbatim_on_invalid() {
        assert_eq!(canonicalize_string("not json"), "not json");
    }
}
