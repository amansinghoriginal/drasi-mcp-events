//! Canonical JSON used for subscription-key params equality (design sketch
//! §Subscription Identity: "params is compared by canonical-JSON equality").
//!
//! The sketch does not define "canonical JSON"; this implementation uses
//! recursive lexicographic key sort with compact separators. Numbers are
//! rendered as serde_json renders them (no float normalization); array order
//! is significant.

use serde_json::Value;

pub fn canonical_json(v: &Value) -> String {
    let mut out = String::new();
    write_canonical(v, &mut out);
    out
}

fn write_canonical(v: &Value, out: &mut String) {
    match v {
        Value::Object(map) => {
            let mut pairs: Vec<(&String, &Value)> = map.iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            out.push('{');
            for (i, (k, val)) in pairs.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                // Value::String's Display performs JSON string escaping.
                out.push_str(&Value::String((*k).clone()).to_string());
                out.push(':');
                write_canonical(val, out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sorts_keys_recursively_no_whitespace() {
        let v = json!({"b": 1, "a": {"d": 2, "c": [3, {"z": 0, "y": 1}]}});
        assert_eq!(
            canonical_json(&v),
            r#"{"a":{"c":[3,{"y":1,"z":0}],"d":2},"b":1}"#
        );
    }

    #[test]
    fn scalars_render_as_compact_json() {
        assert_eq!(canonical_json(&json!(null)), "null");
        assert_eq!(canonical_json(&json!(true)), "true");
        assert_eq!(canonical_json(&json!(42)), "42");
        assert_eq!(canonical_json(&json!(-1.5)), "-1.5");
        assert_eq!(canonical_json(&json!("a\"b\n")), r#""a\"b\n""#);
        assert_eq!(canonical_json(&json!({})), "{}");
        assert_eq!(canonical_json(&json!([])), "[]");
    }

    #[test]
    fn array_order_is_significant() {
        assert_ne!(canonical_json(&json!([1, 2])), canonical_json(&json!([2, 1])));
    }

    #[test]
    fn semantically_equal_objects_canonicalize_identically() {
        let a: Value = serde_json::from_str(r#"{"severity":"P1","service":"db"}"#).unwrap();
        let b: Value = serde_json::from_str(r#"{ "service" : "db", "severity": "P1" }"#).unwrap();
        assert_eq!(canonical_json(&a), canonical_json(&b));
    }

    #[test]
    fn escapes_keys() {
        let v = json!({"a\"b": 1});
        assert_eq!(canonical_json(&v), r#"{"a\"b":1}"#);
    }
}
