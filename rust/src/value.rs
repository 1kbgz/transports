//! The typed value representation that travels the wire.
//!
//! A model instance is a [`Value`] (typically a [`Value::Map`] of fields). Nested models are
//! referenced by [`ModelId`] rather than inlined ([`Value::Submodel`]), which ports the prototype's
//! recursive transport attachment: a model graph is a flat registry of values linked by id.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Stable per-instance identity for a hosted model (and any nested submodels).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ModelId(pub u64);

impl std::fmt::Display for ModelId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A typed value. Maps use `BTreeMap` so serialization and diffing are deterministic.
///
/// `PartialEq` (not `Eq`) because of `Float(f64)`.
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    List(Vec<Value>),
    Map(BTreeMap<String, Value>),
    /// A reference to a nested model held elsewhere in the registry.
    Submodel(ModelId),
}

impl Value {
    /// Borrow as a map, panicking if this value is not a map. Used by [`crate::apply`] where the
    /// patch path guarantees the type.
    pub fn as_map_mut(&mut self) -> &mut BTreeMap<String, Value> {
        match self {
            Value::Map(m) => m,
            other => panic!("expected Value::Map, found {other:?}"),
        }
    }

    /// Borrow as a list, panicking if this value is not a list.
    pub fn as_list_mut(&mut self) -> &mut Vec<Value> {
        match self {
            Value::List(l) => l,
            other => panic!("expected Value::List, found {other:?}"),
        }
    }

    /// Build a map value from `(key, value)` pairs.
    pub fn map<K: Into<String>, I: IntoIterator<Item = (K, Value)>>(entries: I) -> Value {
        Value::Map(entries.into_iter().map(|(k, v)| (k.into(), v)).collect())
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Value {
        Value::Bool(v)
    }
}
impl From<i64> for Value {
    fn from(v: i64) -> Value {
        Value::Int(v)
    }
}
impl From<i32> for Value {
    fn from(v: i32) -> Value {
        Value::Int(v as i64)
    }
}
impl From<f64> for Value {
    fn from(v: f64) -> Value {
        Value::Float(v)
    }
}
impl From<&str> for Value {
    fn from(v: &str) -> Value {
        Value::Str(v.to_string())
    }
}
impl From<String> for Value {
    fn from(v: String) -> Value {
        Value::Str(v)
    }
}
impl From<ModelId> for Value {
    fn from(v: ModelId) -> Value {
        Value::Submodel(v)
    }
}
impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(v: Vec<T>) -> Value {
        Value::List(v.into_iter().map(Into::into).collect())
    }
}

/*********************************/
#[cfg(test)]
mod value_tests {
    use super::*;

    #[test]
    fn test_from_and_map() {
        let v = Value::map([
            ("on", Value::from(true)),
            ("n", Value::from(3i64)),
            ("ref", Value::from(ModelId(7))),
        ]);
        if let Value::Map(m) = &v {
            assert_eq!(m.get("on"), Some(&Value::Bool(true)));
            assert_eq!(m.get("ref"), Some(&Value::Submodel(ModelId(7))));
        } else {
            panic!("not a map");
        }
    }

    #[test]
    fn test_json_round_trip() {
        let v = Value::map([
            ("list", Value::from(vec!["a", "b"])),
            ("sub", Value::Submodel(ModelId(42))),
        ]);
        let json = serde_json::to_string(&v).unwrap();
        let back: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }
}
