//! Model schemas and the per-process registry.
//!
//! A [`Schema`] describes a model type's fields; the [`Registry`] maps a registered type name to
//! its schema. This ports the prototype's `model_map` (class-name → type), which let the receiving
//! side reconstruct a model from a `model_type` hint on the wire. Validation is intentionally
//! lenient for Phase 0 — it checks shape, not exhaustive types.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::value::Value;

/// The declared type of a field. `Submodel` carries the referenced model's type name.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FieldType {
    Bool,
    Int,
    Float,
    Str,
    List,
    Map,
    Submodel(String),
    /// Escape hatch for fields whose type isn't pinned yet.
    Any,
}

/// One field of a model.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub ty: FieldType,
}

impl Field {
    pub fn new(name: impl Into<String>, ty: FieldType) -> Field {
        Field {
            name: name.into(),
            ty,
        }
    }
}

/// A model type's schema: a name plus an ordered list of fields.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    pub type_name: String,
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn new(type_name: impl Into<String>, fields: Vec<Field>) -> Schema {
        Schema {
            type_name: type_name.into(),
            fields,
        }
    }

    /// Lenient check that `value` is a map whose present fields have the right primitive shape.
    /// Missing fields are allowed (partial models); unknown fields are allowed (forward-compat).
    pub fn validate(&self, value: &Value) -> bool {
        let Value::Map(map) = value else { return false };
        self.fields.iter().all(|f| match map.get(&f.name) {
            None => true,
            Some(v) => matches!(
                (&f.ty, v),
                (FieldType::Any, _)
                    | (FieldType::Bool, Value::Bool(_))
                    | (FieldType::Int, Value::Int(_))
                    | (FieldType::Float, Value::Float(_))
                    | (FieldType::Str, Value::Str(_))
                    | (FieldType::List, Value::List(_))
                    | (FieldType::Map, Value::Map(_))
                    | (FieldType::Submodel(_), Value::Submodel(_))
            ),
        })
    }
}

/// Maps registered type names to schemas (the wire's source of truth for reconstruction).
#[derive(Clone, Debug, Default)]
pub struct Registry {
    schemas: BTreeMap<String, Schema>,
}

impl Registry {
    pub fn new() -> Registry {
        Registry::default()
    }

    /// Register (or replace) a schema by its type name.
    pub fn register(&mut self, schema: Schema) {
        self.schemas.insert(schema.type_name.clone(), schema);
    }

    pub fn get(&self, type_name: &str) -> Option<&Schema> {
        self.schemas.get(type_name)
    }

    pub fn contains(&self, type_name: &str) -> bool {
        self.schemas.contains_key(type_name)
    }

    pub fn len(&self) -> usize {
        self.schemas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    fn device_schema() -> Schema {
        Schema::new(
            "Device",
            vec![
                Field::new("name", FieldType::Str),
                Field::new("on", FieldType::Bool),
            ],
        )
    }

    #[test]
    fn test_registry() {
        let mut reg = Registry::new();
        assert!(reg.is_empty());
        reg.register(device_schema());
        assert!(reg.contains("Device"));
        assert_eq!(reg.get("Device").unwrap().fields.len(), 2);
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn test_validate() {
        let s = device_schema();
        assert!(s.validate(&Value::map([
            ("name", Value::from("lamp")),
            ("on", Value::from(true))
        ])));
        // partial (missing field) is allowed
        assert!(s.validate(&Value::map([("name", Value::from("lamp"))])));
        // wrong type is rejected
        assert!(!s.validate(&Value::map([("on", Value::from(1i64))])));
        // non-map is rejected
        assert!(!s.validate(&Value::from(5i64)));
    }
}
