//! Serializable merge semantics for composite CRDT values.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const SPEC_VERSION: u32 = 1;

fn default_version() -> u32 {
    SPEC_VERSION
}

fn register_policy() -> Box<CrdtPolicy> {
    Box::new(CrdtPolicy::Register {})
}

/// How an ordered sequence materializes into ordinary [`crate::Value`] data.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SequenceMaterialization {
    #[default]
    List,
    String,
}

/// Merge behavior for one node in a [`CrdtSpec`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CrdtPolicy {
    /// A deterministic last-writer-wins register.
    Register {},
    /// A recursively merged map. `fields` override `values`, which applies to all other keys.
    Map {
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        fields: BTreeMap<String, CrdtPolicy>,
        #[serde(default = "register_policy")]
        values: Box<CrdtPolicy>,
    },
    /// An add-wins observed-remove set. Each entry in `keys` is a path within a member value;
    /// multiple paths form a composite identity. An empty list identifies members by whole value.
    Set {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        keys: Vec<Vec<String>>,
        #[serde(default = "register_policy")]
        element: Box<CrdtPolicy>,
    },
    /// A stable-ID ordered sequence. Element identities are CRDT metadata, never list indexes.
    Sequence {
        #[serde(default)]
        materialization: SequenceMaterialization,
        #[serde(default = "register_policy")]
        element: Box<CrdtPolicy>,
    },
}

/// Versioned merge semantics for one model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrdtSpec {
    #[serde(default = "default_version")]
    pub version: u32,
    pub root: CrdtPolicy,
}

impl CrdtSpec {
    /// Parse and validate a specification.
    pub fn from_json(json: &str) -> Result<Self, String> {
        let mut spec: Self = serde_json::from_str(json).map_err(|error| error.to_string())?;
        spec.validate()?;
        normalize_policy(&mut spec.root);
        Ok(spec)
    }

    /// Serialize a validated specification in its deterministic canonical form.
    pub fn to_json(&self) -> Result<String, String> {
        self.validate()?;
        let mut spec = self.clone();
        normalize_policy(&mut spec.root);
        serde_json::to_string(&spec).map_err(|error| error.to_string())
    }

    /// SHA-256 of the canonical specification, including its version.
    pub fn hash(&self) -> Result<String, String> {
        let digest = Sha256::digest(self.to_json()?.as_bytes());
        Ok(format!("sha256:{digest:x}"))
    }

    /// Reject a peer using different merge semantics.
    pub fn require_hash(&self, peer_hash: &str) -> Result<(), String> {
        let local_hash = self.hash()?;
        if local_hash == peer_hash {
            Ok(())
        } else {
            Err(format!(
                "incompatible CRDT spec: local hash {local_hash}, peer hash {peer_hash}"
            ))
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.version != SPEC_VERSION {
            return Err(format!(
                "unsupported CRDT spec version {}; expected {SPEC_VERSION}",
                self.version
            ));
        }
        validate_policy(&self.root, "root")
    }
}

fn normalize_policy(policy: &mut CrdtPolicy) {
    match policy {
        CrdtPolicy::Register {} => {}
        CrdtPolicy::Map { fields, values } => {
            for child in fields.values_mut() {
                normalize_policy(child);
            }
            normalize_policy(values);
        }
        CrdtPolicy::Set { keys, element } => {
            keys.sort();
            normalize_policy(element);
        }
        CrdtPolicy::Sequence { element, .. } => normalize_policy(element),
    }
}

fn validate_policy(policy: &CrdtPolicy, path: &str) -> Result<(), String> {
    match policy {
        CrdtPolicy::Register {} => Ok(()),
        CrdtPolicy::Map { fields, values } => {
            for (field, child) in fields {
                if field.is_empty() {
                    return Err(format!("{path}.fields contains an empty field name"));
                }
                validate_policy(child, &format!("{path}.fields.{field}"))?;
            }
            validate_policy(values, &format!("{path}.values"))
        }
        CrdtPolicy::Set { keys, element } => {
            validate_policy(element, &format!("{path}.element"))?;
            let mut seen = BTreeSet::new();
            for key in keys {
                if key.is_empty() || key.iter().any(String::is_empty) {
                    return Err(format!(
                        "{path}.keys contains an empty identity path or path segment"
                    ));
                }
                if !seen.insert(key) {
                    return Err(format!("{path}.keys contains duplicate path {key:?}"));
                }
                validate_key_path(element, key, path)?;
            }
            Ok(())
        }
        CrdtPolicy::Sequence {
            materialization,
            element,
        } => {
            if *materialization == SequenceMaterialization::String
                && !matches!(element.as_ref(), CrdtPolicy::Register {})
            {
                return Err(format!(
                    "{path}.element must be a register for string materialization"
                ));
            }
            validate_policy(element, &format!("{path}.element"))
        }
    }
}

fn validate_key_path(element: &CrdtPolicy, key: &[String], path: &str) -> Result<(), String> {
    let mut current = element;
    for segment in key {
        current = match current {
            CrdtPolicy::Map { fields, values } => {
                fields.get(segment).unwrap_or_else(|| values.as_ref())
            }
            _ => {
                return Err(format!(
                    "{path}.keys path {key:?} is not reachable in the element policy"
                ));
            }
        };
    }
    if matches!(current, CrdtPolicy::Register {}) {
        Ok(())
    } else {
        Err(format!(
            "{path}.keys path {key:?} must resolve to a register"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPEC: &str = r#"{
        "root": {
            "kind": "map",
            "fields": {
                "document": {"kind": "sequence", "materialization": "string"},
                "rows": {
                    "kind": "set",
                    "keys": [["id"]],
                    "element": {
                        "kind": "map",
                        "fields": {"title": {"kind": "register"}}
                    }
                }
            },
            "values": {"kind": "register"}
        }
    }"#;

    #[test]
    fn canonicalizes_and_hashes_equivalent_specs() {
        let spec = CrdtSpec::from_json(SPEC).unwrap();
        let reordered = CrdtSpec::from_json(
            r#"{"version":1,"root":{"values":{"kind":"register"},"fields":{"rows":{"element":{"values":{"kind":"register"},"fields":{"title":{"kind":"register"}},"kind":"map"},"keys":[["id"]],"kind":"set"},"document":{"materialization":"string","kind":"sequence"}},"kind":"map"}}"#,
        )
        .unwrap();

        assert_eq!(spec.to_json().unwrap(), reordered.to_json().unwrap());
        assert_eq!(spec.hash().unwrap(), reordered.hash().unwrap());
        spec.require_hash(&reordered.hash().unwrap()).unwrap();
    }

    #[test]
    fn rejects_incompatible_hash() {
        let error = CrdtSpec::from_json(SPEC)
            .unwrap()
            .require_hash("sha256:other")
            .unwrap_err();
        assert!(error.contains("incompatible CRDT spec"));
    }

    #[test]
    fn rejects_unknown_version_and_invalid_paths() {
        assert!(
            CrdtSpec::from_json(r#"{"version":2,"root":{"kind":"register"}}"#)
                .unwrap_err()
                .contains("unsupported CRDT spec version")
        );
        assert!(CrdtSpec::from_json(
            r#"{"root":{"kind":"set","keys":[[""]],"element":{"kind":"register"}}}"#
        )
        .unwrap_err()
        .contains("empty identity path or path segment"));
    }

    #[test]
    fn rejects_unknown_register_fields() {
        assert!(
            CrdtSpec::from_json(r#"{"root":{"kind":"register","fields":{}}}"#)
                .unwrap_err()
                .contains("unknown field")
        );
    }

    #[test]
    fn map_values_default_to_register() {
        assert_eq!(
            CrdtSpec::from_json(r#"{"root":{"kind":"map"}}"#)
                .unwrap()
                .to_json()
                .unwrap(),
            r#"{"version":1,"root":{"kind":"map","values":{"kind":"register"}}}"#
        );
    }

    #[test]
    fn keyed_sets_require_unique_reachable_register_paths() {
        for invalid in [
            r#"{"root":{"kind":"set","keys":[["id"]],"element":{"kind":"register"}}}"#,
            r#"{"root":{"kind":"set","keys":[["id"],["id"]],"element":{"kind":"map"}}}"#,
            r#"{"root":{"kind":"set","keys":[["nested","id"]],"element":{"kind":"map"}}}"#,
            r#"{"root":{"kind":"set","keys":[["nested"]],"element":{"kind":"map","fields":{"nested":{"kind":"map"}}}}}"#,
        ] {
            assert!(CrdtSpec::from_json(invalid).is_err(), "accepted {invalid}");
        }

        CrdtSpec::from_json(
            r#"{"root":{"kind":"set","keys":[["tenant"],["meta","id"]],"element":{"kind":"map","fields":{"meta":{"kind":"map"}}}}}"#,
        )
        .unwrap();
    }

    #[test]
    fn canonicalization_is_idempotent() {
        let once = CrdtSpec::from_json(SPEC).unwrap().to_json().unwrap();
        let twice = CrdtSpec::from_json(&once).unwrap().to_json().unwrap();
        assert_eq!(once, twice);
    }

    #[test]
    fn composite_key_path_order_does_not_change_hash() {
        let first = CrdtSpec::from_json(
            r#"{"root":{"kind":"set","keys":[["tenant"],["meta","id"]],"element":{"kind":"map","fields":{"meta":{"kind":"map"}}}}}"#,
        )
        .unwrap();
        let reversed = CrdtSpec::from_json(
            r#"{"root":{"kind":"set","keys":[["meta","id"],["tenant"]],"element":{"kind":"map","fields":{"meta":{"kind":"map"}}}}}"#,
        )
        .unwrap();

        assert_eq!(first.to_json().unwrap(), reversed.to_json().unwrap());
        assert_eq!(first.hash().unwrap(), reversed.hash().unwrap());
    }

    #[test]
    fn string_sequences_require_register_elements() {
        let error = CrdtSpec::from_json(
            r#"{"root":{"kind":"sequence","materialization":"string","element":{"kind":"map"}}}"#,
        )
        .unwrap_err();
        assert!(error.contains("must be a register"));
    }
}
