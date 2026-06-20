//! JSON string facade for the language bindings.
//!
//! The PyO3 and wasm bindings are kept maximally thin by routing everything through these helpers,
//! so all (de)serialization lives in the core and both languages exchange byte-identical wire data.
//! This is the concrete "one core, two bindings": a patch produced by [`diff_json`] in Python is
//! consumed by [`apply_json`] in the browser using the same Rust.

use crate::codec::{codec_for, Codec, JsonCodec};
use crate::diff::{apply, diff, Patch};
use crate::store::Store;
use crate::value::{ModelId, Value};

/// Diff two JSON-encoded models, returning the JSON-encoded patch.
pub fn diff_json(old: &str, new: &str) -> Result<String, String> {
    let old: Value = serde_json::from_str(old).map_err(|e| e.to_string())?;
    let new: Value = serde_json::from_str(new).map_err(|e| e.to_string())?;
    serde_json::to_string(&diff(&old, &new)).map_err(|e| e.to_string())
}

/// Apply a JSON-encoded patch to a JSON-encoded model, returning the JSON-encoded result.
pub fn apply_json(value: &str, patch: &str) -> Result<String, String> {
    let mut value: Value = serde_json::from_str(value).map_err(|e| e.to_string())?;
    let patch: Patch = serde_json::from_str(patch).map_err(|e| e.to_string())?;
    apply(&mut value, &patch);
    serde_json::to_string(&value).map_err(|e| e.to_string())
}

/// Encode a JSON-encoded model to codec bytes (JSON codec ⇒ canonical JSON bytes).
pub fn encode_json(value: &str) -> Result<Vec<u8>, String> {
    let value: Value = serde_json::from_str(value).map_err(|e| e.to_string())?;
    Ok(JsonCodec.encode_value(&value))
}

/// Decode codec bytes back to a JSON-encoded model string.
pub fn decode_json(bytes: &[u8]) -> Result<String, String> {
    let value = JsonCodec.decode_value(bytes).map_err(|e| e.to_string())?;
    serde_json::to_string(&value).map_err(|e| e.to_string())
}

/// Encode a JSON-encoded model to bytes using the codec named by `content_type`
/// (`"application/json"`, `"application/msgpack"`, ...).
pub fn encode_as(value: &str, content_type: &str) -> Result<Vec<u8>, String> {
    let value: Value = serde_json::from_str(value).map_err(|e| e.to_string())?;
    let codec = codec_for(content_type).ok_or_else(|| format!("unknown codec: {content_type}"))?;
    Ok(codec.encode_value(&value))
}

/// Decode codec bytes (produced by `content_type`'s codec) back to a JSON-encoded model string.
pub fn decode_as(bytes: &[u8], content_type: &str) -> Result<String, String> {
    let codec = codec_for(content_type).ok_or_else(|| format!("unknown codec: {content_type}"))?;
    let value = codec.decode_value(bytes).map_err(|e| e.to_string())?;
    serde_json::to_string(&value).map_err(|e| e.to_string())
}

/// Convert an arbitrary JSON document to MessagePack bytes.
///
/// Unlike [`encode_as`], this works on *any* JSON (not just a model [`Value`]) — the connection
/// layer uses it to encode whole protocol messages in the negotiated codec.
pub fn json_to_msgpack(json: &str) -> Result<Vec<u8>, String> {
    let v: serde_json::Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
    rmp_serde::to_vec_named(&v).map_err(|e| e.to_string())
}

/// Convert MessagePack bytes back to a JSON document.
pub fn msgpack_to_json(bytes: &[u8]) -> Result<String, String> {
    let v: serde_json::Value = rmp_serde::from_slice(bytes).map_err(|e| e.to_string())?;
    serde_json::to_string(&v).map_err(|e| e.to_string())
}

/// A string-in/string-out facade over [`Store`] for the bindings.
#[derive(Default)]
pub struct JsonStore {
    inner: Store,
}

impl JsonStore {
    pub fn new() -> JsonStore {
        JsonStore {
            inner: Store::new(),
        }
    }

    /// Host a model from its JSON; returns the assigned id.
    pub fn host(&mut self, type_name: &str, value_json: &str) -> Result<u64, String> {
        let value: Value = serde_json::from_str(value_json).map_err(|e| e.to_string())?;
        Ok(self.inner.host(type_name, value).0)
    }

    /// `{"type_name":..,"rev":..,"value":..}` for a hosted model, or `None` if unknown.
    pub fn snapshot(&self, id: u64) -> Option<String> {
        self.inner
            .snapshot(ModelId(id))
            .map(|(type_name, value, rev)| {
                serde_json::json!({"type_name": type_name, "rev": rev, "value": value}).to_string()
            })
    }

    /// Replace a hosted model from JSON; returns the JSON patch, or `None` if the id is unknown.
    pub fn mutate(&mut self, id: u64, value_json: &str) -> Result<Option<String>, String> {
        let value: Value = serde_json::from_str(value_json).map_err(|e| e.to_string())?;
        Ok(self
            .inner
            .mutate(ModelId(id), value)
            .map(|p| serde_json::to_string(&p).expect("patch serializes")))
    }

    /// Apply a JSON patch to a mirrored model; returns whether the id was known.
    pub fn apply(&mut self, id: u64, patch_json: &str) -> Result<bool, String> {
        let patch: Patch = serde_json::from_str(patch_json).map_err(|e| e.to_string())?;
        Ok(self.inner.apply(ModelId(id), &patch))
    }
}

#[cfg(test)]
mod bridge_tests {
    use super::*;

    #[test]
    fn test_diff_apply_json() {
        let old = r#"{"Map":{"on":{"Bool":false}}}"#;
        let new = r#"{"Map":{"on":{"Bool":true}}}"#;
        let patch = diff_json(old, new).unwrap();
        let got: serde_json::Value =
            serde_json::from_str(&apply_json(old, &patch).unwrap()).unwrap();
        let want: serde_json::Value = serde_json::from_str(new).unwrap();
        assert_eq!(got, want);
    }

    #[test]
    fn test_encode_decode_json() {
        let model = r#"{"Map":{"n":{"Int":3}}}"#;
        let bytes = encode_json(model).unwrap();
        let back: serde_json::Value = serde_json::from_str(&decode_json(&bytes).unwrap()).unwrap();
        assert_eq!(
            back,
            serde_json::from_str::<serde_json::Value>(model).unwrap()
        );
    }

    #[test]
    fn test_json_msgpack_round_trip() {
        let json = r#"{"t":"patch","id":1,"patch":{"rev":2,"ops":[]}}"#;
        let bytes = json_to_msgpack(json).unwrap();
        let back: serde_json::Value =
            serde_json::from_str(&msgpack_to_json(&bytes).unwrap()).unwrap();
        assert_eq!(
            back,
            serde_json::from_str::<serde_json::Value>(json).unwrap()
        );
    }

    #[test]
    fn test_json_store_mirror() {
        let mut s = JsonStore::new();
        let id = s
            .host("Device", r#"{"Map":{"on":{"Bool":false}}}"#)
            .unwrap();
        let patch = s
            .mutate(id, r#"{"Map":{"on":{"Bool":true}}}"#)
            .unwrap()
            .unwrap();
        assert!(s.apply(id, &patch).unwrap());
        assert!(s.snapshot(id).unwrap().contains("\"rev\":1"));
    }
}
