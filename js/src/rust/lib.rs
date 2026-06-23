//! wasm surface over the shared core. Mirrors the PyO3 binding exactly; all logic is in the
//! `transports` core, so a patch produced in Python applies in the browser via the same Rust.

use wasm_bindgen::prelude::*;

/// Diff two JSON-encoded models, returning the JSON-encoded patch.
#[wasm_bindgen]
pub fn diff(old: &str, new: &str) -> Result<String, JsError> {
    transports::diff_json(old, new).map_err(|e| JsError::new(&e))
}

/// Apply a JSON-encoded patch to a JSON-encoded model, returning the JSON-encoded result.
#[wasm_bindgen]
pub fn apply(value: &str, patch: &str) -> Result<String, JsError> {
    transports::apply_json(value, patch).map_err(|e| JsError::new(&e))
}

/// Encode a JSON-encoded model to codec bytes (a `Uint8Array` in JS).
#[wasm_bindgen]
pub fn encode(value: &str) -> Result<Vec<u8>, JsError> {
    transports::encode_json(value).map_err(|e| JsError::new(&e))
}

/// Decode codec bytes back to a JSON-encoded model string.
#[wasm_bindgen]
pub fn decode(data: &[u8]) -> Result<String, JsError> {
    transports::decode_json(data).map_err(|e| JsError::new(&e))
}

/// Encode a JSON-encoded model with the codec named by `codec` (e.g. `"application/msgpack"`).
#[wasm_bindgen]
pub fn encode_as(value: &str, codec: &str) -> Result<Vec<u8>, JsError> {
    transports::encode_as(value, codec).map_err(|e| JsError::new(&e))
}

/// Decode bytes (produced by `codec`'s codec) back to a JSON-encoded model string.
#[wasm_bindgen]
pub fn decode_as(data: &[u8], codec: &str) -> Result<String, JsError> {
    transports::decode_as(data, codec).map_err(|e| JsError::new(&e))
}

/// Convert an arbitrary JSON document to MessagePack bytes (a `Uint8Array` in JS).
#[wasm_bindgen]
pub fn json_to_msgpack(json: &str) -> Result<Vec<u8>, JsError> {
    transports::json_to_msgpack(json).map_err(|e| JsError::new(&e))
}

/// Convert MessagePack bytes back to a JSON document.
#[wasm_bindgen]
pub fn msgpack_to_json(data: &[u8]) -> Result<String, JsError> {
    transports::msgpack_to_json(data).map_err(|e| JsError::new(&e))
}

/// Convert an arbitrary JSON document to CBOR bytes (a `Uint8Array` in JS).
#[wasm_bindgen]
pub fn json_to_cbor(json: &str) -> Result<Vec<u8>, JsError> {
    transports::json_to_cbor(json).map_err(|e| JsError::new(&e))
}

/// Convert CBOR bytes back to a JSON document.
#[wasm_bindgen]
pub fn cbor_to_json(data: &[u8]) -> Result<String, JsError> {
    transports::cbor_to_json(data).map_err(|e| JsError::new(&e))
}

/// In-process model store: host / mutate → patch / apply / snapshot.
#[wasm_bindgen]
pub struct Store {
    inner: transports::JsonStore,
}

#[wasm_bindgen]
impl Store {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Store {
        Store {
            inner: transports::JsonStore::new(),
        }
    }

    /// Host a model from its JSON; returns the assigned id.
    pub fn host(&mut self, type_name: &str, value_json: &str) -> Result<u64, JsError> {
        self.inner
            .host(type_name, value_json)
            .map_err(|e| JsError::new(&e))
    }

    /// `{"type_name":..,"rev":..,"value":..}` for a hosted model, or `undefined`.
    pub fn snapshot(&self, id: u64) -> Option<String> {
        self.inner.snapshot(id)
    }

    /// Replace a hosted model from JSON; returns the JSON patch (or `undefined` if id unknown).
    pub fn mutate(&mut self, id: u64, value_json: &str) -> Result<Option<String>, JsError> {
        self.inner
            .mutate(id, value_json)
            .map_err(|e| JsError::new(&e))
    }

    /// Apply a JSON patch to a mirrored model; returns whether the id was known.
    pub fn apply(&mut self, id: u64, patch_json: &str) -> Result<bool, JsError> {
        self.inner
            .apply(id, patch_json)
            .map_err(|e| JsError::new(&e))
    }
}

impl Default for Store {
    fn default() -> Store {
        Store::new()
    }
}
