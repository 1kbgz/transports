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

/// Parse, validate, and deterministically serialize a CRDT specification.
#[wasm_bindgen]
pub fn normalize_crdt_spec(json: &str) -> Result<String, JsError> {
    transports::normalize_crdt_spec_json(json).map_err(|e| JsError::new(&e))
}

/// Return the deterministic SHA-256 hash of a CRDT specification.
#[wasm_bindgen]
pub fn crdt_spec_hash(json: &str) -> Result<String, JsError> {
    transports::crdt_spec_hash_json(json).map_err(|e| JsError::new(&e))
}

/// Reject a peer hash that does not match the local CRDT specification.
#[wasm_bindgen]
pub fn require_crdt_spec_hash(json: &str, peer_hash: &str) -> Result<(), JsError> {
    transports::require_crdt_spec_hash_json(json, peer_hash).map_err(|e| JsError::new(&e))
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

/// Parse and serialize one typed live protocol message as compact JSON.
#[wasm_bindgen]
pub fn normalize_message(json: &str) -> Result<String, JsError> {
    transports::normalize_message_json(json).map_err(|e| JsError::new(&e))
}

/// Encode one JSON live protocol message with a built-in connection codec.
#[wasm_bindgen]
pub fn encode_message(json: &str, codec: &str) -> Result<Vec<u8>, JsError> {
    transports::encode_message(json, codec).map_err(|e| JsError::new(&e))
}

/// Decode one built-in connection-codec payload as a typed live protocol message JSON string.
#[wasm_bindgen]
pub fn decode_message(data: &[u8], codec: &str) -> Result<String, JsError> {
    transports::decode_message(data, codec).map_err(|e| JsError::new(&e))
}

/// Shared revision and proposal reducer for a language-level client adapter.
#[wasm_bindgen]
pub struct ClientState {
    inner: transports::ClientState,
}

#[wasm_bindgen]
impl ClientState {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: transports::ClientState::new(),
        }
    }

    pub fn prepare(&self, message_json: &str) -> Result<String, JsError> {
        self.inner
            .prepare_json(message_json)
            .map_err(|e| JsError::new(&e))
    }

    pub fn commit(&mut self, effect_json: &str) -> Result<(), JsError> {
        self.inner
            .commit_json(effect_json)
            .map_err(|e| JsError::new(&e))
    }

    pub fn proposal(
        &mut self,
        id: u64,
        ops_json: &str,
        proposal: Option<String>,
    ) -> Result<String, JsError> {
        self.inner
            .proposal_json(id, ops_json, proposal.as_deref())
            .map_err(|e| JsError::new(&e))
    }

    pub fn disconnect(&mut self) -> Result<String, JsError> {
        self.inner.disconnect_json().map_err(|e| JsError::new(&e))
    }

    pub fn revisions(&self) -> Result<String, JsError> {
        self.inner.revisions_json().map_err(|e| JsError::new(&e))
    }

    pub fn pending(&self) -> Result<String, JsError> {
        self.inner.pending_json().map_err(|e| JsError::new(&e))
    }

    pub fn abandon(&mut self, proposal: &str) -> bool {
        self.inner.abandon(proposal)
    }
}

impl Default for ClientState {
    fn default() -> Self {
        Self::new()
    }
}

/// Schema-directed CRDT reducer backed by the shared core.
#[wasm_bindgen]
pub struct CrdtDocument {
    inner: transports::JsonCrdtDocument,
}

#[wasm_bindgen]
impl CrdtDocument {
    #[wasm_bindgen(constructor)]
    pub fn new(spec_json: &str, value_json: &str, replica: &str) -> Result<Self, JsError> {
        Ok(Self {
            inner: transports::JsonCrdtDocument::new(spec_json, value_json, replica)
                .map_err(|error| JsError::new(&error))?,
        })
    }

    pub fn from_state(spec_json: &str, state_json: &str, replica: &str) -> Result<Self, JsError> {
        Ok(Self {
            inner: transports::JsonCrdtDocument::from_state(spec_json, state_json, replica)
                .map_err(|error| JsError::new(&error))?,
        })
    }

    pub fn value(&self) -> Result<String, JsError> {
        self.inner.value().map_err(|error| JsError::new(&error))
    }

    pub fn state(&self) -> Result<String, JsError> {
        self.inner.state().map_err(|error| JsError::new(&error))
    }

    pub fn mutate(&mut self, mutations_json: &str) -> Result<String, JsError> {
        self.inner
            .mutate(mutations_json)
            .map_err(|error| JsError::new(&error))
    }

    pub fn apply(&mut self, ops_json: &str) -> Result<String, JsError> {
        self.inner
            .apply(ops_json)
            .map_err(|error| JsError::new(&error))
    }

    pub fn member_key(&self, path_json: &str, value_json: &str) -> Result<String, JsError> {
        self.inner
            .member_key(path_json, value_json)
            .map_err(|error| JsError::new(&error))
    }

    pub fn compact(&mut self, frontier_json: &str) -> Result<usize, JsError> {
        self.inner
            .compact(frontier_json)
            .map_err(|error| JsError::new(&error))
    }
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
