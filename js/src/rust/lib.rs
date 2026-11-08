//! wasm surface over the shared core. Mirrors the PyO3 binding exactly; all logic is in the
//! `transports` core, so a patch produced in Python applies in the browser via the same Rust.

use wasm_bindgen::prelude::*;

/// Diff two JSON-encoded models, returning the JSON-encoded patch.
#[wasm_bindgen]
pub fn diff(old: &str, new: &str, list_keys: Option<String>) -> Result<String, JsError> {
    transports::diff_json_with_list_keys(old, new, list_keys.as_deref())
        .map_err(|e| JsError::new(&e))
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

    pub fn type_name(&self, id: u64) -> Option<String> {
        self.inner.type_name(id).map(str::to_owned)
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

    pub fn diff(&self, path_json: &str, value_json: &str) -> Result<String, JsError> {
        self.inner
            .diff(path_json, value_json)
            .map_err(|e| JsError::new(&e))
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
    pub fn host(
        &mut self,
        type_name: &str,
        value_json: &str,
        list_keys: Option<String>,
    ) -> Result<u64, JsError> {
        self.inner
            .host_with_list_keys(type_name, value_json, list_keys.as_deref())
            .map_err(|e| JsError::new(&e))
    }

    /// `{"type_name":..,"rev":..,"value":..}` for a hosted model, or `undefined`.
    pub fn snapshot(&self, id: u64) -> Option<String> {
        self.inner.snapshot(id)
    }

    pub fn revision(&self, id: u64) -> Option<u64> {
        self.inner.revision(id)
    }

    pub fn field(&self, id: u64, name: &str) -> Option<String> {
        self.inner.field(id, name)
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

/// A JSON control frame with native binary attachments.
#[wasm_bindgen]
pub struct BufferFrame {
    inner: transports::BufferFrame,
}

#[wasm_bindgen]
impl BufferFrame {
    #[wasm_bindgen(constructor)]
    pub fn new(header: String) -> Self {
        Self {
            inner: transports::BufferFrame {
                header,
                buffers: Vec::new(),
            },
        }
    }

    pub fn from_json(json: &str) -> Result<Self, JsError> {
        let message = transports::Message::from_json(json).map_err(|e| JsError::new(&e))?;
        Ok(Self {
            inner: transports::BufferFrame::from_message(&message).map_err(|e| JsError::new(&e))?,
        })
    }

    pub fn decode(data: &[u8], codec: &str) -> Result<Self, JsError> {
        let message = transports::Message::decode(data, codec).map_err(|e| JsError::new(&e))?;
        Ok(Self {
            inner: transports::BufferFrame::from_message(&message).map_err(|e| JsError::new(&e))?,
        })
    }

    #[wasm_bindgen(getter)]
    pub fn header(&self) -> String {
        self.inner.header.clone()
    }

    pub fn count(&self) -> usize {
        self.inner.buffers.len()
    }

    pub fn buffer(&self, index: usize) -> Result<Vec<u8>, JsError> {
        self.inner
            .buffers
            .get(index)
            .cloned()
            .ok_or_else(|| JsError::new("buffer index out of bounds"))
    }

    pub fn push(&mut self, buffer: Vec<u8>) {
        self.inner.buffers.push(buffer);
    }

    pub fn encode(&self, codec: &str) -> Result<Vec<u8>, JsError> {
        self.inner
            .message()
            .and_then(|m| m.encode(codec))
            .map_err(|e| JsError::new(&e))
    }

    pub fn to_json(&self) -> Result<String, JsError> {
        self.inner
            .message()
            .and_then(|m| m.to_json())
            .map_err(|e| JsError::new(&e))
    }
}

/// Bounded per-model replay with shared binary attachments.
#[wasm_bindgen]
pub struct ReplayLog {
    inner: transports::ReplayLog,
}

#[wasm_bindgen]
impl ReplayLog {
    #[wasm_bindgen(constructor)]
    pub fn new(cap: usize, max_bytes: usize, blob_threshold: usize) -> Self {
        Self {
            inner: transports::ReplayLog::new(cap, max_bytes, blob_threshold),
        }
    }
    pub fn append(&mut self, patch: &str) -> Result<(), JsError> {
        self.inner.append_json(patch).map_err(|e| JsError::new(&e))
    }
    pub fn since(&self, rev: u64, current: u64) -> Result<Option<String>, JsError> {
        self.inner
            .since_json(rev, current)
            .map_err(|e| JsError::new(&e))
    }
    pub fn bytes(&self) -> usize {
        self.inner.bytes()
    }
    pub fn count(&self) -> usize {
        self.inner.len()
    }
    pub fn blobs(&self) -> usize {
        self.inner.blobs()
    }
}

/// Single-server authority for one model's client-owned fields.
#[wasm_bindgen]
pub struct FieldAuthority {
    inner: transports::FieldAuthority,
}
#[wasm_bindgen]
impl FieldAuthority {
    #[wasm_bindgen(constructor)]
    pub fn new(id: u64, fields: &str, command_cap: usize) -> Result<Self, JsError> {
        Ok(Self {
            inner: transports::FieldAuthority::from_json(id, fields, command_cap)
                .map_err(|e| JsError::new(&e))?,
        })
    }
    pub fn open(&mut self, connection: &str) -> Result<String, JsError> {
        self.inner
            .open_json(connection)
            .map_err(|e| JsError::new(&e))
    }
    pub fn grant(&self, connection: &str) -> Result<String, JsError> {
        self.inner
            .grant_json(connection)
            .map_err(|e| JsError::new(&e))
    }
    pub fn command(&mut self, field: &str, value: &str) -> Result<u64, JsError> {
        self.inner
            .command_json(field, value)
            .map_err(|e| JsError::new(&e))
    }
    pub fn next_command(&mut self) -> Result<Option<String>, JsError> {
        self.inner.next_json().map_err(|e| JsError::new(&e))
    }
    pub fn validate(&self, connection: &str, message: &str) -> Result<bool, JsError> {
        self.inner
            .validate_json(connection, message)
            .map_err(|e| JsError::new(&e))
    }
    pub fn acknowledge(
        &mut self,
        connection: &str,
        epoch: u64,
        command: u64,
    ) -> Result<(), JsError> {
        self.inner
            .acknowledge(connection, epoch, command)
            .map_err(|e| JsError::new(&e))
    }
    pub fn close(&mut self, connection: &str) -> bool {
        self.inner.close(connection)
    }
    pub fn owner(&self) -> Option<String> {
        self.inner.owner().map(str::to_owned)
    }
    pub fn commit(&mut self, sequence: u64) {
        self.inner.commit(sequence);
    }
}
/// Client-side ownership classification and sequence generation.
#[wasm_bindgen]
#[derive(Default)]
pub struct OwnedFields {
    inner: transports::OwnedFields,
}
#[wasm_bindgen]
impl OwnedFields {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: transports::OwnedFields::default(),
        }
    }
    pub fn receive(&mut self, message: &str) -> Result<bool, JsError> {
        self.inner
            .receive_json(message)
            .map_err(|e| JsError::new(&e))
    }
    pub fn classify(&self, id: u64, ops: &str) -> Result<bool, JsError> {
        self.inner
            .classify_json(id, ops)
            .map_err(|e| JsError::new(&e))
    }
    pub fn edit(&mut self, id: u64, ops: &str) -> Result<String, JsError> {
        self.inner.edit_json(id, ops).map_err(|e| JsError::new(&e))
    }
    pub fn queue(&mut self, id: u64, ops: &str) -> Result<String, JsError> {
        self.inner.queue_json(id, ops).map_err(|e| JsError::new(&e))
    }
    pub fn drain(&mut self) -> Result<String, JsError> {
        self.inner.drain_json().map_err(|e| JsError::new(&e))
    }
    pub fn server_ops(&self, id: u64, ops: &str) -> Result<String, JsError> {
        self.inner
            .server_ops_json(id, ops)
            .map_err(|e| JsError::new(&e))
    }
    pub fn owns(&self, id: u64, field: &str) -> bool {
        self.inner.owns(id, field)
    }
    pub fn disconnect(&mut self) {
        self.inner.disconnect();
    }
}
