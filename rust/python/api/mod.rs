//! PyO3 surface over the shared core. All logic lives in `transports` core; this only adapts types
//! and errors. The same surface is exposed from the wasm binding.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Diff two JSON-encoded models, returning the JSON-encoded patch.
#[pyfunction]
#[pyo3(signature = (old, new, *, list_keys=None))]
pub fn diff(old: &str, new: &str, list_keys: Option<&str>) -> PyResult<String> {
    transports::diff_json_with_list_keys(old, new, list_keys).map_err(PyValueError::new_err)
}

/// Apply a JSON-encoded patch to a JSON-encoded model, returning the JSON-encoded result.
#[pyfunction]
pub fn apply(value: &str, patch: &str) -> PyResult<String> {
    transports::apply_json(value, patch).map_err(PyValueError::new_err)
}

/// Parse, validate, and deterministically serialize a CRDT specification.
#[pyfunction]
pub fn normalize_crdt_spec(json: &str) -> PyResult<String> {
    transports::normalize_crdt_spec_json(json).map_err(PyValueError::new_err)
}

/// Return the deterministic SHA-256 hash of a CRDT specification.
#[pyfunction]
pub fn crdt_spec_hash(json: &str) -> PyResult<String> {
    transports::crdt_spec_hash_json(json).map_err(PyValueError::new_err)
}

/// Reject a peer hash that does not match the local CRDT specification.
#[pyfunction]
pub fn require_crdt_spec_hash(json: &str, peer_hash: &str) -> PyResult<()> {
    transports::require_crdt_spec_hash_json(json, peer_hash).map_err(PyValueError::new_err)
}

/// Encode a JSON-encoded model to codec bytes.
#[pyfunction]
pub fn encode<'py>(py: Python<'py>, value: &str) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = transports::encode_json(value).map_err(PyValueError::new_err)?;
    Ok(PyBytes::new(py, &bytes))
}

/// Decode codec bytes back to a JSON-encoded model string.
#[pyfunction]
pub fn decode(data: &[u8]) -> PyResult<String> {
    transports::decode_json(data).map_err(PyValueError::new_err)
}

/// Encode a JSON-encoded model with the codec named by `codec` (e.g. `"application/msgpack"`).
#[pyfunction]
pub fn encode_as<'py>(py: Python<'py>, value: &str, codec: &str) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = transports::encode_as(value, codec).map_err(PyValueError::new_err)?;
    Ok(PyBytes::new(py, &bytes))
}

/// Decode bytes (produced by `codec`'s codec) back to a JSON-encoded model string.
#[pyfunction]
pub fn decode_as(data: &[u8], codec: &str) -> PyResult<String> {
    transports::decode_as(data, codec).map_err(PyValueError::new_err)
}

/// Convert an arbitrary JSON document to MessagePack bytes (for encoding whole protocol messages).
#[pyfunction]
pub fn json_to_msgpack<'py>(py: Python<'py>, json: &str) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = transports::json_to_msgpack(json).map_err(PyValueError::new_err)?;
    Ok(PyBytes::new(py, &bytes))
}

/// Convert MessagePack bytes back to a JSON document.
#[pyfunction]
pub fn msgpack_to_json(data: &[u8]) -> PyResult<String> {
    transports::msgpack_to_json(data).map_err(PyValueError::new_err)
}

/// Convert an arbitrary JSON document to CBOR bytes (for encoding whole protocol messages).
#[pyfunction]
pub fn json_to_cbor<'py>(py: Python<'py>, json: &str) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = transports::json_to_cbor(json).map_err(PyValueError::new_err)?;
    Ok(PyBytes::new(py, &bytes))
}

/// Convert CBOR bytes back to a JSON document.
#[pyfunction]
pub fn cbor_to_json(data: &[u8]) -> PyResult<String> {
    transports::cbor_to_json(data).map_err(PyValueError::new_err)
}

/// Parse and serialize one typed live protocol message as compact JSON.
#[pyfunction]
pub fn normalize_message(json: &str) -> PyResult<String> {
    transports::normalize_message_json(json).map_err(PyValueError::new_err)
}

/// Encode one JSON live protocol message with a built-in connection codec.
#[pyfunction]
pub fn encode_message<'py>(
    py: Python<'py>,
    json: &str,
    codec: &str,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = transports::encode_message(json, codec).map_err(PyValueError::new_err)?;
    Ok(PyBytes::new(py, &bytes))
}

/// Decode one built-in connection-codec payload as a typed live protocol message JSON string.
#[pyfunction]
pub fn decode_message(data: &[u8], codec: &str) -> PyResult<String> {
    transports::decode_message(data, codec).map_err(PyValueError::new_err)
}

/// Shared revision and proposal reducer for a language-level client adapter.
#[pyclass]
pub struct ClientState {
    inner: transports::ClientState,
}

#[pymethods]
impl ClientState {
    #[new]
    fn new() -> Self {
        Self {
            inner: transports::ClientState::new(),
        }
    }

    fn prepare(&self, message_json: &str) -> PyResult<String> {
        self.inner
            .prepare_json(message_json)
            .map_err(PyValueError::new_err)
    }

    fn commit(&mut self, effect_json: &str) -> PyResult<()> {
        self.inner
            .commit_json(effect_json)
            .map_err(PyValueError::new_err)
    }

    #[pyo3(signature = (id, ops_json, proposal=None))]
    fn proposal(&mut self, id: u64, ops_json: &str, proposal: Option<&str>) -> PyResult<String> {
        self.inner
            .proposal_json(id, ops_json, proposal)
            .map_err(PyValueError::new_err)
    }

    fn disconnect(&mut self) -> PyResult<String> {
        self.inner.disconnect_json().map_err(PyValueError::new_err)
    }

    fn type_name(&self, id: u64) -> Option<String> {
        self.inner.type_name(id).map(str::to_owned)
    }

    fn revisions(&self) -> PyResult<String> {
        self.inner.revisions_json().map_err(PyValueError::new_err)
    }

    fn pending(&self) -> Vec<String> {
        self.inner.pending()
    }

    fn abandon(&mut self, proposal: &str) -> bool {
        self.inner.abandon(proposal)
    }
}

/// Schema-directed CRDT reducer backed by the shared core.
#[pyclass]
pub struct CrdtDocument {
    inner: transports::JsonCrdtDocument,
}

#[pymethods]
impl CrdtDocument {
    #[new]
    fn new(spec_json: &str, value_json: &str, replica: &str) -> PyResult<Self> {
        Ok(Self {
            inner: transports::JsonCrdtDocument::new(spec_json, value_json, replica)
                .map_err(PyValueError::new_err)?,
        })
    }

    #[staticmethod]
    fn from_state(spec_json: &str, state_json: &str, replica: &str) -> PyResult<Self> {
        Ok(Self {
            inner: transports::JsonCrdtDocument::from_state(spec_json, state_json, replica)
                .map_err(PyValueError::new_err)?,
        })
    }

    fn value(&self) -> PyResult<String> {
        self.inner.value().map_err(PyValueError::new_err)
    }

    fn state(&self) -> PyResult<String> {
        self.inner.state().map_err(PyValueError::new_err)
    }

    fn mutate(&mut self, mutations_json: &str) -> PyResult<String> {
        self.inner
            .mutate(mutations_json)
            .map_err(PyValueError::new_err)
    }

    fn apply(&mut self, ops_json: &str) -> PyResult<String> {
        self.inner.apply(ops_json).map_err(PyValueError::new_err)
    }

    fn diff(&self, path_json: &str, value_json: &str) -> PyResult<String> {
        self.inner
            .diff(path_json, value_json)
            .map_err(PyValueError::new_err)
    }

    fn member_key(&self, path_json: &str, value_json: &str) -> PyResult<String> {
        self.inner
            .member_key(path_json, value_json)
            .map_err(PyValueError::new_err)
    }

    fn compact(&mut self, frontier_json: &str) -> PyResult<usize> {
        self.inner
            .compact(frontier_json)
            .map_err(PyValueError::new_err)
    }
}

/// In-process model store: host / mutate → patch / apply / snapshot.
#[pyclass]
pub struct Store {
    inner: transports::JsonStore,
}

#[pymethods]
impl Store {
    #[new]
    fn new() -> Store {
        Store {
            inner: transports::JsonStore::new(),
        }
    }

    /// Host a model from its JSON; returns the assigned id.
    #[pyo3(signature = (type_name, value_json, *, list_keys=None))]
    fn host(
        &mut self,
        type_name: &str,
        value_json: &str,
        list_keys: Option<&str>,
    ) -> PyResult<u64> {
        self.inner
            .host_with_list_keys(type_name, value_json, list_keys)
            .map_err(PyValueError::new_err)
    }

    /// `{"type_name":..,"rev":..,"value":..}` for a hosted model, or `None`.
    fn snapshot(&self, id: u64) -> Option<String> {
        self.inner.snapshot(id)
    }

    fn revision(&self, id: u64) -> Option<u64> {
        self.inner.revision(id)
    }

    fn field(&self, id: u64, name: &str) -> Option<String> {
        self.inner.field(id, name)
    }

    /// Replace a hosted model from JSON; returns the JSON patch (or `None` if id unknown).
    fn mutate(&mut self, id: u64, value_json: &str) -> PyResult<Option<String>> {
        self.inner
            .mutate(id, value_json)
            .map_err(PyValueError::new_err)
    }

    /// Apply a JSON patch to a mirrored model; returns whether the id was known.
    fn apply(&mut self, id: u64, patch_json: &str) -> PyResult<bool> {
        self.inner
            .apply(id, patch_json)
            .map_err(PyValueError::new_err)
    }
}

/// A JSON control frame with native binary attachments.
#[pyclass]
pub struct BufferFrame {
    inner: transports::BufferFrame,
}

#[pymethods]
impl BufferFrame {
    #[new]
    fn new(header: String) -> Self {
        Self {
            inner: transports::BufferFrame {
                header,
                buffers: Vec::new(),
            },
        }
    }

    #[staticmethod]
    fn from_json(json: &str) -> PyResult<Self> {
        let message = transports::Message::from_json(json).map_err(PyValueError::new_err)?;
        Ok(Self {
            inner: transports::BufferFrame::from_message(&message)
                .map_err(PyValueError::new_err)?,
        })
    }

    #[staticmethod]
    fn decode(data: &[u8], codec: &str) -> PyResult<Self> {
        let message = transports::Message::decode(data, codec).map_err(PyValueError::new_err)?;
        Ok(Self {
            inner: transports::BufferFrame::from_message(&message)
                .map_err(PyValueError::new_err)?,
        })
    }

    #[getter]
    fn header(&self) -> &str {
        &self.inner.header
    }

    fn count(&self) -> usize {
        self.inner.buffers.len()
    }

    fn buffer<'py>(&self, py: Python<'py>, index: usize) -> PyResult<Bound<'py, PyBytes>> {
        let buffer = self
            .inner
            .buffers
            .get(index)
            .ok_or_else(|| PyValueError::new_err("buffer index out of bounds"))?;
        Ok(PyBytes::new(py, buffer))
    }

    fn push(&mut self, buffer: Vec<u8>) {
        self.inner.buffers.push(buffer);
    }

    fn encode<'py>(&self, py: Python<'py>, codec: &str) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self
            .inner
            .message()
            .and_then(|m| m.encode(codec))
            .map_err(PyValueError::new_err)?;
        Ok(PyBytes::new(py, &bytes))
    }

    fn to_json(&self) -> PyResult<String> {
        self.inner
            .message()
            .and_then(|m| m.to_json())
            .map_err(PyValueError::new_err)
    }
}

/// Bounded per-model replay with shared binary attachments.
#[pyclass]
pub struct ReplayLog {
    inner: transports::ReplayLog,
}

#[pymethods]
impl ReplayLog {
    #[new]
    fn new(cap: usize, max_bytes: usize, blob_threshold: usize) -> Self {
        Self {
            inner: transports::ReplayLog::new(cap, max_bytes, blob_threshold),
        }
    }
    fn append(&mut self, patch: &str) -> PyResult<()> {
        self.inner.append_json(patch).map_err(PyValueError::new_err)
    }
    fn since(&self, rev: u64, current: u64) -> PyResult<Option<String>> {
        self.inner
            .since_json(rev, current)
            .map_err(PyValueError::new_err)
    }
    fn bytes(&self) -> usize {
        self.inner.bytes()
    }
    fn count(&self) -> usize {
        self.inner.len()
    }
    fn blobs(&self) -> usize {
        self.inner.blobs()
    }
}

/// Single-server authority for one model's client-owned fields.
#[pyclass]
pub struct FieldAuthority {
    inner: transports::FieldAuthority,
}
#[pymethods]
impl FieldAuthority {
    #[new]
    fn new(id: u64, fields: &str, command_cap: usize) -> Result<Self, PyErr> {
        Ok(Self {
            inner: transports::FieldAuthority::from_json(id, fields, command_cap)
                .map_err(PyValueError::new_err)?,
        })
    }
    fn open(&mut self, connection: &str) -> Result<String, PyErr> {
        self.inner
            .open_json(connection)
            .map_err(PyValueError::new_err)
    }
    fn grant(&self, connection: &str) -> Result<String, PyErr> {
        self.inner
            .grant_json(connection)
            .map_err(PyValueError::new_err)
    }
    fn command(&mut self, field: &str, value: &str) -> Result<u64, PyErr> {
        self.inner
            .command_json(field, value)
            .map_err(PyValueError::new_err)
    }
    fn next_command(&mut self) -> Result<Option<String>, PyErr> {
        self.inner.next_json().map_err(PyValueError::new_err)
    }
    fn validate(&self, connection: &str, message: &str) -> Result<bool, PyErr> {
        self.inner
            .validate_json(connection, message)
            .map_err(PyValueError::new_err)
    }
    fn acknowledge(&mut self, connection: &str, epoch: u64, command: u64) -> Result<(), PyErr> {
        self.inner
            .acknowledge(connection, epoch, command)
            .map_err(PyValueError::new_err)
    }
    fn close(&mut self, connection: &str) -> bool {
        self.inner.close(connection)
    }
    fn owner(&self) -> Option<String> {
        self.inner.owner().map(str::to_owned)
    }
    fn commit(&mut self, sequence: u64) {
        self.inner.commit(sequence);
    }
}
/// Client-side ownership classification and sequence generation.
#[pyclass]
pub struct OwnedFields {
    inner: transports::OwnedFields,
}
#[pymethods]
impl OwnedFields {
    #[new]
    fn new() -> Self {
        Self {
            inner: transports::OwnedFields::default(),
        }
    }
    fn receive(&mut self, message: &str) -> Result<bool, PyErr> {
        self.inner
            .receive_json(message)
            .map_err(PyValueError::new_err)
    }
    fn classify(&self, id: u64, ops: &str) -> Result<bool, PyErr> {
        self.inner
            .classify_json(id, ops)
            .map_err(PyValueError::new_err)
    }
    fn edit(&mut self, id: u64, ops: &str) -> Result<String, PyErr> {
        self.inner.edit_json(id, ops).map_err(PyValueError::new_err)
    }
    fn queue(&mut self, id: u64, ops: &str) -> Result<String, PyErr> {
        self.inner
            .queue_json(id, ops)
            .map_err(PyValueError::new_err)
    }
    fn drain(&mut self) -> Result<String, PyErr> {
        self.inner.drain_json().map_err(PyValueError::new_err)
    }
    fn server_ops(&self, id: u64, ops: &str) -> Result<String, PyErr> {
        self.inner
            .server_ops_json(id, ops)
            .map_err(PyValueError::new_err)
    }
    fn owns(&self, id: u64, field: &str) -> bool {
        self.inner.owns(id, field)
    }
    fn disconnect(&mut self) {
        self.inner.disconnect();
    }
}
