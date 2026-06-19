//! PyO3 surface over the shared core. All logic lives in `transports` core; this only adapts types
//! and errors. The same surface is exposed from the wasm binding.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Diff two JSON-encoded models, returning the JSON-encoded patch.
#[pyfunction]
pub fn diff(old: &str, new: &str) -> PyResult<String> {
    transports::diff_json(old, new).map_err(PyValueError::new_err)
}

/// Apply a JSON-encoded patch to a JSON-encoded model, returning the JSON-encoded result.
#[pyfunction]
pub fn apply(value: &str, patch: &str) -> PyResult<String> {
    transports::apply_json(value, patch).map_err(PyValueError::new_err)
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
    fn host(&mut self, type_name: &str, value_json: &str) -> PyResult<u64> {
        self.inner
            .host(type_name, value_json)
            .map_err(PyValueError::new_err)
    }

    /// `{"type_name":..,"rev":..,"value":..}` for a hosted model, or `None`.
    fn snapshot(&self, id: u64) -> Option<String> {
        self.inner.snapshot(id)
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
