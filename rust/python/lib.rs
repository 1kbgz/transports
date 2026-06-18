use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

mod api;

#[pymodule]
fn transports(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(api::diff, m)?)?;
    m.add_function(wrap_pyfunction!(api::apply, m)?)?;
    m.add_function(wrap_pyfunction!(api::encode, m)?)?;
    m.add_function(wrap_pyfunction!(api::decode, m)?)?;
    m.add_function(wrap_pyfunction!(api::encode_as, m)?)?;
    m.add_function(wrap_pyfunction!(api::decode_as, m)?)?;
    m.add_class::<api::Store>()?;
    Ok(())
}
