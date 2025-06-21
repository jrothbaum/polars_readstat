//  use log::{debug, info, warn, error};
mod read_cppsas;
mod read;

mod readstat_py;
mod read_cppsas_py;
use polars::prelude::*;
use pyo3::prelude::*;
#[pymodule]
fn polars_readstat_rs(m: &Bound<PyModule>) -> PyResult<()> {
    // Add classes directly to the main module
    m.add_class::<readstat_py::read_readstat>()?;
    m.add_class::<read_cppsas_py::read_cppsas_py>()?;
    
    Ok(())
}