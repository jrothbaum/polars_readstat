mod read;
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::error::PyPolarsErr;
use pyo3_polars::export::polars_plan::dsl::Expr;
use pyo3_polars::{PyDataFrame, PySchema,PyExpr};


// #[pyclass]
// pub struct read_dta {
//     columns: Vec<PlSmallStr>,
//     size_hint: usize,
//     n_rows: usize,
//     predicate: Option<Expr>,
//     with_columns: Option<Vec<usize>>,
// }


// #[pymethods]
// impl read_dta {
//     #[new]
//     #[pyo3(signature = (columns, size_hint, n_rows))]