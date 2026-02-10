use polars::prelude::*;
use polars_readstat_rs::{
    readstat_metadata_json,
    readstat_schema,
    readstat_scan,
    Sas7bdatReader,
    ScanOptions,
    SpssReader,
    SpssWriter,
    StataReader,
    StataWriter,
};
use num_cpus;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use pyo3::types::{PyDict, PyList};
use pyo3_polars::{PyDataFrame, PyLazyFrame, PySchema};
use std::cmp::min;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

enum ReadstatFormat {
    Sas,
    Stata,
    Spss,
}

enum ReadstatReader {
    Sas(Sas7bdatReader),
    Stata(StataReader),
    Spss(SpssReader),
}

fn detect_format(path: &str) -> PyResult<ReadstatFormat> {
    let ext = Path::new(path)
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.to_ascii_lowercase())
        .ok_or_else(|| PyValueError::new_err("missing file extension"))?;

    match ext.as_str() {
        "sas7bdat" | "sas7bcat" => Ok(ReadstatFormat::Sas),
        "dta" => Ok(ReadstatFormat::Stata),
        "sav" | "zsav" => Ok(ReadstatFormat::Spss),
        _ => Err(PyValueError::new_err("unknown file extension")),
    }
}

#[pymodule]
pub fn polars_readstat_bindings(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<PyPolarsReadstat>()?;
    m.add_function(wrap_pyfunction!(readstat_schema_rs, m)?)?;
    m.add_function(wrap_pyfunction!(readstat_metadata_json_rs, m)?)?;
    m.add_function(wrap_pyfunction!(write_stata, m)?)?;
    m.add_function(wrap_pyfunction!(write_spss, m)?)?;
    m.add_function(wrap_pyfunction!(scan_readstat_rs, m)?)?;
    Ok(())
}

#[pyclass]
pub struct PyPolarsReadstat {
    reader: ReadstatReader,
    path: String,
    batch_size: usize,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    with_columns: Option<Vec<String>>,
    offset: usize,
    remaining: usize,
}

#[pymethods]
impl PyPolarsReadstat {
    #[new]
    #[pyo3(signature = (path, size_hint, n_rows, threads, missing_string_as_null, value_labels_as_strings=false))]
    fn new_source(
        path:String,
        size_hint: usize,
        n_rows: Option<usize>,
        threads: Option<usize>,
        missing_string_as_null: bool,
        value_labels_as_strings: bool,
    ) -> PyResult<Self> {
        let max_useful_threads = num_cpus::get_physical();
        let threads = if threads.is_none() {
            Some(num_cpus::get_physical())
        } else {
            Some(min(threads.unwrap(),max_useful_threads))
        };

        let format = detect_format(&path)?;
        let reader = match format {
            ReadstatFormat::Sas => Sas7bdatReader::open(&path)
                .map(ReadstatReader::Sas)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
            ReadstatFormat::Stata => StataReader::open(&path)
                .map(ReadstatReader::Stata)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
            ReadstatFormat::Spss => SpssReader::open(&path)
                .map(ReadstatReader::Spss)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
        };

        let total_rows = match &reader {
            ReadstatReader::Sas(r) => r.metadata().row_count,
            ReadstatReader::Stata(r) => r.metadata().row_count as usize,
            ReadstatReader::Spss(r) => r.metadata().row_count as usize,
        };
        let remaining = n_rows.unwrap_or(total_rows).min(total_rows);
        let batch_size = size_hint.max(1);

        Ok(Self {
            reader,
            path,
            batch_size,
            threads,
            missing_string_as_null,
            value_labels_as_strings,
            with_columns: None,
            offset: 0,
            remaining,
        })
    }

    fn schema(&self) -> PyResult<PySchema> {
        let opts = ScanOptions {
            threads: self.threads,
            chunk_size: Some(self.batch_size),
            missing_string_as_null: Some(self.missing_string_as_null),
            value_labels_as_strings: Some(self.value_labels_as_strings),
        };
        let schema = readstat_schema(&self.path, Some(opts), None)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(PySchema(schema))
    }

    fn set_with_columns(&mut self, columns: Vec<String>) {
        self.with_columns = Some(columns);
    }

    fn next(&mut self) -> PyResult<Option<PyDataFrame>> {
        if self.remaining == 0 {
            return Ok(None);
        }

        let offset = self.offset;
        let take = self.batch_size.min(self.remaining);
        let columns = self.with_columns.clone();
        let threads = self.threads;
        let batch_size = self.batch_size;
        let missing_string_as_null = self.missing_string_as_null;
        let value_labels_as_strings = self.value_labels_as_strings;

        let result: Result<DataFrame, String> = Python::with_gil(|py| {
            py.allow_threads(|| match &self.reader {
                ReadstatReader::Sas(reader) => {
                    let mut builder = reader
                        .read()
                        .with_offset(offset)
                        .with_limit(take)
                        .missing_string_as_null(missing_string_as_null)
                        .with_chunk_size(batch_size);
                    if let Some(cols) = columns {
                        builder = builder.with_columns(cols);
                    }
                    if let Some(n) = threads {
                        builder = builder.with_n_threads(n);
                    }
                    builder.finish().map_err(|e| e.to_string())
                }
                ReadstatReader::Stata(reader) => {
                    let mut builder = reader
                        .read()
                        .with_offset(offset)
                        .with_limit(take)
                        .missing_string_as_null(missing_string_as_null)
                        .value_labels_as_strings(value_labels_as_strings)
                        .with_chunk_size(batch_size);
                    if let Some(cols) = columns {
                        builder = builder.with_columns(cols);
                    }
                    if let Some(n) = threads {
                        builder = builder.with_n_threads(n);
                    }
                    builder.finish().map_err(|e| e.to_string())
                }
                ReadstatReader::Spss(reader) => {
                    let mut builder = reader
                        .read()
                        .with_offset(offset)
                        .with_limit(take)
                        .missing_string_as_null(missing_string_as_null)
                        .value_labels_as_strings(value_labels_as_strings)
                        .with_chunk_size(batch_size);
                    if let Some(cols) = columns {
                        builder = builder.with_columns(cols);
                    }
                    if let Some(n) = threads {
                        builder = builder.with_n_threads(n);
                    }
                    builder.finish().map_err(|e| e.to_string())
                }
            })
        });

        match result {
            Ok(df) => {
                self.offset += take;
                self.remaining -= take;
                Ok(Some(PyDataFrame(df)))
            }
            Err(e) => Err(PyRuntimeError::new_err(e)),
        }
    }

    fn get_metadata(&self, py: Python) -> PyResult<PyObject> {
        let json = readstat_metadata_json(&self.path, None)
            .map_err(PyValueError::new_err)?;
        let json_mod = py.import("json")?;
        let obj = json_mod.call_method1("loads", (json,))?;
        Ok(obj.into())
    }

    fn get_column_info(&self, py: Python) -> PyResult<PyObject> {
        let md_obj = self.get_metadata(py)?;
        let md = md_obj.bind(py).downcast::<PyDict>()?;
        if let Ok(Some(cols)) = md.get_item("columns") {
            return Ok(cols.unbind().into());
        }
        if let Ok(Some(vars)) = md.get_item("variables") {
            return Ok(vars.unbind().into());
        }
        Err(PyValueError::new_err("No columns/variables in metadata"))
    }

    fn get_file_info(&self, py: Python) -> PyResult<PyObject> {
        let md_obj = self.get_metadata(py)?;
        let md = md_obj.bind(py).downcast::<PyDict>()?;
        let info = PyDict::new(py);

        if let Ok(Some(rows)) = md.get_item("row_count") {
            info.set_item("rows", rows)?;
        }
        if let Ok(Some(cols)) = md.get_item("column_count") {
            info.set_item("columns", cols)?;
        } else if let Ok(Some(cols)) = md.get_item("columns") {
            let cols = cols.downcast::<PyList>()?;
            info.set_item("columns", cols.len())?;
        } else if let Ok(Some(vars)) = md.get_item("variables") {
            let vars = vars.downcast::<PyList>()?;
            info.set_item("columns", vars.len())?;
        }
        if let Ok(Some(encoding)) = md.get_item("encoding") {
            info.set_item("encoding", encoding)?;
        }
        if let Ok(Some(compression)) = md.get_item("compression") {
            info.set_item("compression", compression)?;
        }
        if let Ok(Some(creator)) = md.get_item("creator") {
            info.set_item("creator", creator)?;
        }
        if let Ok(Some(creator_proc)) = md.get_item("creator_proc") {
            info.set_item("creator_proc", creator_proc)?;
        }

        Ok(info.into())
    }
}

fn _ignore_compress_opts(_opts: Option<&Bound<PyDict>>) {}

#[pyfunction]
#[pyo3(signature = (
    path,
    threads=None,
    chunk_size=None,
    missing_string_as_null=false,
    user_missing_as_null=false,
    value_labels_as_strings=false,
    preserve_order=false,
    compress=None
))]
fn scan_readstat_rs(
    path: String,
    threads: Option<usize>,
    chunk_size: Option<usize>,
    missing_string_as_null: bool,
    user_missing_as_null: bool,
    value_labels_as_strings: bool,
    preserve_order: bool,
    compress: Option<&Bound<PyDict>>,
) -> PyResult<PyLazyFrame> {
    let _ = user_missing_as_null;
    let _ = preserve_order;
    _ignore_compress_opts(compress);
    let opts = ScanOptions {
        threads,
        chunk_size,
        missing_string_as_null: Some(missing_string_as_null),
        value_labels_as_strings: Some(value_labels_as_strings),
    };
    let lf = readstat_scan(&path, Some(opts), None)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(PyLazyFrame(lf))
}

#[pyfunction]
#[pyo3(signature = (
    path,
    threads=None,
    chunk_size=None,
    missing_string_as_null=false,
    user_missing_as_null=false,
    value_labels_as_strings=false,
    preserve_order=false,
    compress=None
))]
fn readstat_schema_rs(
    path: String,
    threads: Option<usize>,
    chunk_size: Option<usize>,
    missing_string_as_null: bool,
    user_missing_as_null: bool,
    value_labels_as_strings: bool,
    preserve_order: bool,
    compress: Option<&Bound<PyDict>>,
) -> PyResult<PySchema> {
    let _ = user_missing_as_null;
    let _ = preserve_order;
    _ignore_compress_opts(compress);
    let opts = ScanOptions {
        threads,
        chunk_size,
        missing_string_as_null: Some(missing_string_as_null),
        value_labels_as_strings: Some(value_labels_as_strings),
    };
    let schema = readstat_schema(&path, Some(opts), None)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(PySchema(schema))
}

#[pyfunction]
fn readstat_metadata_json_rs(path: String) -> PyResult<String> {
    readstat_metadata_json(&path, None).map_err(PyValueError::new_err)
}

#[pyfunction]
fn write_stata(
    df: PyDataFrame,
    path: String,
    compress: Option<bool>,
    threads: Option<usize>,
    value_labels: Option<&Bound<PyDict>>,
    variable_labels: Option<&Bound<PyDict>>,
) -> PyResult<()> {
    let mut writer = StataWriter::new(path);
    if let Some(enable) = compress {
        writer = writer.with_compress(enable);
    }
    if let Some(n) = threads {
        writer = writer.with_n_threads(n);
    }
    if let Some(labels) = value_labels {
        writer = writer.with_value_labels(parse_stata_value_labels(labels)?);
    }
    if let Some(labels) = variable_labels {
        writer = writer.with_variable_labels(parse_stata_variable_labels(labels)?);
    }
    writer
        .write_df(&df.0)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
fn write_spss(
    df: PyDataFrame,
    path: String,
    value_labels: Option<&Bound<PyDict>>,
    variable_labels: Option<&Bound<PyDict>>,
) -> PyResult<()> {
    let mut writer = SpssWriter::new(path);
    if let Some(labels) = value_labels {
        writer = writer.with_value_labels(parse_spss_value_labels(labels)?);
    }
    if let Some(labels) = variable_labels {
        writer = writer.with_variable_labels(parse_spss_variable_labels(labels)?);
    }
    writer
        .write_df(&df.0)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

fn parse_stata_value_labels(labels: &Bound<PyDict>) -> PyResult<polars_readstat_rs::ValueLabels> {
    let mut out: polars_readstat_rs::ValueLabels = HashMap::new();
    for (col_obj, map_obj) in labels.iter() {
        let col = col_obj.extract::<String>()?;
        let map = map_obj.downcast::<PyDict>()?;
        let mut inner: polars_readstat_rs::ValueLabelMap = BTreeMap::new();
        for (key_obj, val_obj) in map.iter() {
            let key_i64 = key_obj.extract::<i64>()?;
            if key_i64 < i32::MIN as i64 || key_i64 > i32::MAX as i64 {
                return Err(PyValueError::new_err(format!(
                    "Stata value label key out of range for i32: {key_i64}"
                )));
            }
            let value = val_obj.extract::<String>()?;
            inner.insert(key_i64 as i32, value);
        }
        out.insert(col, inner);
    }
    Ok(out)
}

fn parse_stata_variable_labels(
    labels: &Bound<PyDict>,
) -> PyResult<polars_readstat_rs::VariableLabels> {
    let mut out: polars_readstat_rs::VariableLabels = HashMap::new();
    for (col_obj, label_obj) in labels.iter() {
        let col = col_obj.extract::<String>()?;
        let label = label_obj.extract::<String>()?;
        out.insert(col, label);
    }
    Ok(out)
}

fn parse_spss_value_labels(
    labels: &Bound<PyDict>,
) -> PyResult<polars_readstat_rs::SpssValueLabels> {
    let mut out: polars_readstat_rs::SpssValueLabels = HashMap::new();
    for (col_obj, map_obj) in labels.iter() {
        let col = col_obj.extract::<String>()?;
        let map = map_obj.downcast::<PyDict>()?;
        let mut inner: polars_readstat_rs::SpssValueLabelMap = HashMap::new();
        for (key_obj, val_obj) in map.iter() {
            let key = if let Ok(v) = key_obj.extract::<f64>() {
                polars_readstat_rs::SpssValueLabelKey::from(v)
            } else if let Ok(v) = key_obj.extract::<i64>() {
                polars_readstat_rs::SpssValueLabelKey::from(v as f64)
            } else {
                return Err(PyValueError::new_err(
                    "SPSS value label keys must be int or float",
                ));
            };
            let value = val_obj.extract::<String>()?;
            inner.insert(key, value);
        }
        out.insert(col, inner);
    }
    Ok(out)
}

fn parse_spss_variable_labels(
    labels: &Bound<PyDict>,
) -> PyResult<polars_readstat_rs::SpssVariableLabels> {
    let mut out: polars_readstat_rs::SpssVariableLabels = HashMap::new();
    for (col_obj, label_obj) in labels.iter() {
        let col = col_obj.extract::<String>()?;
        let label = label_obj.extract::<String>()?;
        out.insert(col, label);
    }
    Ok(out)
}
