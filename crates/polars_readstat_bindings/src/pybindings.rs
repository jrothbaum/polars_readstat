use num_cpus;
use polars::prelude::*;
use polars_readstat_rs::{
    readstat_metadata_json, readstat_scan, readstat_schema, Sas7bdatReader, ScanOptions,
    SpssReader, SpssWriter, StataReader, StataWriter,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::wrap_pyfunction;
use pyo3_polars::{PyDataFrame, PyLazyFrame, PySchema};
use std::cmp::min;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};

#[derive(Clone, Copy)]
enum ReadstatFormat {
    Sas,
    Stata,
    Spss,
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

#[derive(Clone)]
struct ParsedCompressOptions {
    opts: polars_readstat_rs::CompressOptionsLite,
    infer_compress_length: Option<usize>,
}

fn parse_compress_opts(compress: Option<&Bound<PyDict>>) -> PyResult<ParsedCompressOptions> {
    let mut opts = polars_readstat_rs::CompressOptionsLite::default();
    let mut infer_compress_length: Option<usize> = None;

    if let Some(dict) = compress {
        if let Ok(Some(v)) = dict.get_item("enabled") {
            opts.enabled = v.extract::<bool>()?;
        }
        if let Ok(Some(v)) = dict.get_item("cols") {
            opts.cols = v.extract::<Option<Vec<String>>>()?;
        }
        if let Ok(Some(v)) = dict.get_item("compress_numeric") {
            opts.compress_numeric = v.extract::<bool>()?;
        }
        if let Ok(Some(v)) = dict.get_item("datetime_to_date") {
            opts.datetime_to_date = v.extract::<bool>()?;
        }
        if let Ok(Some(v)) = dict.get_item("string_to_numeric") {
            opts.string_to_numeric = v.extract::<bool>()?;
        }
        if let Ok(Some(v)) = dict.get_item("infer_compress_length") {
            infer_compress_length = v.extract::<Option<usize>>()?;
        }
    }

    Ok(ParsedCompressOptions {
        opts,
        infer_compress_length,
    })
}

fn read_df_with_options(
    path: &str,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    columns: Option<Vec<String>>,
    n_rows: Option<usize>,
) -> PyResult<DataFrame> {
    let format = detect_format(path)?;
    let df = match format {
        ReadstatFormat::Sas => {
            let reader =
                Sas7bdatReader::open(path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let mut builder = reader.read().missing_string_as_null(missing_string_as_null);
            if let Some(cols) = columns.clone() {
                builder = builder.with_columns(cols);
            }
            if let Some(limit) = n_rows {
                builder = builder.with_limit(limit);
            }
            if let Some(n) = threads {
                builder = builder.with_n_threads(n);
            }
            builder
                .finish()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        }
        ReadstatFormat::Stata => {
            let reader =
                StataReader::open(path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let mut builder = reader
                .read()
                .missing_string_as_null(missing_string_as_null)
                .value_labels_as_strings(value_labels_as_strings);
            if let Some(cols) = columns.clone() {
                builder = builder.with_columns(cols);
            }
            if let Some(limit) = n_rows {
                builder = builder.with_limit(limit);
            }
            if let Some(n) = threads {
                builder = builder.with_n_threads(n);
            }
            builder
                .finish()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        }
        ReadstatFormat::Spss => {
            let reader =
                SpssReader::open(path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let mut builder = reader
                .read()
                .missing_string_as_null(missing_string_as_null)
                .value_labels_as_strings(value_labels_as_strings);
            if let Some(cols) = columns {
                builder = builder.with_columns(cols);
            }
            if let Some(limit) = n_rows {
                builder = builder.with_limit(limit);
            }
            if let Some(n) = threads {
                builder = builder.with_n_threads(n);
            }
            builder
                .finish()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        }
    };
    Ok(df)
}

fn infer_compressed_schema_and_cast_map(
    path: &str,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    columns: Option<Vec<String>>,
    compress_opts: &polars_readstat_rs::CompressOptionsLite,
    infer_compress_length: Option<usize>,
) -> PyResult<(Schema, HashMap<String, DataType>)> {
    let probe_df = read_df_with_options(
        path,
        threads,
        missing_string_as_null,
        value_labels_as_strings,
        columns,
        infer_compress_length,
    )?;
    let compressed = polars_readstat_rs::compress_df_if_enabled(&probe_df, compress_opts)
        .map_err(PyRuntimeError::new_err)?;

    let mut schema = Schema::with_capacity(compressed.width());
    let mut cast_map = HashMap::with_capacity(compressed.width());
    for col in compressed.get_columns() {
        let name = col.name().as_str().to_string();
        let dtype = col.dtype().clone();
        schema.with_column(name.as_str().into(), dtype.clone());
        cast_map.insert(name, dtype);
    }
    Ok((schema, cast_map))
}

fn cast_df_to_dtypes(df: &DataFrame, cast_map: &HashMap<String, DataType>) -> PyResult<DataFrame> {
    let mut out_cols: Vec<Column> = Vec::with_capacity(df.width());
    for col in df.get_columns() {
        let name = col.name().as_str();
        match cast_map.get(name) {
            Some(target_dtype) if col.dtype() != target_dtype => {
                let casted = col
                    .as_materialized_series()
                    .cast(target_dtype)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                out_cols.push(casted.into_column());
            }
            _ => out_cols.push(col.clone()),
        }
    }
    DataFrame::new(out_cols).map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

enum ChunkMessage {
    Data { idx: usize, df: DataFrame },
    Done,
    Err(String),
}

struct StreamState {
    rx: mpsc::Receiver<ChunkMessage>,
    buffer: BTreeMap<usize, DataFrame>,
    next_idx: usize,
    completed: usize,
    total_workers: usize,
    handles: Vec<std::thread::JoinHandle<()>>,
}

#[pymodule]
pub fn polars_readstat_bindings(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<PyPolarsReadstat>()?;
    m.add_function(wrap_pyfunction!(readstat_schema_rs, m)?)?;
    m.add_function(wrap_pyfunction!(readstat_metadata_json_rs, m)?)?;
    m.add_function(wrap_pyfunction!(read_readstat_rs, m)?)?;
    m.add_function(wrap_pyfunction!(write_stata, m)?)?;
    m.add_function(wrap_pyfunction!(write_spss, m)?)?;
    m.add_function(wrap_pyfunction!(scan_readstat_rs, m)?)?;
    Ok(())
}

#[pyclass]
pub struct PyPolarsReadstat {
    path: String,
    format: ReadstatFormat,
    batch_size: usize,
    threads: usize,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    preserve_order: bool,
    with_columns: Option<Vec<String>>,
    compress_opts: polars_readstat_rs::CompressOptionsLite,
    infer_compress_length: Option<usize>,
    inferred_schema: Option<Schema>,
    inferred_cast_map: Option<HashMap<String, DataType>>,
    total_rows: usize,
    total_chunks: usize,
    stream: Option<Mutex<StreamState>>,
}

#[pymethods]
impl PyPolarsReadstat {
    #[new]
    #[pyo3(signature = (path, size_hint, n_rows, threads, missing_string_as_null, value_labels_as_strings=false, preserve_order=false, compress=None))]
    fn new_source(
        path: String,
        size_hint: usize,
        n_rows: Option<usize>,
        threads: Option<usize>,
        missing_string_as_null: bool,
        value_labels_as_strings: bool,
        preserve_order: bool,
        compress: Option<&Bound<PyDict>>,
    ) -> PyResult<Self> {
        let max_useful_threads = num_cpus::get_physical();
        let threads = if threads.is_none() {
            num_cpus::get_physical()
        } else {
            min(threads.unwrap(), max_useful_threads)
        };

        let format = detect_format(&path)?;
        let total_rows = match format {
            ReadstatFormat::Sas => {
                let reader = Sas7bdatReader::open(&path)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                reader.metadata().row_count
            }
            ReadstatFormat::Stata => {
                let reader =
                    StataReader::open(&path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                reader.metadata().row_count as usize
            }
            ReadstatFormat::Spss => {
                let reader =
                    SpssReader::open(&path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                reader.metadata().row_count as usize
            }
        };
        let total_rows = n_rows.unwrap_or(total_rows).min(total_rows);
        let batch_size = size_hint.max(1);
        let total_chunks = (total_rows + batch_size - 1) / batch_size;
        let parsed_compress = parse_compress_opts(compress)?;

        Ok(Self {
            path,
            format,
            batch_size,
            threads,
            missing_string_as_null,
            value_labels_as_strings,
            preserve_order,
            with_columns: None,
            compress_opts: parsed_compress.opts,
            infer_compress_length: parsed_compress.infer_compress_length,
            inferred_schema: None,
            inferred_cast_map: None,
            total_rows,
            total_chunks,
            stream: None,
        })
    }

    fn schema(&mut self) -> PyResult<PySchema> {
        if self.compress_opts.enabled {
            self.ensure_inferred_compress_schema()?;
            if let Some(schema) = self.inferred_schema.as_ref() {
                return Ok(PySchema(Arc::new(schema.clone())));
            }
        }

        let opts = ScanOptions {
            threads: Some(self.threads),
            chunk_size: Some(self.batch_size),
            missing_string_as_null: Some(self.missing_string_as_null),
            value_labels_as_strings: Some(self.value_labels_as_strings),
            ..Default::default()
        };
        let schema = readstat_schema(&self.path, Some(opts), None)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(PySchema(schema))
    }

    fn set_with_columns(&mut self, columns: Vec<String>) {
        self.with_columns = Some(columns);
    }

    fn next(&mut self) -> PyResult<Option<PyDataFrame>> {
        if self.total_chunks == 0 {
            return Ok(None);
        }

        if self.stream.is_none() {
            self.start_stream()?;
        }

        let result: Result<Option<DataFrame>, String> =
            Python::with_gil(|py| py.allow_threads(|| self.next_batch()));

        match result {
            Ok(Some(df)) => {
                let out = if self.compress_opts.enabled {
                    self.ensure_inferred_compress_schema()?;
                    if let Some(cast_map) = self.inferred_cast_map.as_ref() {
                        cast_df_to_dtypes(&df, cast_map)?
                    } else {
                        df
                    }
                } else {
                    df
                };
                Ok(Some(PyDataFrame(out)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(PyRuntimeError::new_err(e)),
        }
    }

    fn get_metadata(&self, py: Python) -> PyResult<PyObject> {
        let json = readstat_metadata_json(&self.path, None).map_err(PyValueError::new_err)?;
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

impl PyPolarsReadstat {
    fn ensure_inferred_compress_schema(&mut self) -> PyResult<()> {
        if !self.compress_opts.enabled || self.inferred_schema.is_some() {
            return Ok(());
        }

        let infer_rows = self
            .infer_compress_length
            .map(|n| n.min(self.total_rows))
            .or(Some(self.total_rows));

        let (schema, cast_map) = infer_compressed_schema_and_cast_map(
            &self.path,
            Some(self.threads),
            self.missing_string_as_null,
            self.value_labels_as_strings,
            self.with_columns.clone(),
            &self.compress_opts,
            infer_rows,
        )?;
        self.inferred_schema = Some(schema);
        self.inferred_cast_map = Some(cast_map);
        Ok(())
    }

    fn start_stream(&mut self) -> PyResult<()> {
        let total_chunks = self.total_chunks;
        let n_workers = min(self.threads.max(1), total_chunks.max(1));
        let window_chunks = 8usize;
        let batch_size = self.batch_size;
        let total_rows = self.total_rows;
        let path = self.path.clone();
        let format = self.format;
        let columns = self.with_columns.clone();
        let missing_string_as_null = self.missing_string_as_null;
        let value_labels_as_strings = self.value_labels_as_strings;

        let (tx, rx) = mpsc::channel::<ChunkMessage>();
        let mut handles = Vec::with_capacity(n_workers);

        let base_chunks = total_chunks / n_workers;
        let extra_chunks = total_chunks % n_workers;
        let mut worker_ranges: Vec<(usize, usize)> = Vec::with_capacity(n_workers);
        let mut next_chunk = 0usize;
        for worker_idx in 0..n_workers {
            let worker_chunk_count = base_chunks + usize::from(worker_idx < extra_chunks);
            if worker_chunk_count == 0 {
                continue;
            }
            let start_chunk = next_chunk;
            let end_chunk = start_chunk + worker_chunk_count;
            worker_ranges.push((start_chunk, end_chunk));
            next_chunk = end_chunk;
        }

        for (start_chunk, end_chunk) in worker_ranges {
            let tx = tx.clone();
            let path = path.clone();
            let columns = columns.clone();
            let total_rows = total_rows;
            let batch_size = batch_size;
            let format = format;
            let missing_string_as_null = missing_string_as_null;
            let value_labels_as_strings = value_labels_as_strings;
            let window_chunks = window_chunks;

            let handle = std::thread::spawn(move || {
                enum WorkerReader {
                    Sas(Sas7bdatReader),
                    Stata(StataReader),
                    Spss(SpssReader),
                }

                let reader = match format {
                    ReadstatFormat::Sas => match Sas7bdatReader::open(&path) {
                        Ok(r) => WorkerReader::Sas(r),
                        Err(e) => {
                            let _ = tx.send(ChunkMessage::Err(e.to_string()));
                            return;
                        }
                    },
                    ReadstatFormat::Stata => match StataReader::open(&path) {
                        Ok(r) => WorkerReader::Stata(r),
                        Err(e) => {
                            let _ = tx.send(ChunkMessage::Err(e.to_string()));
                            return;
                        }
                    },
                    ReadstatFormat::Spss => match SpssReader::open(&path) {
                        Ok(r) => WorkerReader::Spss(r),
                        Err(e) => {
                            let _ = tx.send(ChunkMessage::Err(e.to_string()));
                            return;
                        }
                    },
                };

                let mut chunk_idx = start_chunk;
                while chunk_idx < end_chunk {
                    let window_end = (chunk_idx + window_chunks).min(end_chunk);
                    let offset = chunk_idx * batch_size;
                    if offset >= total_rows {
                        break;
                    }
                    let window_rows = (window_end - chunk_idx) * batch_size;
                    let take = (total_rows - offset).min(window_rows);

                    let result: Result<DataFrame, String> = match &reader {
                        WorkerReader::Sas(reader) => {
                            let mut builder = reader
                                .read()
                                .with_offset(offset)
                                .with_limit(take)
                                .missing_string_as_null(missing_string_as_null)
                                .sequential();
                            if let Some(cols) = columns.as_ref() {
                                builder = builder.with_columns(cols.clone());
                            }
                            builder.finish().map_err(|e| e.to_string())
                        }
                        WorkerReader::Stata(reader) => {
                            let mut builder = reader
                                .read()
                                .with_offset(offset)
                                .with_limit(take)
                                .missing_string_as_null(missing_string_as_null)
                                .value_labels_as_strings(value_labels_as_strings)
                                .sequential();
                            if let Some(cols) = columns.as_ref() {
                                builder = builder.with_columns(cols.clone());
                            }
                            builder.finish().map_err(|e| e.to_string())
                        }
                        WorkerReader::Spss(reader) => {
                            let mut builder = reader
                                .read()
                                .with_offset(offset)
                                .with_limit(take)
                                .missing_string_as_null(missing_string_as_null)
                                .value_labels_as_strings(value_labels_as_strings)
                                .sequential();
                            if let Some(cols) = columns.as_ref() {
                                builder = builder.with_columns(cols.clone());
                            }
                            builder.finish().map_err(|e| e.to_string())
                        }
                    };

                    match result {
                        Ok(window_df) => {
                            let mut sent = 0usize;
                            for out_idx in chunk_idx..window_end {
                                let local_offset = (out_idx - chunk_idx) * batch_size;
                                if local_offset >= window_df.height() {
                                    break;
                                }
                                let local_take = batch_size.min(window_df.height() - local_offset);
                                let df = window_df.slice(local_offset as i64, local_take);
                                let _ = tx.send(ChunkMessage::Data { idx: out_idx, df });
                                sent += 1;
                            }
                            if sent == 0 {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(ChunkMessage::Err(e));
                            return;
                        }
                    }

                    chunk_idx = window_end;
                }
                let _ = tx.send(ChunkMessage::Done);
            });

            handles.push(handle);
        }

        self.stream = Some(Mutex::new(StreamState {
            rx,
            buffer: BTreeMap::new(),
            next_idx: 0,
            completed: 0,
            total_workers: n_workers,
            handles,
        }));

        Ok(())
    }

    fn next_batch(&mut self) -> Result<Option<DataFrame>, String> {
        let stream = self.stream.as_ref().ok_or("stream not initialized")?;
        let mut state = stream
            .lock()
            .map_err(|_| "stream mutex poisoned".to_string())?;

        if !self.preserve_order {
            loop {
                if state.completed == state.total_workers {
                    return Ok(None);
                }
                match state.rx.recv() {
                    Ok(ChunkMessage::Data { df, .. }) => return Ok(Some(df)),
                    Ok(ChunkMessage::Done) => state.completed += 1,
                    Ok(ChunkMessage::Err(e)) => return Err(e),
                    Err(_) => return Ok(None),
                }
            }
        }

        loop {
            let next_idx = state.next_idx;
            if let Some(df) = state.buffer.remove(&next_idx) {
                state.next_idx += 1;
                return Ok(Some(df));
            }

            if state.completed == state.total_workers {
                return Ok(None);
            }

            match state.rx.recv() {
                Ok(ChunkMessage::Data { idx, df }) => {
                    state.buffer.insert(idx, df);
                }
                Ok(ChunkMessage::Done) => {
                    state.completed += 1;
                }
                Ok(ChunkMessage::Err(e)) => {
                    return Err(e);
                }
                Err(_) => {
                    return Ok(None);
                }
            }
        }
    }
}

impl Drop for PyPolarsReadstat {
    fn drop(&mut self) {
        if let Some(state) = self.stream.take() {
            if let Ok(state) = state.into_inner() {
                for handle in state.handles {
                    let _ = handle.join();
                }
            }
        }
    }
}

#[pyfunction]
#[pyo3(signature = (
    path,
    threads=None,
    chunk_size=None,
    missing_string_as_null=false,
    value_labels_as_strings=false,
    preserve_order=false,
    compress=None
))]
fn scan_readstat_rs(
    path: String,
    threads: Option<usize>,
    chunk_size: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    preserve_order: bool,
    compress: Option<&Bound<PyDict>>,
) -> PyResult<PyLazyFrame> {
    let parsed_compress = parse_compress_opts(compress)?;
    let opts = ScanOptions {
        threads,
        chunk_size,
        missing_string_as_null: Some(missing_string_as_null),
        value_labels_as_strings: Some(value_labels_as_strings),
        preserve_order: Some(preserve_order),
        compress_opts: parsed_compress.opts,
        ..Default::default()
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
    value_labels_as_strings=false,
    preserve_order=false,
    compress=None
))]
fn readstat_schema_rs(
    path: String,
    threads: Option<usize>,
    chunk_size: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    preserve_order: bool,
    compress: Option<&Bound<PyDict>>,
) -> PyResult<PySchema> {
    let parsed_compress = parse_compress_opts(compress)?;
    let _ = preserve_order;
    if parsed_compress.opts.enabled {
        let (schema, _) = infer_compressed_schema_and_cast_map(
            &path,
            threads,
            missing_string_as_null,
            value_labels_as_strings,
            None,
            &parsed_compress.opts,
            parsed_compress.infer_compress_length,
        )?;
        return Ok(PySchema(Arc::new(schema)));
    }
    let opts = ScanOptions {
        threads,
        chunk_size,
        missing_string_as_null: Some(missing_string_as_null),
        value_labels_as_strings: Some(value_labels_as_strings),
        compress_opts: parsed_compress.opts,
        ..Default::default()
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
#[pyo3(signature = (
    path,
    threads=None,
    missing_string_as_null=false,
    value_labels_as_strings=false,
    columns=None,
    compress=None
))]
fn read_readstat_rs(
    path: String,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    columns: Option<Vec<String>>,
    compress: Option<&Bound<PyDict>>,
) -> PyResult<PyDataFrame> {
    let parsed_compress = parse_compress_opts(compress)?;
    let df = read_df_with_options(
        &path,
        threads,
        missing_string_as_null,
        value_labels_as_strings,
        columns.clone(),
        None,
    )?;
    let out = if parsed_compress.opts.enabled {
        let (_, cast_map) = infer_compressed_schema_and_cast_map(
            &path,
            threads,
            missing_string_as_null,
            value_labels_as_strings,
            columns,
            &parsed_compress.opts,
            parsed_compress.infer_compress_length,
        )?;
        cast_df_to_dtypes(&df, &cast_map)?
    } else {
        df
    };
    Ok(PyDataFrame(out))
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
