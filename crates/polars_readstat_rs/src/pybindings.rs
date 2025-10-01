use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::{
    PyDataFrame, 
    PySchema
};
use pyo3::types::{PyDict, PyList};
use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use num_cpus;
use std::cmp::min;
use std::sync::Arc;
use std::collections::HashMap;


use crate::stream::PolarsReadstat;


#[pymodule]
pub fn polars_readstat_rs(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<PyPolarsReadstat>()?;
    Ok(())
}

#[pyclass]
pub struct PyPolarsReadstat {
    pub prs:PolarsReadstat,
    n_rows:Option<usize>,
}

#[pymethods]
impl PyPolarsReadstat {
    #[new]
    #[pyo3(signature = (path, size_hint, n_rows, threads, engine, use_mmap
    ))]
    fn new_source(
        path:String,
        size_hint: usize,
        n_rows: Option<usize>,
        threads: Option<usize>,
        engine: String,
        use_mmap:bool,
    ) -> Self {

        let max_useful_threads = num_cpus::get_physical();
    
        let threads = if threads.is_none() {
            num_cpus::get_physical()
        } else {
            min(threads.unwrap(),max_useful_threads)
        };
        Self {
            prs:PolarsReadstat::new(
                path,
                size_hint,
                None,
                threads,
                engine,
                use_mmap
            ),
            n_rows
        }
    }

    fn schema(&mut self) -> PySchema {
        let mut reader = self.prs.reader.lock().unwrap();
        PySchema(Arc::new(
            reader.schema_with_projection_pushdown()
                .unwrap()
                .clone()
        ))
    }

    
    fn set_with_columns(
        &mut self, 
        columns: Vec<String>
    ) {
        let mut reader = self.prs.reader.lock().unwrap();
        reader.with_columns = Some(columns);
    }
    
    fn next(&mut self) -> PyResult<Option<PyDataFrame>> {
        // Do Python-related work with GIL held
        let scan_opts = {
            let mut reader = self.prs.reader.lock().unwrap();
            let schema = reader.schema_with_projection_pushdown()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;
            
            AnonymousScanArgs {
                with_columns: None,
                schema: Arc::new(schema.clone()),
                output_schema: None,
                predicate: None,
                n_rows: self.n_rows,
            }
        };

        // Release GIL for the Rust computation
        let result = Python::with_gil(|py| {
            py.allow_threads(|| {
                // Clone what we need to avoid holding locks across GIL boundary
                self.prs.next_batch(scan_opts)
            })
        });

        result
            .map(|opt_df| opt_df.map(PyDataFrame))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))
    }

    /// Get metadata as a Python dictionary
    fn get_metadata(&self, py: Python) -> PyResult<PyObject> {
        let metadata_dict = PyDict::new(py);
        let mut reader = self.prs.reader.lock().unwrap();
        let md_option = reader.metadata().map_err(|e| PyValueError::new_err(e.to_string()))?;

        if let Some(md) = md_option {
            // Basic file info from file_info struct
            metadata_dict.set_item("row_count", md.file_info.n_rows)?;
            metadata_dict.set_item("var_count", md.file_info.n_cols)?;
            metadata_dict.set_item("table_name", &md.file_info.name)?;
            metadata_dict.set_item("file_encoding", &md.file_info.encoding)?;
            metadata_dict.set_item("file_type", &md.file_info.file_type)?;
            metadata_dict.set_item("sas_release", &md.file_info.sas_release)?;
            metadata_dict.set_item("sas_server_type", &md.file_info.sas_server_type)?;
            metadata_dict.set_item("os_name", &md.file_info.os_name)?;
            metadata_dict.set_item("creator_proc", &md.file_info.creator_proc)?;
            metadata_dict.set_item("compression", &md.file_info.compression)?;
            
            // Optional fields that might be None
            metadata_dict.set_item("file_label", match &md.file_info.file_label {
                Some(label) => label.into_py(py),
                None => py.None(),
            })?;
            metadata_dict.set_item("created_date", match md.file_info.created_date {
                Some(date) => date.into_py(py),
                None => py.None(),
            })?;
            metadata_dict.set_item("modified_date", match md.file_info.modified_date {
                Some(date) => date.into_py(py),
                None => py.None(),
            })?;
            
            // Page and size info
            metadata_dict.set_item("page_size", &md.file_info.page_size)?;
            metadata_dict.set_item("page_count", &md.file_info.page_count)?;
            metadata_dict.set_item("header_length", &md.file_info.header_length)?;
            metadata_dict.set_item("row_length", &md.file_info.row_length)?;
            
            // Variables info from column_info Vec
            let vars_list = PyList::empty(py);
            for col in &md.column_info {
                let var_dict = PyDict::new(py);
                var_dict.set_item("name", &col.name)?;
                var_dict.set_item("index", col.index)?;
                var_dict.set_item("type_class", &col.type_class)?;
                var_dict.set_item("format", &col.format)?;
                var_dict.set_item("label", &col.label)?;
                var_dict.set_item("length", &col.length)?;

                match &col.value_labels {
                    Some(labels) => {
                        let labels_dict = PyDict::new(py);
                        let is_numeric = col.type_class
                            .as_ref()
                            .map(|t| t == "number" || t == "numeric")
                            .unwrap_or(false);
                        
                        for (key, value) in labels {
                            let parsed_key = if is_numeric {
                                key.parse::<i64>()
                                    .map(|v| v.into_py(py))
                                    .or_else(|_| key.parse::<f64>().map(|v| v.into_py(py)))
                                    .unwrap_or_else(|_| key.into_py(py))
                            } else {
                                key.into_py(py)
                            };
                            labels_dict.set_item(parsed_key, value)?;
                        }
                        var_dict.set_item("value_labels", labels_dict)?;
                    },
                    None => {
                        var_dict.set_item("value_labels", py.None())?;
                    }
                }
                vars_list.append(var_dict)?;
            }
            metadata_dict.set_item("variables", vars_list)?;
            
            Ok(metadata_dict.into())
        } else {
            // Handle the case where metadata is not available.
            Err(PyValueError::new_err("Metadata not available for this file."))
        }
    }

    /// Get column info as a simple list of dictionaries
    fn get_column_info(&self, py: Python) -> PyResult<PyObject> {
        let mut reader = self.prs.reader.lock().unwrap();
        let md_option = reader.metadata().map_err(|e| PyValueError::new_err(e.to_string()))?;

        if let Some(md) = md_option {
            let vars_list = PyList::empty(py);
            for col in &md.column_info {
                let var_dict = PyDict::new(py);
                var_dict.set_item("name", &col.name)?;
                var_dict.set_item("index", col.index)?;
                var_dict.set_item("type_class", &col.type_class)?;
                var_dict.set_item("format", &col.format)?;
                var_dict.set_item("label", &col.label)?;
                var_dict.set_item("length", &col.length)?;

                match &col.value_labels {
                    Some(labels) => {
                        let labels_dict = PyDict::new(py);
                        let is_numeric = col.type_class
                            .as_ref()
                            .map(|t| t == "number" || t == "numeric")
                            .unwrap_or(false);
                        
                        for (key, value) in labels {
                            let parsed_key = if is_numeric {
                                key.parse::<i64>()
                                    .map(|v| v.into_py(py))
                                    .or_else(|_| key.parse::<f64>().map(|v| v.into_py(py)))
                                    .unwrap_or_else(|_| key.into_py(py))
                            } else {
                                key.into_py(py)
                            };
                            labels_dict.set_item(parsed_key, value)?;
                        }
                        var_dict.set_item("value_labels", labels_dict)?;
                    },
                    None => {
                        var_dict.set_item("value_labels", py.None())?;
                    }
                }
                vars_list.append(var_dict)?;
            }
            Ok(vars_list.into())
        } else {
            // Handle the case where metadata is not available.
            Err(PyValueError::new_err("Metadata not available for this file."))
        }
    }

    /// Get basic file stats
    fn get_file_info(&self, py: Python) -> PyResult<PyObject> {
        let info_dict = PyDict::new(py);
        let mut reader = self.prs.reader.lock().unwrap();
        let md_option = reader.metadata().map_err(|e| PyValueError::new_err(e.to_string()))?;

        if let Some(md) = md_option {
            info_dict.set_item("rows", md.file_info.n_rows)?;
            info_dict.set_item("columns", md.file_info.n_cols)?;
            info_dict.set_item("table_name", &md.file_info.name)?;
            info_dict.set_item("encoding", &md.file_info.encoding)?;
            info_dict.set_item("file_type", &md.file_info.file_type)?;
            info_dict.set_item("file_label", match &md.file_info.file_label {
                Some(label) => label.into_py(py),
                None => py.None(),
            })?;
            info_dict.set_item("created", match md.file_info.created_date {
                Some(date) => date.into_py(py),
                None => py.None(),
            })?;
            info_dict.set_item("modified", match md.file_info.modified_date {
                Some(date) => date.into_py(py),
                None => py.None(),
            })?;
            Ok(info_dict.into())
        } else {
            // Handle the case where metadata is not available.
            Err(PyValueError::new_err("Metadata not available for this file."))
        }
    }


}


fn parse_value_label_key(key: &str, type_class: &str) -> PyObject {
    Python::with_gil(|py| {
        if type_class == "number" || type_class == "numeric" {
            // Try to parse as integer first
            if let Ok(int_val) = key.parse::<i64>() {
                return int_val.into_py(py);
            }
            // If that fails, try as float
            if let Ok(float_val) = key.parse::<f64>() {
                return float_val.into_py(py);
            }
        }
        // Fall back to string if not numeric type or parsing fails
        key.into_py(py)
    })
}