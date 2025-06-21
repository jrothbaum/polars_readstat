use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::{PyDataFrame, PySchema};

use cpp_sas7bdat::SasReader;
use crate::read_cppsas;

#[pyclass]
pub struct read_cppsas_py {
    sas_reader:read_cppsas::CppSasScan,
    schema:Schema,
    columns:Option<Vec<String>>,
}

#[pymethods]
impl read_cppsas_py {
    #[new]
    #[pyo3(signature = (
        path, 
        batch_size, 
        n_rows, 
        with_columns
    ))]
    fn new_source(
        path:String,
        batch_size: Option<u32>,
        n_rows:Option<u64>,
        with_columns: Option<Vec<usize>>,
    ) -> Self {
        let mut schema_reader = SasReader::new(&path, None, None)
            .expect("Failed to create SAS reader");
        
        let schema = schema_reader.get_schema()
            .expect("Failed to get schema from sas file");


        let selected_columns: Option<Vec<String>> = with_columns.as_ref().map(|indices| {
            indices.iter()
                .filter_map(|&idx| {
                    schema.get_at_index(idx).map(|(name, _dtype)| name.to_string())
                })
                .collect()
        });
        let reader = read_cppsas::CppSasScan::new(
            std::path::PathBuf::from(path.clone()),
            batch_size,
            Some(0),
            Some(n_rows.unwrap_or(0)),
            selected_columns.clone(),
        ).expect("Cannot get sas batch scanner");


        Self {
            sas_reader:reader,
            schema:schema.clone(),
            columns:selected_columns,
        }
    }

    fn schema(&mut self) -> PySchema {
        let mut full_schema = self.schema.clone();
        
        let filtered_schema = match &self.columns {
            Some(selected_columns) => {
                // Create new schema with only selected columns
                Schema::from_iter(
                    selected_columns
                        .iter()
                        .filter_map(|col_name| {
                            full_schema.get(col_name).map(|dtype| {
                                (PlSmallStr::from(col_name), dtype.clone())
                            })
                        })
                )
            }
            None => full_schema, // Use full schema if no columns specified
        };
        
        PySchema(Arc::new(filtered_schema))
    }

    
    
    fn next(&mut self) -> PyResult<Option<PyDataFrame>> {
        // Create scan options 
        
        let scan_opts = AnonymousScanArgs {
            with_columns: None,
            schema: Arc::new(self.schema.clone()),
            output_schema:None,
            predicate: None,
            n_rows:None,
        };

        // Get the next batch from the scanner
        match self.sas_reader.next_batch(scan_opts) {
            Ok(Some(df)) => Ok(Some(PyDataFrame(df))),
            Ok(None) => Ok(None),
            Err(polars_error) => {
                Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Failed to get next batch: {}", polars_error)
                ))
            }
        }
    }
}

