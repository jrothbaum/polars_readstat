//  use log::{debug, info, warn, error};

mod read;
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::{PyDataFrame, PySchema};

use read::{
    read_metadata,
    read_chunks_parallel
};

use std::cmp::min;

use readstat::ReadStatMetadata;

#[pyclass]
pub struct read_readstat {
    path:String,
    size_hint: usize,
    n_rows: usize,
    threads: Option<usize>,
    with_columns: Option<Vec<usize>>,
    n_rows_read: usize,
    md:ReadStatMetadata
}

#[pymethods]
impl read_readstat {
    #[new]
    #[pyo3(signature = (path, size_hint, n_rows, threads
    ))]
    fn new_source(
        path:String,
        size_hint: Option<usize>,
        n_rows: Option<usize>,
        threads: Option<usize>
    ) -> Self {
        let size_hint = size_hint.unwrap_or(100_000);
        
        let n_rows_read = 0 as usize;

        //  Pre-fetch the metadata from the file, to populate the schema
        let md = read_metadata(std::path::PathBuf::from(path.clone()), false).unwrap();
        //  dbg!("n_rows = {}",n_rows);
        let n_rows = n_rows.unwrap_or(md.row_count as usize);
        let n_rows = min(n_rows, md.row_count as usize);
        //  dbg!("n_rows = {}",n_rows);
        Self {
            path,
            size_hint,
            n_rows,
            threads,
            with_columns: None,
            n_rows_read: n_rows_read,
            md:md
        }
    }

    fn schema(&mut self) -> PySchema {
        PySchema(Arc::new(self.md.clone().schema_with_filter_pushdown(
            self.with_columns.clone()
        )))
    }

    
    

    // fn try_set_predicate(&mut self, predicate: PyExpr) {
    //     self.predicate = Some(predicate.0);
    // }

    fn set_with_columns(
        &mut self, 
        columns: Vec<String>
    ) {
        let schema = self.schema().0;

        let indexes = columns
            .iter()
            .map(|name| {
                schema
                    .index_of(name.as_ref())
                    .expect("schema should be correct")
            })
            .collect();

        self.with_columns = Some(indexes)
    }
    
    fn next(&mut self) -> PyResult<Option<PyDataFrame>> {
        if self.n_rows > 0 && self.n_rows_read < self.n_rows {
            let in_path = std::path::PathBuf::from(&self.path);
            // dbg!(&self.md.schema);
            // dbg!("n_rows_read = {}", self.n_rows_read);
            // dbg!("size_hint = {}", self.size_hint);

            let rows_to_read = min(self.size_hint,self.n_rows - self.n_rows_read);

            let df = Python::with_gil(|py| {
                py.allow_threads(|| {
                    read_chunks_parallel(
                        in_path,
                        Some(&self.md),
                        Some(self.n_rows_read as u32),
                        Some(rows_to_read as u32),
                        self.with_columns.clone(),
                        self.threads
                    ).unwrap()
                })
            });
            // let mut df = read_chunks_parallel(
            //     in_path,
            //     Some(&self.md),
            //     Some(self.n_rows_read as u32),
            //     Some(rows_to_read as u32),
            //     self.with_columns.clone(),
            //     self.threads
            // ).unwrap();
            // dbg!(&df);


            // Apply predicate pushdown.
            // This is done after the fact, but there could be sources where this could be applied
            // lower.
            // if let Some(predicate) = &self.predicate {
            //     df = df
            //         .lazy()
            //         .filter(predicate.clone())
            //         ._with_eager(true)
            //         .collect()
            //         .map_err(PyPolarsErr::from)?;
            // }
            
            //  Update number of rows read
            self.n_rows_read = self.n_rows_read + self.size_hint;
            Ok(Some(PyDataFrame(df)))
        } else {
            Ok(None)
        }
    }
}


#[pymodule]
fn polars_readstat_rs(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<read_readstat>().unwrap();
    Ok(())
}

