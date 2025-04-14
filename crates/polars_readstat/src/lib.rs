use log::{debug, info, warn, error};

mod read;
use polars_lazy::frame::IntoLazy;
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::error::PyPolarsErr;
use pyo3_polars::export::polars_plan::dsl::Expr;
use pyo3_polars::{PyDataFrame, PySchema,PyExpr};
use rayon::iter::empty;
use read::{
    read_metadata,
    read_chunk
};
use std::env;
use std::thread;

use readstat::ReadStatMetadata;

#[pyclass]
pub struct read_readstat {
    path:String,
    columns: Vec<String>,
    size_hint: usize,
    n_rows: usize,
    //  threads:Option<usize>,
    predicate: Option<Expr>,
    with_columns: Option<Vec<usize>>,
    n_rows_read: usize,
    md:ReadStatMetadata
}

#[pymethods]
impl read_readstat {
    #[new]
    #[pyo3(signature = (path, columns, size_hint, n_rows, 
        //  threads
    ))]
    fn new_source(
        path:String,
        columns: Option<Vec<String>>,
        size_hint: Option<usize>,
        n_rows: Option<usize>,
        //  threads: Option<usize>
    ) -> Self {
        let columns = columns.unwrap_or(Vec::new());
        
        let size_hint = size_hint.unwrap_or(10_000);
        //  let threads = configure_threads(threads);
        let n_rows_read = 0 as usize;

        //  Pre-fetch the metadata from the file, to populate the schema
        let md = read_metadata(std::path::PathBuf::from(path.clone()), false).unwrap();
        let n_rows = n_rows.unwrap_or(md.row_count as usize);

        Self {
            path,
            columns,
            size_hint,
            n_rows,
            //  threads:Some(threads),
            predicate: None,
            with_columns: None,
            n_rows_read: n_rows_read,
            md:md
        }
    }

    fn schema(&mut self) -> PySchema {
        PySchema(Arc::new(self.md.schema.clone()))
    }

    fn try_set_predicate(&mut self, predicate: PyExpr) {
        self.predicate = Some(predicate.0);
    }

    fn set_with_columns(&mut self, columns: Vec<String>) {
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
        
            // let s_iter = if let Some(idx) = &self.with_columns {
            //     Box::new(idx.iter().copied().map(|i| &self.columns[i]))
            //         as Box<dyn Iterator<Item = _>>
            // } else {
            //     Box::new(self.columns.iter())
            // };

            // let columns = s_iter
            //     .map(|s| {
            //         let mut s = s.0.lock().unwrap();

            //         // Apply slice pushdown.
            //         // This prevents unneeded sampling.
            //         s.next_n(std::cmp::min(self.size_hint, self.n_rows))
            //             .into_column()
            //     })
            //     .collect::<Vec<_>>();

            let in_path = std::path::PathBuf::from(&self.path);
            // dbg!(&self.md.schema);
            // dbg!("n_rows_read = {}", self.n_rows_read);
            // dbg!("size_hint = {}", self.size_hint);
            let mut df = read_chunk(
                in_path,
                Some(&self.md),
                Some(self.n_rows_read as u32),
                Some(self.size_hint as u32)
            ).unwrap();
            // dbg!(&df);


            // Apply predicate pushdown.
            // This is done after the fact, but there could be sources where this could be applied
            // lower.
            if let Some(predicate) = &self.predicate {
                df = df
                    .lazy()
                    .filter(predicate.clone())
                    ._with_eager(true)
                    .collect()
                    .map_err(PyPolarsErr::from)?;
            }
            
            //  Update number of rows read
            self.n_rows_read = self.n_rows_read + self.size_hint;
            Ok(Some(PyDataFrame(df)))
        } else {
            Ok(None)
        }
    }
}


// fn configure_threads(threads: Option<usize>) -> usize {
//     threads.unwrap_or_else(|| {
//         env::var("NUM_THREADS")
//             .ok()
//             .and_then(|s| s.parse::<usize>().ok())
//             .unwrap_or_else(|| thread::available_parallelism().unwrap().get())
//     })
// }

#[pymodule]
fn polars_readstat(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<read_readstat>().unwrap();
    Ok(())
}

