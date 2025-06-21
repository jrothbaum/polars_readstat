use std::{env, path::PathBuf};
use std::sync::Mutex;
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::{PyDataFrame, PySchema};


use cpp_sas7bdat::{
    SasReader,
    SasBatchIterator
};

#[pyclass]
pub struct CppSasScan {
    path:PathBuf,
    schema:Schema,
    iterator: Mutex<Option<SasBatchIterator>>,
}

impl CppSasScan {
    pub fn new(
        path:PathBuf,
        batch_size: Option<u32>,
        start_row:Option<u64>,
        end_row:Option<u64>,
        columns:Option<Vec<String>>,
    ) -> PolarsResult<Self> {
        let batch_size_or_default = batch_size.unwrap_or(100_000);
        
        let schema = match SasReader::read_sas_schema(path.to_str().unwrap()) {
            Ok(schema_read) => {
                schema_read
            }
            Err(e) => {
                println!("Failed to get schema: {}", e);
                return Err(e);
            }
        };
        
        let mut iterator = match cpp_sas7bdat::SasBatchIterator::new(
            path.to_str().unwrap(), 
            Some(batch_size_or_default),
            columns,
            start_row,
            end_row,
        ) {
            Ok(new_iterator) => {
                new_iterator
            }
            Err(e) => {
                println!("Failed to get iterator: {}", e);
                return Err(e);
            }
        };

        Ok(CppSasScan {
            path,
            schema,
            iterator: Mutex::new(Some(iterator)), // Wrap in Mutex::new(Some(...))
        })
    }
}

unsafe impl Send for CppSasScan {}
unsafe impl Sync for CppSasScan {}

impl AnonymousScan for CppSasScan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    

    fn schema(
        &self,
        _infer_schema_length: Option<usize>,
    ) -> Result<Arc<Schema>, PolarsError> {
        match SasReader::read_sas_schema(self.path.to_str().unwrap()) {
            Ok(schema_read) => {
                Ok(Arc::new(schema_read))
            }
            Err(e) => {
                println!("Failed to get schema: {}", e);
                Err(e)
            }
        }
    }

    #[allow(unused)]
    fn scan(&self, scan_opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let mut dataframes = Vec::new();
        let mut total_rows = 0;
        
        let mut iterator_guard = self.iterator.lock()
            .map_err(|e| PolarsError::ComputeError(
                format!("Failed to acquire iterator lock: {}", e).into()
            ))?;
        
        if let Some(ref mut sas_iter) = *iterator_guard {
            for (i, batch_result) in sas_iter.enumerate() {
                let df = match batch_result {
                    Ok(df) => {
                        println!("Batch {}: DataFrame shape: {:?}", i, df.shape());
                        total_rows += df.height();
                        df
                    },
                    Err(e) => {
                        println!("Polars error in batch {}: {}", i, e);
                        // You might want to return the error instead of continuing
                        // return Err(e);
                        DataFrame::empty_with_schema(&self.schema)
                    }
                };
                
                if df.height() > 0 {
                    dataframes.push(df.lazy());
                }
            }
        } else {
            return Ok(DataFrame::empty_with_schema(&self.schema));
        }
        
        println!("Total rows processed: {}", total_rows);
        
        if dataframes.is_empty() {
            Ok(DataFrame::empty_with_schema(&self.schema))
        } else {
            let lf = polars::prelude::concat(dataframes, UnionArgs::default())?;
            lf.collect()
        }
    }
    
    #[allow(unused)]
    fn next_batch(
        &self,
        scan_opts: AnonymousScanArgs,
    ) -> PolarsResult<Option<DataFrame>> {
        let mut iterator_guard = self.iterator.lock().unwrap();
        
        if let Some(ref mut iter) = *iterator_guard {
            // Get the next batch from the iterator
            match iter.next() {
                Some(batch_result) => {
                    match batch_result {
                        Ok(df) => {
                            Ok(Some(df))
                        }
                        Err(e) => {
                            println!("Polars error: {}", e);
                            Err(e)
                        }
                    }
                }
                None => {
                    // Iterator is exhausted - no more batches
                    //  println!("No more batches available");
                    Ok(None)
                }
            }
        } else {
            // Iterator was not initialized or already consumed
            Ok(None)
        }
    }
    
    fn allows_predicate_pushdown(&self) -> bool {
        false
    }
    fn allows_projection_pushdown(&self) -> bool {
        true
    }
    fn allows_slice_pushdown(&self) -> bool {
        false
    }
}



