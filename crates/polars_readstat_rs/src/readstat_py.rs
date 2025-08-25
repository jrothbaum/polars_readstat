use log::{info, error};

use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::{
    PyDataFrame, 
    PySchema
};
use pyo3::types::{PyDict, PyList};

use crate::read::{
    read_metadata
};

use std::cmp::min;

use readstat::{
    ReadStatMetadata,
    ReadStatData,
    ReadStatPath,
    ReadStatStreamer
};
use std::sync::{Arc, Mutex};
use std::thread;



#[pyclass]
pub struct read_readstat {
    path:String,
    size_hint: usize,
    n_rows: usize,
    with_columns: Option<Vec<usize>>,
    n_rows_read: usize,
    md:ReadStatMetadata,
    rsd: Option<Arc<Mutex<ReadStatData>>>,                       // producer side must be kept alive
    
    // Streaming components
    streamer: Option<Arc<Mutex<ReadStatStreamer>>>,
    is_started: bool,
}

#[pymethods]
impl read_readstat {
    #[new]
    #[pyo3(signature = (path, size_hint, n_rows
    ))]
    fn new_source(
        path:String,
        size_hint: Option<usize>,
        n_rows: Option<usize>,
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
            with_columns: None,
            n_rows_read: n_rows_read,
            md:md,
            rsd:None,
            streamer: None,
            is_started: false,
        }
    }

    fn schema(&mut self) -> PySchema {
        PySchema(Arc::new(self.md.clone().schema_with_filter_pushdown(
            self.with_columns.clone()
        )))
    }

    
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
    
    /// Start the streaming if not already started
    fn start_streaming(&mut self) -> PyResult<()> {
        //  println!("start_streaming called, is_started: {}", self.is_started);

        if !self.is_started {
            //  println!("Creating ReadStatStreamer...");
            let (consumer, chunk_buffer, notifier, is_complete) = ReadStatStreamer::new();
            //  println!("ReadStatStreamer created");

            // clone BEFORE moving `notifier` into data.init
            let completion_notifier = notifier.clone();

            let path_buf = std::path::PathBuf::from(&self.path);
            //  println!("Path: {:?}", path_buf);

            let rsp = ReadStatPath::new(path_buf).map_err(|e| {
                error!("Path error: {}", e);
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Path error: {}", e))
            })?;
            //  println!("ReadStatPath created");

            let mut data = ReadStatData::new(self.with_columns.clone());
            //  println!("ReadStatData created, calling init...");
            //  println!("{:?}",self.n_rows);
            data = data.init(
                self.md.clone(),
                0,
                (self.md.row_count as u32).saturating_sub(1) as u32,
                self.size_hint,
                chunk_buffer,
                notifier,              // original goes into the producer
            );

            let data_arc = Arc::new(Mutex::new(data));
            self.rsd = Some(data_arc.clone());
            //  println!("ReadStatData initialized");

            let is_complete_clone = is_complete.clone();
            //  println!("About to spawn background thread...");

            std::thread::spawn(move || {
                //  println!("Background thread started");

                let mut data_guard = data_arc.lock().unwrap();
                match data_guard.read_data(&rsp) {
                    Ok(_) => (), //println!("read_data completed successfully"),
                    Err(e) => error!("read_data failed: {}", e),
                }
                // Release the lock
                drop(data_guard);
                // 1) mark complete
                *is_complete_clone.lock().unwrap() = true;

                // 2) and WAKE any sleeping consumers
                completion_notifier.notify_all();

                //  println!("Background thread finished");
            });

            //  println!("Background thread spawned, setting up consumer...");
            self.streamer = Some(Arc::new(Mutex::new(consumer)));
            self.is_started = true;
            //  println!("start_streaming completed");
        }
        Ok(())
    }

    fn next(&mut self) -> PyResult<Option<PyDataFrame>> {
        //  println!("next() called");
        
        if !self.is_started {
            //  println!("Not started, calling start_streaming...");
            self.start_streaming()?;
            //  println!("start_streaming returned");
        }
        
        Python::with_gil(|py| {
            py.allow_threads(|| {
                if let Some(streamer) = &self.streamer {
                    //  println!("About to call streamer.next()");
            
                    let mut s = streamer.lock().unwrap();
                    if let Some(df) = s.next() {
                        //  println!("Got dataframe: {:?}", df);  // More detailed output like Rust version
                        self.n_rows_read += df.height();
                        Ok(Some(PyDataFrame(df)))
                    } else {
                        //  println!("consumer.next() returned None");
                        Ok(None)
                    }
                } else {
                    //  println!("No streamer available");
                    Ok(None)
                }
            })
        })
    }

    // Get metadata as a Python dictionary
    // fn get_metadata(&self, py: Python) -> PyResult<PyObject> {
    //     let metadata_dict = PyDict::new(py);
        
    //     // Basic file info
    //     metadata_dict.set_item("row_count", self.md.row_count)?;
    //     metadata_dict.set_item("var_count", self.md.var_count)?;
    //     metadata_dict.set_item("table_name", &self.md.table_name)?;
    //     metadata_dict.set_item("file_label", &self.md.file_label)?;
    //     metadata_dict.set_item("file_encoding", &self.md.file_encoding)?;
    //     metadata_dict.set_item("version", self.md.version)?;
    //     metadata_dict.set_item("is64bit", self.md.is64bit)?;
    //     metadata_dict.set_item("creation_time", &self.md.creation_time)?;
    //     metadata_dict.set_item("modified_time", &self.md.modified_time)?;
    //     metadata_dict.set_item("compression", format!("{:?}", self.md.compression))?;
        
    //     // Endianness
    //     metadata_dict.set_item("endianness", format!("{:?}", self.md.endianness))?;
        
    //     // Variables info
    //     let vars_list = PyList::empty(py);
    //     for (_, var_meta) in &self.md.vars {
    //         let var_dict = PyDict::new(py);
    //         var_dict.set_item("name", &var_meta.var_name)?;
    //         var_dict.set_item("type", format!("{:?}", var_meta.var_type))?;
    //         var_dict.set_item("type_class", format!("{:?}", var_meta.var_type_class))?;
    //         var_dict.set_item("label", &var_meta.var_label)?;
    //         var_dict.set_item("format", &var_meta.var_format)?;
    //         var_dict.set_item("format_class", match &var_meta.var_format_class {
    //             Some(fc) => format!("{:?}", fc).into_py(py),
    //             None => py.None(),
    //         })?;
            
    //         vars_list.append(var_dict)?;
    //     }
    //     metadata_dict.set_item("variables", vars_list)?;
        
    //     Ok(metadata_dict.into())
    // }
    
    /// Get column info as a simple dictionary (name -> type)
    fn get_column_info(&self, py: Python) -> PyResult<PyObject> {
        // Variables info
        let vars_list = PyList::empty(py);
        for (_, var_meta) in &self.md.vars {
            let var_dict = PyDict::new(py);
            var_dict.set_item("name", &var_meta.var_name)?;
            var_dict.set_item("type", format!("{:?}", var_meta.var_type))?;
            var_dict.set_item("type_class", format!("{:?}", var_meta.var_type_class))?;
            var_dict.set_item("label", &var_meta.var_label)?;
            var_dict.set_item("format", &var_meta.var_format)?;
            var_dict.set_item("format_class", match &var_meta.var_format_class {
                Some(fc) => format!("{:?}", fc).into_py(py),
                None => py.None(),
            })?;
            
            vars_list.append(var_dict)?;
        }

        Ok(vars_list.into())
    }
    
    /// Get basic file stats
    fn get_file_info(&self, py: Python) -> PyResult<PyObject> {
        let info_dict = PyDict::new(py);
        info_dict.set_item("rows", self.md.row_count)?;
        info_dict.set_item("columns", self.md.var_count)?;
        info_dict.set_item("table_name", &self.md.table_name)?;
        info_dict.set_item("encoding", &self.md.file_encoding)?;
        info_dict.set_item("created", &self.md.creation_time)?;
        info_dict.set_item("modified", &self.md.modified_time)?;
        Ok(info_dict.into())
    }
}

