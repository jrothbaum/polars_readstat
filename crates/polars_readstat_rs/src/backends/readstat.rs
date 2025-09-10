use log::error;
use polars::prelude::*;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};


use readstat::{
    ReadStatMetadata,
    ReadStatData, 
    ReadStatPath,
    ReadStatStreamer,
    ReadStatCompress,
    ReadStatVarTypeClass,
    ReadStatVarFormatClass,
    LabelValue,
    SharedMmap
};
use crate::backends::{ReaderBackend};
use crate::metadata::{
    Metadata,
    MetadataFileInfo,
    MetadataColumnInfo,
};

pub struct ReadStatBackend {
    rsp: ReadStatPath,
    size_hint: usize,
    with_columns: Option<Vec<String>>,
    threads: usize,
    pub md: Option<ReadStatMetadata>,
    rsd: Option<Arc<Mutex<ReadStatData>>>,
    _metadata: Option<Metadata>,
    

    streamer: Option<Arc<Mutex<ReadStatStreamer>>>,
    is_started:bool,
    is_complete:Option<Arc<Mutex<bool>>>,
    notifier:Option<Arc<Condvar>>,

    // Cancellation flag
    cancel_flag: Arc<AtomicBool>,

    use_mmap:bool,
    pub shared_mmap: Option<SharedMmap>,
}


impl ReaderBackend for ReadStatBackend {
    fn schema(&mut self) -> Result<&Schema, Box<dyn std::error::Error>> {
        if self.md.is_none() {
            self.read_metadata(false);
        }
        
        self.md.as_ref()
            .map(|md| &md.schema)
            .ok_or_else(|| "Schema not available".into())
    }

    fn metadata(&mut self) -> Result<&crate::metadata::Metadata, Box<dyn std::error::Error>> {
        if self.md.is_none() {
            self.read_metadata(false);
        }

        match &self._metadata {
            Some(meta) => Ok(meta),
            None => Err("Schema not available".into()),
        }
    }

    fn set_columns_to_read(
        &mut self,
        columns:Option<Vec<String>>,
    ) -> PolarsResult<()> {
        self.with_columns = columns;
        
        Ok(())
    }
    
    fn initialize_reader(
        &mut self,
        row_start:usize,
        row_end:usize,
    ) -> PolarsResult<()>{
        let schema = self.schema().unwrap().clone();
        let (mut consumer, chunk_buffer, notifier, is_complete) = ReadStatStreamer::new();
        
        self.is_complete = Some(is_complete);
        self.notifier = Some(notifier.clone());

        let column_indices: Option<Vec<usize>> = self.with_columns.as_ref().map(|cols| {
            cols.iter()
                .filter_map(|col_name| schema.index_of(col_name))
                .collect()
        });
        let mut rsd = ReadStatData::new(column_indices)
            .init(
                self.md.as_ref().unwrap().clone(),
                row_start as u32,
                row_end as u32,
                self.size_hint,
                chunk_buffer,
                notifier
            );

        // Set the cancel flag in ReadStatData
        rsd.cancel_flag = Arc::clone(&self.cancel_flag);
        let rsd_arc = Arc::new(Mutex::new(rsd));
        self.rsd = Some(rsd_arc.clone());
        self.streamer = Some(Arc::new(Mutex::new(consumer)));
        Ok(())
    }

    fn next(&mut self) -> PolarsResult<Option<DataFrame>> {
        if !self.is_started {
            //  println!("Starting streaming...");
            self.start_streaming();
            self.is_started = true;
        }

        if let Some(streamer) = &self.streamer {
            if let Some(is_complete) = &self.is_complete {
                let complete_status = *is_complete.lock().unwrap();
                //  println!("Background thread complete: {}", complete_status);
            }

            let mut s = self.streamer.as_mut().unwrap().lock().unwrap();
            
            match s.next() {
                Some(df) => {
                    //  println!("SUCCESS: Got dataframe with {} rows", df.height());
                    Ok(Some(df))
                }
                None => {
                    //  println!("s.next() returned None");
                    Ok(None)
                }
            }
        } else {
            println!("No streamer available");
            Ok(None)
        }
    }

    fn cancel(&mut self) -> PolarsResult<()>{
        self.cancel_flag.store(true, Ordering::Relaxed);

        Ok(())
    }
}

impl ReadStatBackend {
    pub fn new(
        path: String,
        size_hint: usize,
        with_columns: Option<Vec<String>>,
        threads: usize,
        md: Option<ReadStatMetadata>,
        _metadata: Option<Metadata>,
        use_mmap:bool,
    ) -> Self {
        return ReadStatBackend { 
            rsp: ReadStatPath::new(PathBuf::from(path.clone())).unwrap(), 
            size_hint: size_hint, 
            with_columns: with_columns, 
            threads: threads, 
            md: md,
            _metadata: _metadata,
            rsd: None,
            streamer: None,
            is_started: false,
            is_complete: None,
            notifier: None,
            cancel_flag: Arc::new(AtomicBool::new(false)),

            shared_mmap: None,
            use_mmap:use_mmap,
        }
    }

    fn read_metadata(
        &mut self,
        skip_row_count:bool,
    ) {
        // Instantiate ReadStatMetadata
        let mut md = ReadStatMetadata::new();

        // Read metadata
        let _ = md.read_metadata(&self.rsp, skip_row_count);

        self.md = Some(md.clone());

        let md_for_labels = md.clone();
        
        let file_info = MetadataFileInfo {
            n_rows: md.row_count as u64,
            n_cols: md.var_count as u32,
            size: None,
            page_size: None,
            page_count: None,
            header_length: None,
            row_length: None,
            compression: Some(match md.compression {
                ReadStatCompress::None => "None".to_string(),
                ReadStatCompress::Rows => "RLE".to_string(),
                ReadStatCompress::Binary => "RDC".to_string(),
            }),
            name: if md.table_name.is_empty() { 
                None 
            } else { 
                Some(md.table_name) 
            },
            encoding: if md.file_encoding.is_empty() { 
                None 
            } else { 
                Some(md.file_encoding) 
            },
            file_type: Some(md.extension),
            sas_release: Some(md.version.to_string()),
            sas_server_type: None,
            os_name: None,
            creator: None,
            creator_proc: None,
            created_date: parse_datetime_to_timestamp(&md.creation_time),
            modified_date: parse_datetime_to_timestamp(&md.modified_time),
        };

        let mut column_info: Vec<MetadataColumnInfo> = md.vars
            .into_iter()
            .map(|(index, var_meta)| {
                // Get value labels for this variable if they exist
                let value_labels = md_for_labels.get_value_labels_for_var(&var_meta.var_name)
                    .map(|labels| {
                        labels.labels.iter()
                            .map(|(key, value)| {
                                let key_str = match key {
                                    LabelValue::String(s) => s.clone(),
                                    LabelValue::Int32(i) => i.to_string(),
                                    LabelValue::Int64(i) => i.to_string(),
                                    LabelValue::Float32Bits(bits) => f32::from_bits(*bits).to_string(),
                                    LabelValue::Float64Bits(bits) => f64::from_bits(*bits).to_string(),
                                    LabelValue::TaggedMissing(c) => format!("MISSING_{}", c),
                                };
                                (key_str, value.clone())
                            })
                            .collect()
                    });

                MetadataColumnInfo {
                    name: var_meta.var_name,
                    index: index as u32,
                    type_class: Some(match var_meta.var_format_class {
                        Some(ReadStatVarFormatClass::Date) => "date".to_string(),
                        Some(ReadStatVarFormatClass::DateTime) => "datetime".to_string(), 
                        Some(ReadStatVarFormatClass::Time) => "time".to_string(),
                        None => match var_meta.var_type_class {
                            ReadStatVarTypeClass::String => "string".to_string(),
                            ReadStatVarTypeClass::Numeric => "number".to_string(),
                        }
                    }),
                    format: if var_meta.var_format.is_empty() { 
                        None 
                    } else { 
                        Some(var_meta.var_format) 
                    },
                    label: if var_meta.var_label.is_empty() { 
                        None 
                    } else { 
                        Some(var_meta.var_label) 
                    },
                    length: None,
                    value_labels,
                }
            })
            .collect();

        // Sort by index to ensure consistent ordering
        column_info.sort_by_key(|col| col.index);

        self._metadata = Some(Metadata {
            file_info,
            column_info,
        });

        ()
    }

    fn start_streaming(&mut self) -> PolarsResult<()> {
        let rsd_clone = Arc::clone(self.rsd.as_ref().unwrap());
        let rsp_clone = self.rsp.clone(); // Assuming rsp implements Clone
        
        let is_complete_clone = Arc::clone(self.is_complete.as_ref().unwrap());
        let notifier_clone = Arc::clone(self.notifier.as_ref().unwrap());

        let shared_mmap = if self.use_mmap {
            Some(SharedMmap::new(&rsp_clone.path.to_str().unwrap()).unwrap())
        } else {
            None
        };
        
    

        std::thread::spawn(move || {
            // Give consumer time to be called
            let mut data_guard = rsd_clone.lock().unwrap();
            match data_guard.read_data(
                &rsp_clone,
                //  None    
                shared_mmap.clone().as_ref()
            ) {
            //  match data_guard.read_data(&rsp_clone,None) {   
                Ok(_) => {
                    //  println!("Background thread: read_data completed successfully");   
                    ()
                },
                Err(e) => error!("read_data failed: {}", e),
            }
            
            *is_complete_clone.lock().unwrap() = true;
            notifier_clone.notify_all();
            //  println!("Background thread: set is_complete = true");
            drop(data_guard);
        });
        
        Ok(())
    }


    pub fn set_mmap(
        &mut self,
        shared_mmap: Option<SharedMmap>
    ) -> PolarsResult<()> {
        self.shared_mmap = shared_mmap;

        Ok(())
    }
}

impl Drop for ReadStatBackend {
    fn drop(&mut self) {
        self.cancel(); // Just set the flag and exit
    }
}

fn parse_datetime_to_timestamp(datetime_str: &str) -> Option<i64> {
    if datetime_str.is_empty() {
        return None;
    }
    
    // Parse "2018-08-16 16:21:52" format
    if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(datetime_str, "%Y-%m-%d %H:%M:%S") {
        Some(naive_dt.and_utc().timestamp())
    } else {
        None
    }
}