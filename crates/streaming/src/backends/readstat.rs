use log::{
    debug,
    error
};
use polars::prelude::*;
use polars_core::schema;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use path_abs::{PathAbs, PathInfo};


use readstat::{ReadStatMetadata, ReadStatData, ReadStatPath, ReadStatStreamer};
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
    
    fn initialize_reader(
        &mut self,
        row_start:usize,
        row_end:usize,
    ) -> PolarsResult<()>{
        let schema = self.schema();
        let (mut consumer, chunk_buffer, notifier, is_complete) = ReadStatStreamer::new();
        
        self.is_complete = Some(is_complete);
        self.notifier = Some(notifier.clone());
        let mut rsd = ReadStatData::new(None)
            .init(
                self.md.as_ref().unwrap().clone(),
                row_start as u32,
                row_end as u32,
                self.size_hint,
                chunk_buffer,
                notifier
            );

            
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
}

impl ReadStatBackend {
    pub fn new(
        path: String,
        size_hint: usize,
        with_columns: Option<Vec<String>>,
        threads: usize,
        md: Option<ReadStatMetadata>,
    ) -> Self {
        return ReadStatBackend { 
            rsp: ReadStatPath::new(PathBuf::from(path.clone())).unwrap(), 
            size_hint: size_hint, 
            with_columns: with_columns, 
            threads: threads, 
            md: md,
            _metadata: None,
            rsd: None,
            streamer: None,
            is_started: false,
            is_complete: None,
            notifier: None,
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

        self.md = Some(md.to_owned());
    }

    fn start_streaming(&mut self) -> PolarsResult<()> {
        let rsd_clone = Arc::clone(self.rsd.as_ref().unwrap());
        let rsp_clone = self.rsp.clone(); // Assuming rsp implements Clone
        
        let is_complete_clone = Arc::clone(self.is_complete.as_ref().unwrap());
        let notifier_clone = Arc::clone(self.notifier.as_ref().unwrap());

        std::thread::spawn(move || {
            // Give consumer time to be called
            let mut data_guard = rsd_clone.lock().unwrap();
            match data_guard.read_data(&rsp_clone) {
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

}