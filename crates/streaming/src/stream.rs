use std::sync::{Arc, Mutex, mpsc};
use std::collections::{HashMap, BTreeMap};
use std::thread;
use std::any::Any;
use std::cmp::min;
use polars::prelude::*;
use polars::functions::concat_df_horizontal;
use polars_core::utils::arrow::io::ipc::format::ipc::Schema;

use crate::read::Reader;
use crate::metadata::{self, Metadata};

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct ChunkCoordinate {
    row_partition: usize,
    col_partition: usize,
    col_chunk: usize,
}

struct ChunkResult {
    coordinate: ChunkCoordinate,
    data: Option<DataFrame>, // None signals completion
}

struct ScanState {
    // Key: The chunk index (e.g., 0 for rows 0-999).
    // Value: A map of column partitions received for that chunk,
    // keyed by the partition index (0, 1, 2...) to keep them in order.
    buffer: HashMap<usize, BTreeMap<usize, DataFrame>>,
    
    // The index of the next complete chunk we should assemble and return.
    next_chunk_to_emit: usize,
    
    // The number of column partitions we expect for each chunk.
    p_col: usize,

    // To know when all threads are done
    completed_threads: usize,
    total_threads: usize,
}

pub struct PolarsReadstat {
    reader:Mutex<Reader>,
    thread_handle: Mutex<Option<mpsc::Receiver<ChunkResult>>>,
    scan_state: Mutex<Option<ScanState>>,
    p_col: Mutex<Option<usize>>,
    p_row: Mutex<Option<usize>>,
}

unsafe impl Send for PolarsReadstat {}
unsafe impl Sync for PolarsReadstat {}

impl AnonymousScan for PolarsReadstat {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn scan(
        &self, 
        scan_opts: AnonymousScanArgs
    ) -> PolarsResult<DataFrame> {

        //  Call next_batch in a while loop to read the whole file
        todo!()   
    }
    fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef> {
        let mut reader = self.reader.lock().unwrap();
        let _schema = reader.schema().unwrap().clone();
        Ok(Arc::new(_schema))
    }
    
    fn next_batch(&self, scan_opts: AnonymousScanArgs) -> PolarsResult<Option<DataFrame>> {
        let mut state_guard = self.scan_state.lock().unwrap();

        // One-time initialization of threads and state
        if state_guard.is_none() {
            drop(state_guard); // Avoid deadlock
            self.initialize_reader(scan_opts)?; // Your original init function is good for this
            
            // Re-lock and populate the initial state
            state_guard = self.scan_state.lock().unwrap();
            let p_col = *self.p_col.lock().unwrap().as_ref().unwrap();
            let p_row = *self.p_row.lock().unwrap().as_ref().unwrap();

            *state_guard = Some(ScanState {
                buffer: HashMap::new(),
                next_chunk_to_emit: 0,
                p_col: p_col,
                completed_threads: 0,
                total_threads: p_col * p_row,
            });
        }
        
        let state = state_guard.as_mut().unwrap();
        let receiver = self.thread_handle.lock().unwrap(); // Assuming this is initialized
        let receiver = receiver.as_ref().unwrap();

        loop {
            // First, check if the next chunk we need is already complete in the buffer
            if let Some(partitions) = state.buffer.get(&state.next_chunk_to_emit) {
                if partitions.len() == state.p_col {
                    // It's complete! Assemble and return it.
                    let partitions = state.buffer.remove(&state.next_chunk_to_emit).unwrap();
                    let dfs: Vec<DataFrame> = partitions.into_values().collect();
                    
                    // Horizontally stack the dataframes to form a complete chunk
                    let result_df = concat_df_horizontal(&dfs,false)?;

                    state.next_chunk_to_emit += 1;
                    return Ok(Some(result_df));
                }
            }
            
            // If we are here, the next chunk is not ready yet.
            // We must receive more data from the threads.
            match receiver.recv() {
                Ok(chunk_result) => {
                    // Check for completion signal
                    if chunk_result.coordinate.col_chunk == usize::MAX {
                        state.completed_threads += 1;
                        continue;
                    }

                    if let Some(df) = chunk_result.data {
                        // Insert the received piece into the buffer
                        state.buffer
                            .entry(chunk_result.coordinate.col_chunk)
                            .or_default()
                            .insert(chunk_result.coordinate.col_partition, df); // You'll need a col partition index
                    }
                },
                Err(_) => {
                    // All threads are done. The channel is empty.
                    // Is there one last complete chunk in the buffer?
                    if state.buffer.get(&state.next_chunk_to_emit).is_some() {
                        // Loop back to the top to process the final buffered chunk
                        continue; 
                    } else {
                        // No more chunks can be formed. We are finished.
                        return Ok(None);
                    }
                }
            }
        }
    }

    fn allows_predicate_pushdown(&self) -> bool {
        false
    }

    fn allows_projection_pushdown(&self) -> bool {
        true
    }
    
    fn allows_slice_pushdown(&self) -> bool {
        true
    }    
}

impl PolarsReadstat {
    pub fn new(
        path: String,
        size_hint: usize,
        with_columns: Option<Vec<String>>,
        threads: usize,
        engine: String,
    ) -> Self {
        let reader = Reader::new(
            path,
            size_hint,
            with_columns,
            threads,
            engine,
            None,
            None,
            None
        );

        PolarsReadstat { 
            reader: Mutex::new(reader),
            thread_handle: Mutex::new(None),
            scan_state: Mutex::new(None),
            p_col: Mutex::new(None),
            p_row: Mutex::new(None),
        }
    }

    pub fn metadata(&self) -> PolarsResult<Arc<Metadata>> {
        let mut reader = self.reader.lock().unwrap();
        let _metadata = reader.metadata().unwrap().clone().unwrap();
        Ok(Arc::new(_metadata))
    }

    fn initialize_reader(
        &self,
        scan_opts: AnonymousScanArgs
    )  -> PolarsResult<()> {
        let mut reader = self.reader.lock().unwrap();
        //  Make sure the schema and metadata are loaded
       
        let (schema, sub_schema, chunk_size, threads) = {
            let mut reader = self.reader.lock().unwrap();
            let schema = reader.schema().unwrap().clone();
            let sub_schema = reader.schema_with_projection_pushdown().unwrap().clone();
            let chunk_size = reader.size_hint;
            let threads = reader.threads;
            (schema, sub_schema, chunk_size, threads)
        }; 

        let n_rows = match scan_opts.n_rows {
            Some(n) => n,
            None => {
                let n_rows = match reader.metadata().unwrap() {
                    Some(metadata) => metadata.file_info.n_rows,
                    None => return Err(PolarsError::SchemaFieldNotFound("No metadata available".into())),
                };

                n_rows as usize
            }
        };

        let n_cols = sub_schema.len();
        //  Integer ceiling division
        let n_chunks = (n_rows + chunk_size - 1) / chunk_size;

        let (mut p_col, mut p_row);
        if n_cols >= threads {
            p_col = threads;
            p_row = 1;
        } else if n_cols >= threads / 2 {
            p_col = n_cols;
            p_row = min(n_chunks,threads / n_cols);
        } else {
            p_col = 1;
            p_row = min(n_chunks,threads);
        }
 
        *self.p_col.lock().unwrap() = Some(p_col);
        *self.p_row.lock().unwrap() = Some(p_row);

        
        //  Spawn threads here
        let (sender, receiver) = mpsc::channel::<ChunkResult>();
        // Store the receiver
        *self.thread_handle.lock().unwrap() = Some(receiver);


        let chunks_per_partition = (n_chunks + p_row - 1) / p_row;
        let cols_per_partition = (n_cols + p_col - 1)/p_col;
        let row_per_chunk = chunks_per_partition*chunk_size;

        for rowi  in 0..p_row {
            let chunk_start = chunks_per_partition*rowi;
            let row_start = rowi*row_per_chunk;
            let row_end = min(row_start + row_per_chunk,n_rows);

            for coli  in 0..p_col {
                let col_start = coli*cols_per_partition;
                let col_end = min(col_start + cols_per_partition, n_cols);

                let partition_columns: Vec<String> = schema.iter()
                    .map(|(name, _)| name.clone().into_string())
                    .skip(col_start)
                    .take(col_end - col_start)
                    .collect();


                let reader_thread = reader.copy_for_reading();
                self.initialize_thread(
                    reader_thread,
                    partition_columns,
                    row_start, 
                    row_end, 
                    chunk_start, 
                    coli,
                    rowi, 
                    sender.clone()
                );
            }
        }
        Ok(())
    }


    fn initialize_thread(
        &self,
        reader_thread: Reader,
        with_columns:Vec<String>,
        row_start: usize,
        row_end: usize,
        chunk_start: usize,
        col_index: usize,
        row_index: usize,
        sender: mpsc::Sender<ChunkResult>,
    ) -> PolarsResult<()> {
        
        

        thread::spawn(move || {
            let mut reader_thread = reader_thread;
            reader_thread.with_columns = Some(with_columns);

            if let Err(e) = reader_thread.initialize_reader(row_start, row_end) {
                eprintln!("Thread ({}, {}) failed to initialize: {}", row_index, col_index, e);
                return;
            }
            
            let mut col_chunk = 0;
            while let Ok(Some(df)) = reader_thread.next() {
                let chunk_result = ChunkResult {
                    coordinate: ChunkCoordinate {
                        row_partition: row_index,
                        col_partition: col_index,
                        col_chunk: chunk_start + col_chunk,
                    },
                    data: Some(df),
                };
                
                if sender.send(chunk_result).is_err() {
                    break;
                }
                col_chunk += 1;
            }
            
            // Send completion signal
            let _ = sender.send(ChunkResult {
                coordinate: ChunkCoordinate { 
                    row_partition: row_index, 
                    col_chunk: usize::MAX,
                    col_partition: col_index
                },
                data: None,
            });
        });
        
        Ok(())
    }
}

