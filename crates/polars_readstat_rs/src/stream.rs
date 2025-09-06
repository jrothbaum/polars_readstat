use std::sync::{Arc, Mutex, mpsc};
use std::collections::{HashMap, BTreeMap};
use std::thread;
use std::cmp::min;
use std::time::Duration;
use polars::prelude::*;
use polars::functions::concat_df_horizontal;

use crate::read::Reader;
use crate::metadata::Metadata;
use readstat::SharedMmap;


#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct ChunkCoordinate {
    row_partition: usize,
    col_partition: usize,
    row_chunk_index: usize, // Renamed for clarity
}

#[derive(Debug)]
enum ChunkMessage {
    Data {
        coordinate: ChunkCoordinate,
        data: DataFrame,
    },
    ThreadComplete {
        row_partition: usize,
        col_partition: usize,
    },
    ThreadError {
        row_partition: usize,
        col_partition: usize,
        error: String,
    },
}

struct ScanState {
    // Key: The row chunk index (e.g., 0 for rows 0-999).
    // Value: A map of column partitions received for that chunk,
    // keyed by the partition index (0, 1, 2...) to keep them in order.
    buffer: HashMap<usize, BTreeMap<usize, DataFrame>>,
    
    // The index of the next complete chunk we should assemble and return.
    next_chunk_to_emit: usize,
    
    // The number of column partitions we expect for each chunk.
    p_col: usize,

    // Thread management
    completed_threads: usize,
    total_threads: usize,
    thread_handles: Vec<thread::JoinHandle<()>>,
    
    // Error tracking
    thread_errors: Vec<String>,
}

pub struct PolarsReadstat {
    pub reader: Mutex<Reader>,
    thread_handle: Mutex<Option<mpsc::Receiver<ChunkMessage>>>,
    scan_state: Mutex<Option<ScanState>>,
    p_col: Mutex<Option<usize>>,
    p_row: Mutex<Option<usize>>,
}

unsafe impl Send for PolarsReadstat {}
unsafe impl Sync for PolarsReadstat {}

impl Drop for PolarsReadstat {
    fn drop(&mut self) {
        // Wait for all threads to complete
        if let Ok(mut state_guard) = self.scan_state.lock() {
            if let Some(state) = state_guard.take() {
                for handle in state.thread_handles {
                    let _ = handle.join();
                }
            }
        }
    }
}

impl AnonymousScan for PolarsReadstat {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn scan(
        &self, 
        scan_opts: AnonymousScanArgs
    ) -> PolarsResult<DataFrame> {
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
            self.initialize_reader(scan_opts)?;
            
            // Re-lock and get the initialized state
            state_guard = self.scan_state.lock().unwrap();
        }
        
        let state = match state_guard.as_mut() {
            Some(s) => s,
            None => return Err(PolarsError::InvalidOperation("State not initialized".into())),
        };
        
        let receiver_guard = self.thread_handle.lock().unwrap();
        let receiver = match receiver_guard.as_ref() {
            Some(r) => r,
            None => return Err(PolarsError::InvalidOperation("Receiver not initialized".into())),
        };

        loop {
            // First, check if we have any thread errors
            if !state.thread_errors.is_empty() {
                let error_msg = state.thread_errors.join("; ");
                return Err(PolarsError::InvalidOperation(format!("Thread errors: {}", error_msg).into()));
            }
            
            // Check if the next chunk we need is already complete in the buffer
            if let Some(partitions) = state.buffer.get(&state.next_chunk_to_emit) {
                if partitions.len() == state.p_col {
                    // It's complete! Assemble and return it.
                    let partitions = state.buffer.remove(&state.next_chunk_to_emit).unwrap();
                    let dfs: Vec<DataFrame> = partitions.into_values().collect();
                    
                    // Horizontally stack the dataframes to form a complete chunk
                    let result_df = concat_df_horizontal(&dfs, false)?;

                    state.next_chunk_to_emit += 1;
                    return Ok(Some(result_df));
                }
            }
            
            // Check if all threads are complete and no more data is coming
            if state.completed_threads == state.total_threads {
                // Check if there's a partial chunk we can still return
                if let Some(partitions) = state.buffer.remove(&state.next_chunk_to_emit) {
                    if !partitions.is_empty() {
                        let dfs: Vec<DataFrame> = partitions.into_values().collect();
                        let result_df = concat_df_horizontal(&dfs, false)?;
                        state.next_chunk_to_emit += 1;
                        return Ok(Some(result_df));
                    }
                }
                // No more data available
                return Ok(None);
            }
            
            // Receive more data from threads with timeout
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(message) => {
                    match message {
                        ChunkMessage::Data { coordinate, data } => {
                            // Insert the received piece into the buffer
                            state.buffer
                                .entry(coordinate.row_chunk_index)
                                .or_default()
                                .insert(coordinate.col_partition, data);
                        },
                        ChunkMessage::ThreadComplete { row_partition: _, col_partition: _ } => {
                            state.completed_threads += 1;
                        },
                        ChunkMessage::ThreadError { row_partition, col_partition, error } => {
                            state.thread_errors.push(format!("Thread ({}, {}): {}", row_partition, col_partition, error));
                            state.completed_threads += 1;
                        },
                    }
                },
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Continue the loop - this prevents infinite blocking
                    continue;
                },
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // All senders dropped - threads are done
                    return Ok(None);
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
        let mut reader = Reader::new(
            path.clone(),
            size_hint,
            with_columns,
            threads,
            engine.clone(),
            None,
            None,
            None
        );

        if engine == "readstat" {
            let shared_mmap = SharedMmap::new(&path).unwrap();
            reader.set_mmap(Some(shared_mmap));
        }

        PolarsReadstat { 
            reader: Mutex::new(reader),
            thread_handle: Mutex::new(None),
            scan_state: Mutex::new(None),
            p_col: Mutex::new(None),
            p_row: Mutex::new(None),
        }
    }

    pub fn set_columns_to_read(
        &mut self,
        columns:Option<Vec<String>>,
    ) -> PolarsResult<()> {
        let mut reader = self.reader.lock().unwrap();
        let _ = reader.set_columns_to_read(columns);
        Ok(())
    }
    pub fn metadata(&self) -> PolarsResult<Arc<Metadata>> {
        let mut reader = self.reader.lock().unwrap();
        let _metadata = reader.metadata().unwrap().clone().unwrap();
        Ok(Arc::new(_metadata))
    }

    fn initialize_reader(
        &self,
        scan_opts: AnonymousScanArgs
    ) -> PolarsResult<()> {
        let (schema, sub_schema, chunk_size, threads, n_rows) = {
            let mut reader = self.reader.lock().unwrap();
            let schema = reader.schema().unwrap().clone();
            let sub_schema = reader.schema_with_projection_pushdown().unwrap().clone();
            let chunk_size = reader.size_hint;
            let threads = reader.threads;
            
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
            
            (schema, sub_schema, chunk_size, threads, n_rows)
        };

        let n_cols = sub_schema.len();
        let n_chunks = (n_rows + chunk_size - 1) / chunk_size;

        // Validate inputs
        if threads == 0 {
            return Err(PolarsError::InvalidOperation("Thread count cannot be zero".into()));
        }
        if n_cols == 0 {
            return Err(PolarsError::InvalidOperation("No columns to process".into()));
        }
        if n_rows == 0 {
            return Err(PolarsError::InvalidOperation("No rows to process".into()));
        }

        let p_row = min(n_chunks, threads);
        let p_col = 1;

        // Validate partitioning
        if p_col == 0 || p_row == 0 {
            return Err(PolarsError::InvalidOperation("Invalid partitioning configuration".into()));
        }

        *self.p_col.lock().unwrap() = Some(p_col);
        *self.p_row.lock().unwrap() = Some(p_row);

        // Use bounded channel to prevent excessive memory usage
        let (sender, receiver) = mpsc::channel::<ChunkMessage>();
        *self.thread_handle.lock().unwrap() = Some(receiver);

        let chunks_per_partition = (n_chunks + p_row - 1) / p_row;
        let cols_per_partition = (n_cols + p_col - 1) / p_col;
        let row_per_chunk = chunks_per_partition * chunk_size;

        let mut thread_handles = Vec::new();

        for row_i in 0..p_row {
            let chunk_start = chunks_per_partition * row_i;
            let row_start = row_i * row_per_chunk;
            let row_end = min(row_start + row_per_chunk, n_rows);

            for col_i in 0..p_col {
                let col_start = col_i * cols_per_partition;
                let col_end = min(col_start + cols_per_partition, n_cols);

                let partition_columns: Vec<String> = schema.iter()
                    .map(|(name, _)| name.clone().into_string())
                    .skip(col_start)
                    .take(col_end - col_start)
                    .collect();

                let reader_thread = self.reader.lock().unwrap().copy_for_reading();
                let handle = self.spawn_worker_thread(
                    reader_thread,
                    partition_columns,
                    row_start, 
                    row_end, 
                    chunk_start, 
                    col_i,
                    row_i, 
                    sender.clone()
                )?;
                
                thread_handles.push(handle);
            }
        }

        // Initialize the scan state with thread handles
        let mut state_guard = self.scan_state.lock().unwrap();
        *state_guard = Some(ScanState {
            buffer: HashMap::new(),
            next_chunk_to_emit: 0,
            p_col,
            completed_threads: 0,
            total_threads: p_col * p_row,
            thread_handles,
            thread_errors: Vec::new(),
        });

        Ok(())
    }

    fn spawn_worker_thread(
        &self,
        reader_thread: Reader,
        with_columns: Vec<String>,
        row_start: usize,
        row_end: usize,
        chunk_start: usize,
        col_index: usize,
        row_index: usize,
        sender: mpsc::Sender<ChunkMessage>,
    ) -> PolarsResult<thread::JoinHandle<()>> {
        let handle = thread::spawn(move || {
            let mut reader_thread = reader_thread;
            reader_thread.with_columns = Some(with_columns);

            // Initialize the reader for this thread
            if let Err(e) = reader_thread.initialize_reader(row_start, row_end) {
                let _ = sender.send(ChunkMessage::ThreadError {
                    row_partition: row_index,
                    col_partition: col_index,
                    error: format!("Failed to initialize reader: {}", e),
                });
                return;
            }
            
            let mut row_chunk_index = 0;
            loop {
                match reader_thread.next() {
                    Ok(Some(df)) => {
                        let message = ChunkMessage::Data {
                            coordinate: ChunkCoordinate {
                                row_partition: row_index,
                                col_partition: col_index,
                                row_chunk_index: chunk_start + row_chunk_index,
                            },
                            data: df,
                        };
                        
                        if sender.send(message).is_err() {
                            // Receiver dropped, stop processing
                            break;
                        }
                        row_chunk_index += 1;
                    },
                    Ok(None) => {
                        // No more data from this thread
                        break;
                    },
                    Err(e) => {
                        let _ = sender.send(ChunkMessage::ThreadError {
                            row_partition: row_index,
                            col_partition: col_index,
                            error: format!("Error reading data: {}", e),
                        });
                        return;
                    }
                }
            }
            
            // Send completion signal
            let _ = sender.send(ChunkMessage::ThreadComplete {
                row_partition: row_index,
                col_partition: col_index,
            });
        });
        
        Ok(handle)
    }
}