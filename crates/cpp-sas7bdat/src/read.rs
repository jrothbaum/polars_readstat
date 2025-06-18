// use std::ffi::{CStr, CString};
// use std::os::raw::c_char;
// use std::ptr;
// use polars::prelude::*;
// use rayon::prelude::*; 

// // Make sure these match the C header exactly.
// #[allow(non_camel_case_types)]
// pub type ChunkedReaderHandle = *mut std::ffi::c_void;
// #[allow(non_camel_case_types)]
// pub type ChunkIteratorHandle = *mut std::ffi::c_void;

// use crate::utilities::{set_max_threads};


// #[repr(C)]
// #[derive(Debug, Copy, Clone)]
// pub struct CColumnValue {
//     pub string_val: *const c_char,
//     pub numeric_val: f64,
//     pub value_type: u8,
//     pub is_null: u8,
// }

// #[repr(C)]
// #[derive(Debug)]
// pub struct CRowData {
//     pub values: *mut CColumnValue,
//     pub column_count: usize,
// }

// #[repr(C)]
// #[derive(Debug)]
// pub struct CChunkInfo {
//     pub row_count: usize,
//     pub start_row: usize,
//     pub end_row: usize,
// }

// #[repr(C)]
// #[derive(Debug)]
// pub struct CColumnInfo {
//     pub name: *const c_char,
//     pub column_type: u8,
//     pub length: usize,
// }

// #[repr(C)]
// #[derive(Debug)]
// pub struct CProperties {
//     pub columns: *mut CColumnInfo,
//     pub column_count: usize,
//     pub total_rows: usize,
// }

// // Declare external C functions
// extern "C" {
//     pub fn chunked_reader_create(filename: *const c_char, chunk_size: usize) -> ChunkedReaderHandle;
//     pub fn chunked_reader_get_properties(handle: ChunkedReaderHandle, properties: *mut CProperties) -> i32;
//     pub fn chunked_reader_next_chunk(handle: ChunkedReaderHandle, chunk_info: *mut CChunkInfo) -> i32;
//     pub fn chunked_reader_has_chunk(handle: ChunkedReaderHandle) -> i32;
//     pub fn chunked_reader_destroy(handle: ChunkedReaderHandle);

//     pub fn chunk_iterator_create(reader_handle: ChunkedReaderHandle) -> ChunkIteratorHandle;
//     pub fn chunk_iterator_next_row(handle: ChunkIteratorHandle, row_data: *mut CRowData) -> i32;
//     pub fn chunk_iterator_has_next(handle: ChunkIteratorHandle) -> i32;
//     pub fn chunk_iterator_destroy(handle: ChunkIteratorHandle);

//     pub fn free_row_data(row_data: *mut CRowData);
//     pub fn free_properties(properties: *mut CProperties);
// }



// // Simplified enum - we'll build directly into ColumnData instead of intermediate types
// #[derive(Debug)]
// pub enum ColumnData {
//     String(Vec<Option<String>>),
//     Numeric(Vec<Option<f64>>),
//     Date(Vec<Option<i32>>),
//     DateTime(Vec<Option<i64>>),
//     Time(Vec<Option<i64>>),
// }

// impl ColumnData {
//     fn new(column_type: &SasColumnType, capacity: usize) -> Self {
//         match column_type {
//             SasColumnType::String => ColumnData::String(Vec::with_capacity(capacity)),
//             SasColumnType::Numeric => ColumnData::Numeric(Vec::with_capacity(capacity)),
//             SasColumnType::Date => ColumnData::Date(Vec::with_capacity(capacity)),
//             SasColumnType::DateTime => ColumnData::DateTime(Vec::with_capacity(capacity)),
//             SasColumnType::Time => ColumnData::Time(Vec::with_capacity(capacity)),
//         }
//     }

//     fn push_null(&mut self) {
//         match self {
//             ColumnData::String(vec) => vec.push(None),
//             ColumnData::Numeric(vec) => vec.push(None),
//             ColumnData::Date(vec) => vec.push(None),
//             ColumnData::DateTime(vec) => vec.push(None),
//             ColumnData::Time(vec) => vec.push(None),
//         }
//     }

//     fn to_series(self, name: &str) -> PolarsResult<Series> {
//         match self {
//             ColumnData::String(data) => {
//                 Ok(Series::new(name.into(), data))
//             }
//             ColumnData::Numeric(data) => {
//                 Ok(Series::new(name.into(), data))
//             }
//             ColumnData::Date(data) => {
//                 let series = Series::new(name.into(), data);
//                 // SAS Date (days since 1960-01-01) to Polars Date (days since 1970-01-01)
//                 // 3653 days between 1960-01-01 and 1970-01-01 (excluding leap days)
//                 let date_series = series.cast(&DataType::Int32)?;
//                 Ok(date_series.cast(&DataType::Date)?)
//             }
//             ColumnData::DateTime(data) => {
//                 let series = Series::new(name.into(), data);
//                 // SAS DateTime (seconds since 1960-01-01) to Polars Datetime (microseconds since epoch)
//                 // 315619200.0 seconds between 1960-01-01 00:00:00 and 1970-01-01 00:00:00 UTC
//                 // Polars expects microseconds since epoch (1970-01-01 UTC)
//                 Ok(series.cast(&DataType::Datetime(TimeUnit::Microseconds, None))?)
//             }
//             ColumnData::Time(data) => {
//                 let series = Series::new(name.into(), data);
//                 // SAS Time (seconds since midnight) to Polars Time (nanoseconds since midnight)
//                 Ok(series.cast(&DataType::Time)?)
//             }
//         }
//     }
// }

// #[derive(Debug, Clone)]
// pub struct SasColumn {
//     pub name: String,
//     pub column_type: SasColumnType,
//     pub length: usize,
// }

// #[derive(Debug, Clone, Copy)] // Added Copy trait for convenience in match
// #[repr(u8)] // Ensure explicit discriminants for C API mapping
// pub enum SasColumnType {
//     String = 0,
//     Numeric = 1,
//     Date = 2,
//     DateTime = 3,
//     Time = 4,
//     // Add Unknown or default if the C API can return other values
// }

// // Helper enum for values passed across threads (must be Send + Sync)
// // This holds the owned Rust data after FFI conversion.
// #[derive(Debug)]
// enum FfiValue {
//     Null,
//     String(String),
//     Numeric(f64),
//     Date(i32),
//     DateTime(i64),
//     Time(i64),
// }

// pub struct SasChunkedReader {
//     handle: ChunkedReaderHandle,
//     properties: Vec<SasColumn>,
// }

// impl SasChunkedReader {
//     pub fn new(filename: &str, chunk_size: usize) -> Result<Self, Box<dyn std::error::Error>> {
//         let _ = set_max_threads();
//         let c_filename = CString::new(filename)?;
//         let handle = unsafe { chunked_reader_create(c_filename.as_ptr(), chunk_size) };

//         if handle.is_null() {
//             return Err("Failed to create chunked reader".into());
//         }

//         // Get properties
//         let mut c_properties = CProperties {
//             columns: ptr::null_mut(),
//             column_count: 0,
//             total_rows: 0,
//         };

//         let result = unsafe { chunked_reader_get_properties(handle, &mut c_properties) };
//         if result != 0 {
//             unsafe { chunked_reader_destroy(handle) };
//             return Err("Failed to get properties".into());
//         }

//         // Convert properties to Rust
//         let properties = unsafe {
//             let slice = std::slice::from_raw_parts(c_properties.columns, c_properties.column_count);
//             slice.iter().map(|c_col| {
//                 let name = CStr::from_ptr(c_col.name).to_string_lossy().into_owned();
//                 let column_type = match c_col.column_type {
//                     0 => SasColumnType::String,
//                     1 => SasColumnType::Numeric,
//                     2 => SasColumnType::Date,
//                     3 => SasColumnType::DateTime,
//                     4 => SasColumnType::Time,
//                     _ => SasColumnType::String, // Default to String for unknown types
//                 };
//                 SasColumn {
//                     name,
//                     column_type,
//                     length: c_col.length,
//                 }
//             }).collect()
//         };

//         // Free C properties
//         unsafe { free_properties(&mut c_properties) };

//         Ok(SasChunkedReader { handle, properties })
//     }

//     pub fn properties(&self) -> &[SasColumn] {
//         &self.properties
//     }

//     pub fn chunk_iterator(&mut self) -> Result<Option<Vec<Series>>, Box<dyn std::error::Error>> {
//         let mut chunk_info = CChunkInfo {
//             row_count: 0,
//             start_row: 0,
//             end_row: 0,
//         };

//         let result = unsafe { chunked_reader_next_chunk(self.handle, &mut chunk_info) };

//         if result != 0 {
//             return Ok(None); // No more chunks
//         }

//         let iterator_handle = unsafe { chunk_iterator_create(self.handle) };

//         if iterator_handle.is_null() {
//             return Err("Failed to create chunk iterator".into());
//         }

//         // --- Stage 1: Sequentially pull and convert all CRowData for the chunk ---
//         // We will store the *already converted* Rust-owned data here.
//         // Each inner Vec<FfiValue> represents a single row.
//         let mut processed_values_per_row_sequential: Vec<Vec<FfiValue>> = Vec::with_capacity(chunk_info.row_count);
//         let properties_ref = &self.properties; // Capture for use in the closure

//         for _ in 0..chunk_info.row_count {
//             let has_next = unsafe { chunk_iterator_has_next(iterator_handle) };
//             if has_next == 0 {
//                 // This means the actual row count from C API was less than expected
//                 // (e.g., end of file in the middle of a chunk). Break gracefully.
//                 break;
//             }

//             let mut row_data = CRowData {
//                 values: ptr::null_mut(),
//                 column_count: 0,
//             };

//             let result = unsafe { chunk_iterator_next_row(iterator_handle, &mut row_data) };
//             if result != 0 {
//                 // Handle error from C API if needed, then break.
//                 break;
//             }

//             // Immediately convert CRowData into owned Rust data (Vec<FfiValue>)
//             // This part is still sequential, but it's where the FFI "unsafe" boundary
//             // is crossed and data is made safe for Rust.
//             let mut row_values: Vec<FfiValue> = Vec::with_capacity(row_data.column_count);
//             unsafe {
//                 let c_val_slice = std::slice::from_raw_parts(row_data.values, row_data.column_count);
//                 for (col_idx, &c_val) in c_val_slice.iter().enumerate() {
//                     let prop = &properties_ref[col_idx]; // Use the captured reference
//                     let value = if c_val.is_null != 0 {
//                         FfiValue::Null
//                     } else {
//                         match prop.column_type {
//                             SasColumnType::String => {
//                                 // CRITICAL: .into_owned() copies the string, making it owned Rust data.
//                                 FfiValue::String(CStr::from_ptr(c_val.string_val).to_string_lossy().into_owned())
//                             },
//                             SasColumnType::Numeric => FfiValue::Numeric(c_val.numeric_val),
//                             SasColumnType::Date => FfiValue::Date(c_val.numeric_val as i32 - 3653),
//                             SasColumnType::DateTime => FfiValue::DateTime(((c_val.numeric_val - 315619200.0) * 1_000_000.0) as i64),
//                             SasColumnType::Time => FfiValue::Time((c_val.numeric_val * 1_000_000_000.0) as i64),
//                         }
//                     };
//                     row_values.push(value);
//                 }
//                 free_row_data(&mut row_data); // IMPORTANT: Free C memory for this row's values after extraction
//             }
//             processed_values_per_row_sequential.push(row_values); // Add the owned Rust row to our sequential Vec
//         }

//         // Clean up iterator handle immediately after pulling all CRowData for the chunk.
//         unsafe { chunk_iterator_destroy(iterator_handle) };


//         // --- Stage 2: Parallel processing of the already-converted Rust-owned data ---
//         // This step is now parallel-friendly because `Vec<Vec<FfiValue>>` is `Send`.
//         // The actual "processing" here is just preparing the `ColumnData` vectors for transposition.
//         // The conversion from C to Rust types happened in Stage 1.
//         let mut column_data: Vec<ColumnData> = self.properties
//             .iter()
//             .map(|col| ColumnData::new(&col.column_type, processed_values_per_row_sequential.len()))
//             .collect();

//         // This is where the transposition logic used to be, but we need to iterate
//         // the `processed_values_per_row_sequential` in parallel to fill `column_data`.
//         // However, filling `column_data` in parallel directly is tricky as `Vec` is not `Sync`.
//         // The most idiomatic way for Polars is to collect `Series` per chunk.

//         // Re-thinking the parallel "processing":
//         // The most expensive part is the conversion from raw bytes to Rust String/f64 etc.
//         // This is now done sequentially in Stage 1 for each row immediately after FFI call.
//         // So the "parallel processing" will be the subsequent step of getting it into `ColumnData`.
//         // If the number of columns is large, or the number of rows is large,
//         // then the transposition itself can be parallelized.

//         // Let's re-frame Stage 2 and 3 for maximum parallelism:
//         // Instead of collecting `Vec<Vec<FfiValue>>` and then transposing,
//         // we can create the `ColumnData` vectors and fill them in parallel,
//         // but this requires careful synchronization or a different pattern.

//         // Simpler and still effective parallelization strategy:
//         // Use `processed_values_per_row_sequential` (which is `Send`) directly
//         // and then transpose it using parallel iterators.
//         // However, transposing a `Vec<Vec<T>>` into `Vec<Vec<T>>` with Rayon
//         // can be less straightforward than filling pre-allocated columnar vectors.

//         // **Optimal Approach for `Vec<Vec<FfiValue>>` to `Vec<ColumnData>`:**
//         // Allocate `num_columns` `Vec<Option<T>>`s.
//         // Iterate `processed_values_per_row_sequential` in parallel, taking each row.
//         // For each row, iterate its `FfiValue`s.
//         // Push each `FfiValue` into the correct *column's* vector.
//         // This requires `Vec` to be `Sync`, which it is not.
//         // So, you need to use `Mutex` or `parking_lot::Mutex` or collect into per-thread partial vectors.

//         // Let's stick to a simpler, still effective parallel model.
//         // The `processed_values_per_row_sequential` (which is now `Send`)
//         // represents the "rows" of data. We can parallelize the *conversion*
//         // of these into Polars Series, but typically Polars handles chunking itself.

//         // Let's refine the approach to utilize `rayon` for the *transposition* or *final series construction*.

//         // Stage 2 (Revised): Convert `Vec<Vec<FfiValue>>` into `Vec<ColumnData>` in parallel.
//         // This is where we need to be careful with mutable shared state.
//         // One way: Each column is processed independently.
//         // This would involve creating `num_columns` separate parallel tasks.

//         // Initializing `column_data` as before.
//         // `column_data` is `Vec<ColumnData>`, which holds `Vec<Option<T>>` inside.
//         // We need to fill these `Vec`s.

//         // A common pattern for this is to collect results into an intermediate
//         // `Vec<Vec<Option<T>>>` (Rust Vec of Vecs, all owned) and then convert to `Series`.
//         // This step is often sequential as it's a transpose, but it can be very fast.

//         // Given `processed_values_per_row_sequential: Vec<Vec<FfiValue>>`,
//         // let's create the final `Vec<Series>` directly.
//         // We can zip the properties with column index and iterate in parallel.

//         // Stage 2 & 3 Combined: Parallel creation of individual column Series.
//         // Each column can be processed independently.
//         let num_columns = self.properties.len();
//         let column_series: PolarsResult<Vec<Series>> = (0..num_columns).into_par_iter()
//             .map(|col_idx| {
//                 let col_prop = &self.properties[col_idx];
//                 let mut col_data_vec = ColumnData::new(&col_prop.column_type, processed_values_per_row_sequential.len());

//                 // Iterate over each row and extract the value for this specific column.
//                 // This is still sequential over rows for *this* column, but columns are parallel.
//                 for row_values in &processed_values_per_row_sequential {
//                     let value = &row_values[col_idx];
//                     match value {
//                         FfiValue::String(s) => {
//                             if let ColumnData::String(ref mut vec) = col_data_vec {
//                                 vec.push(Some(s.clone())); // .clone() is necessary as `s` is borrowed
//                             }
//                         },
//                         FfiValue::Numeric(n) => {
//                             if let ColumnData::Numeric(ref mut vec) = col_data_vec {
//                                 vec.push(Some(*n));
//                             }
//                         },
//                         FfiValue::Date(d) => {
//                             if let ColumnData::Date(ref mut vec) = col_data_vec {
//                                 vec.push(Some(*d));
//                             }
//                         },
//                         FfiValue::DateTime(dt) => {
//                             if let ColumnData::DateTime(ref mut vec) = col_data_vec {
//                                 vec.push(Some(*dt));
//                             }
//                             // else { println!("Type mismatch: Expected DateTime, got {:?}", value); } // Debugging
//                         },
//                         FfiValue::Time(t) => {
//                             if let ColumnData::Time(ref mut vec) = col_data_vec {
//                                 vec.push(Some(*t));
//                             }
//                         },
//                         FfiValue::Null => {
//                             col_data_vec.push_null();
//                         },
//                     }
//                 }
//                 col_data_vec.to_series(&col_prop.name)
//             })
//             .collect(); // Collects the results into a `PolarsResult<Vec<Series>>`

//         Ok(Some(column_series?))
//     }

//     pub fn has_more_chunks(&self) -> bool {
//         unsafe { chunked_reader_has_chunk(self.handle) != 0 }
//     }
// }

// impl Drop for SasChunkedReader {
//     fn drop(&mut self) {
//         unsafe { chunked_reader_destroy(self.handle) };
//     }
// }

// // Example usage (main.rs or a test)
// // #[cfg(test)]
// // mod tests {
// //     use super::*;
// //     #[test]
// //     fn test_read_sas_file() -> Result<(), Box<dyn std::error::Error>> {
// //         // Create a dummy SAS file for testing, or use a real one
// //         // This part is illustrative; you'd need a real .sas7bdat file
// //         // For local testing: you might need to create a small .sas7bdat file
// //         // or modify the path to an existing one.
// //         // Example: create a simple_test.sas7bdat
// //         // Data example for simple_test.sas7bdat:
// //         //  ID  NAME      VALUE
// //         //  1   "Alpha"   10.5
// //         //  2   "Beta"    20.0
// //         //  3   "Gamma"   NaN (missing)
// //         //  4   ""        30.0
// //         //  5   "Delta"   . (numeric missing)
// //         //  6   "Epsilon" 40.0
// //         //
// //         // Or use the test files from the original cpp-sas7bdat project if available.
// //         let filename = "path/to/your/test.sas7bdat"; // <--- CHANGE THIS PATH
// //         let chunk_size = 1000; // Number of rows per chunk
// //
// //         let mut reader = SasChunkedReader::new(filename, chunk_size)?;
// //         println!("Properties: {:?}", reader.properties());
// //
// //         let mut all_series: Vec<Series> = Vec::new();
// //
// //         loop {
// //             match reader.chunk_iterator()? {
// //                 Some(series_vec) => {
// //                     if all_series.is_empty() {
// //                         all_series = series_vec;
// //                     } else {
// //                         // Append new chunk series to existing ones
// //                         for (i, new_series) in series_vec.into_iter().enumerate() {
// //                             all_series[i].append(&new_series)?;
// //                         }
// //                     }
// //                 }
// //                 None => break, // No more chunks
// //             }
// //         }
// //
// //         if !all_series.is_empty() {
// //             let df = DataFrame::new(all_series)?;
// //             println!("{}", df);
// //             // You can add assertions here to check the data
// //         } else {
// //             println!("No data read from SAS file.");
// //         }
// //
// //         Ok(())
// //     }
// // }