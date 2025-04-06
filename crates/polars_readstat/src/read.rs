use log::{debug, info, warn, error};

use crate::error::{check_readstat_error, check_readstat_error_code, ReadstatError};
use crate::types;
use crate::common::ptr_to_string;
use crate::formats::{match_var_format, ReadStatVarFormatClass};
use readstat_sys::*;


use polars_arrow::datatypes::{ArrowSchema,ArrowDataType,Field,TimeUnit};
//  use polars_arrow::Arr
// --- Arrow/Polars-Arrow Imports ---
// Array-related imports
use polars_arrow::array::{Array, ArrayRef, MutableArray, new_empty_array};
use polars_arrow::array::{MutablePrimitiveArray, MutableUtf8Array, BooleanArray};
// RecordBatch import
use polars_arrow::record_batch::RecordBatch;
// Compute functions
use polars_arrow::compute;

// --- External Crate Imports ---
use rayon::prelude::*; // For parallel iteration
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::path::Path;
use std::ptr;
use std::sync::Arc;

/// Enum to specify the type of file being read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadstatFileType {
    Sas7bdat,
    Dta,
    // Add Sav, Por, Xpt etc. as needed
}

// --- Metadata Reading Section ---

/// Holds state during the initial metadata scan.
#[derive(Debug, Default)]
struct MetadataState {
    row_count: Option<i32>,
    variables: Vec<(String, readstat_type_t, String, Option<ReadStatVarFormatClass>)>, // (name, readstat_type, format, format class)
    schema: ArrowSchema,
    error: Option<ReadstatError>, // Errors encountered during callbacks
    file_type: Option<ReadstatFileType>,
}


impl MetadataState {
    /// Records the first error encountered.
    fn set_error(&mut self, err: ReadstatError) {
        if self.error.is_none() {
            self.error = Some(err);
        }
    }

    /// Builds the Arrow Schema from collected variable information.
    fn build_schema(&mut self) -> Result<(), ReadstatError> {
        self.schema = ArrowSchema::with_capacity(self.variables.len());

        for (i, (name, r_type, format, format_class)) in self.variables.iter().enumerate() {
            // Convert readstat type + format hint to DataType
            
            debug!("Name          ={name}");
            debug!("r_type        ={r_type}");
            debug!("format        ={format}");
            debug!("format_class  ={:?}", format_class);
            let data_type:ArrowDataType = types::readstat_type_to_arrow(*r_type, format, format_class.as_ref())?;
            let fieldi = Field::new(name.into(),data_type,true);

            debug!("fieldi        ={:?}", fieldi);
            
            // Assume variables are nullable
            let _ = self.schema.insert_at_index(i,name.into(),fieldi);
        }
        
        Ok(())
        
    }
}


unsafe extern "C" fn handle_metadata(
    metadata: *mut readstat_metadata_t,
    ctx: *mut c_void
) -> i32 {
    debug!("Metadata handler called");
    
    if metadata.is_null() {
        debug!("Metadata pointer is null!");
        return readstat_error_e_READSTAT_ERROR_USER_ABORT as i32;
    }
    
    if ctx.is_null() {
        debug!("Context pointer is null in handle_metadata");
        return readstat_error_e_READSTAT_ERROR_USER_ABORT as i32;
    }

    let state = &mut *(ctx as *mut MetadataState);
    
    // Get various metadata and print for debugging
    let row_count = readstat_get_row_count(metadata);
    debug!("Row count: {}", row_count);
    
    let var_count = readstat_get_var_count(metadata);
    debug!("Variable count: {}", var_count);
    
    // Get file label if available
    let file_label_ptr = readstat_get_file_label(metadata);
    if !file_label_ptr.is_null() {
        let file_label = match CStr::from_ptr(file_label_ptr).to_str() {
            Ok(s) => s,
            Err(_) => "Invalid encoding in file label"
        };
        #[cfg(debug_assertions)]
        debug!("File label: {}", file_label);
    } else {
        #[cfg(debug_assertions)]
        debug!("File label pointer is null");
    }
    
    // Get table name if available
    let table_name_ptr = readstat_get_table_name(metadata);
    if !table_name_ptr.is_null() {
        let table_name = match CStr::from_ptr(table_name_ptr).to_str() {
            Ok(s) => s,
            Err(_) => "Invalid encoding in table name"
        };
        #[cfg(debug_assertions)]
        debug!("Table name: {}", table_name);
    } else {
        debug!("Table name pointer is null");
    }
    
    // Store row count
    if row_count >= 0 {
        state.row_count = Some(row_count);
    }
    
    readstat_error_e_READSTAT_OK as i32
}

unsafe extern "C" fn handle_metadata_variable(
    index: i32,
    variable: *mut readstat_variable_t,
    name: *const i8,
    ctx: *mut c_void
) -> i32 {
    if ctx.is_null() {
        return readstat_error_e_READSTAT_ERROR_USER_ABORT as i32;
    }

    let state = &mut *(ctx as *mut MetadataState);
    
    // Extract variable information
    // Note: We're using the 'name' parameter directly since it's provided by the callback
    let var_name = unsafe { ptr_to_string(readstat_variable_get_name(variable)) };
    let var_type = unsafe{readstat_variable_get_type(variable)};
    let var_format = unsafe { ptr_to_string(readstat_variable_get_format(variable)) };
    
    // Get the file_type from the state
    debug!("filetype = {:?}", &state.file_type);
    let var_format_class = if let Some(file_type) = &state.file_type {
        match_var_format(&var_format, file_type)
    } else {
        None // Handle the case where file_type is None
    };
    debug!("class = {:?}", var_format_class);
    
    // Add the variable to our list
    state.variables.push((var_name, var_type, var_format, var_format_class));

    
    
    readstat_error_e_READSTAT_OK as i32
}

/// Performs the initial metadata scan of the file.
fn read_metadata(
    parser: *mut readstat_parser_t,
    path_cstr: &CString,
    file_type: ReadstatFileType,
) -> Result<MetadataState, ReadstatError> {
    let mut state = MetadataState::default();
    state.file_type = Some(file_type);
    let state_ptr = &mut state as *mut _ as *mut c_void;


    // Set handlers, disable value handler, set userdata
    unsafe {
        check_readstat_error_code(readstat_set_metadata_handler(parser, Some(handle_metadata)))?;
        check_readstat_error_code(readstat_set_variable_handler(parser, Some(handle_metadata_variable)))?;
        check_readstat_error_code(readstat_set_value_handler(parser, None))?;
        
        // Execute parse with state_ptr as the user_ctx parameter
        let parse_result = match file_type {
            ReadstatFileType::Sas7bdat => readstat_parse_sas7bdat(parser, path_cstr.as_ptr(), state_ptr),
            ReadstatFileType::Dta => readstat_parse_dta(parser, path_cstr.as_ptr(), state_ptr),
            // Add other file types as needed
        };

        // Check parse result
        check_readstat_error_code(parse_result)?;
        
        // Reset handlers/userdata
        readstat_set_metadata_handler(parser, None);
        readstat_set_variable_handler(parser, None);
        readstat_set_value_handler(parser, None);
    
    }

    // Check if any error was set during the callbacks
    if let Some(error) = state.error {
        return Err(error);
    }

    if let Some(err) = state.error.take() { return Err(err); }
    if state.row_count.is_none() { state.row_count = Some(0); }

    // Build the ArrowSchema
    state.build_schema()?;
    if state.schema.is_empty() {
        return Err(ReadstatError::Callback("Schema could not be built after metadata scan".to_string()));
    }
    Ok(state)
}



/// Reads a SAS or Stata file into an Arrow `RecordBatch` using parallel processing.
pub fn read_file_parallel(
    path: &Path,
    file_type: ReadstatFileType,
    num_threads: usize,
) -> Result<bool, ReadstatError> {//Result<RecordBatch, ReadstatError> {

    let path_str = path.to_str().ok_or_else(|| {
        ReadstatError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput, 
            "Invalid path encoding"
        ))
    })?;
    
    // Then create the CString separately
    let path_cstr = CString::new(path_str).map_err(|_| {
        ReadstatError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput, 
            "Path contains null bytes"
        ))
    })?;

    // --- 1. Metadata Scan ---
    let mut metadata_parser: *mut readstat_parser_t = ptr::null_mut();
    metadata_parser = unsafe { readstat_parser_init() };
    
    if metadata_parser.is_null() {
        // Handle specific malloc error if possible, otherwise generic error
        return Err(ReadstatError::ReadstatC {
             code: readstat_error_e_READSTAT_ERROR_MALLOC,
             message: "Failed to initialize readstat parser for metadata scan".into()
         });
    }

    let metadata = read_metadata(metadata_parser, &path_cstr, file_type)?;
    unsafe { readstat_parser_free(metadata_parser) };

    let total_rows = metadata.row_count.unwrap_or(0) as usize;
    debug!("total_rows = {total_rows}");

    if metadata.schema.is_empty() {
        return Err(ReadstatError::Callback("Schema missing after metadata scan".into()))
    }

    //  No rows, return an empty record batch
    if total_rows == 0 {
        let mut empty_arrays: Vec<ArrayRef> = Vec::with_capacity(metadata.schema.len());
    
        for (_, field) in metadata.schema.iter() {
            // Get the data type directly - appears it's not wrapped in an Option
            let data_type_ref = field.dtype();
            
            // Create a clone of the data type
            let owned_data_type = data_type_ref.clone();
            
            // Create the empty array
            empty_arrays.push(new_empty_array(owned_data_type));
        }
    }
 
 
 /*
    // --- 2. Divide Work ---
    let actual_threads = std::cmp::min(num_threads, total_rows);
    let actual_threads = std::cmp::max(1, actual_threads);
    let rows_per_thread = (total_rows + actual_threads - 1) / actual_threads;

    // --- 3. Spawn Threads ---
    let batch_results: Vec<Result<RecordBatch, ReadstatError>> = (0..actual_threads)
        .into_par_iter()
        .map(|thread_index| {
            let offset = (thread_index * rows_per_thread) as libc::c_longlong;
            let limit = std::cmp::min(rows_per_thread, total_rows - (offset as usize)) as libc::c_longlong;

            if limit <= 0 {
                let empty_arrays: Vec<ArrayRef> = schema
                    .fields().iter().map(|f| new_empty_array(f.data_type())).collect();
                let schema_clone = schema.clone();
                return Ok(RecordBatch::try_new(schema_clone, empty_arrays)?);
            }

            let mut parser: *mut readstat_parser_t = ptr::null_mut();
            let mut state_ptr: *mut c_void = ptr::null_mut();

            // IIFE for setup and parsing
            let parse_result_code: readstat_error_t = (|| {
                parser = unsafe { readstat_parser_init() };
                if parser.is_null() { return readstat_error_t::READSTAT_ERROR_MALLOC; }
                // Create state Box with Arc<ArrowSchema>
                let thread_state = Box::new(ChunkParserState::new(schema.clone()));
                state_ptr = Box::into_raw(thread_state) as *mut c_void;
                // Set offset/limit/handler/userdata...
                 let res_offset = unsafe { readstat_set_row_offset(parser, offset) }; if res_offset != readstat_error_t::READSTAT_OK { return res_offset; }
                 let res_limit = unsafe { readstat_set_row_limit(parser, limit) }; if res_limit != readstat_error_t::READSTAT_OK { return res_limit; }
                 let res_handler = unsafe { readstat_set_value_handler(parser, Some(handle_chunk_value)) }; if res_handler != readstat_error_t::READSTAT_OK { return res_handler; }
                 // let res_userdata = unsafe { readstat_set_userdata(parser, state_ptr) }; if res_userdata != readstat_error_t::READSTAT_OK { return res_userdata; }
                // Parse
                match file_type {
                    ReadstatFileType::Sas7bdat => unsafe {
                        readstat_parse_sas7bdat(parser, path_cstr.as_ptr(), ptr::null_mut())
                    },
                    ReadstatFileType::Dta => unsafe {
                        readstat_parse_dta(parser, path_cstr.as_ptr(), ptr::null_mut())
                    },
                    // Add other file types here
                }
            })();

            // Cleanup state Box
            let final_state: Option<Box<ChunkParserState>> = if !state_ptr.is_null() {
                Some(unsafe { Box::from_raw(state_ptr as *mut ChunkParserState) })
            } else { None };

            // Free parser
            if !parser.is_null() { unsafe { readstat_parser_free(parser) }; }

            // Check C API result
            check_readstat_error_code(parse_result_code)?;

            // Process state and create RecordBatch
            match final_state {
                Some(mut state) => {
                    if let Some(err) = state.error.take() { return Err(err); }
                    let arrays: Vec<ArrayRef> = state
                        .builders.into_iter().map(|mut b| b.finish()).collect();
                    let schema_clone = state.schema.clone(); // Get Arc<ArrowSchema>
                    Ok(RecordBatch::try_new(schema_clone, arrays)?) // Create RecordBatch
                }
                None => Err(ReadstatError::Callback("Parser state recovery failed".into())),
            }
        })
        .collect();

    // --- 4. Combine Results ---
    let mut batches: Vec<RecordBatch> = Vec::with_capacity(actual_threads);
    for result in batch_results {
        let batch = result?;
        if batch.num_rows() > 0 { batches.push(batch); }
    }

    if batches.is_empty() {
        let empty_arrays: Vec<ArrayRef> = schema
            .fields().iter().map(|f| new_empty_array(f.data_type())).collect();
        Ok(RecordBatch::try_new(schema.clone(), empty_arrays)?)
    } else {
        // Concatenate RecordBatches
        compute::concat::concatenate(&batches).map_err(ReadstatError::Arrow)
    }

     */
    Ok(true)
     
}