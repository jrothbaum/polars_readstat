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
use polars_arrow::array::{new_empty_array, Array, ArrayRef, DictionaryKey, MutableArray};
use polars_arrow::array::{MutablePrimitiveArray, MutableUtf8Array, BooleanArray};
// RecordBatch import
use polars_arrow::record_batch::RecordBatch;
// Compute functions
use polars_arrow::compute;
use polars_core::POOL;

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

impl Default for ReadstatFileType {
    fn default() -> Self {
        // Choose one of the variants as the default
        ReadstatFileType::Sas7bdat 
    }
}
// --- Metadata Reading Section ---

/// Holds state during the initial metadata scan.
#[derive(Debug, Default)]
struct MetadataState {
    row_count: Option<i32>,
    variable_names: Vec<String>,
    variable_types: Vec<readstat_type_t>,
    variable_formats: Vec<String>,
    variable_format_classes: Vec <Option<ReadStatVarFormatClass>>,
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
        self.schema = ArrowSchema::with_capacity(self.variable_names.len());

        for i in 0..self.variable_names.len() {
            let name = self.variable_names[i].clone();
            let r_type = self.variable_types[i];
            let format  = self.variable_formats[i].clone();
            let format_class = self.variable_format_classes[i];

            // Convert readstat type + format hint to DataType
            
            debug!("Name          ={name}");
            debug!("r_type        ={r_type}");
            debug!("format        ={format}");
            debug!("format_class  ={:?}", format_class);
            let data_type:ArrowDataType = types::readstat_type_to_arrow(r_type.clone(), &format, format_class.as_ref())?;
            let fieldi = Field::new(name.clone().into(),data_type,true);

            debug!("fieldi        ={:?}", fieldi);
            
            // Assume variables are nullable
            let _ = self.schema.insert_at_index(i,name.clone().into(),fieldi);
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
    debug!("Metadata variable handler called");
    if ctx.is_null() {
        return readstat_error_e_READSTAT_ERROR_USER_ABORT as i32;
    }

    let state = &mut *(ctx as *mut MetadataState);
    
    // Extract variable information
    // Note: We're using the 'name' parameter directly since it's provided by the callback
    let var_name = unsafe { ptr_to_string(readstat_variable_get_name(variable)) }.clone();
    let var_type = unsafe{readstat_variable_get_type(variable)};
    let var_format = unsafe { ptr_to_string(readstat_variable_get_format(variable)) };
    
    // Get the file_type from the state
    //  debug!("filetype = {:?}", &state.file_type);
    let var_format_class = if let Some(file_type) = &state.file_type {
        match_var_format(&var_format, file_type)
    } else {
        None // Handle the case where file_type is None
    };
    //  debug!("class = {:?}", var_format_class);
    
    // Add the variable to our list
    debug!("var_name = {var_name}");
    if !state.variable_names.contains(&var_name) {
        state.variable_names.push(var_name);
        state.variable_types.push(var_type);
        state.variable_formats.push(var_format);
        state.variable_format_classes.push(var_format_class);
    }


    readstat_error_e_READSTAT_OK as i32
}



unsafe extern "C" fn handle_value(
    obs_index: c_int,
    variable: *mut readstat_variable_t,
    value: readstat_value_t,
    ctx: *mut c_void
) -> c_int {
   

    debug!("Row:      {obs_index}");
    
    
    if ctx.is_null() {
        return readstat_error_e_READSTAT_ERROR_USER_ABORT as i32;
    }
    
    let state = &mut *(ctx as *mut DataState);
   /* 
    // Get variable index and value information
    
    let var_index = readstat_variable_get_index(variable) as usize;
    debug!("Variable: {var_index}");
    if var_index >= state.arrays.len() {
        state.set_error(ReadstatError::Callback(format!(
            "Variable index {} exceeds number of arrays {}", 
            var_index, state.arrays.len()
        )));
        return readstat_error_e_READSTAT_ERROR_USER_ABORT as i32;
    }
    
    // Get format for type conversion hints
    let format = ptr_to_string(readstat_variable_get_format(variable));
    let var_type = readstat_variable_get_type(variable);
    let format_class = match_var_format(&format, &state.file_type);
    
     
    use crate::common::ptr_to_string;
    // Insert the value into the array array based on types and position
    let value_type: readstat_type_t = unsafe { readstat_value_type(value) };
    debug!("value_type = {value_type}");
    use readstat_sys::readstat_type_e_READSTAT_TYPE_STRING as RS_TYPE_STRING;
    if value_type == RS_TYPE_STRING {
        debug!("value = {:?}",ptr_to_string(readstat_sys::readstat_string_value(value)));
    }
    else {

    }
    */
    /*
    match state.schema.get_at_index(var_index) {
        Some((name, field)) => {
            // Now you have access to the field object
            let data_type = &field.dtype;
            let mut array = state.arrays.get_mut(var_index).unwrap();
            
            //  _ = types::convert_readstat_value(value,data_type,array,obs_index as usize);
        },
        None => {
            //  Do nothing
        }
    }
     */
    
    /*
    if let Err(err) = types::append_value_to_array(
        &mut state.arrays[var_index], 
        value, 
        var_type,
        format_class.as_ref()
    ) {
        state.set_error(err);
        return readstat_error_e_READSTAT_ERROR_USER_ABORT as i32;
    }
     */


    
    readstat_error_e_READSTAT_OK as i32
    
}



fn initialize_readstat_parser(
    path_cstr: *const c_char,
    file_type: ReadstatFileType,
    get_values: bool,
) -> Result<* mut readstat_parser_t, ReadstatError> {
    let mut parser: *mut readstat_parser_t = ptr::null_mut();
    parser = unsafe { readstat_parser_init() };
    
    if parser.is_null() {
        // Handle specific malloc error if possible, otherwise generic error
        return Err(ReadstatError::ReadstatC {
             code: readstat_error_e_READSTAT_ERROR_MALLOC,
             message: "Failed to initialize readstat parser for metadata scan".into()
         });
    }


    unsafe {

        if get_values {
            check_readstat_error_code(readstat_set_metadata_handler(parser, Some(handle_metadata)))?;
            check_readstat_error_code(readstat_set_variable_handler(parser, Some(handle_metadata_variable)))?;
            check_readstat_error_code(readstat_set_value_handler(parser, Some(handle_value)))?;
        }
        else {
            check_readstat_error_code(readstat_set_metadata_handler(parser, Some(handle_metadata)))?;
            check_readstat_error_code(readstat_set_variable_handler(parser, Some(handle_metadata_variable)))?;
            check_readstat_error_code(readstat_set_value_handler(parser, None))?;
        }
    }

    

    Ok(parser)


}


fn clean_up_readstat_parser(parser:* mut readstat_parser_t,free_parser:bool) -> Result<(), ReadstatError> {
    unsafe {
        readstat_set_metadata_handler(parser, None);
        readstat_set_variable_handler(parser, None);
        readstat_set_value_handler(parser, None);

        if free_parser {
            readstat_parser_free(parser);
        }
    }
    Ok(())
}

/// Performs the initial metadata scan of the file.
fn read_metadata(
    path_cstr: *const c_char,
    file_type: ReadstatFileType,
) -> Result<MetadataState, ReadstatError> {
    let mut state = MetadataState::default();
    state.file_type = Some(file_type);
    let state_ptr = &mut state as *mut _ as *mut c_void;

    let parser = initialize_readstat_parser(path_cstr,file_type,false)?;
    // Set handlers, disable value handler, set userdata
    unsafe {
        // Execute parse with state_ptr as the user_ctx parameter
        let parse_result = match file_type {
            ReadstatFileType::Sas7bdat => readstat_parse_sas7bdat(parser, path_cstr, state_ptr),
            ReadstatFileType::Dta => readstat_parse_dta(parser, path_cstr, state_ptr),
            // Add other file types as needed
        };

        // Check parse result
        check_readstat_error_code(parse_result)?;
    }

    _ = clean_up_readstat_parser(parser, true);

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


/// Holds state during data reading.
#[derive(Debug, Default)]
struct DataState {
    schema: Arc<ArrowSchema>,
    arrays: Vec<Box<dyn MutableArray>>,
    row_count: usize,
    start_row: usize,
    end_row: usize,
    current_row: usize,
    error: Option<ReadstatError>,
    file_type: ReadstatFileType,
}


impl DataState {
    /// Creates a new DataState for reading a specific chunk of rows.
    fn new(
        schema: Arc<ArrowSchema>,
        start_row: usize, 
        end_row: usize,
        file_type: ReadstatFileType
    ) -> Self {

        let row_count = end_row - start_row + 1;
        let mut arrays: Vec<Box<dyn MutableArray>> = Vec::with_capacity(schema.len());
        
        // Create the appropriate mutable arrays based on schema
        for (_, field) in schema.iter() {
            let data_type = field.dtype();
            let array = types::create_mutable_array_for_type(data_type,row_count);
            arrays.push(array);
        }
        debug!("Arrays = {:?}", arrays.len());
        DataState {
            schema,
            arrays,
            row_count: row_count,
            start_row,
            end_row,
            current_row: start_row,
            error: None,
            file_type,
        }
    }
    
    /// Records the first error encountered.
    fn set_error(&mut self, err: ReadstatError) {
        if self.error.is_none() {
            self.error = Some(err);
        }
    }
    
    /// Converts the collected data to a RecordBatch.
    fn to_record_batch(self) -> Result<RecordBatch, ReadstatError> {
        let mut column_arrays: Vec<ArrayRef> = Vec::with_capacity(self.arrays.len());
        
        for mut array in self.arrays {
            column_arrays.push(array.as_box());
        }
        
        // Create the RecordBatch with the schema and column arrays
        RecordBatch::try_new(
            self.row_count,
            self.schema,
            column_arrays)
            .map_err(|e| ReadstatError::Arrow(e))
    }
}



/// Reads a specific chunk of data from a SAS or Stata file.
pub fn read_chunk(
    parser: *mut readstat_parser_t,
    path_cstr: *const c_char,
    file_type: ReadstatFileType,
    schema: ArrowSchema,
    start_row: usize,
    end_row: usize
) -> Result<bool, ReadstatError> {
// Result<RecordBatch, ReadstatError> {
    debug!("Reading chunk: rows {}-{}", start_row, end_row);
    
    // Validate input
    if end_row <= start_row {
        return Err(ReadstatError::InvalidArgument(
            format!("Invalid row range: start={}, end={}", start_row, end_row)
        ));
    }
    
    // Validate parser
    if parser.is_null() {
        return Err(ReadstatError::ReadstatC { 
            code: readstat_error_e_READSTAT_ERROR_MALLOC, 
            message: "Invalid parser provided for chunk reading".into() 
        });
    }
    
    // Set up state for chunk reading
    let schema_arc = Arc::new(schema);
    let mut state = DataState::new(schema_arc, start_row, end_row, file_type);
    let state_ptr = &mut state as *mut _ as *mut c_void;
    debug!("n vars (1)= {:?}",state.arrays.len());
    debug!("start row = {start_row}");
    unsafe {
        //  let _ = check_readstat_error_code(readstat_set_row_offset(parser, ((start_row as isize - 1) as c_int).into()));
        //  let _ = check_readstat_error_code(readstat_set_row_limit(parser, ((end_row as isize - start_row as isize) as c_int).into()));
    }
    debug!("n vars (2)= {:?}",state.arrays.len());
    let parse_result = unsafe {
        match file_type {
            ReadstatFileType::Sas7bdat => readstat_parse_sas7bdat(parser, path_cstr, state_ptr),
            ReadstatFileType::Dta => readstat_parse_dta(parser, path_cstr, state_ptr),
            // Add other file types as needed
        }
    };
    debug!("n vars (3)= {:?}",state.arrays.len());
    
    // Check parse result
    check_readstat_error_code(parse_result)?;
    let _ = clean_up_readstat_parser(parser, false);
    
    // Check for errors set during callbacks
    if let Some(error) = state.error {
        return Err(error);
    }

    // Incremental dropping for debugging
    debug!("Starting incremental drop test");
    
    
   
    
    // 3. Try dropping error field
    debug!("Test dropping error field");
    let error = state.error.take();
    drop(error);
    debug!("Error field dropped successfully");
    // 1. First try dropping just the arrays
    debug!("Test dropping arrays");

    debug!("n vars = {:?}",state.arrays.len());
    let mut arrays = std::mem::take(&mut state.arrays);

    // Drop them one by one and see which one fails
    
    for (i, array) in arrays.drain(..).enumerate() {
        debug!("Dropping array {}", i);
        if let Some((name, field)) = state.schema.get_at_index(i) {
            let data_type = field.dtype();
            
            // Now you can use name and data_type
            debug!("Field name: {}, type: {:?}", name, data_type);
            
            // Do something with them...
        }
        
        drop(array);
        debug!("Array {} dropped successfully", i);
    }
    debug!("Arrays dropped successfully");
    

     // 2. Try dropping schema next
     debug!("Test dropping schema");
     let schema = std::mem::take(&mut state.schema);
     drop(schema);
     debug!("Schema dropped successfully");

    
    // 4. Now drop the entire state
    debug!("Test dropping entire state");
    drop(state);
    debug!("State dropped successfully");
    
    // 5. Now free the parser
    unsafe {
        debug!("Test freeing parser");
        readstat_parser_free(parser);
        debug!("Parser freed successfully");
    }

    
    
    // Convert to RecordBatch
    //  state.to_record_batch()
    debug!("Before dropping state");
    
    debug!("After dropping state");
    Ok(true)
}


/// Reads a SAS or Stata file into an Arrow `RecordBatch` using parallel processing.
pub fn read_file_parallel(
    path: &Path,
    file_type: ReadstatFileType,
    chunk_size:Option<usize>,
) -> Result<bool, ReadstatError> { //Result<RecordBatch, ReadstatError> {

    // Default chunk size
    let chunk_size = chunk_size.unwrap_or(10_000);

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
    let metadata = read_metadata(path_cstr.as_ptr(), file_type)?;
    //  unsafe { readstat_parser_free(metadata_parser) };

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
 

    // --- 2. Divide Work ---
    let actual_threads = POOL.current_num_threads();

    let chunks_to_read = (total_rows as f64/chunk_size as f64).ceil() as usize;
    debug!("actual_threads = {actual_threads}");
    debug!("chunks_to_read = {chunks_to_read}");

    let actual_threads = std::cmp::min(chunks_to_read,actual_threads);
    debug!("actual_threads = {actual_threads}");

    let rows_per_thread = (total_rows as f64 / actual_threads as f64).ceil() as usize;
    debug!("Rows per thread: {}", rows_per_thread);
    
    // Create chunk boundaries
    let mut chunks = Vec::with_capacity(actual_threads);
    for thread_idx in 0..actual_threads {
        debug!("Thread = {thread_idx}");
        let start_row = thread_idx * rows_per_thread + 1;
        let end_row = (start_row + rows_per_thread - 1).min(total_rows);
        
        if start_row <= total_rows {
            debug!("    Chunk = [{start_row},{end_row}]");
            chunks.push((start_row, end_row));
        }
    }
    
    
    let parser = initialize_readstat_parser(path_cstr.as_ptr(), file_type, true)?;
    let schema = metadata.schema.clone();
    drop(metadata);
    let output = read_chunk(
        parser,
        path_cstr.as_ptr(),
        file_type,
        schema,
        1 as usize,
        total_rows
    );

    debug!("Finished read - pre cleanup");
    let _= clean_up_readstat_parser(parser, true);
    debug!("Finished read");
    /*
    // --- 3. Process Chunks in Parallel ---
    let schema_arc = Arc::new(metadata.schema.clone());
    
    // Create a shareable CString for the path
    let path_arc = Arc::new(path_cstr);

    */
 /*
    

    

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