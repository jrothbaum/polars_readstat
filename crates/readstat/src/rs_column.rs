// In readstat_sys/src/lib.rs or wherever your FFI bindings are defined
use std::os::raw::{c_char, c_int, c_void};

use std::ptr;
use log::debug;
use num_traits::FromPrimitive;

use crate::err::ReadStatError;
use crate::rs_parser::ReadStatParser;
use crate::rs_data::TypedColumn;
use crate::common::ptr_to_string;
use crate::rs_var::ReadStatVarType;
use readstat_sys;


extern "C" {
    pub fn readstat_column_init(
        variable: *mut readstat_sys::readstat_variable_t,
        max_rows: c_int
    ) -> *mut readstat_sys::readstat_column_t;
    
    pub fn readstat_column_read(
        parser: *mut readstat_sys::readstat_parser_t,
        variable: *mut readstat_sys::readstat_variable_t,
        column: *mut readstat_sys::readstat_column_t
    ) -> c_int;
    
    pub fn readstat_column_free(column: *mut readstat_sys::readstat_column_t);
    
}





// Define a Column reader struct
pub struct ReadStatColumnReader {
    column_ptr: *mut readstat_sys::readstat_column_t,
}

impl ReadStatColumnReader {
    // Initialize a new column reader for a variable
    pub fn new(variable: *mut readstat_sys::readstat_variable_t, max_rows: i32) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let column_ptr = unsafe { readstat_sys::readstat_column_init(variable, max_rows) };
        
        if column_ptr.is_null() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to initialize column reader"
            )));
        }
        
        Ok(Self { column_ptr })
    }
    
    // Read the entire column using the parser
    pub fn read_column(&self, parser: &mut ReadStatParser, variable: *mut readstat_sys::readstat_variable_t) 
        -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        
        let error = unsafe { readstat_sys::readstat_column_read(
            parser.parser, 
            variable, 
            self.column_ptr
        )};
        
        #[allow(clippy::useless_conversion)]
        match FromPrimitive::from_i32(error.try_into().unwrap()) {
            Some(ReadStatError::READSTAT_OK) => Ok(()),
            Some(e) => Err(From::from(format!(
                "Error when attempting to read column: {:#?}",
                e
            ))),
            None => Err(From::from(
                "Error when attempting to read column: Unknown return value",
            )),
        }
    }
    
    // Convert column data to appropriate Rust type and return as TypedColumn
    pub fn into_typed_column(&self) -> Result<TypedColumn, Box<dyn std::error::Error + Send + Sync>> {
        unsafe {
            let column = &*self.column_ptr;
            let type_val = column.type_;
            let rows = column.rows as usize;
            let missing_arr = std::slice::from_raw_parts(column.missing, rows);
            
            match FromPrimitive::from_i32(type_val.try_into().unwrap()) {
                Some(ReadStatVarType::String) => {
                    let string_arr = std::slice::from_raw_parts(column.data as *const *const c_char, rows);
                    let mut result = Vec::with_capacity(rows);
                    
                    for i in 0..rows {
                        if missing_arr[i] != 0 {
                            result.push(None);
                        } else if !string_arr[i].is_null() {
                            result.push(Some(ptr_to_string(string_arr[i])));
                        } else {
                            result.push(None);
                        }
                    }
                    
                    Ok(TypedColumn::StringColumn(result))
                },
                Some(ReadStatVarType::Int8) => {
                    let int_arr = std::slice::from_raw_parts(column.data as *const i8, rows);
                    let mut result = Vec::with_capacity(rows);
                    
                    for i in 0..rows {
                        if missing_arr[i] != 0 {
                            result.push(None);
                        } else {
                            result.push(Some(int_arr[i] as i32));
                        }
                    }
                    
                    Ok(TypedColumn::I8Column(result))
                },
                Some(ReadStatVarType::Int16) => {
                    let int_arr = std::slice::from_raw_parts(column.data as *const i16, rows);
                    let mut result = Vec::with_capacity(rows);
                    
                    for i in 0..rows {
                        if missing_arr[i] != 0 {
                            result.push(None);
                        } else {
                            result.push(Some(int_arr[i] as i32));
                        }
                    }
                    
                    Ok(TypedColumn::I16Column(result))
                },
                Some(ReadStatVarType::Int32) => {
                    let int_arr = std::slice::from_raw_parts(column.data as *const i32, rows);
                    let mut result = Vec::with_capacity(rows);
                    
                    for i in 0..rows {
                        if missing_arr[i] != 0 {
                            result.push(None);
                        } else {
                            result.push(Some(int_arr[i]));
                        }
                    }
                    
                    Ok(TypedColumn::I32Column(result))
                },
                Some(ReadStatVarType::Float) => {
                    let float_arr = std::slice::from_raw_parts(column.data as *const f32, rows);
                    let mut result = Vec::with_capacity(rows);
                    
                    for i in 0..rows {
                        if missing_arr[i] != 0 {
                            result.push(None);
                        } else {
                            result.push(Some(float_arr[i]));
                        }
                    }
                    
                    Ok(TypedColumn::F32Column(result))
                },
                Some(ReadStatVarType::Double) => {
                    let double_arr = std::slice::from_raw_parts(column.data as *const f64, rows);
                    let mut result = Vec::with_capacity(rows);
                    
                    for i in 0..rows {
                        if missing_arr[i] != 0 {
                            result.push(None);
                        } else {
                            result.push(Some(double_arr[i]));
                        }
                    }
                    
                    Ok(TypedColumn::F64Column(result))
                },
                _ => Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Unsupported column type: {}", type_val)
                )))
            }
        }
    }
}

// Implement Drop to ensure proper cleanup
impl Drop for ReadStatColumnReader {
    fn drop(&mut self) {
        if !self.column_ptr.is_null() {
            unsafe { readstat_sys::readstat_column_free(self.column_ptr) };
        }
    }
}