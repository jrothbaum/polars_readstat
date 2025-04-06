use thiserror::Error;

// Re-export readstat error type if needed directly
use readstat_sys::readstat_error_t;
use std::ffi::CStr;
use std::os::raw::c_char;

#[derive(Error, Debug)]
pub enum ReadstatError {
    #[error("Readstat C API error: {message} (Code: {code:?})")]
    ReadstatC {
        code: readstat_error_t,
        message: String,
        
    },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    //  #[error("Arrow error: {0}")]
    //  Arrow(#[from] polars_arrow::error::ArrowError),

    #[error("UTF-8 conversion error: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("Invalid C string pointer")]
    InvalidCString,

    #[error("Unsupported readstat type: {0}")]
    UnsupportedType(i32),

    #[error("Callback error: {0}")]
    Callback(String),

    #[error("Variable '{0}' not found")]
    VariableNotFound(String),

     #[error("Date/Time conversion error: {0}")]
    DateTimeConversion(String),
}


// Helper to convert readstat_error_t and message ptr to ReadstatError
pub(crate) unsafe fn check_readstat_error(
    code: readstat_error_t,
    message_ptr: *const c_char,
) -> Result<(), ReadstatError> {
    if code == readstat_sys::readstat_error_e_READSTAT_OK {
        Ok(())
    } else {
        let message = if message_ptr.is_null() {
            "Unknown error".to_string()
        } else {
            CStr::from_ptr(message_ptr)
                .to_str()
                .unwrap_or("Invalid error message encoding")
                .to_string()
        };
        Err(ReadstatError::ReadstatC { code, message })
    }
}


// Overload for functions returning only an error code
pub(crate) fn check_readstat_error_code(code: readstat_error_t) -> Result<(), ReadstatError> {
     if code == readstat_sys::readstat_error_e_READSTAT_OK {
        Ok(())
    } else {
         // Try to get a generic message for the code
         let msg_ptr = unsafe {readstat_sys::readstat_error_message(code) };
         
         unsafe { check_readstat_error(code, msg_ptr) }
    }
}

