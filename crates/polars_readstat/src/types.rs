use libc::read;
use polars_arrow::array::*;
use polars_arrow::datatypes::ArrowDataType;
use polars_arrow::scalar::Scalar;
use polars_arrow::datatypes::TimeUnit;
use chrono::{NaiveDate, NaiveDateTime, TimeZone, Duration};
use readstat_sys;

use readstat_sys::readstat_type_e_READSTAT_TYPE_STRING as RS_TYPE_STRING;
use readstat_sys::readstat_type_e_READSTAT_TYPE_STRING_REF as RS_TYPE_STRING_REF;
use readstat_sys::readstat_type_e_READSTAT_TYPE_INT8 as RS_TYPE_INT8;
use readstat_sys::readstat_type_e_READSTAT_TYPE_INT16 as RS_TYPE_INT16;
use readstat_sys::readstat_type_e_READSTAT_TYPE_INT32 as RS_TYPE_INT32;
use readstat_sys::readstat_type_e_READSTAT_TYPE_FLOAT as RS_TYPE_FLOAT;
use readstat_sys::readstat_type_e_READSTAT_TYPE_DOUBLE as RS_TYPE_DOUBLE;

use crate::formats::ReadStatVarFormatClass;

// SAS/Stata epoch: 1960-01-01
const SAS_EPOCH_DATE: i32 = -3653; // Days from 1970-01-01 to 1960-01-01
const SAS_EPOCH_DATETIME_SECONDS: i64 = -315619200; // Seconds from 1970-01-01T00:00:00Z to 1960-01-01T00:00:00Z

// Function to map readstat type and format to Arrow DataType
pub(crate) fn readstat_type_to_arrow(
    var_type: readstat_sys::readstat_type_t,
    format: &str, // Format string can distinguish dates/times stored as numbers
    format_class:Option<&ReadStatVarFormatClass>,
) -> Result<ArrowDataType, crate::error::ReadstatError> {
    


    match var_type {
        RS_TYPE_STRING | RS_TYPE_STRING_REF => Ok(ArrowDataType::BinaryView),
        RS_TYPE_INT8 => Ok(ArrowDataType::Int8),
        RS_TYPE_INT16 => Ok(ArrowDataType::Int16),
        RS_TYPE_INT32 => Ok(ArrowDataType::Int32),
        RS_TYPE_FLOAT => Ok(ArrowDataType::Float32),
        RS_TYPE_DOUBLE => {
            
            match format_class {
                Some(ReadStatVarFormatClass::Date) => Ok(ArrowDataType::Date32),
                Some(ReadStatVarFormatClass::DateTime) => Ok(ArrowDataType::Timestamp(TimeUnit::Second, None)),
                Some(ReadStatVarFormatClass::DateTimeWithMicroseconds) => Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None)),
                Some(ReadStatVarFormatClass::DateTimeWithMilliseconds) => Ok(ArrowDataType::Timestamp(TimeUnit::Millisecond, None)),
                Some(ReadStatVarFormatClass::DateTimeWithNanoseconds) => Ok(ArrowDataType::Timestamp(TimeUnit::Nanosecond, None)),
                Some(ReadStatVarFormatClass::Time) => Ok(ArrowDataType::Time32(TimeUnit::Second)),
                Some(ReadStatVarFormatClass::TimeWithMilliseconds) => Ok(ArrowDataType::Time32(TimeUnit::Millisecond)),
                Some(ReadStatVarFormatClass::TimeWithMicroseconds) => Ok(ArrowDataType::Time32(TimeUnit::Microsecond)),
                Some(ReadStatVarFormatClass::TimeWithNanoseconds) => Ok(ArrowDataType::Time32(TimeUnit::Nanosecond)),
                None => {
                    Ok(ArrowDataType::Float64)
                },
            }
        }
        _ => Err(crate::error::ReadstatError::UnsupportedType(var_type as i32)),
    }
}



// Convert readstat value to Arrow scalar representation (for appending to builders)
// This requires the target Arrow DataType determined previously.
pub(crate) fn convert_readstat_value(
    value: readstat_sys::readstat_value_t,
    target_type: &ArrowDataType,
) -> Result<Option<Box<dyn Scalar>>, crate::error::ReadstatError> {
    use readstat_sys::readstat_value_type;
    use readstat_sys::readstat_value_is_missing;
    use readstat_sys::readstat_type_t;
 
    

    use polars_arrow::scalar::{PrimitiveScalar, Utf8Scalar};
    
    let value_type: readstat_type_t = unsafe { readstat_value_type(value) };
    let is_missing: bool = unsafe { readstat_value_is_missing(value, core::ptr::null_mut()) } != 0; // Basic missing check
    
    

    macro_rules! convert_primitive {
        ($func:path, $ArrowDataType:ident, $rs_type:ty) => {{
            let val = unsafe { $func(value) };
            Ok(Some(Box::new(PrimitiveScalar::new(ArrowDataType::$ident,val)) as Box<dyn Scalar>))
        }};
         ($func:ident, $ArrowDataType:ident, $rs_type:ty, $cast_as:ty) => {{
            let val = unsafe { $func(value) } as $cast_as;
             Ok(Some(Box::new(PrimitiveScalar::new(ArrowDataType::$ident,val)) as Box<dyn Scalar>))
        }};
    }

    match value_type {
        RS_TYPE_STRING => {
            let c_str = unsafe { readstat_sys::readstat_string_value(value) };
            if c_str.is_null() {
                Ok(None) // Should be caught by is_missing, but check anyway
            } else {
                let rust_str = unsafe { std::ffi::CStr::from_ptr(c_str) }.to_str()?;
                Ok(Some(Box::new(Utf8Scalar::<i32>::new(Some(rust_str))) as Box<dyn Scalar>))
            }
        },
        RS_TYPE_INT8 => {
            let val = unsafe { readstat_sys::readstat_int8_value(value) };
            Ok(Some(Box::new(PrimitiveScalar::new(ArrowDataType::Int8,Some(val))) as Box<dyn Scalar>))
        },
        RS_TYPE_INT16 => {
            let val = unsafe { readstat_sys::readstat_int16_value(value) };
            Ok(Some(Box::new(PrimitiveScalar::new(ArrowDataType::Int16,Some(val))) as Box<dyn Scalar>))
        },
        RS_TYPE_INT32 => {
            let val = unsafe { readstat_sys::readstat_int32_value(value) };
            Ok(Some(Box::new(PrimitiveScalar::new(ArrowDataType::Int32,Some(val))) as Box<dyn Scalar>))
        },
        RS_TYPE_FLOAT => {
            let val = unsafe { readstat_sys::readstat_float_value(value) };
            Ok(Some(Box::new(PrimitiveScalar::new(ArrowDataType::Float32,Some(val))) as Box<dyn Scalar>))
        },
        RS_TYPE_DOUBLE => {
            let double_val = unsafe { readstat_sys::readstat_double_value(value) };
            match target_type {
                ArrowDataType::Float64 => {
                    Ok(Some(Box::new(PrimitiveScalar::new(ArrowDataType::Float64, Some(double_val))) as Box<dyn Scalar>))
                },
                ArrowDataType::Date32 => {
                    // Assume double_val is days since SAS/Stata epoch (1960-01-01)
                    // Convert to days since UNIX epoch (1970-01-01)
                    let days_since_unix_epoch = (double_val as i32).saturating_add(SAS_EPOCH_DATE);
                    Ok(Some(Box::new(PrimitiveScalar::new(ArrowDataType::Date32, Some(days_since_unix_epoch))) as Box<dyn Scalar>))
                },
                ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => {
                    // Assume double_val is seconds since SAS/Stata epoch (1960-01-01)
                    // Convert to milliseconds since UNIX epoch (1970-01-01)
                    let seconds_since_unix_epoch = (double_val as i64).saturating_add(SAS_EPOCH_DATETIME_SECONDS);
                    let millis_since_unix_epoch = seconds_since_unix_epoch.saturating_mul(1000);
                    Ok(Some(Box::new(PrimitiveScalar::new(ArrowDataType::Timestamp(TimeUnit::Millisecond,None), Some(millis_since_unix_epoch))) as Box<dyn Scalar>))
                },
                ArrowDataType::Timestamp(TimeUnit::Second, _) => {
                    // Assume double_val is seconds since SAS/Stata epoch (1960-01-01)
                    // Convert to seconds since UNIX epoch (1970-01-01)
                    let seconds_since_unix_epoch = (double_val as i64).saturating_add(SAS_EPOCH_DATETIME_SECONDS);
                    Ok(Some(Box::new(PrimitiveScalar::new(ArrowDataType::Timestamp(TimeUnit::Second,None), Some(seconds_since_unix_epoch))) as Box<dyn Scalar>))
                },
                // Add other time units if needed (Microsecond, Nanosecond)
                _ => {
                    // Default to Float64 if target isn't a known time type derived from double
                    Ok(Some(Box::new(PrimitiveScalar::new(ArrowDataType::Float64, Some(double_val))) as Box<dyn Scalar>))
                }
            }
        },
        _ => Err(crate::error::ReadstatError::UnsupportedType(value_type as i32)), // Changed * to _ and fixed value*type
    }
}

// Convert Arrow value back to readstat value for writing
// This is more complex as you need to call specific readstat_insert_* functions.
// We'll handle this directly in the write logic.
