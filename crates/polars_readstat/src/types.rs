use libc::read;
use log::warn;
use polars_arrow::array::iterator::ArrayAccessor;
use polars_arrow::array::*;
use polars_arrow::datatypes::ArrowDataType;
use polars_arrow::scalar::Scalar;
use polars_arrow::datatypes::TimeUnit;
use chrono::{NaiveDate, NaiveDateTime, TimeZone, Duration};
use crate::common::ptr_to_string;
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
            Ok(ArrowDataType::Float64)
            /*
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
             */
        }
        _ => Err(crate::error::ReadstatError::UnsupportedType(var_type as i32)),
    }
}



// Convert readstat value to Arrow scalar representation (for appending to builders)
// This requires the target Arrow DataType determined previously.
pub(crate) fn convert_readstat_value(
    value: readstat_sys::readstat_value_t,
    target_type: &ArrowDataType,
    array: &mut dyn MutableArray,
    row:usize
) -> Result<(), crate::error::ReadstatError> {
    use readstat_sys::readstat_value_type;
    use readstat_sys::readstat_value_is_missing;
    use readstat_sys::readstat_type_t;
 
    

    use polars_arrow::scalar::{PrimitiveScalar, Utf8Scalar};
    
    let value_type: readstat_type_t = unsafe { readstat_value_type(value) };
    let is_missing: bool = unsafe { readstat_value_is_missing(value, core::ptr::null_mut()) } != 0; // Basic missing check
    
    
    match value_type {
        RS_TYPE_STRING => {
            let val = unsafe { ptr_to_string(readstat_sys::readstat_string_value(value)) };

            let array_typed: &mut MutablePlString = array.as_mut_any()
            .downcast_mut::<MutablePlString>()
            .unwrap();
            
            if val.is_empty() {
                array_typed.push(None::<String>);
            }
            else {
                array_typed.push(Some(val));
            }

        },
        RS_TYPE_INT8 => {
            let val = unsafe { readstat_sys::readstat_int8_value(value) };
            let array_typed: &mut MutablePrimitiveArray<i8> = array.as_mut_any()
                .downcast_mut::<MutablePrimitiveArray<i8>>()
                .unwrap();
            array_typed.push(Some(val));
        },
        RS_TYPE_INT16 => {
            let val = unsafe { readstat_sys::readstat_int16_value(value) };
            let array_typed: &mut MutablePrimitiveArray<i16>= array.as_mut_any()
                .downcast_mut::<MutablePrimitiveArray<i16>>()
                .unwrap();
            array_typed.push(Some(val));
        },
        RS_TYPE_INT32 => {
            let val = unsafe { readstat_sys::readstat_int32_value(value) };
            let array_typed: &mut MutablePrimitiveArray<i32>= array.as_mut_any()
                .downcast_mut::<MutablePrimitiveArray<i32>>()
                .unwrap();
            array_typed.push(Some(val));

        },
        RS_TYPE_FLOAT => {
            let val: f32 = unsafe { readstat_sys::readstat_float_value(value) };
            let array_typed: &mut MutablePrimitiveArray<f32>= array.as_mut_any()
                .downcast_mut::<MutablePrimitiveArray<f32>>()
                .unwrap();
            array_typed.push(Some(val));
        },
        RS_TYPE_DOUBLE => {
            let val: f64 = unsafe { readstat_sys::readstat_double_value(value) };
            let array_typed: &mut MutablePrimitiveArray<f64>= array.as_mut_any()
                .downcast_mut::<MutablePrimitiveArray<f64>>()
                .unwrap();
            array_typed.push(Some(val));
        },
        /*
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
        */
        _  => {},

        
    }

    Ok(())
}


/*
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
         */
pub fn create_mutable_array_for_type(data_type: &ArrowDataType,n_rows:usize) -> Box<dyn MutableArray> {
    match data_type {
        ArrowDataType::Boolean => Box::new(MutableBooleanArray::with_capacity(n_rows)),
        ArrowDataType::Int8 => Box::new(MutablePrimitiveArray::<i8>::with_capacity(n_rows)),
        ArrowDataType::Int16 => Box::new(MutablePrimitiveArray::<i16>::with_capacity(n_rows)),
        ArrowDataType::Int32 => Box::new(MutablePrimitiveArray::<i32>::with_capacity(n_rows)),
        ArrowDataType::Int64 => Box::new(MutablePrimitiveArray::<i64>::with_capacity(n_rows)),
        ArrowDataType::UInt8 => Box::new(MutablePrimitiveArray::<u8>::with_capacity(n_rows)),
        ArrowDataType::UInt16 => Box::new(MutablePrimitiveArray::<u16>::with_capacity(n_rows)),
        ArrowDataType::UInt32 => Box::new(MutablePrimitiveArray::<u32>::with_capacity(n_rows)),
        ArrowDataType::UInt64 => Box::new(MutablePrimitiveArray::<u64>::with_capacity(n_rows)),
        ArrowDataType::Float32 => Box::new(MutablePrimitiveArray::<f32>::with_capacity(n_rows)),
        ArrowDataType::Float64 => Box::new(MutablePrimitiveArray::<f64>::with_capacity(n_rows)),
        ArrowDataType::Utf8 => Box::new(MutableUtf8Array::<i32>::with_capacity(n_rows)),
        ArrowDataType::LargeUtf8 => Box::new(MutableUtf8Array::<i64>::with_capacity(n_rows)),
        ArrowDataType::BinaryView => Box::new(MutablePlString::new()),
        
        // Add other types as needed
        _ => {
            // For unsupported types, default to Utf8
            warn!("Unsupported data type: {:?}, defaulting to BinaryView", data_type);
            Box::new(MutableBinaryViewArray::<[u8]>::with_capacity(n_rows))
        }
    }
}
// Convert Arrow value back to readstat value for writing
// This is more complex as you need to call specific readstat_insert_* functions.
// We'll handle this directly in the write logic.
