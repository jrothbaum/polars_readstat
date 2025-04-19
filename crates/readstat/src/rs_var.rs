use num_derive::FromPrimitive;
use serde::Serialize;

use crate::common::ptr_to_string;

use crate::rs_data::Extensions;
// Constants
const DIGITS: usize = 14;
const DAY_SHIFT_SAS_STATA: i32 = 3653;
const SEC_SHIFT_SAS_STATA: i64 = 315619200;

//  Not needed - dates are stored as datetimes in SPSS, it seems
//  const DAY_SHIFT_SPSS: i32 = 141428;
const SEC_SHIFT_SPSS: i64 = 12219379200;

pub const SEC_MILLISECOND: i64 = 1_000;
pub const SEC_MICROSECOND: i64 = 1_000_000;
//  pub const SEC_NANOSECOND: i64 = 1_000_000_000;



#[derive(Debug, Clone)]
pub enum ReadStatVar {
    ReadStat_String(Option<String>),
    ReadStat_i8(Option<i8>),
    ReadStat_i16(Option<i16>),
    ReadStat_i32(Option<i32>),
    ReadStat_f32(Option<f32>),
    ReadStat_f64(Option<f64>),
    ReadStat_Date(Option<i32>),
    ReadStat_DateTime(Option<i64>),
    // ReadStat_DateTimeWithMilliseconds(Option<i64>),
    // ReadStat_DateTimeWithMicroseconds(Option<i64>),
    // ReadStat_DateTimeWithNanoseconds(Option<i64>),
    ReadStat_Time(Option<i64>),
    // ReadStat_TimeWithMilliseconds(Option<i64>),
    // ReadStat_TimeWithMicroseconds(Option<i64>),
    // ReadStat_TimeWithNanoseconds(Option<i64>),
}

impl ReadStatVar {
    pub fn get_value_string(
        value: readstat_sys::readstat_value_t
    ) -> String {
        unsafe { ptr_to_string(readstat_sys::readstat_string_value(value))}
    }

    pub fn get_value_i8(
        value: readstat_sys::readstat_value_t
    ) -> i32 {
        unsafe { readstat_sys::readstat_int8_value(value) as i32 }
    }

    pub fn get_value_i16(
        value: readstat_sys::readstat_value_t
    ) -> i32 {
        unsafe { readstat_sys::readstat_int16_value(value) as i32 }
    }

    pub fn get_value_i32(
        value: readstat_sys::readstat_value_t
    ) -> i32 {
        unsafe { readstat_sys::readstat_int32_value(value) }
    }

    pub fn get_value_i64(
        value: readstat_sys::readstat_value_t
    ) -> i64 {
        unsafe { readstat_sys::readstat_double_value(value) as i64}
    }

    pub fn get_value_f32(
        value: readstat_sys::readstat_value_t
    ) -> f32 {
        let value = unsafe { readstat_sys::readstat_float_value(value) };
        let value: f32 = lexical::parse(format!("{1:.0$}", DIGITS, value)).unwrap();

        value
    }

    pub fn get_value_f64(
        value: readstat_sys::readstat_value_t
    ) -> f64 {
        let value = unsafe { readstat_sys::readstat_double_value(value) };
        let value: f64 = lexical::parse(format!("{1:.0$}", DIGITS, value)).unwrap();

        value
    }

    pub fn get_value_date32(
        value: readstat_sys::readstat_value_t,
        extension:&Extensions
    ) -> i32 {

        let value = match extension {
            Extensions::sas7bdat |
                Extensions::dta => {
                let value = unsafe { readstat_sys::readstat_int32_value(value) };
                value - DAY_SHIFT_SAS_STATA
            },
            Extensions::sav => {
                let value = unsafe { readstat_sys::readstat_double_value(value) as i64};
                //  Convert to date from datetime
                ((value - SEC_SHIFT_SPSS as i64)/(60*60*24 as i64)) as i32
            },
            _ => {
                unsafe { readstat_sys::readstat_int32_value(value) }
                
            }
        };
        value
    }


    pub fn get_value_time64(
        value: readstat_sys::readstat_value_t,
        extension:&Extensions
    ) -> i64 {
        let value = unsafe { readstat_sys::readstat_int32_value(value) as i64};
        let value = match &extension {
            Extensions::sas7bdat |
                Extensions::dta => {
                value * SEC_MICROSECOND //- SEC_SHIFT_SAS_STATA 
            },
            Extensions::sav => {
                value * SEC_MICROSECOND //- SEC_SHIFT_SPSS 
            },
            _ => {
                value
            }
        };
        value 
    }

    pub fn get_value_datetime64(
        value: readstat_sys::readstat_value_t,
        extension:&Extensions
    ) -> i64 {
        let value = unsafe { readstat_sys::readstat_double_value(value) as i64};
        let value = match extension {
            Extensions::sas7bdat |
                Extensions::dta => {
                value - SEC_SHIFT_SAS_STATA* SEC_MILLISECOND
            },
            Extensions::sav => {
                value - SEC_SHIFT_SPSS* SEC_MILLISECOND
            },
            _ => {
                value 
            }
        };
        value 
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ReadStatVarFormatClass {
    Date,
    DateTime,
    // DateTimeWithMilliseconds,
    // DateTimeWithMicroseconds,
    // DateTimeWithNanoseconds,
    Time,
    // TimeWithMilliseconds,
    // TimeWithMicroseconds,
    // TimeWithNanoseconds
}

#[derive(Clone, Copy, Debug, FromPrimitive, Serialize)]
pub enum ReadStatVarType {
    String = readstat_sys::readstat_type_e_READSTAT_TYPE_STRING as isize,
    Int8 = readstat_sys::readstat_type_e_READSTAT_TYPE_INT8 as isize,
    Int16 = readstat_sys::readstat_type_e_READSTAT_TYPE_INT16 as isize,
    Int32 = readstat_sys::readstat_type_e_READSTAT_TYPE_INT32 as isize,
    Float = readstat_sys::readstat_type_e_READSTAT_TYPE_FLOAT as isize,
    Double = readstat_sys::readstat_type_e_READSTAT_TYPE_DOUBLE as isize,
    StringRef = readstat_sys::readstat_type_e_READSTAT_TYPE_STRING_REF as isize,
    Unknown,
}

#[derive(Clone, Copy, Debug, FromPrimitive, Serialize)]
pub enum ReadStatVarTypeClass {
    String = readstat_sys::readstat_type_class_e_READSTAT_TYPE_CLASS_STRING as isize,
    Numeric = readstat_sys::readstat_type_class_e_READSTAT_TYPE_CLASS_NUMERIC as isize,
}
