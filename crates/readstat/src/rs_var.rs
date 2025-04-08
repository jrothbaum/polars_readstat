use log::debug;
use num_derive::FromPrimitive;
use serde::Serialize;
use std::{collections::BTreeMap, os::raw::c_int};

use crate::{common::ptr_to_string, rs_metadata::ReadStatVarMetadata};

// Constants
const DIGITS: usize = 14;
const DAY_SHIFT: i32 = 3653;
const SEC_SHIFT: i64 = 315619200;

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
    ReadStat_DateTimeWithMilliseconds(Option<i64>),
    ReadStat_DateTimeWithMicroseconds(Option<i64>),
    ReadStat_DateTimeWithNanoseconds(Option<i64>),
    ReadStat_Time(Option<i32>),
    // TODO
    // ReadStat_TimeWithMilliseconds(Option<i32>),
    // ReadStat_TimeWithMicroseconds(Option<i32>),
    // ReadStat_TimeWithNanoseconds(Option<i32>),
}

impl ReadStatVar {
    pub fn get_readstat_value(
        value: readstat_sys::readstat_value_t,
        value_type: readstat_sys::readstat_type_t,
        is_missing: c_int,
        vars: &BTreeMap<i32, ReadStatVarMetadata>,
        var_index: i32,
    ) -> Self {
        match value_type {
            readstat_sys::readstat_type_e_READSTAT_TYPE_STRING
            | readstat_sys::readstat_type_e_READSTAT_TYPE_STRING_REF => {
                if is_missing == 1 {
                    // return
                    Self::ReadStat_String(None)
                } else {
                    // get value
                    let value =
                        unsafe { ptr_to_string(readstat_sys::readstat_string_value(value)) };

                    // debug
                    debug!("value is {:#?}", &value);

                    // return
                    Self::ReadStat_String(Some(value))
                }
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_INT8 => {
                if is_missing == 1 {
                    Self::ReadStat_i8(None)
                } else {
                    // get value
                    let value = unsafe { readstat_sys::readstat_int8_value(value) };

                    // debug
                    debug!("value is {:#?}", value);

                    // return
                    Self::ReadStat_i8(Some(value))
                }
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_INT16 => {
                if is_missing == 1 {
                    Self::ReadStat_i16(None)
                } else {
                    // get value
                    let value = unsafe { readstat_sys::readstat_int16_value(value) };

                    // debug
                    debug!("value is {:#?}", value);

                    // return
                    Self::ReadStat_i16(Some(value))
                }
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_INT32 => {
                if is_missing == 1 {
                    Self::ReadStat_i32(None)
                } else {
                    // get value
                    let value = unsafe { readstat_sys::readstat_int32_value(value) };

                    // debug
                    debug!("value is {:#?}", value);

                    // return
                    Self::ReadStat_i32(Some(value))
                }
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_FLOAT => {
                if is_missing == 1 {
                    Self::ReadStat_f32(None)
                } else {
                    // get value
                    let value = unsafe { readstat_sys::readstat_float_value(value) };

                    // debug
                    debug!("value (before parsing) is {:#?}", value);

                    let value: f32 = lexical::parse(format!("{1:.0$}", DIGITS, value)).unwrap();

                    // debug
                    debug!("value (after parsing) is {:#?}", value);

                    // return
                    Self::ReadStat_f32(Some(value))
                }
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_DOUBLE => {
                let var_format_class = vars.get(&var_index).unwrap().var_format_class;

                if is_missing == 1 {
                    match var_format_class {
                        None => Self::ReadStat_f64(None),
                        Some(fc) => match fc {
                            ReadStatVarFormatClass::Date => Self::ReadStat_Date(None),
                            ReadStatVarFormatClass::DateTime => Self::ReadStat_DateTime(None),
                            ReadStatVarFormatClass::DateTimeWithMilliseconds => {
                                Self::ReadStat_DateTimeWithMilliseconds(None)
                            }
                            ReadStatVarFormatClass::DateTimeWithMicroseconds => {
                                Self::ReadStat_DateTimeWithMicroseconds(None)
                            }
                            ReadStatVarFormatClass::DateTimeWithNanoseconds => {
                                Self::ReadStat_DateTimeWithNanoseconds(None)
                            }
                            ReadStatVarFormatClass::Time => Self::ReadStat_Time(None),
                        },
                    }
                } else {
                    // get value
                    let value = unsafe { readstat_sys::readstat_double_value(value) };

                    // debug
                    debug!("value (before parsing) is {:#?}", value);

                    let value: f64 = lexical::parse(format!("{1:.0$}", DIGITS, value)).unwrap();

                    // debug
                    debug!("value (after parsing) is {:#?}", value);

                    // is double a value or is it really a date, time, or datetime?
                    match var_format_class {
                        None => Self::ReadStat_f64(Some(value)),
                        Some(fc) => match fc {
                            ReadStatVarFormatClass::Date => Self::ReadStat_Date(Some(
                                (value as i32).checked_sub(DAY_SHIFT).unwrap(),
                            )),
                            ReadStatVarFormatClass::DateTime => Self::ReadStat_DateTime(Some(
                                (value as i64).checked_sub(SEC_SHIFT).unwrap(),
                            )),
                            ReadStatVarFormatClass::DateTimeWithMilliseconds => {
                                Self::ReadStat_DateTime(Some(
                                    (value as i64).checked_sub(SEC_SHIFT).unwrap() * 1000,
                                ))
                            }
                            ReadStatVarFormatClass::DateTimeWithMicroseconds => {
                                Self::ReadStat_DateTime(Some(
                                    (value as i64).checked_sub(SEC_SHIFT).unwrap() * 1000000,
                                ))
                            }
                            ReadStatVarFormatClass::DateTimeWithNanoseconds => {
                                Self::ReadStat_DateTime(Some(
                                    (value as i64).checked_sub(SEC_SHIFT).unwrap() * 1000000000,
                                ))
                            }
                            ReadStatVarFormatClass::Time => Self::ReadStat_Time(Some(value as i32)),
                        },
                    }
                }
            }
            // exhaustive
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ReadStatVarFormatClass {
    Date,
    DateTime,
    DateTimeWithMilliseconds,
    DateTimeWithMicroseconds,
    DateTimeWithNanoseconds,
    Time,
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
