use chrono::DateTime;
use log::debug;
use num_traits::FromPrimitive;
use std::os::raw::{c_char, c_int, c_void};

use crate::{
    common::ptr_to_string,
    formats,
    rs_data::{TypedColumn,ReadStatData},
    rs_metadata::{ReadStatCompress, ReadStatEndian, ReadStatMetadata, ReadStatVarMetadata},
    rs_var::{ReadStatVar, ReadStatVarType, ReadStatVarTypeClass},
};

// C types
#[allow(dead_code)]
#[derive(Debug)]
#[repr(C)]
enum ReadStatHandler {
    READSTAT_HANDLER_OK,
    READSTAT_HANDLER_ABORT,
    READSTAT_HANDLER_SKIP_VARIABLE,
}

// C callback functions

// TODO: May need a version of handle_metadata that only gets metadata
//       and a version that does very little and instead metadata handling occurs
//       in handle_value function
//       As an example see the below from the readstat binary
//         https://github.com/WizardMac/ReadStat/blob/master/src/bin/readstat.c#L98
pub extern "C" fn handle_metadata(
    metadata: *mut readstat_sys::readstat_metadata_t,
    ctx: *mut c_void,
) -> c_int {
    // dereference ctx pointer
    let m = unsafe { &mut *(ctx as *mut ReadStatMetadata) };

    // get metadata
    let rc: c_int = unsafe { readstat_sys::readstat_get_row_count(metadata) };
    let vc: c_int = unsafe { readstat_sys::readstat_get_var_count(metadata) };
    let table_name = unsafe { ptr_to_string(readstat_sys::readstat_get_table_name(metadata)) };
    let file_label = unsafe { ptr_to_string(readstat_sys::readstat_get_file_label(metadata)) };
    let file_encoding =
        unsafe { ptr_to_string(readstat_sys::readstat_get_file_encoding(metadata)) };
    let version: c_int = unsafe { readstat_sys::readstat_get_file_format_version(metadata) };
    let is64bit = unsafe { readstat_sys::readstat_get_file_format_is_64bit(metadata) };
    let ct = DateTime::from_timestamp(
        unsafe { readstat_sys::readstat_get_creation_time(metadata) },
        0,
    )
    .expect("Panics (returns None) on the out-of-range number of seconds (more than 262 000 years away from common era) and/or invalid nanosecond (2 seconds or more")
    .format("%Y-%m-%d %H:%M:%S")
    .to_string();
    let mt = DateTime::from_timestamp   (
        unsafe { readstat_sys::readstat_get_modified_time(metadata) },
        0,
    )
    .expect("Panics (returns None) on the out-of-range number of seconds (more than 262 000 years away from common era) and/or invalid nanosecond (2 seconds or more")
    .format("%Y-%m-%d %H:%M:%S")
    .to_string();

    #[allow(clippy::useless_conversion)]
    let compression = match FromPrimitive::from_i32(unsafe {
        readstat_sys::readstat_get_compression(metadata)
            .try_into()
            .unwrap()
    }) {
        Some(t) => t,
        None => ReadStatCompress::None,
    };

    #[allow(clippy::useless_conversion)]
    let endianness = match FromPrimitive::from_i32(unsafe {
        readstat_sys::readstat_get_endianness(metadata)
            .try_into()
            .unwrap()
    }) {
        Some(t) => t,
        None => ReadStatEndian::None,
    };

    debug!("row_count is {}", rc);
    debug!("var_count is {}", vc);
    debug!("table_name is {}", &table_name);
    debug!("file_label is {}", &file_label);
    debug!("file_encoding is {}", &file_encoding);
    debug!("version is {}", version);
    debug!("is64bit is {}", is64bit);
    debug!("creation_time is {}", &ct);
    debug!("modified_time is {}", &mt);
    debug!("compression is {:#?}", &compression);
    debug!("endianness is {:#?}", &endianness);

    // insert into ReadStatMetadata struct
    m.row_count = rc;
    m.var_count = vc;
    m.table_name = table_name;
    m.file_label = file_label;
    m.file_encoding = file_encoding;
    m.version = version;
    m.is64bit = is64bit;
    m.creation_time = ct;
    m.modified_time = mt;
    m.compression = compression;
    m.endianness = endianness;

    debug!("metadata struct is {:#?}", &m);

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

/*
pub extern "C" fn handle_metadata_row_count_only(
    metadata: *mut readstat_sys::readstat_metadata_t,
    ctx: *mut c_void,
) -> c_int {
    // dereference ctx pointer
    let mut d = unsafe { &mut *(ctx as *mut ReadStatData) };

    // get metadata
    let rc: c_int = unsafe { readstat_sys::readstat_get_row_count(metadata) };
    debug!("row_count is {}", rc);

    // insert into ReadStatMetadata struct
    d.metadata.row_count = rc;
    debug!("d.metadata struct is {:#?}", &d.metadata);

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}
*/

pub extern "C" fn handle_variable(
    index: c_int,
    variable: *mut readstat_sys::readstat_variable_t,
    #[allow(unused_variables)] val_labels: *const c_char,
    ctx: *mut c_void,
) -> c_int {
    // dereference ctx pointer
    let m = unsafe { &mut *(ctx as *mut ReadStatMetadata) };

    // get variable metadata
    #[allow(clippy::useless_conversion)]
    let var_type = match FromPrimitive::from_i32(unsafe {
        readstat_sys::readstat_variable_get_type(variable)
            .try_into()
            .unwrap()
    }) {
        Some(t) => t,
        None => ReadStatVarType::Unknown,
    };

    #[allow(clippy::useless_conversion)]
    let var_type_class = match FromPrimitive::from_i32(unsafe {
        readstat_sys::readstat_variable_get_type_class(variable)
            .try_into()
            .unwrap()
    }) {
        Some(t) => t,
        None => ReadStatVarTypeClass::Numeric,
    };

    let var_name = unsafe { ptr_to_string(readstat_sys::readstat_variable_get_name(variable)) };
    let var_label = unsafe { ptr_to_string(readstat_sys::readstat_variable_get_label(variable)) };
    let var_format = unsafe { ptr_to_string(readstat_sys::readstat_variable_get_format(variable)) };
    let var_format_class = formats::match_var_format(&var_format,&m.extension);

    debug!("var_type is {:#?}", &var_type);
    debug!("var_type_class is {:#?}", &var_type_class);
    debug!("var_name is {}", &var_name);
    debug!("var_label is {}",    &var_label);
    debug!("var_format is {}", &var_format);
    debug!("var_format_class is {:#?}", &var_format_class);

    // insert into BTreeMap within ReadStatMetadata struct
    m.vars.insert(
        index,
        ReadStatVarMetadata::new(
            var_name,
            var_type,
            var_type_class,
            var_label,
            var_format,
            var_format_class,
        ),
    );

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}


pub extern "C" fn handle_variable_noop(
    _index: c_int,
    _variable: *mut readstat_sys::readstat_variable_t,
    _val_labels: *const c_char,
    _ctx: *mut c_void,
) -> c_int {
    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

pub extern "C" fn handle_value(
    obs_index: c_int,
    variable: *mut readstat_sys::readstat_variable_t,
    value: readstat_sys::readstat_value_t,
    ctx: *mut c_void,
) -> c_int {
    // dereference ctx pointer
    let d = unsafe { &mut *(ctx as *mut ReadStatData) };

    // get index, type, and missingness
    let var_index: c_int = unsafe { readstat_sys::readstat_variable_get_index(variable) };

    let var_index_usize = var_index as usize;

    //  If d.columns_to_read is not none,
    //      Get the assignment position of the variable (if it's there)
    //          and None if it isn't
    //  If d.columns_to_read
    let var_index_assign = if d.columns_to_read.is_none() {
        // If columns_to_read is None, use the variable index directly
        Some(var_index_usize)
    } else {
        //  Lookup the value (or None) of the assignment column index
        d.columns_original_index_to_data.as_ref().unwrap()[var_index_usize]

    };

    if var_index_assign.is_none() {
        //  Don't read it, we're good
        return ReadStatHandler::READSTAT_HANDLER_OK as c_int;
    }


    // let value_type: readstat_sys::readstat_type_t =
    //     unsafe { readstat_sys::readstat_value_type(value) };
    let is_missing: bool = unsafe { readstat_sys::readstat_value_is_system_missing(value) } > 0;

    if is_missing {
        //  Already set to missing, we're good
        return ReadStatHandler::READSTAT_HANDLER_OK as c_int;
    }
    // debug!("chunk_rows_to_process is {}", d.chunk_rows_to_process);
    // debug!("chunk_row_start is {}", d.chunk_row_start);
    // debug!("chunk_row_end is {}", d.chunk_row_end);
    // debug!("chunk_rows_processed is {}", d.chunk_rows_processed);
    // debug!("var_count is {}", d.var_count);
    // debug!("obs_index is {}", obs_index);
    // debug!("var_index is {}", var_index);
    // debug!("value_type is {:#?}", &value_type);
    // debug!("is_missing is {}", is_missing);

    // get value and assign to
    match &mut d.cols[var_index_assign.unwrap()] {
        TypedColumn::StringColumn(vec) => {
            vec[obs_index as usize] = Some(ReadStatVar::get_value_string(value));
        },
        TypedColumn::I8Column(vec) => {
            vec[obs_index as usize] = Some(ReadStatVar::get_value_i8(value));
        },
        TypedColumn::I16Column(vec) => {
            vec[obs_index as usize] = Some(ReadStatVar::get_value_i16(value));
        },
        TypedColumn::I32Column(vec) => {
            vec[obs_index as usize] = Some(ReadStatVar::get_value_i32(value));
        },
        TypedColumn::I64Column(vec) => {
            vec[obs_index as usize] = Some(ReadStatVar::get_value_i64(value));
        },
        TypedColumn::F32Column(vec) => {
            vec[obs_index as usize] = Some(ReadStatVar::get_value_f32(value));
        },
        TypedColumn::F64Column(vec) => {
            vec[obs_index as usize] = Some(ReadStatVar::get_value_f64(value));
        },
        TypedColumn::DateColumn(vec) => {
            vec[obs_index as usize] = Some(ReadStatVar::get_value_date32(
                value,
                &d.extension
            ));
        },
        TypedColumn::TimeColumn(vec) => {
            vec[obs_index as usize] = Some(ReadStatVar::get_value_time64(
                value,
                &d.extension
            ));
        },
        TypedColumn::DateTimeColumn(vec) => {
            vec[obs_index as usize] = Some(ReadStatVar::get_value_datetime64(
                value,
                &d.extension
            ));
        },
    }
    
    // if row is complete
    if var_index == (d.var_count - 1) {
        d.chunk_rows_processed += 1;
        if let Some(trp) = &d.total_rows_processed {
            trp.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    };

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}
