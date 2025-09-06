use log::debug;
use num_traits::FromPrimitive;
use std::{
    error::Error,
    os::raw::{c_char, c_long, c_void},
};

use crate::err::ReadStatError;
use readstat_sys::SharedMmap; 

pub struct ReadStatParser {
    parser: *mut readstat_sys::readstat_parser_t,
}

impl ReadStatParser {
    pub fn new() -> Self {
        let parser: *mut readstat_sys::readstat_parser_t =
            unsafe { readstat_sys::readstat_parser_init() };

        Self { parser }
    }

    // Add new constructor for mmap
    pub fn new_with_mmap(file_path: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let shared_mmap = SharedMmap::new(file_path)?;
        let parser = shared_mmap.create_parser()?;
        Ok(Self { parser })
    }

    pub fn new_from_shared_mmap(shared_mmap: &SharedMmap) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let parser = shared_mmap.create_parser()?;
        Ok(Self { parser })
    }

    pub fn set_metadata_handler(
        self,
        metadata_handler: readstat_sys::readstat_metadata_handler,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let set_metadata_handler_error =
            unsafe { readstat_sys::readstat_set_metadata_handler(self.parser, metadata_handler) };

        debug!(
            "After setting metadata handler, error ==> {}",
            &set_metadata_handler_error
        );

        #[allow(clippy::useless_conversion)]
        match FromPrimitive::from_i32(set_metadata_handler_error.try_into().unwrap()) {
            Some(ReadStatError::READSTAT_OK) => Ok(self),
            Some(e) => Err(From::from(format!(
                "Unable to set metdata handler: {:#?}",
                e
            ))),
            None => Err(From::from(
                "Error when attempting to set metadata handler: Unknown return value",
            )),
        }
    }

    pub fn set_row_limit(
        self,
        row_limit: Option<u32>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        match row_limit {
            Some(r) => {
                let set_row_limit_error =
                    unsafe { readstat_sys::readstat_set_row_limit(self.parser, r as c_long) };

                debug!(
                    "After setting row limit, error ==> {}",
                    &set_row_limit_error
                );

                #[allow(clippy::useless_conversion)]
                match FromPrimitive::from_i32(set_row_limit_error.try_into().unwrap()) {
                    Some(ReadStatError::READSTAT_OK) => Ok(self),
                    Some(e) => Err(From::from(format!("Unable to set row limit: {:#?}", e))),
                    None => Err(From::from(
                        "Error when attempting to set row limit: Unknown return value",
                    )),
                }
            }
            None => Ok(self),
        }
    }

    pub fn set_row_offset(
        self,
        row_offset: Option<u32>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        match row_offset {
            Some(r) => {
                let set_row_offset_error =
                    unsafe { readstat_sys::readstat_set_row_offset(self.parser, r as c_long) };

                debug!(
                    "After setting row offset, error ==> {}",
                    &set_row_offset_error
                );

                #[allow(clippy::useless_conversion)]
                match FromPrimitive::from_i32(set_row_offset_error.try_into().unwrap()) {
                    Some(ReadStatError::READSTAT_OK) => Ok(self),
                    Some(e) => Err(From::from(format!("Unable to set row limit: {:#?}", e))),
                    None => Err(From::from(
                        "Error when attempting to set row limit: Unknown return value",
                    )),
                }
            }
            None => Ok(self),
        }
    }

    pub fn set_value_label_handler(
        self,
        value_label_handler: readstat_sys::readstat_value_label_handler,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let set_value_label_handler_error =
            unsafe { readstat_sys::readstat_set_value_label_handler(self.parser, value_label_handler) };
        debug!(
            "After setting value label handler, error ==> {}",
            &set_value_label_handler_error
        );
        #[allow(clippy::useless_conversion)]
        match FromPrimitive::from_i32(set_value_label_handler_error.try_into().unwrap()) {
            Some(ReadStatError::READSTAT_OK) => Ok(self),
            Some(e) => Err(From::from(format!(
                "Unable to set value label handler: {:#?}",
                e
            ))),
            None => Err(From::from(
                "Error when attempting to set value label handler: Unknown return value",
            )),
        }
    }
    pub fn set_variable_handler(
        self,
        variable_handler: readstat_sys::readstat_variable_handler,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let set_variable_handler_error =
            unsafe { readstat_sys::readstat_set_variable_handler(self.parser, variable_handler) };

        debug!(
            "After setting variable handler, error ==> {}",
            &set_variable_handler_error
        );

        #[allow(clippy::useless_conversion)]
        match FromPrimitive::from_i32(set_variable_handler_error.try_into().unwrap()) {
            Some(ReadStatError::READSTAT_OK) => Ok(self),
            Some(e) => Err(From::from(format!(
                "Unable to set variable handler: {:#?}",
                e
            ))),
            None => Err(From::from(
                "Error when attempting to set variable handler: Unknown return value",
            )),
        }
    }

    pub fn set_value_handler(
        self,
        value_handler: readstat_sys::readstat_value_handler,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let set_value_handler_error =
            unsafe { readstat_sys::readstat_set_value_handler(self.parser, value_handler) };

        debug!(
            "After setting value handler, error ==> {}",
            &set_value_handler_error
        );

        #[allow(clippy::useless_conversion)]
        match FromPrimitive::from_i32(set_value_handler_error.try_into().unwrap()) {
            Some(ReadStatError::READSTAT_OK) => Ok(self),
            Some(e) => Err(From::from(format!("Unable to set value handler: {:#?}", e))),
            None => Err(From::from(
                "Error when attempting to set value handler: Unknown return value",
            )),
        }
    }

    pub fn parse_sav(
        &mut self,
        path: *const c_char,
        user_ctx: *mut c_void,
    ) -> readstat_sys::readstat_error_t {
        let parse_sav_error: readstat_sys::readstat_error_t =
            unsafe { readstat_sys::readstat_parse_sav(self.parser, path, user_ctx) };

        debug!(
            "After calling parse dta, error ==> {}",
            &parse_sav_error
        );

        parse_sav_error
    }

    pub fn parse_dta(
        &mut self,
        path: *const c_char,
        user_ctx: *mut c_void,
    ) -> readstat_sys::readstat_error_t {
        let parse_dta_error: readstat_sys::readstat_error_t =
            unsafe { readstat_sys::readstat_parse_dta(self.parser, path, user_ctx) };

        debug!(
            "After calling parse dta, error ==> {}",
            &parse_dta_error
        );

        parse_dta_error
    }
    pub fn parse_sas7bdat(
        &mut self,
        path: *const c_char,
        user_ctx: *mut c_void,
    ) -> readstat_sys::readstat_error_t {
        let parse_sas7bdat_error: readstat_sys::readstat_error_t =
            unsafe { readstat_sys::readstat_parse_sas7bdat(self.parser, path, user_ctx) };

        debug!(
            "After calling parse sas7bdat, error ==> {}",
            &parse_sas7bdat_error
        );

        parse_sas7bdat_error
    }
}

impl Drop for ReadStatParser {
    fn drop(&mut self) {
        debug!("Freeing parser");

        unsafe { readstat_sys::readstat_parser_free(self.parser) };
    }
}


pub fn create_parser(shared_mmap: Option<&SharedMmap>) -> Result<ReadStatParser, Box<dyn Error + Send + Sync>> {
    match shared_mmap {
        Some(mmap) => {
            //  println!("Using existing shared mmap");
            ReadStatParser::new_from_shared_mmap(mmap)
        },
        None => {
            //  println!("Using regular file-based parser");
            Ok(ReadStatParser::new())
        }
    }
}