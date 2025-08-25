use polars::prelude::{
    datatypes::DataType, DataFrame, PolarsResult, Schema, Series
};
use log::debug;
use num_traits::FromPrimitive;
use std::{
    collections::BTreeMap,
    error::Error,
    os::raw::c_void,
    sync::{Arc, Mutex, Condvar},
    thread,
    time::Duration
};

use crate::{
    cb,
    err::ReadStatError,
    rs_metadata::{
        ReadStatMetadata, 
        ReadStatVarMetadata,
        schema_with_filter_pushdown
    },
    rs_parser::ReadStatParser,
    rs_path::ReadStatPath,
    series_builder::SeriesBuilder
};

pub enum Extensions  {
    sas7bdat,
    dta,
    sav,
    NotSet
}
impl Default for Extensions {
    fn default() -> Self {
        // Choose which variant should be the default
        Extensions::NotSet
    }
}

#[derive(Default)]
pub struct ReadStatData {
    pub var_count: i32,
    pub vars: BTreeMap<i32, ReadStatVarMetadata>,
    // data
    pub cols: Vec<SeriesBuilder>,
    pub schema: Schema,
    pub extension: Extensions,
    // chunk
    pub df: Option<DataFrame>,
    pub chunk_size: usize,
    pub chunk_rows_processed: usize,
    pub chunk_index: usize,

    // total rows
    pub row_start: usize,
    pub row_end: usize,
    pub rows_to_process: usize,
    pub total_rows_processed: usize,
    // errors
    pub errors: Vec<String>,
    pub columns_to_read: Option<Vec<usize>>,
    pub columns_original_index_to_data: Option<Vec<Option<usize>>>,
    

    //  Callback
    pub chunk_buffer: Arc<Mutex<Vec<DataFrame>>>,
    pub notifier: Option<Arc<Condvar>>,
}

impl ReadStatData {
    pub fn new(
        columns_to_read: Option<Vec<usize>>,
    ) -> Self {


        Self {
            // metadata
            var_count: 0,
            vars: BTreeMap::new(),
            // data
            cols: Vec::new(),
            schema: Schema::default(),
            extension: Extensions::NotSet,
            // df/chunk
            df: None,
            chunk_size: 0,
            chunk_rows_processed: 0,
            chunk_index: 0,

            // total rows
            row_start: 0,
            row_end: 0,
            total_rows_processed: 0,
            rows_to_process: 0,
            // errors
            errors: Vec::new(),
            columns_to_read: columns_to_read,
            columns_original_index_to_data: None,
            
            chunk_buffer: Arc::new(Mutex::new(Vec::new())),
            notifier: None,

        }
    }


    fn allocate_cols(&mut self) {
        let column_count = if !self.columns_to_read.is_none() {
            self.columns_to_read.as_ref().unwrap().len()
        } else {
            self.var_count as usize
        };
        
        // Initialize the columns
        let sub_schema = schema_with_filter_pushdown(
            &self.schema,
            self.columns_to_read.clone()
        );

        let n_row_builder = std::cmp::min(
            self.chunk_size,
            (self.row_end.saturating_sub(self.row_start) + 1).saturating_sub(self.total_rows_processed) as usize
        );

        let mut cols = Vec::with_capacity(column_count);
        for (name, dtype) in sub_schema.iter() {
            let builder = SeriesBuilder::new_with_nulls(
                name.clone(),
                dtype,
                n_row_builder // Pre-allocate with nulls (your preference)
            );
            cols.push(builder);
        }

        self.cols = cols;
    }

    pub fn cols_to_df(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // for each column in cols
        use polars::prelude::*;

        let sub_schema = schema_with_filter_pushdown(
            &self.schema,
            self.columns_to_read.clone()
        );


        let series_vec: Result<Vec<Series>, _> = self
            .cols
            .drain(..) // Take ownership and clear the vec
            .map(|builder| builder.into_series())
            .collect();


        // Create a DataFrame from the Series collection
        let series_vec = series_vec?;
        let df = DataFrame::from_iter(series_vec.into_iter());

        
        self.df = Some(df);
        
        
        Ok(())
    }

    


    pub fn read_data(&mut self, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        // parse data and if successful then convert cols into a dataframe
        self.parse_data(rsp)?;
        self.cols_to_df()?;
        Ok(())
    }

    fn parse_data(&mut self, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        // path as pointer
        debug!("Path as C string is {:?}", &rsp.cstring_path);
        let ppath = rsp.cstring_path.as_ptr();

        
        // initialize context
        let ctx = self as *mut ReadStatData as *mut c_void;

        // initialize error
        // let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        // debug!("Initially, error ==> {:#?}", &error);

        
        
        self.extension = match rsp.extension.as_ref() {
            "sas7bdat" => {
                Extensions::sas7bdat
            },
            "dta" => {
                Extensions::dta
            },
            "sav" | "zsav" => {
                Extensions::sav
            },
            _ => {
                Extensions::NotSet
            }
        };

        let error = match rsp.extension.as_ref() {
            "sas7bdat" => {
                ReadStatParser::new()
                    // do not set metadata handler nor variable handler as already processed
                    .set_value_handler(Some(cb::handle_value))?
                    .set_row_limit(Some(self.rows_to_process.try_into().unwrap()))?
                    .set_row_offset(Some(self.row_start.try_into().unwrap()))?
                    .parse_sas7bdat(ppath, ctx)
            },
            "dta" => {
                ReadStatParser::new()
                    // .set_metadata_handler(Some(cb::handle_metadata))?
                    .set_variable_handler(Some(cb::handle_variable_noop))?
                    .set_value_handler(Some(cb::handle_value))?
                    .set_row_limit(Some(self.rows_to_process.try_into().unwrap()))?
                    .set_row_offset(Some(self.row_start.try_into().unwrap()))?
                    .parse_dta(ppath, ctx)
            },
            "sav" | "zsav" => {
                ReadStatParser::new()
                    // do not set metadata handler nor variable handler as already processed
                    .set_variable_handler(Some(cb::handle_variable_noop))?
                    .set_value_handler(Some(cb::handle_value))?
                    .set_row_limit(Some(self.rows_to_process.try_into().unwrap()))?
                    .set_row_offset(Some(self.row_start.try_into().unwrap()))?
                    .parse_sav(ppath, ctx)
            }
            _ => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Unsupported file extension: {}", rsp.extension)
                )))
            }
        };

        #[allow(clippy::useless_conversion)]
        match FromPrimitive::from_i32(error.try_into().unwrap()) {
            Some(ReadStatError::READSTAT_OK) => Ok(()),
            Some(e) => Err(From::from(format!(
                "Error when attempting to parse sas7bdat: {:#?}",
                e
            ))),
            None => Err(From::from(
                "Error when attempting to parse sas7bdat: Unknown return value",
            )),
        }
    }

    /*
    pub fn get_row_count(&mut self) -> Result<u32, Box<dyn Error>> {
        debug!("Path as C string is {:?}", &self.cstring_path);
        let ppath = self.cstring_path.as_ptr();

        let ctx = self as *mut ReadStatData as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let error = ReadStatParser::new()
            .set_metadata_handler(Some(cb::handle_metadata_row_count_only))?
            .parse_sas7bdat(ppath, ctx);

        Ok(error as u32)
    }
    */

    fn map_cols_to_lookup_indices(self) -> Self {
        if !self.columns_to_read.is_none() {
            let n_cols: usize = self.schema.len();
            let mut cols_index:Vec<Option<usize>> = Vec::with_capacity(n_cols);

            for idx in 0..n_cols {
                if self.columns_to_read.as_ref().unwrap().contains(&idx) {
                    let value = self.columns_to_read.as_ref()
                        .unwrap()
                        .iter()
                        .position(|&x| x == idx).unwrap();
                    cols_index.push(Some(value));
                } else {
                    cols_index.push(None)
                }

                // let idx = idx + 1;
            }
            
            Self {
                columns_original_index_to_data:Some(cols_index),
                ..self
            }
        } else {
            Self {
                ..self
            }
        }


        
    }

    pub fn init(
        self, 
        md: ReadStatMetadata, 
        row_start: u32, 
        row_end: u32,
        chunk_size:usize,
        chunk_buffer: Arc<Mutex<Vec<DataFrame>>>,
        notifier: Arc<Condvar>
    ) -> Self {

        let mut self_out = self.set_metadata(&md)
                               .set_row_info(row_start, row_end);
        self_out.chunk_size = chunk_size;
        self_out.allocate_cols();
        self_out.chunk_buffer = chunk_buffer;
        self_out.notifier = Some(notifier);
        self_out.map_cols_to_lookup_indices()
    }

    fn set_row_info(mut self, row_start: u32, row_end: u32) -> Self {
        self.row_start = row_start as usize;
        self.row_end = row_end as usize;
        self.rows_to_process = (row_end.saturating_sub(row_start) + 1) as usize;
        
        self
    }

    fn set_metadata(mut self, md: &ReadStatMetadata) -> Self {
        self.var_count = md.var_count;
        self.vars = md.vars.clone();
        self.schema = md.schema.clone();
        
        self
    }


    // Method called when a row is complete (from your handle_value callback)
    pub fn on_row_complete(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.chunk_rows_processed += 1;
        
        self.total_rows_processed += 1;
        
        // Check if we should emit a chunk
        if self.chunk_rows_processed >= self.chunk_size {
            self.send_chunk()?;
        }
        
        // Check if we've completed all rows
        if self.total_rows_processed >= self.rows_to_process {
            self.send_chunk()?;
        }

        Ok(())
    }    

    pub fn send_chunk(&mut self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        if self.chunk_rows_processed > 0 {
            // Convert current builders to DataFrame
            self.cols_to_df()?;
            
            println!("Rows processed: {}",self.chunk_rows_processed);
            if let Some(df) = self.df.take() {
                self.chunk_buffer.lock().unwrap().push(df);

                // Notify waiting .next() call
                if let Some(notifier) = &self.notifier {
                    notifier.notify_one();
                }
            }
        
            // Reset for next chunk
            if self.total_rows_processed < self.rows_to_process {
                self.allocate_cols();
            }    
            self.chunk_rows_processed = 0;
            self.chunk_index += 1;
            return Ok(true);
        }
        Ok(false)
    }

}

