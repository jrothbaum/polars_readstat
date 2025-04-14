
use chrono::DateTime;
use polars_arrow::array::{Array,PrimitiveArray,Utf8Array,MutableUtf8Array};
use polars::prelude::{
    Series,
    DataFrame,
    datatypes::DataType,
    datatypes::TimeUnit,
    PolarsResult,
    Schema};
/*
use arrow2::{
    array::{Array, PrimitiveArray, Utf8Array},
    chunk::Chunk,
    datatypes::{DataType, Schema, TimeUnit},
};
 */
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use num_traits::FromPrimitive;
use path_abs::PathInfo;
use std::{
    collections::BTreeMap,
    error::Error,
    os::raw::c_void,
    sync::{atomic::AtomicUsize, Arc},
};

use crate::{
    cb,
    err::ReadStatError,
    rs_metadata::{ReadStatMetadata, ReadStatVarMetadata},
    rs_parser::ReadStatParser,
    rs_path::ReadStatPath,
    rs_var::{ReadStatVar,SEC_MICROSECOND},
};

#[derive(Default)]
pub struct ReadStatData {
    // metadata
    pub var_count: i32,
    pub vars: BTreeMap<i32, ReadStatVarMetadata>,
    // data
    pub cols: Vec<Vec<ReadStatVar>>,
    pub schema: Schema,
    // chunk
    pub df: Option<DataFrame>,
    pub chunk_rows_to_process: usize, // min(stream_rows, row_limit, row_count)
    pub chunk_row_start: usize,
    pub chunk_row_end: usize,
    pub chunk_rows_processed: usize,
    // total rows
    pub total_rows_to_process: usize,
    pub total_rows_processed: Option<Arc<AtomicUsize>>,
    // progress
    pub pb: Option<ProgressBar>,
    pub no_progress: bool,
    // errors
    pub errors: Vec<String>,
}

impl ReadStatData {
    pub fn new() -> Self {
        Self {
            // metadata
            var_count: 0,
            vars: BTreeMap::new(),
            // data
            cols: Vec::new(),
            schema: Schema::default(),
            // df/chunk
            df: None,
            chunk_rows_to_process: 0,
            chunk_rows_processed: 0,
            chunk_row_start: 0,
            chunk_row_end: 0,
            // total rows
            total_rows_to_process: 0,
            total_rows_processed: None,
            // progress
            pb: None,
            no_progress: false,
            // errors
            errors: Vec::new(),
        }
    }

    fn allocate_cols(self) -> Self {
        let mut cols = Vec::with_capacity(self.var_count as usize);
        for _ in 0..self.var_count {
            cols.push(Vec::with_capacity(self.chunk_rows_to_process))
        }
        Self { cols, ..self }
    }

    fn cols_to_df(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // for each column in cols
        use polars::prelude::*;

        let series_vec: Vec<Series> = self
            .cols
            .iter()
            .zip(self.schema.iter())
            .map(|(col, (name, _field))| {
                let series = match &col[0] {
                    ReadStatVar::ReadStat_String(_) => {
                        let values = col.iter().map(|v| {
                            if let ReadStatVar::ReadStat_String(s) = v {
                                s.clone()
                            } else {
                                unreachable!()
                            }
                        }).collect::<Vec<Option<String>>>();

                        Series::new(name.clone(),values)
                    }
                    ReadStatVar::ReadStat_i8(_) => {
                        let values = col.iter().map(|v| {
                            if let ReadStatVar::ReadStat_i8(i) = v {
                                i.map(|x| x as i32)
                            } else {
                                unreachable!()
                            }
                        }).collect::<Vec<Option<i32>>>();

                        Series::new(name.clone(), values)
                    }
                    ReadStatVar::ReadStat_i16(_) => {
                        let values = col.iter().map(|v| {
                            if let ReadStatVar::ReadStat_i16(i) = v {
                                i.map(|x| x as i32)
                            } else {
                                unreachable!()
                            }
                        }).collect::<Vec<Option<i32>>>();

                        Series::new(name.clone(), values)
                    }
                    ReadStatVar::ReadStat_i32(_) => {
                        let values = col.iter().map(|v| {
                            if let ReadStatVar::ReadStat_i32(i) = v {
                                *i
                            } else {
                                unreachable!()
                            }
                        }).collect::<Vec<Option<i32>>>();

                        Series::new(name.clone(), values)
                    }
                    ReadStatVar::ReadStat_f32(_) => {
                        let values = col.iter().map(|v| {
                            if let ReadStatVar::ReadStat_f32(f) = v {
                                *f
                            } else {
                                unreachable!()
                            }
                        }).collect::<Vec<Option<f32>>>();

                        Series::new(name.clone(), values)
                    }
                    ReadStatVar::ReadStat_f64(_) => {
                        let values = col.iter().map(|v| {
                            if let ReadStatVar::ReadStat_f64(f) = v {
                                *f
                            } else {
                                unreachable!()
                            }
                        }).collect::<Vec<Option<f64>>>();

                        Series::new(name.clone(), values)
                    }
                    ReadStatVar::ReadStat_Date(_) => {
                        let values = col.iter().map(|v| {
                            if let ReadStatVar::ReadStat_Date(i) = v {
                                *i
                            } else {
                                unreachable!()
                            }
                        }).collect::<Vec<Option<i32>>>();

                        cast_series(Series::new(name.clone(), values),&DataType::Date).unwrap()
                    }
                    ReadStatVar::ReadStat_Time(_) => {
                        
                        let values = col.iter().map(|v| {
                            if let ReadStatVar::ReadStat_Time(i) = v {
                                i.map(|val| (val as i64*SEC_MICROSECOND)) //  Convert to milliseconds
                            } else {
                                None
                            }
                        }).collect::<Vec<Option<i64>>>();
                        cast_series(Series::new(name.clone(), values),&DataType::Time).unwrap()
                    }
                    ReadStatVar::ReadStat_DateTime(_) => {
                        let values = col.iter().map(|v| {
                            if let ReadStatVar::ReadStat_DateTime(i) = v {
                                *i
                            } else {
                                None
                            }
                        }).collect::<Vec<Option<i64>>>();
                        cast_series(Series::new(name.clone(), values),&DataType::Datetime(TimeUnit::Milliseconds,None)).unwrap()
                    }
                    ReadStatVar::ReadStat_DateTimeWithMilliseconds(_) => {
                        let values = col.iter().map(|v| {
                            if let ReadStatVar::ReadStat_DateTime(i) = v {
                                *i
                            } else {
                                None
                            }
                        }).collect::<Vec<Option<i64>>>();
                        cast_series(Series::new(name.clone(), values),&DataType::Datetime(TimeUnit::Milliseconds,None)).unwrap()
                    }
                    ReadStatVar::ReadStat_DateTimeWithMicroseconds(_) => {
                        let values = col.iter().map(|v| {
                            if let ReadStatVar::ReadStat_DateTime(i) = v {
                                *i
                            } else {
                                None
                            }
                        }).collect::<Vec<Option<i64>>>();
                        cast_series(Series::new(name.clone(), values),&DataType::Datetime(TimeUnit::Microseconds,None)).unwrap()
                    }
                    ReadStatVar::ReadStat_DateTimeWithNanoseconds(_) => {
                        let values = col.iter().map(|v| {
                            if let ReadStatVar::ReadStat_DateTime(i) = v {
                                *i
                            } else {
                                None
                            }
                        }).collect::<Vec<Option<i64>>>();
                        cast_series(Series::new(name.clone(), values),&DataType::Datetime(TimeUnit::Nanoseconds,None)).unwrap()
                    }
                    ReadStatVar::ReadStat_TimeWithMilliseconds(_) => {
                        let values = col.iter().map(|v| {
                            if let ReadStatVar::ReadStat_DateTime(i) = v {
                                *i
                            } else {
                                None
                            }
                        }).collect::<Vec<Option<i64>>>();
                        cast_series(Series::new(name.clone(), values),&DataType::Time).unwrap()
                    }
                    // ReadStatVar::ReadStat_TimeWithMicroseconds(_) => {
                        
                    //     let values = col.iter().map(|v| {
                    //         if let ReadStatVar::ReadStat_TimeWithMicroseconds(i) = v {
                    //             *i
                    //         } else {
                    //             None
                    //         }
                    //     }).collect::<Vec<Option<i64>>>();
                    //     cast_series(Series::new(name.clone(), values),&DataType::Time).unwrap()
                    // }
                    // | ReadStatVar::ReadStat_TimeWithNanoseconds(_) => {
                    //     //  TODO - check and fix if needed...
                    //     let values = col.iter().map(|v| {
                    //         if let ReadStatVar::ReadStat_TimeWithNanoseconds(i) = v {
                    //             i.map(|val| (val as i64)) //  Convert to milliseconds
                    //         } else {
                    //             None
                    //         }
                    //     }).collect::<Vec<Option<i64>>>();
                    //     cast_series(Series::new(name.clone(), values),&DataType::Time).unwrap()
                    // }
                };

                series
            })
            .collect();

        let schema = &self.schema; // Your existing schema
        
        // // Get the field names from the schema
        // let field_names: Vec<String> = schema.iter()
        //     .map(|(name, _)| name.to_string())
        //     .collect();
        
        // // Create a Series for each array
        // let series_vec: Vec<Series> = arrays.iter()
        //     .zip(field_names.iter())
        //     .map(|(array, name)| {
        //         // We can use from_arrow directly with a reference to the Box<dyn Array>
        //         Series::from_arrow(name.as_str().into(), array.clone())
        //             .expect("Failed to convert Arrow array to Series")
        //     })
        //     .collect();
        
        // Create a DataFrame from the Series collection
        let df = DataFrame::from_iter(series_vec);
        self.df = Some(df);
        Ok(())
    }

    pub fn read_data(&mut self, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        // parse data and if successful then convert cols into a chunk
        self.parse_data(rsp)?;
        self.cols_to_df()?;
        Ok(())
    }

    fn parse_data(&mut self, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        // path as pointer
        debug!("Path as C string is {:?}", &rsp.cstring_path);
        let ppath = rsp.cstring_path.as_ptr();

        // spinner
        // TODO - uncomment when ready to reimplement progress bar
        /*
        if !self.no_progress {
            self.pb = Some(ProgressBar::new(!0));
        }
        */

        if let Some(pb) = &self.pb {
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("[{spinner:.green} {elapsed_precise}] {msg}")?,
            );
            let msg = format!(
                "Parsing sas7bdat data from file {}",
                &rsp.path.to_string_lossy().bright_red()
            );
            pb.set_message(msg);
            pb.enable_steady_tick(std::time::Duration::new(120, 0));
        }

        // initialize context
        let ctx = self as *mut ReadStatData as *mut c_void;

        // initialize error
        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {:#?}", &error);

        // setup parser
        // once call parse_sas7bdat, iteration begins
        
        let error = match rsp.extension.as_str() {
            "sas7bdat" => {
                ReadStatParser::new()
                    // do not set metadata handler nor variable handler as already processed
                    .set_value_handler(Some(cb::handle_value))?
                    .set_row_limit(Some(self.chunk_rows_to_process.try_into().unwrap()))?
                    .set_row_offset(Some(self.chunk_row_start.try_into().unwrap()))?
                    .parse_sas7bdat(ppath, ctx)
            },
            "dta" => {
                ReadStatParser::new()
                    // .set_metadata_handler(Some(cb::handle_metadata))?
                    .set_variable_handler(Some(cb::handle_variable_noop))?
                    .set_value_handler(Some(cb::handle_value))?
                    .set_row_limit(Some(self.chunk_rows_to_process.try_into().unwrap()))?
                    .set_row_offset(Some(self.chunk_row_start.try_into().unwrap()))?
                    .parse_dta(ppath, ctx)
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

    pub fn init(self, md: ReadStatMetadata, row_start: u32, row_end: u32) -> Self {
        self.set_metadata(&md)
            .set_chunk_counts(row_start, row_end)
            .allocate_cols()
    }

    fn set_chunk_counts(self, row_start: u32, row_end: u32) -> Self {
        let chunk_rows_to_process = (row_end - row_start) as usize;
        let chunk_row_start = row_start as usize;
        let chunk_row_end = row_end as usize;
        let chunk_rows_processed = 0_usize;

        Self {
            chunk_rows_to_process,
            chunk_row_start,
            chunk_row_end,
            chunk_rows_processed,
            ..self
        }
    }

    fn set_metadata(self, md: &ReadStatMetadata) -> Self {
        let var_count = md.var_count;
        let vars = &md.vars.clone();
        let schema = &md.schema.clone();
        Self {
            var_count,
            vars: vars.clone(),
            schema: schema.clone(),
            ..self
        }
    }

    pub fn set_no_progress(self, no_progress: bool) -> Self {
        Self {
            no_progress,
            ..self
        }
    }

    pub fn set_total_rows_to_process(self, total_rows_to_process: usize) -> Self {
        Self {
            total_rows_to_process,
            ..self
        }
    }

    pub fn set_total_rows_processed(self, total_rows_processed: Arc<AtomicUsize>) -> Self {
        Self {
            total_rows_processed: Some(total_rows_processed),
            ..self
        }
    }
}


fn cast_series(series: Series, dtype: &DataType) -> PolarsResult<Series> {
    series.cast(dtype)
}