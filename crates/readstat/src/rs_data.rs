//  use polars::datatypes::TimeUnit;
//  use polars::prelude::Schema;
//  use polars_arrow::array::{PrimitiveArray,Array,BinaryViewArray};



use arrow2::{
    array::{Array, PrimitiveArray, Utf8Array},
    chunk::Chunk,
    datatypes::{DataType, Schema, TimeUnit},
};
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
    rs_var::ReadStatVar,
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
    pub chunk: Option<Chunk<Box<dyn Array>>>,
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
            // chunk
            chunk: None,
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

    fn cols_to_chunk(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // for each column in cols
        let arrays: Vec<Box<dyn Array>> = self
            .cols
            .iter()
            .map(|col| {
                // what kind of column is this?
                // grab the first element to determine the column type
                let col_type = &col[0];

                // convert from a Vec<ReadStatVar> into a Box<dyn Array>
                let array: Box<dyn Array> = match col_type {
                    ReadStatVar::ReadStat_String(_) => {
                        // get the inner value
                        let vec = col
                            .iter()
                            .map(|s| {
                                if let ReadStatVar::ReadStat_String(v) = s {
                                    v.clone()
                                } else {
                                    // should NEVER fall into this branch
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<String>>>();

                        Box::new(<Utf8Array<i32>>::from(vec))
                    }
                    ReadStatVar::ReadStat_i8(_) => {
                        let vec = col
                            .iter()
                            .map(|i| {
                                if let ReadStatVar::ReadStat_i8(v) = i {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i8>>>();

                        Box::new(<PrimitiveArray<i8>>::from(vec))
                    }
                    ReadStatVar::ReadStat_i16(_) => {
                        let vec = col
                            .iter()
                            .map(|i| {
                                if let ReadStatVar::ReadStat_i16(v) = i {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i16>>>();

                        Box::new(<PrimitiveArray<i16>>::from(vec))
                    }
                    ReadStatVar::ReadStat_i32(_) => {
                        let vec = col
                            .iter()
                            .map(|i| {
                                if let ReadStatVar::ReadStat_i32(v) = i {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i32>>>();

                        Box::new(<PrimitiveArray<i32>>::from(vec))
                    }
                    ReadStatVar::ReadStat_f32(_) => {
                        let vec = col
                            .iter()
                            .map(|f| {
                                if let ReadStatVar::ReadStat_f32(v) = f {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<f32>>>();

                        Box::new(<PrimitiveArray<f32>>::from(vec))
                    }
                    ReadStatVar::ReadStat_f64(_) => {
                        let vec = col
                            .iter()
                            .map(|f| {
                                if let ReadStatVar::ReadStat_f64(v) = f {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<f64>>>();

                        Box::new(<PrimitiveArray<f64>>::from(vec))
                    }
                    ReadStatVar::ReadStat_Date(_) => {
                        let vec = col
                            .iter()
                            .map(|d| {
                                if let ReadStatVar::ReadStat_Date(v) = d {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i32>>>();

                        Box::new(<PrimitiveArray<i32>>::from(vec).to(DataType::Date32))
                    }
                    ReadStatVar::ReadStat_DateTime(_) => {
                        let vec = col
                            .iter()
                            .map(|dt| {
                                if let ReadStatVar::ReadStat_DateTime(v) = dt {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i64>>>();

                        Box::new(
                            <PrimitiveArray<i64>>::from(vec)
                                .to(DataType::Timestamp(TimeUnit::Second, None)),
                        )
                    }
                    ReadStatVar::ReadStat_DateTimeWithMilliseconds(_) => {
                        let vec = col
                            .iter()
                            .map(|dt| {
                                if let ReadStatVar::ReadStat_DateTimeWithMilliseconds(v) = dt {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i64>>>();

                        Box::new(
                            <PrimitiveArray<i64>>::from(vec)
                                .to(DataType::Timestamp(TimeUnit::Millisecond, None)),
                        )
                    }
                    ReadStatVar::ReadStat_DateTimeWithMicroseconds(_) => {
                        let vec = col
                            .iter()
                            .map(|dt| {
                                if let ReadStatVar::ReadStat_DateTimeWithMicroseconds(v) = dt {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i64>>>();

                        Box::new(
                            <PrimitiveArray<i64>>::from(vec)
                                .to(DataType::Timestamp(TimeUnit::Microsecond, None)),
                        )
                    }
                    ReadStatVar::ReadStat_DateTimeWithNanoseconds(_) => {
                        let vec = col
                            .iter()
                            .map(|dt| {
                                if let ReadStatVar::ReadStat_DateTimeWithNanoseconds(v) = dt {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i64>>>();

                        Box::new(
                            <PrimitiveArray<i64>>::from(vec)
                                .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
                        )
                    }
                    ReadStatVar::ReadStat_Time(_) => {
                        let vec = col
                            .iter()
                            .map(|t| {
                                if let ReadStatVar::ReadStat_Time(v) = t {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i32>>>();

                        Box::new(
                            <PrimitiveArray<i32>>::from(vec).to(DataType::Time32(TimeUnit::Second)),
                        )
                    }
                    ReadStatVar::ReadStat_TimeWithMilliseconds(_) => {
                        let vec = col
                            .iter()
                            .map(|dt| {
                                if let ReadStatVar::ReadStat_TimeWithMilliseconds(v) = dt {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i64>>>();

                            Box::new(
                                <PrimitiveArray<i64>>::from(vec)
                                    .to(DataType::Timestamp(TimeUnit::Microsecond, None)),
                            )
                    }
                    ReadStatVar::ReadStat_TimeWithMicroseconds(_) => {
                        let vec = col
                            .iter()
                            .map(|dt| {
                                if let ReadStatVar::ReadStat_TimeWithMicroseconds(v) = dt {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i64>>>();

                        Box::new(
                            <PrimitiveArray<i64>>::from(vec)
                                .to(DataType::Timestamp(TimeUnit::Microsecond, None)),
                        )
                    }
                    ReadStatVar::ReadStat_TimeWithNanoseconds(_) => {
                        let vec = col
                            .iter()
                            .map(|dt| {
                                if let ReadStatVar::ReadStat_TimeWithNanoseconds(v) = dt {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i64>>>();

                        Box::new(
                            <PrimitiveArray<i64>>::from(vec)
                                .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
                        )
                    }
                    
                };

                // return
                array
            })
            .collect();

        // convert into a chunk
        self.chunk = Some(Chunk::try_new(arrays)?);

        Ok(())
    }

    pub fn read_data(&mut self, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        // parse data and if successful then convert cols into a chunk
        self.parse_data(rsp)?;
        self.cols_to_chunk()?;
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
        let error = ReadStatParser::new()
            // do not set metadata handler nor variable handler as already processed
            .set_value_handler(Some(cb::handle_value))?
            .set_row_limit(Some(self.chunk_rows_to_process.try_into().unwrap()))?
            .set_row_offset(Some(self.chunk_row_start.try_into().unwrap()))?
            .parse_sas7bdat(ppath, ctx);

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
        self.set_metadata(md)
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

    fn set_metadata(self, md: ReadStatMetadata) -> Self {
        let var_count = md.var_count;
        let vars = md.vars;
        let schema = md.schema;
        Self {
            var_count,
            vars,
            schema,
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
