use polars::prelude::{
    datatypes::DataType, DataFrame, PolarsResult, Schema, Series
};
use log::debug;
use num_traits::FromPrimitive;
use std::{
    collections::BTreeMap,
    error::Error,
    os::raw::c_void,
    sync::{atomic::AtomicUsize, Arc},
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
    rs_path::ReadStatPath
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

pub enum TypedColumn {
    StringColumn(Vec<Option<String>>),
    I8Column(Vec<Option<i32>>),
    I16Column(Vec<Option<i32>>),
    I32Column(Vec<Option<i32>>),
    I64Column(Vec<Option<i64>>),
    F32Column(Vec<Option<f32>>),
    F64Column(Vec<Option<f64>>),
    DateColumn(Vec<Option<i32>>),
    TimeColumn(Vec<Option<i64>>),
    DateTimeColumn(Vec<Option<i64>>),
}

#[derive(Default)]
pub struct ReadStatData {
    // metadata
    pub var_count: i32,
    pub vars: BTreeMap<i32, ReadStatVarMetadata>,
    // data
    pub cols: Vec<TypedColumn>,
    pub schema: Schema,
    pub extension: Extensions,
    // chunk
    pub df: Option<DataFrame>,
    pub chunk_rows_to_process: usize, // min(stream_rows, row_limit, row_count)
    pub chunk_row_start: usize,
    pub chunk_row_end: usize,
    pub chunk_rows_processed: usize,
    // total rows
    pub total_rows_to_process: usize,
    pub total_rows_processed: Option<Arc<AtomicUsize>>,
    // errors
    pub errors: Vec<String>,
    pub columns_to_read: Option<Vec<usize>>,
    pub columns_original_index_to_data: Option<Vec<Option<usize>>>,
}

impl ReadStatData {
    pub fn new(columns_to_read: Option<Vec<usize>>) -> Self {
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
            chunk_rows_to_process: 0,
            chunk_rows_processed: 0,
            chunk_row_start: 0,
            chunk_row_end: 0,
            // total rows
            total_rows_to_process: 0,
            total_rows_processed: None,
            // errors
            errors: Vec::new(),
            columns_to_read: columns_to_read,
            columns_original_index_to_data: None,
        }
    }

    fn allocate_cols(self) -> Self {
        let column_count = if self.columns_to_read.is_some() {
            self.columns_to_read.as_ref().unwrap().len()
        } else {
            self.var_count as usize
        };
        
        // Initialize the columns
        let sub_schema = schema_with_filter_pushdown(
            &self.schema,
            self.columns_to_read.clone()
        );
        let mut cols = Vec::with_capacity(column_count);
        for (_, dt) in sub_schema.iter() {
            // Create appropriate typed column
            let column = match &dt {
                DataType::String => {
                    TypedColumn::StringColumn(vec![None; self.chunk_rows_to_process])
                },
                DataType::Float64 => {
                    TypedColumn::F64Column(vec![None; self.chunk_rows_to_process])
                },
                DataType::Float32 => {
                    TypedColumn::F32Column(vec![None; self.chunk_rows_to_process])
                },
                DataType::Int8 => {
                    TypedColumn::I8Column(vec![None; self.chunk_rows_to_process])
                },
                DataType::Int16 => {
                    TypedColumn::I16Column(vec![None; self.chunk_rows_to_process])
                },
                DataType::Int32 => {
                    TypedColumn::I32Column(vec![None; self.chunk_rows_to_process])
                },
                DataType::Int64 => {
                    TypedColumn::I64Column(vec![None; self.chunk_rows_to_process])
                },
                DataType::Date => {
                    TypedColumn::DateColumn(vec![None; self.chunk_rows_to_process])
                },
                DataType::Time => {
                    TypedColumn::TimeColumn(vec![None; self.chunk_rows_to_process])
                },
                DataType::Datetime(_, _) => {
                    TypedColumn::DateTimeColumn(vec![None; self.chunk_rows_to_process])
                },
                // Default case
                _ => TypedColumn::StringColumn(vec![None; self.chunk_rows_to_process])
            };

            cols.push(column);
        }

        Self { cols, ..self }
    }

    fn cols_to_df(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // for each column in cols
        use polars::prelude::*;

        let sub_schema = schema_with_filter_pushdown(
            &self.schema,
            self.columns_to_read.clone()
        );


        let series_vec: Vec<Series> = self
            .cols
            .iter_mut()
            .zip(sub_schema.iter())
            .map(|(col, (name, _field))| {
            // Create Series directly from the typed column
            match col {
                TypedColumn::StringColumn(vec) => {
                    // Use mem::take to avoid cloning when possible
                    let values = std::mem::take(vec);
                    Series::new(name.clone(), values)
                },
                TypedColumn::I64Column(vec) => {
                    let values = std::mem::take(vec);
                    Series::new(name.clone(), values)
                },
                TypedColumn::I32Column(vec) => {
                    let values = std::mem::take(vec);
                    Series::new(name.clone(), values)
                },
                TypedColumn::I16Column(vec) => {
                    let values = std::mem::take(vec);
                    Series::new(name.clone(), values)
                    // Series::new(name.clone(), values)
                    //     .cast(&DataType::Int16)
                    //     .unwrap()
                    // let series = Series::new(name.clone(), values);
                    // cast_series(series, &DataType::Int8).unwrap()
                },
                TypedColumn::I8Column(vec) => {
                    let values = std::mem::take(vec);
                    Series::new(name.clone(), values)
                    // Series::new(name.clone(), values)
                    //     .cast(&DataType::Int8)
                    //     .unwrap()
                    //  let series = Series::new(name.clone(), values);
                    //  cast_series(series, &DataType::Int16).unwrap()
                },
                TypedColumn::F64Column(vec) => {
                    let values = std::mem::take(vec);
                    Series::new(name.clone(), values)
                },
                TypedColumn::F32Column(vec) => {
                    let values = std::mem::take(vec);
                    Series::new(name.clone(), values)
                },
                TypedColumn::DateColumn(vec) => {
                    let values = std::mem::take(vec);
                    let series = Series::new(name.clone(), values);
                    cast_series(series, &DataType::Date).unwrap()
                },
                TypedColumn::TimeColumn(vec) => {
                    let values = std::mem::take(vec);
                    let series = Series::new(name.clone(), values);
                    cast_series(series, &DataType::Time).unwrap()
                },
                TypedColumn::DateTimeColumn(vec) => {
                    let values = std::mem::take(vec);
                    let series = Series::new(name.clone(), values);
                    cast_series(series, &DataType::Datetime(TimeUnit::Milliseconds, None)).unwrap()
                },
                // TypedColumn::DateTimeWithMillisecondsColumn(vec) => {
                //     let values = std::mem::take(vec);
                //     let series = Series::new(name.clone(), values);
                //     cast_series(series, &DataType::Datetime(TimeUnit::Milliseconds, None)).unwrap()
                // },
                // TypedColumn::DateTimeWithMicrosecondsColumn(vec) => {
                //     let values = std::mem::take(vec);
                //     let series = Series::new(name, values);
                //     cast_series(series, &DataType::Datetime(TimeUnit::Microseconds, None)).unwrap()
                // },
                // TypedColumn::DateTimeWithNanosecondsColumn(vec) => {
                //     let values = std::mem::take(vec);
                //     let series = Series::new(name, values);
                //     cast_series(series, &DataType::Datetime(TimeUnit::Nanoseconds, None)).unwrap()
                // },
                // TypedColumn::TimeWithMillisecondsColumn(vec) => {
                //     let values = std::mem::take(vec);
                //     let series = Series::new(name, values);
                //     cast_series(series, &DataType::Time).unwrap()
                // },
                // Add any other types your code might use
            }
        })
        .collect();


        // Create a DataFrame from the Series collection
        let df = DataFrame::from_iter(series_vec);

        
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
            },
            "sav" | "zsav" => {
                ReadStatParser::new()
                    // do not set metadata handler nor variable handler as already processed
                    .set_variable_handler(Some(cb::handle_variable_noop))?
                    .set_value_handler(Some(cb::handle_value))?
                    .set_row_limit(Some(self.chunk_rows_to_process.try_into().unwrap()))?
                    .set_row_offset(Some(self.chunk_row_start.try_into().unwrap()))?
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
        row_end: u32) -> Self {

        self.set_metadata(&md)
            .set_chunk_counts(row_start, row_end)
            .allocate_cols()
            .map_cols_to_lookup_indices()
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