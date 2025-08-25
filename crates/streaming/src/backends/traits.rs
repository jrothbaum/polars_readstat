use polars::prelude::*;
use crate::metadata::Metadata;

pub trait ReaderBackend {
    /// Initialize the backend
    // fn new(
    //     path: String,
    //     size_hint: usize,
    //     with_columns: Option<Vec<String>>,
    //     threads: usize,
    //     schema:Option<Schema>,
    // ) -> Self;       


    /// Get the schema of the dataset
    fn schema(&mut self) -> Result<&Schema, Box<dyn std::error::Error>>;

    fn metadata(&mut self) -> Result<&Metadata, Box<dyn std::error::Error>>;

    /// Start reading data
    fn initialize_reader(
        &mut self,
        row_start:usize,
        row_end:usize,
    ) -> PolarsResult<()>;

    fn next(&mut self) -> PolarsResult<Option<DataFrame>>;
}
