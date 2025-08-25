    //  use log::{debug, info, warn, error};
use log::debug;
use polars::frame::DataFrame;
use polars_core::utils::concat_df;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::cmp::min;
use std::{env, path::PathBuf};
use std::{error::Error, thread};

use path_abs::{PathAbs, PathInfo};

use readstat::ReadStatPath;
use readstat::ReadStatMetadata;

pub fn read_metadata(
    in_path:PathBuf,
    skip_row_count:bool,
) -> Result<ReadStatMetadata,Box<dyn Error + Send + Sync>> {
    // Validate and create path to sas7bdat/sas7bcat
    let stat_path = PathAbs::new(in_path)?.as_path().to_path_buf();
    debug!(
        "Retrieving metadata from the file {}",
        &stat_path.to_string_lossy()
    );

    // out_path and format determine the type of writing performed
    let rsp = ReadStatPath::new(stat_path)?;

    // Instantiate ReadStatMetadata
    let mut md = ReadStatMetadata::new();

    // Read metadata
    md.read_metadata(&rsp, skip_row_count)?;

    
    // Return
    Ok(md)
}