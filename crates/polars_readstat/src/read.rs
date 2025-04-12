use log::{debug, info, warn, error};
use polars_arrow::array::Array;
use std::{env, io::Write, path::PathBuf};
use std::{error::Error, fmt, sync::Arc, thread};

use path_abs::{PathAbs, PathInfo};

use readstat::ReadStatPath;
use readstat::ReadStatMetadata;
use readstat::ReadStatData;

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

/*
pub fn read_chunk(
    in_path:PathBuf,
    md:&ReadStatMetadata
) -> Result<Vec<Box<dyn Array>> ,Box<dyn Error + Send + Sync>> {



    debug!("rows = {}", md.row_count);
    let rsp = ReadStatPath::new(
        in_path.clone(),
        None,
        None,
        false,
        true,
        None,
        None).unwrap();
    let mut rsd = ReadStatData::new()
            .init(md.clone(),0,2);
    debug!("Read chunk 1");
    let read_result = rsd.read_data(&rsp);

    let arrays = rsd.chunk.unwrap().into_arrays();

    Ok(arrays)
}

 */