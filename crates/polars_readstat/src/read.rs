use log::{debug, info, warn, error};
use polars::frame::DataFrame;
use polars::prelude::PlSmallStr;
use std::io::Read;
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


pub fn read_chunk(
    in_path:PathBuf,
    md:Option<&ReadStatMetadata>,
    skip_rows:Option<u32>,
    n_rows:Option<u32>,
    columns:Option<Vec<usize>>,
) -> Result<DataFrame,Box<dyn Error + Send + Sync>> {

    let owned_md = if md.is_none() {
        Some(read_metadata(in_path.clone(), false)?)
    } else {
        None
    };
    
    // Get a reference to whichever metadata we're using
    let md_ref = match md {
        Some(ref_md) => ref_md,
        None => owned_md.as_ref().unwrap(),
    };

    debug!("rows = {}", md_ref.row_count);

    let rsp = ReadStatPath::new(
        in_path).unwrap();



    let row_start:u32 = if skip_rows.is_none() {
        0
    } else {
        skip_rows.unwrap()
    };

    let row_end:u32 = if n_rows.is_none() {
        md_ref.row_count as u32
    } else {
        row_start + n_rows.unwrap()
    };

    let mut rsd = ReadStatData::new(columns)
            .init(md_ref.clone(),row_start,row_end);
    let _ = rsd.read_data(&rsp);

    rsd.df.ok_or_else(|| Box::new(std::io::Error::new(
        std::io::ErrorKind::Other, 
        "Failed to read DataFrame"
    )) as Box<dyn Error + Send + Sync>)
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