use log::{debug, info, warn, error};
use polars::frame::DataFrame;
use polars::prelude::PlSmallStr;
use polars_core::utils::concat_df;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::cmp::min;
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


pub fn read_chunks_parallel(
    in_path:PathBuf,
    md:Option<&ReadStatMetadata>,
    skip_rows:Option<u32>,
    n_rows:Option<u32>,
    columns:Option<Vec<usize>>,
    n_threads:Option<usize>,
 ) -> Result<DataFrame,Box<dyn Error + Send + Sync>> {
    
    let n_threads = if n_threads.is_none() {
        Some(get_max_threads())
    } else {
        n_threads
    };
    
    
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

    let n_rows = if n_rows.is_none() || n_rows == Some(0) {
        Some(md_ref.row_count as u32)
    } else {
        Some(min(n_rows.unwrap(),md_ref.row_count as u32))
    };

    let skip_rows = if skip_rows.is_none() {
        Some(0 as u32)
    } else {
        skip_rows
    };

    //  1 thread or fewer than some minimum number of rows
    //      just read the whole thing
    let n_rows_use_1_thread = 10_000 as u32;
    if n_threads.unwrap() == 1 || n_rows.unwrap() <= n_rows_use_1_thread {
        //  println!("Running with 1 thread");
        return read_chunk(
                in_path.clone(),
                Some(&md_ref.clone()),
                skip_rows, 
                n_rows, 
                columns
            )
    }


    

    let chunk_size = ((n_rows.unwrap() as f64/n_threads.unwrap() as f64).ceil()) as u32;

    
    // println!("n_threads = {:?}",n_threads);
    // println!("file = {:?}",in_path);
    // println!("skip_rows = {}",skip_rows.unwrap());
    // println!("n_rows = {}",n_rows.unwrap());
    // println!("chunk_size = {}",chunk_size);


    // Process chunks in parallel
    let results: Vec<Result<DataFrame, _>> = (0..n_threads.unwrap())
        .into_par_iter()
        .map(|thread_idx| {
            let start_row = thread_idx as u32 * chunk_size + skip_rows.unwrap();
            let end_row = std::cmp::min(start_row + chunk_size, skip_rows.unwrap() + n_rows.unwrap());
            
            // println!("start_row = {}",start_row);
            // println!("end_row = {}",end_row);
            // Skip if this chunk is beyond the file
            if start_row >= md_ref.row_count as u32 {
                return Ok(DataFrame::empty());
            }
            
            // Clone path for thread safety
            let thread_path = in_path.clone();
            
            // Use your existing read_chunk function
            read_chunk(
                thread_path,
                md,
                Some(start_row),
                Some(end_row - start_row),
                columns.clone()
            )
        })
        .collect();

    // Collect and concatenate results
    let mut dataframes = Vec::new();
    for result in results {
        match result {
            Ok(df) if !df.is_empty() => dataframes.push(df),
            Ok(_) => {}, // Skip empty dataframes
            Err(e) => return Err(e),
        }
    }


    let df = concat_df(&dataframes);

    Ok(df?)
}
    
fn get_max_threads() -> usize {
    let max_threads = env::var("POLARS_MAX_THREADS")
        .map(|val| val.parse::<usize>().unwrap_or_else(|_| {
            println!("Warning: POLARS_MAX_THREADS is not a valid number, using system thread count");
            thread::available_parallelism().map_or(1, |p| p.get())
        }))
        .unwrap_or_else(|_| {
            // If env var is not set, use the number of available threads
            thread::available_parallelism().map_or(1, |p| p.get())
        });

    max_threads
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