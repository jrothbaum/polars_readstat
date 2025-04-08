#![allow(non_camel_case_types)]
use clap::{Parser, Subcommand, ValueEnum, ValueHint};
use colored::Colorize;
use crossbeam::channel::bounded;
use log::debug;
use path_abs::{PathAbs, PathInfo};
use rayon::prelude::*;
use std::{error::Error, fmt, path::PathBuf, sync::Arc, thread};

pub use common::build_offsets;
pub use err::ReadStatError;
pub use rs_data::ReadStatData;
pub use rs_metadata::{ReadStatCompress, ReadStatEndian, ReadStatMetadata, ReadStatVarMetadata};
pub use rs_path::ReadStatPath;
pub use rs_var::{ReadStatVar, ReadStatVarFormatClass, ReadStatVarType, ReadStatVarTypeClass};
pub use rs_write::ReadStatWriter;

mod cb;
mod common;
mod err;
mod formats;
mod rs_data;
mod rs_metadata;
mod rs_parser;
mod rs_path;
mod rs_var;
mod rs_write;

// GLOBALS
// Default rows to stream
const STREAM_ROWS: u32 = 10000;

// CLI
#[derive(Parser, Debug)]
#[command(version)]
#[command(propagate_version = true)]
/// 💾 Command-line tool for working with SAS binary files
///
/// 🦀 Rust wrapper of ReadStat C library
pub struct ReadStatCli {
    #[command(subcommand)]
    command: ReadStatCliCommands,
}

#[derive(Debug, Subcommand)]
pub enum ReadStatCliCommands {
    /// Display sas7bdat metadata
    Metadata {
        #[arg(value_hint = ValueHint::FilePath, value_parser)]
        input: PathBuf,
        /// Display sas7bdat metadata as json
        #[arg(action, long)]
        as_json: bool,
        /// Do not display progress bar
        #[arg(action, long)]
        no_progress: bool,
        /// Skip calculating row count{n}If only interested in variable metadata speeds up parsing
        #[arg(action, long)]
        skip_row_count: bool,
    },
    /// Preview sas7bdat data
    Preview {
        /// Path to sas7bdat file
        #[arg(value_parser)]
        input: PathBuf,
        /// Number of rows to write
        #[arg(default_value = "10", long, value_parser)]
        rows: u32,
        /// Type of reader{n}    mem = read all data into memory{n}    stream = read at most stream-rows into memory{n}Defaults to stream
        #[arg(value_enum, ignore_case = true, long, value_parser)]
        reader: Option<Reader>,
        /// Number of rows to stream (read into memory) at a time{n}↑ rows = ↑ memory usage{n}Ignored if reader is set to mem{n}Defaults to 10,000 rows
        #[arg(long, value_parser)]
        stream_rows: Option<u32>,
        /// Do not display progress bar
        #[arg(action, long)]
        no_progress: bool,
    },
    /// Convert sas7bdat data to csv, feather (or the Arrow IPC format), ndjson, or parquet format
    Data {
        /// Path to sas7bdat file
        #[arg(value_hint = ValueHint::FilePath, value_parser)]
        input: PathBuf,
        /// Output file path
        #[arg(long, short = 'o', value_parser)]
        output: Option<PathBuf>,
        /// Output file format{n}Defaults to csv
        #[arg(ignore_case = true, long, short = 'f', value_enum, value_parser)]
        format: Option<OutFormat>,
        /// Overwrite output file if it already exists
        #[arg(action, long)]
        overwrite: bool,
        /// Number of rows to write
        #[arg(long, value_parser)]
        rows: Option<u32>,
        /// Type of reader{n}    mem = read all data into memory{n}    stream = read at most stream-rows into memory{n}Defaults to stream
        #[arg(ignore_case = true, long, value_enum, value_parser)]
        reader: Option<Reader>,
        /// Number of rows to stream (read into memory) at a time{n}↑ rows = ↑ memory usage{n}Ignored if reader is set to mem{n}Defaults to 10,000 rows
        #[arg(long, value_parser)]
        stream_rows: Option<u32>,
        /// Do not display progress bar
        #[arg(action, long)]
        no_progress: bool,
        /// Convert sas7bdat data in parallel
        #[arg(action, long)]
        parallel: bool,
        /// Parquet compression algorithm
        #[arg(long, value_enum, value_parser)]
        compression: Option<ParquetCompression>,
        /// Parquet compression level (if applicable)
        #[arg(long, value_parser = clap::value_parser!(u32).range(0..=22))]
        compression_level: Option<u32>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[allow(non_camel_case_types)]
pub enum OutFormat {
    csv,
    feather,
    ndjson,
    parquet,
}

impl fmt::Display for OutFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &self)
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[allow(non_camel_case_types)]
pub enum Reader {
    mem,
    stream,
}

impl fmt::Display for Reader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &self)
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ParquetCompression {
    Uncompressed,
    Snappy,
    Gzip,
    Lz4Raw,
    Brotli,
    Zstd,
}

impl fmt::Display for ParquetCompression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &self)
    }
}

pub fn run(rs: ReadStatCli) -> Result<(), Box<dyn Error + Send + Sync>> {
    env_logger::init();

    match rs.command {
        ReadStatCliCommands::Metadata {
            input: in_path,
            as_json,
            no_progress: _,
            skip_row_count,
        } => {
            // Validate and create path to sas7bdat/sas7bcat
            let sas_path = PathAbs::new(in_path)?.as_path().to_path_buf();
            debug!(
                "Retrieving metadata from the file {}",
                &sas_path.to_string_lossy()
            );

            // out_path and format determine the type of writing performed
            let rsp = ReadStatPath::new(sas_path, None, None, false, false, None, None)?;

            // Instantiate ReadStatMetadata
            let mut md = ReadStatMetadata::new();

            // Read metadata
            md.read_metadata(&rsp, skip_row_count)?;

            // Write metadata
            ReadStatWriter::new().write_metadata(&md, &rsp, as_json)?;

            // Return
            Ok(())
        }
        ReadStatCliCommands::Preview {
            input,
            rows,
            reader,
            stream_rows,
            no_progress,
        } => {
            // Validate and create path to sas7bdat/sas7bcat
            let sas_path = PathAbs::new(input)?.as_path().to_path_buf();
            debug!(
                "Generating data preview from the file {}",
                &sas_path.to_string_lossy()
            );

            // output and format determine the type of writing to be performed
            let rsp = ReadStatPath::new(
                sas_path,
                None,
                Some(OutFormat::csv),
                false,
                false,
                None,
                None,
            )?;

            // instantiate ReadStatMetadata
            let mut md = ReadStatMetadata::new();

            // Read metadata
            md.read_metadata(&rsp, false)?;

            // Determine row count
            let total_rows_to_process = std::cmp::min(rows, md.row_count as u32);

            // Determine stream row count
            // 📝 Default stream rows set to 10,000
            let total_rows_to_stream = match reader {
                Some(Reader::stream) | None => match stream_rows {
                    Some(s) => s,
                    None => STREAM_ROWS,
                },
                Some(Reader::mem) => total_rows_to_process,
            };

            // Initialize AtomicUsize to contain total rows processed
            let total_rows_processed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

            // Build up offsets
            let offsets = build_offsets(total_rows_to_process, total_rows_to_stream)?;
            let offsets_pairs = offsets.windows(2);
            let pairs_cnt = offsets_pairs.len();

            // Initialize writing
            let mut wtr = ReadStatWriter::new();

            // Process data in batches (i.e. stream chunks of rows)
            // Read data - for each iteration create a new instance of ReadStatData
            for (i, w) in offsets_pairs.enumerate() {
                let row_start = w[0];
                let row_end = w[1];

                // Initialize ReadStatData struct
                let mut d = ReadStatData::new()
                    .set_no_progress(no_progress)
                    .set_total_rows_to_process(total_rows_to_process as usize)
                    .set_total_rows_processed(total_rows_processed.clone())
                    .init(md.clone(), row_start, row_end);

                // Read
                d.read_data(&rsp)?;

                // Write
                wtr.write(&d, &rsp)?;

                // Finish
                if i == pairs_cnt {
                    wtr.finish(&d, &rsp)?;
                }
            }

            // Finish writer
            //wtr.finish(&d, &rsp)?;

            // Return
            Ok(())

            // progress bar
            /*
            if let Some(pb) = &d.pb {
                pb.finish_at_current_pos()
            };
            */
        }
        ReadStatCliCommands::Data {
            input,
            output,
            format,
            rows,
            reader,
            stream_rows,
            no_progress,
            overwrite,
            parallel,
            compression,
            compression_level,
        } => {
            // Validate and create path to sas7bdat/sas7bcat
            let sas_path = PathAbs::new(input)?.as_path().to_path_buf();
            debug!(
                "Generating data from the file {}",
                &sas_path.to_string_lossy()
            );

            // output and format determine the type of writing to be performed
            let rsp = ReadStatPath::new(
                sas_path,
                output,
                format,
                overwrite,
                false,
                compression,
                compression_level,
            )?;

            // Instantiate ReadStatMetadata
            let mut md = ReadStatMetadata::new();
            md.read_metadata(&rsp, false)?;

            // If no output path then only read metadata; otherwise read data
            match &rsp.out_path {
                None => {
                    println!("{}: a value was not provided for the parameter {}, thus displaying metadata only\n", "Warning".bright_yellow(), "--output".bright_cyan());

                    // Instantiate ReadStatMetadata
                    let mut md = ReadStatMetadata::new();
                    md.read_metadata(&rsp, false)?;

                    // Write metadata
                    ReadStatWriter::new().write_metadata(&md, &rsp, false)?;

                    // Return
                    Ok(())
                }
                Some(p) => {
                    println!(
                        "Writing parsed data to file {}",
                        p.to_string_lossy().bright_yellow()
                    );

                    // Determine row count
                    let total_rows_to_process = if let Some(r) = rows {
                        std::cmp::min(r, md.row_count as u32)
                    } else {
                        md.row_count as u32
                    };

                    // Determine stream row count
                    // 📝 Default stream rows set to 10,000
                    let total_rows_to_stream = match reader {
                        Some(Reader::stream) | None => match stream_rows {
                            Some(s) => s,
                            None => STREAM_ROWS,
                        },
                        Some(Reader::mem) => total_rows_to_process,
                    };

                    // Initialize AtomicUsize to contain total rows processed
                    let total_rows_processed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

                    // Build up offsets
                    let offsets = build_offsets(total_rows_to_process, total_rows_to_stream)?;

                    // Create channels with a capacity of 10
                    // Unbounded channels can result in extreme memory usage if files are large and
                    //   the reader significantly outpaces the writer
                    let (s, r) = bounded(10);

                    // Process data in batches (i.e. stream chunks of rows)
                    thread::spawn(move || -> Result<(), Box<dyn Error + Send + Sync>> {
                        // Create windows
                        let offsets_pairs = offsets.par_windows(2);
                        let pairs_cnt = offsets_pairs.len();

                        // Run in parallel or not?
                        // Controlled via number of threads in the rayon threadpool
                        if !parallel {
                            rayon::ThreadPoolBuilder::new()
                                .num_threads(1)
                                .build_global()?;
                        };

                        // Iterate over offset pairs, reading data for each iteration and then
                        //   sending the results over a channel to the writer
                        // 📝 For each iteration a new instance of ReadStatData is created
                        // for w in offsets_pairs {
                        let errors: Vec<_> = offsets_pairs
                            .map(|w| -> Result<(), Box<dyn Error + Send + Sync>> {
                                let row_start = w[0];
                                let row_end = w[1];

                                // Initialize ReadStatData struct
                                let mut d = ReadStatData::new()
                                    .set_no_progress(no_progress)
                                    .set_total_rows_to_process(total_rows_to_process as usize)
                                    .set_total_rows_processed(total_rows_processed.clone())
                                    .init(md.clone(), row_start, row_end);

                                // Read
                                d.read_data(&rsp)?;

                                // Send
                                let sent = s.send((d, rsp.clone(), pairs_cnt));

                                // Early return if an error
                                if sent.is_err() {
                                    Err(From::from(
                                        "Error when attempting to send read data for writing",
                                    ))
                                } else {
                                    Ok(())
                                }
                            })
                            .filter_map(|r| -> Option<Box<dyn Error + Send + Sync>> {
                                match r {
                                    Ok(()) => None,
                                    Err(e) => Some(e),
                                }
                            })
                            .collect();

                        // Drop sender so that receive iterator will eventually exit
                        drop(s);

                        if !errors.is_empty() {
                            println!("The following errors occured when processing data:");
                            for e in errors {
                                println!("    Error: {:#?}", e);
                            }
                        }

                        // Return
                        Ok(())
                    });

                    // Write

                    // Initialize writing
                    let mut wtr = ReadStatWriter::new();

                    for (i, (d, rsp, pairs_cnt)) in r.iter().enumerate() {
                        wtr.write(&d, &rsp)?;

                        if i == (pairs_cnt - 1) {
                            wtr.finish(&d, &rsp)?;
                        }

                        // Explicitly drop to save on memory
                        drop(d);
                    }

                    // Return
                    Ok(())

                    // progress bar
                    /*
                    if let Some(pb) = &d.pb {
                        pb.finish_at_current_pos()
                    };
                    */

                    // get and optionally write data - single or multi-threaded
                    /*
                    if let Some(_p) = parallel {
                        let cpu_logical_count: usize = num_cpus::get();
                        let cpu_physical_count: usize = num_cpus::get_physical();
                        println!("Logical count {:?}", cpu_logical_count);
                        println!("Physical count {:?}", cpu_physical_count);

                        d.get_row_count()?;
                        println!("Row count {:?}", d.metadata.row_count);
                        Ok(())
                    */
                }
            }
        }
    }
}
