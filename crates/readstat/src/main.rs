#![allow(non_camel_case_types)]
pub use err::ReadStatError;
pub use rs_data::ReadStatData;
pub use rs_metadata::{ReadStatCompress, ReadStatEndian, ReadStatMetadata, ReadStatVarMetadata};
pub use rs_path::ReadStatPath;
pub use rs_var::{ReadStatVar, ReadStatVarFormatClass, ReadStatVarType, ReadStatVarTypeClass};
pub use rs_parser::ReadStatParser;
pub use stream::ReadStatStreamer;

mod cb;
mod common;
mod err;
//  mod series_builder;
mod formats;
mod rs_data;
mod rs_metadata;
mod rs_parser;
mod rs_path;
mod rs_var;
mod stream;
use std::{
    path::PathBuf
};

use readstat_sys::SharedMmap; 

fn main() {
    // let path_string = "/home/jrothbaum/Coding/polars_readstat/crates/cpp-sas7bdat/vendor/test/data/file1.sas7bdat";
    let path_string = "/home/jrothbaum/Downloads/sas_pil/psam_p17.sas7bdat";
    //  let path_string = "/home/jrothbaum/Downloads/pyreadstat-master/test_data/basic/sample.sas7bdat";
    
    let path = PathBuf::from(&path_string);

    
    
    // out_path and format determine the type of writing performed
    let rsp = ReadStatPath::new(path).unwrap();

    // Instantiate ReadStatMetadata
    let mut md = ReadStatMetadata::new();

    // Read metadata
    md.read_metadata(&rsp, false).unwrap();

    println!("{:?}", md);

    let (mut consumer, chunk_buffer, notifier, is_complete) = ReadStatStreamer::new();
    let stat_path = ReadStatPath::new(PathBuf::from(&path_string)).unwrap();

    use std::time::Instant;
    use std::thread;
    use std::time::Duration;
    let start = Instant::now();

    // let mut columns = Vec::new();
    // columns.push(1 as usize);
    // columns.push(2);
    // columns.push(3);
    let mut rsd = ReadStatData::new(None)
            .init(
                md.clone(),
                0,
                (md.row_count as u32).saturating_sub(1),
                10000,
                chunk_buffer,
                notifier
            );


    let shared_mmap = SharedMmap::new(&path_string).unwrap();
    // Background processing
    let is_complete_clone = is_complete.clone();
    let shared_mmap_clone = shared_mmap.clone(); // Cheap clone
    std::thread::spawn(move || {
        let result = rsd.read_data(&rsp,Some(&shared_mmap_clone));
        *is_complete_clone.lock().unwrap() = true;
        result
    });

    // std::thread::spawn(move || {
    //     let result = rsd.read_data(&rsp,None);
    //     *is_complete_clone.lock().unwrap() = true;
    //     result
    // });

    while let Some(df) = consumer.next() {
        println!("{:?}", df.height());
    }
    let elapsed = start.elapsed();
    println!("Time elapsed: {:?}", elapsed);
    // _ = rsd.read_data(&stat_path);
    // let df = rsd.df.unwrap();

    // println!("{:?}", format!("{:?}",df));


}
