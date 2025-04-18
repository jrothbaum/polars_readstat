use log::{debug, info, warn, error};
use env_logger::Builder;
use polars::prelude::PlSmallStr;
use std::{env, io::Write};


mod read;



fn main() {
    //  env_logger::init();
    Builder::from_default_env()
        .format(|buf, record| {
            writeln!(buf, "[{}] {}", 
                record.level(),
                record.args()
            )
        })
        .init();

    //  let path_metadata:PathBuf = std::path::PathBuf::from("/home/jrothbaum/python/readstat-rs/crates/readstat-tests/tests/data/all_types.sas7bdat");
    //  let path = std::path::PathBuf::from("/home/jrothbaum/Downloads/usa_00008.dta");
    let path = std::path::PathBuf::from("/home/jrothbaum/python/polars_readstat/crates/polars_readstat/tests/data/sample.dta");

    
    let md = read::read_metadata(
        path.clone(),
        false
    ).unwrap();
    
    dbg!(&md.schema);
    debug!("rows = {}", md.row_count);

    let skip_rows:u32 = 0;
    let n_rows:u32 = 1_000_000;

    let columns: Vec<usize> = vec![
        1,
        2,
    ];

    /*
    let df = read::read_chunk(
        path.clone(),
        Some(&md),
        Some(skip_rows),
        Some(n_rows),
        //Some(columns)
        None
    )
        .unwrap();
    */

    use std::time::{Duration, Instant};
    let start = Instant::now();
    let n_threads = 16 as usize;
    let df = read::read_chunks_parallel(
        path.clone(),
        Some(&md),
        Some(skip_rows),
        Some(n_rows),
        // None,
        //Some(columns)
        None,
        Some(n_threads)
    )
        .unwrap();
    let duration = start.elapsed();
    println!("Time elapsed: {:?}", duration);
    dbg!(&df);



    let start = Instant::now();
    let df = read::read_chunks_parallel(
        path.clone(),
        Some(&md),
        Some(skip_rows),
        Some(n_rows),
        // None,
        //Some(columns)
        None,
        Some(1 as usize)
    )
        .unwrap();
    let duration = start.elapsed();
    println!("Time elapsed (1 thread): {:?}", duration);
    dbg!(&df);

    // let rsp = ReadStatPath::new(
    //     path_read).unwrap();
    // let mut rsd = ReadStatData::new()
    //         .init(md.clone(),0,5);
    // debug!("Read chunk 1");
    // let _ = rsd.read_data(&rsp);

    // let df1 = rsd.df.unwrap();
    // dbg!(&df1);
    /*
    debug!("Read chunk 2");
    // let mut rsd = ReadStatData::new()
    //         .init(md.clone(),2,3);
    // let read_result = rsd.read_data(&rsp);

    // let df2 = rsd.df.unwrap();
    dbg!(&df1);
    // dbg!(&df2);
    
    /*
    let _ = read::read_file_parallel(&std::path::PathBuf::from("/home/jrothbaum/python/polars_readstat/crates/polars_readstat/tests/data/sample_pyreadstat.dta"),
                             read::ReadstatFileType::Dta,
                             None // Some(2)
                            );

    let _ = read::read_file_parallel(&std::path::PathBuf::from("/home/jrothbaum/python/readstat-rs/crates/readstat-tests/tests/data/all_types.sas7bdat"),
                            read::ReadstatFileType::Sas7bdat,
                            None
                           );
    */    
    debug!("FINISHED");
    
                            
    //  let _ = test_parser();
     */
}


