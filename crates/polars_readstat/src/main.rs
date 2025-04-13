use log::{debug, info, warn, error};
use env_logger::Builder;
use std::{env, io::Write};


mod read;



fn main() {
    unsafe {
        env::set_var("POLARS_MAX_THREADS", "1");
    }
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
    let path = std::path::PathBuf::from("/home/jrothbaum/python/polars_readstat/crates/polars_readstat/tests/data/sample.dta");

    
    let md = read::read_metadata(
        path.clone(),
        false
    ).unwrap();
    

    debug!("rows = {}", md.row_count);

    let skip_rows:u32 = 2;
    let n_rows:u32 = 1;
    let df = read::read_chunk(
        path.clone(),
        Some(&md),
        Some(skip_rows),
        Some(n_rows))
        .unwrap();
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


