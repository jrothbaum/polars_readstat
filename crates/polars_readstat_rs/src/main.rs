mod read;
mod backends;
mod metadata;
mod stream;
#[cfg(feature = "python")]
mod pybindings;

use std::sync::Arc;
use polars::prelude::{
    AnonymousScan,
    AnonymousScanArgs,
    PlSmallStr
};
use stream::PolarsReadstat;


use crate::{backends::{CppBackend, ReadStatBackend}, read::Reader};
fn main() {
    use std::time::Instant;
    use std::time::Duration;
    use num_cpus;

    let num_threads = num_cpus::get_physical();
    //  println!("{:?}",num_threads);
    let start = Instant::now();
    //  let path = "/home/jrothbaum/Downloads/pyreadstat-master/test_data/basic/sample.sas7bdat";
    let path = "/home/jrothbaum/Downloads/sas_pil/psam_p17.sas7bdat";
    let path = "/home/jrothbaum/Downloads/pyreadstat-master/test_data/basic/ordered_category.sav";
    
    // let vec_strings: Vec<String> = vec![
    //         String::from("mychar"), 
    //         String::from("mynum")
    //     ];
    // let vec_strings: Vec<String> = vec![
    //         String::from("WKL")
    //     ];
    let mut rs = PolarsReadstat::new(
        path.to_string(), 
        10000, 
        None, 
        num_threads, 
        "readstat".to_string()
    );

    println!("{:?}", rs.metadata().clone().unwrap());
    
    
    //  rs.set_columns_to_read(Some(vec_strings));


    let mut batch_count = 0;
    loop {
        let scan_opts = AnonymousScanArgs {
            with_columns: None,
            schema: rs.schema(None).clone().unwrap(),
            output_schema:None,
            predicate: None,
            n_rows:None,
        };
        
        match rs.next_batch(scan_opts) {
            
            Ok(Some(df)) => {
                batch_count += 1;
                println!("Batch {}: {:?},{:?}", batch_count, df,df.column_iter().count());
            },
            Ok(None) => {
                println!("No more batches. Total batches: {}", batch_count);
                break;
            },
            Err(polars_error) => {
                println!("ERROR! {:?}", polars_error);
                break;
            }
        }
    }


    let elapsed = start.elapsed();
    println!("Time elapsed: {:?}", elapsed);








    return ();


    


    //  Use universal reader
    let mut reader = read::Reader::new(
        path.to_string(), 
        100_000,
        None,
        10,
        "readstat".to_string(),
        None,
        None,
        None,
    );

    let schema = reader.schema().unwrap().clone();
    let metadata = reader.metadata().unwrap().clone();
    println!("schema: {:?}", schema);
    println!("Metadata: {:?}",metadata);

    reader.initialize_reader(0, 500_000);

    let mut chunk_count = 0;
    let cancel_after_chunks = 2;
    while let Ok(Some(df)) = reader.next() {
        println!("{:?}",df);
        chunk_count += 1;
        // Test cancellation
        if chunk_count >= cancel_after_chunks {
            let _ = reader.cancel();
            break;
        }
    }

    //  println!("{:?}", reader.metadata());
    
    // let mut reader_2 = reader.copy_for_reading();
    // reader_2.initialize_reader(0, 5);

    // while let Ok(Some(df)) = reader_2.next() {
    //     println!("{:?}",df);
    // }

    // return ();

}