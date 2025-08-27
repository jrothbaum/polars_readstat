mod strategy;
mod read;
mod backends;
mod metadata;
mod stream;

use polars::prelude::{AnonymousScan, AnonymousScanArgs};
use strategy::{
    calculate_partition_strategy,
    PartitionStrategy
};

use stream::PolarsReadstat;


use crate::{backends::{CppBackend, ReadStatBackend, ReaderBackend}, read::Reader};
fn main() {
    use std::time::Instant;
    use std::time::Duration;
    let start = Instant::now();
    //  let path = "/home/jrothbaum/Downloads/pyreadstat-master/test_data/basic/sample.sas7bdat";
    let path = "/home/jrothbaum/Downloads/sas_pil/psam_p17.sas7bdat";

    let rs = PolarsReadstat::new(
        path.to_string(), 
        100000, 
        None, 
        6, 
        "cpp".to_string()
    );

    
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
                println!("Batch {}: {:?}", batch_count, df);
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
    let out = calculate_partition_strategy(
        100_000,
        1_000_000,
        10,
        10
    );

    println!("{:?}", out);



    


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
            println!("=== TEST: Cancelling after {} chunks ===", cancel_after_chunks);
            reader.cancel(); // You'll need to add this method to your Reader
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