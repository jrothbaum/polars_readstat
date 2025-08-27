mod strategy;
mod read;
mod backends;
mod metadata;
mod stream;

use strategy::{
    calculate_partition_strategy,
    PartitionStrategy
};


use crate::{backends::{CppBackend, ReadStatBackend, ReaderBackend}, read::Reader};
fn main() {
    let out = calculate_partition_strategy(
        100_000,
        1_000_000,
        10,
        10
    );

    println!("{:?}", out);



    


    //  let path = "/home/jrothbaum/Downloads/pyreadstat-master/test_data/basic/sample.sas7bdat";
    let path = "/home/jrothbaum/Downloads/sas_pil/psam_p17.sas7bdat";
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