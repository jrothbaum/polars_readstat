mod strategy;
mod read;
mod backends;
mod metadata;

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



    


    let path = "/home/jrothbaum/Downloads/pyreadstat-master/test_data/basic/sample.sas7bdat";
    //  let path = "/home/jrothbaum/Downloads/sas_pil/psam_p17.sas7bdat";
    //  Use universal reader
    let mut reader = read::Reader::new(
        path.to_string(), 
        100_000,
        None,
        10,
        "cpp".to_string(),
        None,
        None
    );

    let schema = reader.schema().unwrap().clone();
    let metadata = reader.metadata().unwrap().clone();
    println!("schema: {:?}", schema);
    println!("Metadata: {:?}",metadata);

    reader.initialize_reader(0, 5);

    while let Ok(Some(df)) = reader.next() {
        println!("{:?}",df);
    }

    
    return ();

    // let mut reader_2 = read::Reader::new(
    //     path.to_string(), 
    //     100_000,
    //     None,
    //     10,
    //     "readstat".to_string(),
    //     reader.metadata().unwrap(),
    //     None
    // );

    // reader_2.initialize_reader(0, 5);

    // while let Ok(Some(df)) = reader_2.next() {
    //     println!("{:?}",df);
    // }

    return ();
    
    
    
    
    
    
    
    let mut rs_reader = ReadStatBackend::new(
        path.to_string(), 
        100_000,
        None,
        10,
        None
    );

    let schema = rs_reader.schema().unwrap().clone();

    println!("{:?}",(rs_reader.md.as_ref().unwrap().row_count as usize));
    rs_reader.initialize_reader(
        0,
        (rs_reader.md.as_ref().unwrap().row_count as usize)
    );

    println!("Readstat: {:?}",schema);

    while let Ok(Some(df)) = rs_reader.next() {
        println!("{:?}",df);
    }
    // let mut cpp_reader = CppBackend::new(
    //     path.to_string(), 
    //     100_000,
    //     None,
    //     10,
    //     None
    // );

    // let schema = cpp_reader.schema().unwrap().clone();

    // cpp_reader.initialize_reader(
    //     0,
    //     5
    // );

    // println!("CPP:     {:?}",schema);

    // //  let mut results = Vec::new();
    // while let Ok(Some(df)) = cpp_reader.next() {
    //     println!("{:?}",df);
    // }
}