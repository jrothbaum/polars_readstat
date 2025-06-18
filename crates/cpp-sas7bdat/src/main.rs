use std::time::Instant;
use cpp_sas7bdat::{SasBatchIterator, SasReader};
use polars::frame::DataFrame;


fn main() {
    // let path = "/home/jrothbaum/Coding/polars_readstat/crates/readstat-tests/tests/data/pyreadstat/basic/sample.sas7bdat";
    let path = "/home/jrothbaum/Downloads/sas_pil/psam_p17.sas7bdat";
    
    let start_schema = Instant::now();
    let schema = match SasReader::read_sas_schema(path) {
        Ok(schema_read) => {
            schema_read
        }
        Err(e) => {
            println!("Failed to get schema: {}", e);
            return;
        }
    };
    
    let duration_schema = start_schema.elapsed();
    
    
    let mut sas_iter = SasBatchIterator::new(
        path, 
        Some(20_000)
    ).unwrap();
    
    let start_read = Instant::now();
    let mut i_rows = 0;
    for (i, batch_result) in sas_iter.enumerate() {
        // Call the method on the iterator
        let df = match batch_result {
            Ok(df) => {
                //  println!("DataFrame shape:  {:?}", df.shape());
                //  println!("          size:   {:?}", df.estimated_size());
                
                //  println!("{:?}", df);
                i_rows = i_rows + df.height();
                df
            },
            Err(e) => {
                print!("Polars error: {}",e);
                DataFrame::empty()
            }
        };
    }
    let duration_read = start_read.elapsed();
    
    println!("Schema:       {:?}", duration_schema);
    println!("Read:         {:?}", duration_read);
    println!("Rows:         {:?}", i_rows);

}