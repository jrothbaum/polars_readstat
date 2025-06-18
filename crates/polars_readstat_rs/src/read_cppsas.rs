use std::{env, path::PathBuf};

use polars::prelude::*;
use cpp_sas7bdat::{
    SasReader,
    SasBatchIterator
};


pub fn read_schema(
    path:PathBuf
) -> PolarsResult<Schema> {
    match SasReader::read_sas_schema(path.to_str().unwrap()) {
        Ok(schema_read) => {
            Ok(schema_read)
        }
        Err(e) => {
            println!("Failed to get schema: {}", e);
            Err(e)
        }
    }
}