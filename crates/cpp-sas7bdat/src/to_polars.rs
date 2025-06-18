// use polars::prelude::*;
// use polars_core;
// use std::collections::HashMap;
// use std::path::Path;
// use crate::read::{
//     SasColumnType,
//     SasChunkedReader,
//     SasColumn,
// };

// /// Convert SasColumnType to Polars DataType
// fn sas_type_to_polars_type(sas_type: &SasColumnType) -> DataType {
//     match sas_type {
//         SasColumnType::String => DataType::String,
//         SasColumnType::Numeric => DataType::Float64,
//         SasColumnType::Date => DataType::Date,
//         SasColumnType::DateTime => DataType::Datetime(TimeUnit::Microseconds, None),
//         SasColumnType::Time => DataType::Time,
//     }
// }

// /// Extract SAS schema as a Polars Schema
// pub fn get_sas_polars_schema<P: AsRef<Path>>(filename: P) -> Result<Schema, Box<dyn std::error::Error>> {
//     // Create reader with minimal chunk size since we don't need data
//     let reader = SasChunkedReader::new(
//         filename.as_ref().to_str().ok_or("Invalid filename")?,
//         1 // Minimal chunk size since we're not reading data
//     )?;
    
//     // Get the schema from the reader
//     let sas_schema = reader.properties();
    
//     // Convert to Polars Schema
//     let mut schema_fields = Vec::new();
//     for column in sas_schema {
//         let polars_type = sas_type_to_polars_type(&column.column_type);
//         schema_fields.push(Field::new(column.name.as_str().into(), polars_type));
//     }
    
//     Ok(Schema::from_iter(schema_fields))
// }

// /// Read entire SAS file into a single DataFrame
// pub fn read_sas_to_dataframe<P: AsRef<Path>>(
//     filename: P, 
//     chunk_size: usize
// ) -> Result<DataFrame, Box<dyn std::error::Error>> {
//     let mut reader = SasChunkedReader::new(
//         filename.as_ref().to_str().ok_or("Invalid filename")?,
//         chunk_size
//     )?;
    
//     let mut all_chunks = Vec::new();
    
//     // Read all chunks
//     while let Some(series_vec) = reader.chunk_iterator()? {
//         let chunk_df = DataFrame::from_iter(series_vec);
//         all_chunks.push(chunk_df);
//     }
    
//     // Combine all chunks
//     if all_chunks.is_empty() {
//         Ok(DataFrame::empty())
//     } else if all_chunks.len() == 1 {
//         Ok(all_chunks.into_iter().next().unwrap())
//     } else {
//         polars_core::utils::concat_df(&all_chunks)
//             .map_err(|e| e.into())
//     }
// }

// /// Iterator for processing SAS files chunk by chunk (for large files)
// pub struct SasDataFrameIterator {
//     reader: SasChunkedReader,
// }

// impl SasDataFrameIterator {
//     pub fn new<P: AsRef<Path>>(filename: P, chunk_size: usize) -> Result<Self, Box<dyn std::error::Error>> {
//         let reader = SasChunkedReader::new(
//             filename.as_ref().to_str().ok_or("Invalid filename")?,
//             chunk_size
//         )?;
        
//         Ok(SasDataFrameIterator { reader })
//     }
    
//     pub fn properties(&self) -> &[SasColumn] {
//         self.reader.properties()
//     }
// }

// impl Iterator for SasDataFrameIterator {
//     type Item = Result<DataFrame, Box<dyn std::error::Error>>;
    
//     fn next(&mut self) -> Option<Self::Item> {
//         match self.reader.chunk_iterator() {
//             Ok(Some(series_vec)) => {
//                 let df = DataFrame::from_iter(series_vec);
//                 Some(Ok(df))
//             }
//             Ok(None) => None, // No more chunks
//             Err(e) => Some(Err(e)),
//         }
//     }
// }