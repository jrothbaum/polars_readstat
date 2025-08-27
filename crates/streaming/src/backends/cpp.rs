use log::debug;
use polars::prelude::*;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use path_abs::{PathAbs, PathInfo};


use cpp_sas7bdat::{
    SasReader,
    SasBatchIterator
};
use crate::backends::{ReaderBackend};
use crate::metadata::{
    Metadata,
    MetadataFileInfo,
    MetadataColumnInfo,
};

pub struct CppBackend {
    path: String,
    size_hint: usize,
    with_columns: Option<Vec<String>>,
    threads: usize,
    _schema: Option<Schema>,
    iterator: Option<SasBatchIterator>,
    _metadata:Option<Metadata>, 
}


impl ReaderBackend for CppBackend {
    fn schema(&mut self) -> Result<&Schema, Box<dyn std::error::Error>> {
        if self._schema.is_none() {
            self.read_metadata();
        }
        
        match &self._schema {
            Some(sch) => Ok(sch),
            None => Err("Schema not available".into()),
        }
    }

    fn metadata(&mut self) -> Result<&crate::metadata::Metadata, Box<dyn std::error::Error>> {
        if self._schema.is_none() {
            self.read_metadata();
        }
        match &self._metadata {
            Some(meta) => Ok(meta),
            None => Err("Metadata not available".into()),
        }
    }

    fn initialize_reader(
        &mut self,
        row_start:usize,
        row_end:usize,
    ) -> PolarsResult<()>{
        let schema = self.schema();

        self.iterator = match cpp_sas7bdat::SasBatchIterator::new(
            &self.path, 
            Some(self.size_hint as u32),
            self.with_columns.clone(),
            Some(row_start as u64),
            Some(row_end as u64),
        ) {
            Ok(new_iterator) => {
                Some(new_iterator)
            }
            Err(e) => {
                println!("Failed to get iterator: {}", e);
                return Err(e)
            }
        };

        Ok(())
    }

    #[allow(unused)]
    fn next(&mut self) -> PolarsResult<Option<DataFrame>> {
        
        if let Some(ref mut iter) = self.iterator {
            // Get the next batch from the iterator
            match iter.next() {
                Some(batch_result) => {
                    match batch_result {
                        Ok(df) => Ok(Some(df)),
                        Err(e) => {
                            println!("Polars error: {}", e);
                            Err(e)
                        }
                    }
                }
                None => {
                    // Iterator is exhausted - no more batches
                    Ok(None)
                }
            }
        } else {
            // Iterator was not initialized
            Ok(None)
        }
    }

    fn cancel(&mut self) -> PolarsResult<()>{
        //  self.cancel_flag.store(true, Ordering::Relaxed);

        Ok(())
    }
}

impl CppBackend {
    pub fn new(
        path: String,
        size_hint: usize,
        with_columns: Option<Vec<String>>,
        threads: usize,
        schema: Option<Schema>,
        _metadata: Option<Metadata>,
    ) -> Self {
        return CppBackend {
            path: path, 
            size_hint: size_hint, 
            with_columns: with_columns, 
            threads: threads, 
            _schema: schema,
            iterator: None,
            _metadata: _metadata,
        }
    }

    fn read_metadata(
        &mut self
    ) {
        let mut reader = SasReader::new(
            &self.path, 
            Some(1),
            None,
        ).unwrap();
        self._schema = Some(reader.get_schema().unwrap().clone());


        let info = reader.get_info();
                    
        // Get properties as strings
        let file_strings = reader.get_properties_strings();
        
        // Create file info
        let file_info = MetadataFileInfo {
            n_rows: info.row_count,
            n_cols: info.num_columns,
            size: None, // Not available
            page_size: Some(info.page_length),
            page_count: Some(info.page_count),
            header_length: Some(info.header_length),
            row_length: Some(info.row_length),
            compression: Some(match info.compression {
                0 => "None".to_string(),
                1 => "RLE".to_string(),
                2 => "RDC".to_string(),
                _ => "Unknown".to_string(),
            }),
            name: if file_strings.dataset_name.is_empty() { 
                None 
            } else { 
                Some(file_strings.dataset_name) 
            },
            encoding: if file_strings.encoding.is_empty() { 
                None 
            } else { 
                Some(file_strings.encoding) 
            },
            file_type: if file_strings.file_type.is_empty() { 
                None 
            } else { 
                Some(file_strings.file_type) 
            },
            sas_release: if file_strings.sas_release.is_empty() { 
                None 
            } else { 
                Some(file_strings.sas_release) 
            },
            sas_server_type: if file_strings.sas_server_type.is_empty() { 
                None 
            } else { 
                Some(file_strings.sas_server_type) 
            },
            os_name: if file_strings.os_name.is_empty() { 
                None 
            } else { 
                Some(file_strings.os_name) 
            },
            creator: if file_strings.creator.is_empty() { 
                None 
            } else { 
                Some(file_strings.creator) 
            },
            creator_proc: if file_strings.creator_proc.is_empty() { 
                None 
            } else { 
                Some(file_strings.creator_proc) 
            },
            created_date: None, // Add if available
            modified_date: None, // Add if available
        };

        // Get detailed column information
        let mut column_info = Vec::with_capacity(info.num_columns as usize);
        for i in 0..info.num_columns {
            match reader.get_column_info(i) {
                Ok((name, sas_type, sas_format, sas_label, length)) => {
                    column_info.push(MetadataColumnInfo {
                        name,
                        index: i,
                        type_class: if sas_type.is_empty() { None } else { Some(sas_type) },
                        format: if sas_format.is_empty() { None } else { Some(sas_format) },
                        label: if sas_label.is_empty() { None } else { Some(sas_label) },
                        length: Some(length),
                    });
                }
                Err(e) => {
                    println!("Failed to get column info for column {}: {}", i, e);
                    // Continue with next column
                }
            }
        }
        column_info.sort_by_key(|col| col.index);

        
        // Create the complete metadata
        self._metadata = Some(Metadata {
            file_info,
            column_info,
        });
        ()
    }

    
}