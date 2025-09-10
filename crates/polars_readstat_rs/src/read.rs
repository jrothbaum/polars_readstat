use polars::prelude::*;
use crate::backends::{CppBackend, ReadStatBackend, ReaderBackend};
use readstat::ReadStatMetadata;

use crate::metadata::{
    Metadata,
};

use readstat::SharedMmap;
enum Engine {
    CppSas7bdat,
    ReadStat,
}

pub enum Backend {
    Cpp(CppBackend),
    ReadStat(ReadStatBackend),
}

pub struct Reader {
    pub backend: Backend,
    pub path: String,
    pub size_hint: usize,
    pub with_columns: Option<Vec<String>>,
    pub threads: usize,
    pub use_mmap: bool,
}
unsafe impl Send for Reader {}
impl Reader {
    pub fn new(
        path: String,
        size_hint: usize,
        with_columns: Option<Vec<String>>,
        threads: usize,
        engine: String,
        md: Option<ReadStatMetadata>,
        schema: Option<Schema>,
        _metadata: Option<Metadata>,
        use_mmap:bool,
    ) -> Self {

        let engine_enum = if path.ends_with(".sas7bdat") & (engine == "cpp") {
            Engine::CppSas7bdat
        } else {
            if engine == "cpp" {
                println!("Using readstat engine for non-sas file ({:?}", path);
            }
            Engine::ReadStat
        };
        let backend = match engine_enum {
            Engine::CppSas7bdat => {
                Backend::Cpp(CppBackend::new(
                    path.clone(),
                    size_hint,
                    with_columns.clone(),
                    threads,
                    schema,
                    _metadata
                ))
            }
            Engine::ReadStat => {
                Backend::ReadStat(ReadStatBackend::new(
                    path.clone(),
                    size_hint,
                    with_columns.clone(),
                    threads,
                    md,
                    _metadata,
                    use_mmap,
                ))
            }
        };

        Reader {
            path, 
            backend,
            size_hint,
            with_columns,
            threads,
            use_mmap,
         }
    }

    pub fn schema(&mut self) -> Result<&Schema, Box<dyn std::error::Error>> {
        match &mut self.backend {
            Backend::Cpp(backend) => backend.schema(),
            Backend::ReadStat(backend) => backend.schema(),
        }
    }

    pub fn schema_with_projection_pushdown(
        &mut self
    ) -> Result<Schema, Box<dyn std::error::Error>> {
        // Get the full schema
        let full_schema = self.schema().unwrap().clone();
        
        let filtered_schema = match &self.with_columns {
            Some(selected_columns) => {
                // Create new schema with only selected columns
                Schema::from_iter(
                    selected_columns
                        .iter()
                        .filter_map(|col_name| {
                            full_schema.get(col_name).map(|dtype| {
                                (PlSmallStr::from(col_name), dtype.clone())
                            })
                        })
                )
            }
            None => full_schema, // Use full schema if no columns specified
        };
        
        Ok(filtered_schema)
    }

    pub fn metadata(&mut self) -> Result<Option<Metadata>, Box<dyn std::error::Error>> {
        match &mut self.backend {
            Backend::Cpp(backend) => Ok(Some(backend.metadata().unwrap().clone())),
            Backend::ReadStat(backend) => Ok(Some(backend.metadata().unwrap().clone())),
        }
    }

    pub fn set_columns_to_read(
        &mut self,
        columns:Option<Vec<String>>,
    ) -> PolarsResult<()> {
        self.with_columns = columns.clone();
    
        let _ = match &mut self.backend {
            Backend::Cpp(backend) => {
                backend.set_columns_to_read(columns)
            },
            Backend::ReadStat(backend) => {
                backend.set_columns_to_read(columns)
            }
        };

        Ok(())
    }

    pub fn initialize_reader(&mut self, row_start: usize, row_end: usize) -> PolarsResult<()> {
        match &mut self.backend {
            Backend::Cpp(backend) => backend.initialize_reader(row_start, row_end),
            Backend::ReadStat(backend) => backend.initialize_reader(row_start, row_end),
        }
    }

    pub fn next(&mut self) -> PolarsResult<Option<DataFrame>> {
        match &mut self.backend {
            Backend::Cpp(backend) => backend.next(),
            Backend::ReadStat(backend) => backend.next(),
        }
    }

    pub fn copy_for_reading(&mut self) -> Reader {
        match &mut self.backend {
            Backend::Cpp(backend) => {
                //  Make sure the schema/metadata has been retrieved
                let schema = backend.schema().unwrap().clone();
                Reader::new(
                    self.path.clone(),
                    self.size_hint.clone(),
                    self.with_columns.clone(),
                    self.threads,
                    "cpp".to_string(),
                    None,
                    Some(schema),
                    Some(backend.metadata().unwrap().clone()),
                    self.use_mmap,
                )
            },
            Backend::ReadStat(backend) => {
                //  Make sure the schema/metadata has been retrieved
                let schema = backend.schema().unwrap().clone();

                let mut reader = Reader::new(
                    self.path.clone(),
                    self.size_hint.clone(),
                    self.with_columns.clone(),
                    self.threads,
                    "readstat".to_string(),
                    backend.md.clone(),
                    Some(schema),
                    Some(backend.metadata().unwrap().clone()),
                    self.use_mmap
                );

                if self.use_mmap {
                    reader.set_mmap(backend.shared_mmap.clone());
                }
                // match &backend.shared_mmap {
                //     Some(mmap) => {
                //         println!("Copying reader with shared mmap at address: {:p}", 
                //                 mmap.memory_address());
                //         reader.set_mmap(backend.shared_mmap.clone());
                //     },
                //     None => println!("Copying reader with no shared mmap"),
                // }
                reader
            },
        }
    }

    pub fn cancel(&mut self) -> PolarsResult<()> {
        match &mut self.backend {
            Backend::Cpp(backend) => backend.cancel(),
            Backend::ReadStat(backend) => backend.cancel(),
        }
    }

    pub fn set_mmap(&mut self, shared_mmap: Option<SharedMmap>) -> PolarsResult<()> {
        match &mut self.backend {
            Backend::ReadStat(backend) => {
                backend.set_mmap(shared_mmap)
            },
            Backend::Cpp(backend) => {
                // No mmap here
                Ok(())
            }
        }
    }
}