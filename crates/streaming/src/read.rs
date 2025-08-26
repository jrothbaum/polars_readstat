use polars::prelude::*;
use crate::backends::{self, CppBackend, ReadStatBackend, ReaderBackend};
use readstat::ReadStatMetadata;

use crate::metadata::{
    Metadata,
};
enum Engine {
    CppSas7bdat,
    ReadStat,
}

pub enum Backend {
    Cpp(CppBackend),
    ReadStat(ReadStatBackend),
}

pub struct Reader {
    backend: Backend,
    path: String,
    size_hint: usize,
    with_columns: Option<Vec<String>>,
    threads: usize
}

impl Reader {
    pub fn new(
        path: String,
        size_hint: usize,
        with_columns: Option<Vec<String>>,
        threads: usize,
        engine: String,
        md: Option<ReadStatMetadata>,
        schema: Option<Schema>,
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
                    schema
                ))
            }
            Engine::ReadStat => {
                Backend::ReadStat(ReadStatBackend::new(
                    path.clone(),
                    size_hint,
                    with_columns.clone(),
                    threads,
                    md
                ))
            }
        };

        Reader {
            path, 
            backend,
            size_hint,
            with_columns,
            threads
         }
    }

    pub fn schema(&mut self) -> Result<&Schema, Box<dyn std::error::Error>> {
        match &mut self.backend {
            Backend::Cpp(backend) => backend.schema(),
            Backend::ReadStat(backend) => backend.schema(),
        }
    }

    pub fn metadata(&mut self) -> Result<Option<Metadata>, Box<dyn std::error::Error>> {
        match &mut self.backend {
            Backend::Cpp(backend) => Ok(Some(backend.metadata().unwrap().clone())),
            Backend::ReadStat(backend) => Ok(Some(backend.metadata().unwrap().clone())),
        }
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
                Reader::new(
                    self.path.clone(),
                    self.size_hint.clone(),
                    self.with_columns.clone(),
                    self.threads,
                    "cpp".to_string(),
                    None,
                    Some(backend.schema().unwrap().clone())
                )
            },
            Backend::ReadStat(backend) => {
                let schema = backend.schema().unwrap().clone();
                Reader::new(
                    self.path.clone(),
                    self.size_hint.clone(),
                    self.with_columns.clone(),
                    self.threads,
                    "readstat".to_string(),
                    backend.md.clone(),
                    Some(backend.schema().unwrap().clone())
                )
            },
        }
    }
}