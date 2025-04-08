use arrow2::{
    array::Array,
    chunk::Chunk,
    error::Error as ArrowError,
    io::{
        csv as csv_arrow2, ipc as ipc_arrow2, ndjson as ndjson_arrow2,
        parquet::{self as parquet_arrow2, write::RowGroupIterator},
    },
};
use colored::Colorize;
// use indicatif::{ProgressBar, ProgressStyle};
use num_format::Locale;
use num_format::ToFormattedString;
use std::{error::Error, fs::OpenOptions, io::stdout};

use crate::rs_data::ReadStatData;
use crate::rs_metadata::ReadStatMetadata;
use crate::rs_path::ReadStatPath;
use crate::rs_var::ReadStatVarFormatClass;
use crate::OutFormat;
use crate::ParquetCompression;

pub struct ReadStatParquetWriter {
    wtr: Box<parquet_arrow2::write::FileWriter<std::fs::File>>,
    options: parquet_arrow2::write::WriteOptions,
    encodings: Vec<Vec<parquet_arrow2::write::Encoding>>,
}

impl ReadStatParquetWriter {
    fn new(
        wtr: Box<parquet_arrow2::write::FileWriter<std::fs::File>>,
        options: parquet_arrow2::write::WriteOptions,
        encodings: Vec<Vec<parquet_arrow2::write::Encoding>>,
    ) -> Self {
        Self {
            wtr,
            options,
            encodings,
        }
    }
}

pub enum ReadStatWriterFormat {
    Csv(std::fs::File),
    CsvStdout(std::io::Stdout),
    Feather(Box<ipc_arrow2::write::FileWriter<std::fs::File>>),
    Ndjson(std::fs::File),
    Parquet(ReadStatParquetWriter),
}

#[derive(Default)]
pub struct ReadStatWriter {
    pub wtr: Option<ReadStatWriterFormat>,
    pub wrote_header: bool,
    pub wrote_start: bool,
}

impl ReadStatWriter {
    pub fn new() -> Self {
        Self {
            wtr: None,
            wrote_header: false,
            wrote_start: false,
        }
    }

    pub fn finish(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match rsp {
            // Write csv data to file
            ReadStatPath {
                out_path: Some(_),
                format: OutFormat::csv,
                ..
            } => self.finish_txt(d, rsp),
            // Write feather data to file
            ReadStatPath {
                format: OutFormat::feather,
                ..
            } => self.finish_feather(d, rsp),
            // Write ndjson data to file
            ReadStatPath {
                format: OutFormat::ndjson,
                ..
            } => self.finish_txt(d, rsp),
            // Write parquet data to file
            ReadStatPath {
                format: OutFormat::parquet,
                ..
            } => self.finish_parquet(d, rsp),
            _ => Ok(()),
        }
    }

    fn _write_message_for_file(&mut self, d: &ReadStatData, rsp: &ReadStatPath) {
        if let Some(pb) = &d.pb {
            let in_f = if let Some(f) = rsp.path.file_name() {
                f.to_string_lossy().bright_red()
            } else {
                String::from("___").bright_red()
            };

            let out_f = if let Some(p) = &rsp.out_path {
                if let Some(f) = p.file_name() {
                    f.to_string_lossy().bright_green()
                } else {
                    String::from("___").bright_green()
                }
            } else {
                String::from("___").bright_green()
            };

            let msg = format!("Writing file {} as {}", in_f, out_f);

            pb.set_message(msg);
        }
    }

    fn write_message_for_rows(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        //if let Some(pb) = &d.pb {
        let in_f = if let Some(f) = rsp.path.file_name() {
            f.to_string_lossy().bright_red()
        } else {
            String::from("___").bright_red()
        };

        let out_f = if let Some(p) = &rsp.out_path {
            if let Some(f) = p.file_name() {
                f.to_string_lossy().bright_green()
            } else {
                String::from("___").bright_green()
            }
        } else {
            String::from("___").bright_green()
        };

        let rows = d
            .chunk_rows_processed
            .to_formatted_string(&Locale::en)
            .truecolor(255, 132, 0);

        let msg = format!("Wrote {} rows from file {} into {}", rows, in_f, out_f);

        println!("{}", msg);
        //pb.set_message(msg);
        //}
        Ok(())
    }

    fn finish_txt(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        //if let Some(pb) = &d.pb {
        let in_f = if let Some(f) = rsp.path.file_name() {
            f.to_string_lossy().bright_red()
        } else {
            String::from("___").bright_red()
        };

        let out_f = if let Some(p) = &rsp.out_path {
            if let Some(f) = p.file_name() {
                f.to_string_lossy().bright_green()
            } else {
                String::from("___").bright_green()
            }
        } else {
            String::from("___").bright_green()
        };

        let rows = if let Some(trp) = &d.total_rows_processed {
            trp.load(std::sync::atomic::Ordering::SeqCst)
                .to_formatted_string(&Locale::en)
                .truecolor(255, 132, 0)
        } else {
            0.to_formatted_string(&Locale::en).truecolor(255, 132, 0)
        };

        let msg = format!(
            "In total, wrote {} rows from file {} into {}",
            rows, in_f, out_f
        );

        println!("{}", msg);

        //pb.set_message(msg);
        //}
        Ok(())
    }

    pub fn write(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match rsp {
            // Write data to standard out
            ReadStatPath {
                out_path: None,
                format: OutFormat::csv,
                ..
            } if self.wrote_header => self.write_data_to_stdout(d),
            // Write header and data to standard out
            ReadStatPath {
                out_path: None,
                format: OutFormat::csv,
                ..
            } => {
                self.write_header_to_stdout(d)?;
                self.write_data_to_stdout(d)
            }
            // Write csv data to file
            ReadStatPath {
                out_path: Some(_),
                format: OutFormat::csv,
                ..
            } if self.wrote_header => self.write_data_to_csv(d, rsp),
            // Write csv header to file
            ReadStatPath {
                out_path: Some(_),
                format: OutFormat::csv,
                ..
            } => {
                self.write_header_to_csv(d, rsp)?;
                self.write_data_to_csv(d, rsp)
            }
            // Write feather data to file
            ReadStatPath {
                format: OutFormat::feather,
                ..
            } => self.write_data_to_feather(d, rsp),
            // Write ndjson data to file
            ReadStatPath {
                format: OutFormat::ndjson,
                ..
            } => self.write_data_to_ndjson(d, rsp),
            // Write parquet data to file
            ReadStatPath {
                format: OutFormat::parquet,
                ..
            } => self.write_data_to_parquet(d, rsp),
        }
    }

    fn write_data_to_csv(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let f = if self.wrote_start {
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(p)?
            };

            // set message for what is being read/written
            self.write_message_for_rows(d, rsp)?;

            // setup writer
            if !self.wrote_start {
                self.wtr = Some(ReadStatWriterFormat::Csv(f))
            };

            // write
            if let Some(ReadStatWriterFormat::Csv(f)) = &mut self.wtr {
                let options = csv_arrow2::write::SerializeOptions::default();

                if let Some(c) = d.chunk.clone() {
                    let cols = &[c];
                    cols.iter()
                        .try_for_each(|batch| csv_arrow2::write::write_chunk(f, batch, &options))?;
                };

                // update
                self.wrote_start = true;
                Ok(())
            } else {
                Err(From::from(
                    "Error writing csv as associated writer is not for the csv format",
                ))
            }
        } else {
            Err(From::from(
                "Error writing csv as output path is set to None",
            ))
        }
    }

    fn write_data_to_feather(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let f = if self.wrote_start {
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(p)?
            };

            // set message for what is being read/written
            self.write_message_for_rows(d, rsp)?;

            // setup writer
            if !self.wrote_start {
                let options = ipc_arrow2::write::WriteOptions {
                    compression: Some(ipc_arrow2::write::Compression::ZSTD),
                };

                let wtr = ipc_arrow2::write::FileWriter::try_new(f, d.schema.clone(), None, options)?;

                self.wtr = Some(ReadStatWriterFormat::Feather(Box::new(wtr)));
            };

            // write
            if let Some(ReadStatWriterFormat::Feather(wtr)) = &mut self.wtr {
                if let Some(c) = d.chunk.clone() {
                    wtr.write(&c, None)?;
                };

                // update
                self.wrote_start = true;

                Ok(())
            } else {
                Err(From::from(
                    "Error writing feather as associated writer is not for the feather format",
                ))
            }
        } else {
            Err(From::from(
                "Error writing feather file as output path is set to None",
            ))
        }
    }

    fn finish_feather(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(ReadStatWriterFormat::Feather(wtr)) = &mut self.wtr {
            wtr.finish()?;

            // set message for what is being read/written
            self.finish_txt(d, rsp)?;

            Ok(())
        } else {
            Err(From::from(
                "Error writing feather as associated writer is not for the feather format",
            ))
        }
    }

    fn write_data_to_ndjson(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let f = if self.wrote_start {
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(p)?
            };

            // set message for what is being read/written
            self.write_message_for_rows(d, rsp)?;

            // setup writer
            if !self.wrote_start {
                self.wtr = Some(ReadStatWriterFormat::Ndjson(f));
            };

            // write
            if let Some(ReadStatWriterFormat::Ndjson(f)) = &mut self.wtr {
                if let Some(c) = d.chunk.clone() {
                    let arrays = c.columns().iter().map(Ok);

                    // serializer
                    let serializer = ndjson_arrow2::write::Serializer::new(arrays, vec![]);

                    // writer
                    let mut wtr = ndjson_arrow2::write::FileWriter::new(f, serializer);

                    // drive iterator
                    wtr.by_ref().collect::<Result<(), ArrowError>>()?;
                }

                // update
                self.wrote_start = true;

                Ok(())
            } else {
                Err(From::from(
                    "Error writing ndjson as associated writer is not for the ndjson format",
                ))
            }
        } else {
            Err(From::from(
                "Error writing ndjson file as output path is set to None",
            ))
        }
    }

    fn write_data_to_parquet(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let f = if self.wrote_start {
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(p)?
            };

            // set message for what is being read/written
            self.write_message_for_rows(d, rsp)?;

            // setup writer
            if !self.wrote_start {
                let options = parquet_arrow2::write::WriteOptions {
                    write_statistics: true,
                    compression: match rsp.compression {
                        Some(ParquetCompression::Uncompressed) => parquet_arrow2::write::CompressionOptions::Uncompressed,
                        Some(ParquetCompression::Snappy) => parquet_arrow2::write::CompressionOptions::Snappy,
                        Some(ParquetCompression::Gzip) => {
                            if let Some(level) = rsp.compression_level {
                                let gzip_level = parquet_arrow2::write::GzipLevel::try_new(level.try_into()
                                    .map_err(|_| "Invalid Gzip compression level")?
                                ).map_err(|_| "Invalid Gzip compression level")?;
                                parquet_arrow2::write::CompressionOptions::Gzip(Some(gzip_level))
                            } else {
                                parquet_arrow2::write::CompressionOptions::Gzip(None)
                            }
                        },
                        Some(ParquetCompression::Lz4Raw) => parquet_arrow2::write::CompressionOptions::Lz4Raw,
                        Some(ParquetCompression::Brotli) => {
                            if let Some(level) = rsp.compression_level {
                                let brotli_level = parquet_arrow2::write::BrotliLevel::try_new(level.try_into()
                                    .map_err(|_| "Invalid Brotli compression level")?
                                ).map_err(|_| "Invalid Brotli compression level")?;
                                parquet_arrow2::write::CompressionOptions::Brotli(Some(brotli_level))
                            } else {
                                parquet_arrow2::write::CompressionOptions::Brotli(None)
                            }
                        },
                        Some(ParquetCompression::Zstd) => {
                            if let Some(level) = rsp.compression_level {
                                let zstd_level = parquet_arrow2::write::ZstdLevel::try_new(level.try_into()
                                    .map_err(|_| "Invalid Zstd compression level")?
                                ).map_err(|_| "Invalid Zstd compression level")?;
                                parquet_arrow2::write::CompressionOptions::Zstd(Some(zstd_level))
                            } else {
                                parquet_arrow2::write::CompressionOptions::Zstd(None)
                            }
                        },
                        None => parquet_arrow2::write::CompressionOptions::Snappy,
                    },
                    version: parquet_arrow2::write::Version::V2,
                    data_pagesize_limit: None 
                };

                let encodings: Vec<Vec<parquet_arrow2::write::Encoding>> = d
                    .schema
                    .fields
                    .iter()
                    .map(|f| {
                        parquet_arrow2::write::transverse(&f.data_type, |_| {
                            parquet_arrow2::write::Encoding::Plain
                        })
                    })
                    .collect();

                let wtr = parquet_arrow2::write::FileWriter::try_new(f, d.schema.clone(), options)?;

                self.wtr = Some(ReadStatWriterFormat::Parquet(ReadStatParquetWriter::new(
                    Box::new(wtr),
                    options,
                    encodings,
                )));
            }

            // write
            if let Some(ReadStatWriterFormat::Parquet(pwtr)) = &mut self.wtr {
                if let Some(c) = d.chunk.clone() {
                    let iter: Vec<Result<Chunk<Box<dyn Array>>, ArrowError>> = vec![Ok(c)];

                    let row_groups = RowGroupIterator::try_new(
                        iter.into_iter(),
                        &d.schema,
                        pwtr.options,
                        pwtr.encodings.clone(),
                    )?;

                    for group in row_groups {
                        pwtr.wtr.write(group?)?;
                    }
                }

                // update
                self.wrote_start = true;

                Ok(())
            } else {
                Err(From::from(
                    "Error writing parquet as associated writer is not for the parquet format",
                ))
            }
        } else {
            Err(From::from(
                "Error writing parquet file as output path is set to None",
            ))
        }
    }

    fn finish_parquet(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(ReadStatWriterFormat::Parquet(pwtr)) = &mut self.wtr {
            let _size = pwtr.wtr.end(None)?;

            // set message for what is being read/written
            self.finish_txt(d, rsp)?;

            Ok(())
        } else {
            Err(From::from(
                "Error writing parquet as associated writer is not for the parquet format",
            ))
        }
    }

    fn write_data_to_stdout(
        &mut self,
        d: &ReadStatData,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(pb) = &d.pb {
            pb.finish_and_clear()
        }

        // writer setup
        if !self.wrote_start {
            self.wtr = Some(ReadStatWriterFormat::CsvStdout(stdout()));
        };

        // write
        if let Some(ReadStatWriterFormat::CsvStdout(f)) = &mut self.wtr {
            let options = csv_arrow2::write::SerializeOptions::default();

            if let Some(c) = d.chunk.clone() {
                let cols = &[c];
                cols.iter()
                    .try_for_each(|batch| csv_arrow2::write::write_chunk(f, batch, &options))?;
            };

            // update
            self.wrote_start = true;

            Ok(())
        } else {
            Err(From::from(
                "Error writing to csv as associated writer is not for the csv format",
            ))
        }
    }

    fn write_header_to_csv(
        &mut self,
        d: &ReadStatData,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(p) = &rsp.out_path {
            // spinner
            /*
            if let Some(pb) = d.pb {
                pb.finish_at_current_pos();
            }
            */

            // spinner
            /*
            if !d.no_progress {
                d.pb = Some(ProgressBar::new(!0));
            }
            if let Some(pb) = d.pb {
                pb.set_style(
                    ProgressStyle::default_spinner()
                        .template("[{spinner:.green} {elapsed_precise} | {bytes}] {msg}"),
                );

                let in_f = if let Some(f) = rsp.path.file_name() {
                    f.to_string_lossy().bright_red()
                } else {
                    String::from("___").bright_red()
                };

                let out_f = if let Some(p) = rsp.out_path {
                    if let Some(f) = p.file_name() {
                        f.to_string_lossy().bright_green()
                    } else {
                        String::from("___").bright_green()
                    }
                } else {
                    String::from("___").bright_green()
                };

                let msg = format!("Writing file {} as {}", in_f, out_f);

                pb.set_message(msg);
                pb.enable_steady_tick(120);
            }
            */
            // progress bar
            /*
            if !self.no_progress {
                self.pb = Some(ProgressBar::new(self.row_count as u64));
            }
            if let Some(pb) = &self.pb {
                pb.set_style(
                    ProgressStyle::default_bar()
                        .template("[{spinner:.green} {elapsed_precise}] {bar:30.cyan/blue} {pos:>7}/{len:7} {msg}")
                        .progress_chars("##-"),
                );
                pb.set_message("Rows processed");
                pb.enable_steady_tick(120);
            }
            */

            // create file
            let mut f = std::fs::File::create(p)?;

            // Get variable names
            let vars: Vec<String> = d.vars.values().map(|m| m.var_name.clone()).collect();

            // write
            let options = csv_arrow2::write::SerializeOptions::default();
            csv_arrow2::write::write_header(&mut f, &vars, &options)?;

            // wrote header
            self.wrote_header = true;

            // return
            Ok(())
        } else {
            Err(From::from(
                "Error writing csv as output path is set to None",
            ))
        }
    }

    fn write_header_to_stdout(
        &mut self,
        d: &ReadStatData,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(pb) = &d.pb {
            pb.finish_and_clear()
        }

        // Get variable names
        let vars: Vec<String> = d.vars.values().map(|m| m.var_name.clone()).collect();

        // write
        let options = csv_arrow2::write::SerializeOptions::default();
        csv_arrow2::write::write_header(&mut stdout(), &vars, &options)?;

        // wrote header
        self.wrote_header = true;

        // return
        Ok(())
    }

    pub fn write_metadata(
        &self,
        md: &ReadStatMetadata,
        rsp: &ReadStatPath,
        as_json: bool,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if as_json {
            self.write_metadata_to_json(md)
        } else {
            self.write_metadata_to_stdout(md, rsp)
        }
    }

    pub fn write_metadata_to_json(
        &self,
        md: &ReadStatMetadata,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match serde_json::to_string_pretty(md) {
            Ok(s) => {
                println!("{}", s);
                Ok(())
            }
            Err(e) => Err(From::from(format!("Error converting to json: {}", e))),
        }
    }

    pub fn write_metadata_to_stdout(
        &self,
        md: &ReadStatMetadata,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!(
            "Metadata for the file {}\n",
            rsp.path.to_string_lossy().bright_yellow()
        );
        println!(
            "{}: {}",
            "Row count".green(),
            md.row_count.to_formatted_string(&Locale::en)
        );
        println!(
            "{}: {}",
            "Variable count".red(),
            md.var_count.to_formatted_string(&Locale::en)
        );
        println!("{}: {}", "Table name".blue(), md.table_name);
        println!("{}: {}", "Table label".cyan(), md.file_label);
        println!("{}: {}", "File encoding".yellow(), md.file_encoding);
        println!("{}: {}", "Format version".green(), md.version);
        println!(
            "{}: {}",
            "Bitness".red(),
            if md.is64bit == 0 { "32-bit" } else { "64-bit" }
        );
        println!("{}: {}", "Creation time".blue(), md.creation_time);
        println!("{}: {}", "Modified time".cyan(), md.modified_time);
        println!("{}: {:#?}", "Compression".yellow(), md.compression);
        println!("{}: {:#?}", "Byte order".green(), md.endianness);
        println!("{}:", "Variable names".purple());
        for (k, v) in md.vars.iter() {
            println!(
                "{}: {} {{ type class: {}, type: {}, label: {}, format class: {}, format: {}, arrow logical data type: {}, arrow physical data type: {} }}",
                (*k).to_formatted_string(&Locale::en),
                v.var_name.bright_purple(),
                format!("{:#?}", v.var_type_class).bright_green(),
                format!("{:#?}", v.var_type).bright_red(),
                v.var_label.bright_blue(),
                (match &v.var_format_class {
                    Some(f) => match f {
                        ReadStatVarFormatClass::Date => "Date",
                        ReadStatVarFormatClass::DateTime |
                        ReadStatVarFormatClass::DateTimeWithMilliseconds | 
                        ReadStatVarFormatClass::DateTimeWithMicroseconds |
                        ReadStatVarFormatClass::DateTimeWithNanoseconds => "DateTime",
                        ReadStatVarFormatClass::Time => "Time",
                    },
                    None => "",
                })
                .bright_cyan(),
                v.var_format.bright_yellow(),
                format!("{:#?}", md.schema.fields[*k as usize].data_type().to_logical_type()).bright_green(),
                format!("{:#?}", md.schema.fields[*k as usize].data_type().to_physical_type()).bright_red(),
            );
        }

        Ok(())
    }
}