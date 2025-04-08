use colored::Colorize;
use path_abs::{PathAbs, PathInfo};
use std::{
    error::Error,
    ffi::CString,
    path::{Path, PathBuf},
};

use crate::OutFormat;
use crate::ParquetCompression;

const IN_EXTENSIONS: &[&str] = &["sas7bdat", "sas7bcat"];

#[derive(Debug, Clone)]
pub struct ReadStatPath {
    pub path: PathBuf,
    pub extension: String,
    pub cstring_path: CString,
    pub out_path: Option<PathBuf>,
    pub format: OutFormat,
    pub overwrite: bool,
    pub no_write: bool,
    pub compression: Option<ParquetCompression>,
    pub compression_level: Option<u32>,
}

impl ReadStatPath {
    pub fn new(
        path: PathBuf,
        out_path: Option<PathBuf>,
        format: Option<OutFormat>,
        overwrite: bool,
        no_write: bool,
        compression: Option<ParquetCompression>,
        compression_level: Option<u32>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let p = Self::validate_path(path)?;
        let ext = Self::validate_in_extension(&p)?;
        let csp = Self::path_to_cstring(&p)?;
        let f = Self::validate_format(format)?;
        let op: Option<PathBuf> = Self::validate_out_path(out_path, overwrite)?;
        let op = match op {
            None => op,
            Some(op) => Self::validate_out_extension(&op, f)?,
        };
        let cl: Option<u32> = match compression {
            None => match compression_level {
                None => None,
                Some(_) => {
                    println!(
                        "Ignoring value of {} as {} was not set",
                        String::from("--compression-level").bright_cyan(),
                        String::from("--compression").bright_cyan()
                    );
                    None
                }
            },
            Some(pc) => Self::validate_compression_level(pc, compression_level)?,
        };

        Ok(Self {
            path: p,
            extension: ext,
            cstring_path: csp,
            out_path: op,
            format: f,
            overwrite,
            no_write,
            compression,
            compression_level: cl,
        })
    }

    #[cfg(unix)]
    pub fn path_to_cstring(path: &PathBuf) -> Result<CString, Box<dyn Error + Send + Sync>> {
        use std::os::unix::ffi::OsStrExt;
        let bytes = path.as_os_str().as_bytes();
        CString::new(bytes).map_err(|_| From::from("Invalid path"))
    }

    #[cfg(not(unix))]
    pub fn path_to_cstring(path: &Path) -> Result<CString, Box<dyn Error + Send + Sync>> {
        let rust_str = path.as_os_str().to_str().ok_or("Invalid path")?;
        CString::new(rust_str).map_err(|_| From::from("Invalid path"))
    }

    fn validate_format(
        format: Option<OutFormat>,
    ) -> Result<OutFormat, Box<dyn Error + Send + Sync>> {
        match format {
            None => Ok(OutFormat::csv),
            Some(f) => Ok(f),
        }
    }

    fn validate_in_extension(path: &Path) -> Result<String, Box<dyn Error + Send + Sync>> {
        path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_owned())
            .map_or(
                Err(From::from(format!(
                    "File {} does not have an extension!",
                    path.to_string_lossy().bright_yellow()
                ))),
                |e|
                    if IN_EXTENSIONS.iter().any(|&ext| ext == e) {
                        Ok(e)
                    } else {
                        Err(From::from(format!("Expecting extension {} or {}.\nFile {} does not have expected extension!", String::from("sas7bdat").bright_green(), String::from("sas7bcat").bright_blue(), path.to_string_lossy().bright_yellow())))
                    }
            )
    }

    fn validate_out_extension(
        path: &Path,
        format: OutFormat,
    ) -> Result<Option<PathBuf>, Box<dyn Error + Send + Sync>> {
        path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_owned())
            .map_or(
                Err(From::from(format!(
                    "File {} does not have an extension!  Expecting extension {}.",
                    path.to_string_lossy().bright_yellow(),
                    format.to_string().bright_green()
                ))),
                |e| match format {
                    OutFormat::csv
                    | OutFormat::ndjson
                    | OutFormat::feather
                    | OutFormat::parquet => {
                        if e == format.to_string() {
                            Ok(Some(path.to_owned()))
                        } else {
                            Err(From::from(format!(
                                "Expecting extension {}.  Instead, file {} has extension {}.",
                                format.to_string().bright_green(),
                                path.to_string_lossy().bright_yellow(),
                                e.bright_red()
                            )))
                        }
                    }
                },
            )
    }

    fn validate_out_path(
        path: Option<PathBuf>,
        overwrite: bool,
    ) -> Result<Option<PathBuf>, Box<dyn Error + Send + Sync>> {
        match path {
            None => Ok(None),
            Some(p) => {
                let abs_path = PathAbs::new(p)?;

                match abs_path.parent() {
                    Err(_) => Err(From::from(format!("The parent directory of the value of the parameter  --output ({}) does not exist", &abs_path.to_string_lossy().bright_yellow()))),
                    Ok(parent) => {
                        if parent.exists() {
                            // Check to see if file already exists
                            if abs_path.exists() {
                                if overwrite {
                                    println!("The file {} will be {}!", abs_path.to_string_lossy().bright_yellow(), String::from("overwritten").truecolor(255, 105, 180));
                                    Ok(Some(abs_path.as_path().to_path_buf()))
                                } else {
                                    Err(From::from(format!("The output file - {} - already exists!  To overwrite the file, utilize the {} parameter", abs_path.to_string_lossy().bright_yellow(), String::from("--overwrite").bright_cyan())))
                                }
                            } else {
                                Ok(Some(abs_path.as_path().to_path_buf()))
                            }
                        } else {
                            Err(From::from(format!("The parent directory of the value of the parameter  --output ({}) does not exist", &parent.to_string_lossy().bright_yellow())))
                        }
                    }
                }
            }
        }
    }

    fn validate_path(path: PathBuf) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
        let abs_path = PathAbs::new(path)?;

        if abs_path.exists() {
            Ok(abs_path.as_path().to_path_buf())
        } else {
            Err(From::from(format!(
                "File {} does not exist!",
                abs_path.to_string_lossy().bright_yellow()
            )))
        }
    }

    fn validate_compression_level(
        compression: ParquetCompression,
        compression_level: Option<u32>,
    ) -> Result<Option<u32>, Box<dyn Error + Send + Sync>> {
        match compression {
            ParquetCompression::Uncompressed => match compression_level {
                None => Ok(compression_level),
                Some(_) => {
                    println!("Compression level is not required for compression={}, ignoring value of {}", String::from("uncompressed").bright_magenta(), String::from("--compression-level").bright_cyan());
                    Ok(None)
                }
            },
            ParquetCompression::Snappy => match compression_level {
                None => Ok(compression_level),
                Some(_) => {
                    println!("Compression level is not required for compression={}, ignoring value of {}", String::from("snappy").bright_magenta(), String::from("--compression-level").bright_cyan());
                    Ok(None)
                }
            },
            ParquetCompression::Lz4Raw => match compression_level {
                None => Ok(compression_level),
                Some(_) => {
                    println!("Compression level is not required for compression={}, ignoring value of {}", String::from("lz4-raw").bright_magenta(), String::from("--compression-level").bright_cyan());
                    Ok(None)
                }
            },
            ParquetCompression::Gzip => match compression_level {
                None => Ok(compression_level),
                Some(c) => {
                    if c <= 9 {
                        Ok(Some(c))
                    } else {
                        Err(From::from(format!("The compression level of {} is not a valid level for {} compression. Instead, please use values between 0-9.", c.to_string().bright_yellow(), String::from("gzip").bright_cyan())))
                    }
                }
            },
            ParquetCompression::Brotli => match compression_level {
                None => Ok(compression_level),
                Some(c) => {
                    if c <= 11 {
                        Ok(Some(c))
                    } else {
                        Err(From::from(format!("The compression level of {} is not a valid level for {} compression. Instead, please use values between 0-11.", c.to_string().bright_yellow(), String::from("brotli").bright_cyan())))
                    }
                }
            },
            ParquetCompression::Zstd => match compression_level {
                None => Ok(compression_level),
                Some(c) => {
                    if c <= 22 {
                        Ok(Some(c))
                    } else {
                        Err(From::from(format!("The compression level of {} is not a valid level for {} compression. Instead, please use values between 0-22.", c.to_string().bright_yellow(), String::from("zstd").bright_cyan())))
                    }
                }
            },
        }
    }
}
