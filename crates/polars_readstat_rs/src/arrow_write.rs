//! Write Stata, SPSS and SAS (CSV + import script) files from Arrow C Data
//! Interface batches.
//!
//! This is the write-side counterpart of the `read_to_arrow_*_ffi` functions: a
//! consumer that has its data as Arrow batches (for example a database exporting
//! query results) hands over one struct-typed schema and the struct-typed arrays
//! that follow it, and the matching format writer is run on the combined table.
//!
//! The format writers need the whole table, so all batches are combined in memory
//! before writing.

use crate::{write_por, PorWriteOptions, SasWriter, SpssWriter, StataWriter, XptWriter};
use polars::prelude::*;
use polars_arrow::array::{Array, PrimitiveArray, StructArray};
use polars_arrow::datatypes::ArrowDataType;
use polars_arrow::ffi::{import_array_from_c, import_field_from_c};

/// The Arrow C Data Interface structs taken by [`write_arrow_ffi`].
pub use polars_arrow::ffi::{ArrowArray, ArrowSchema};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Output format and its format-specific options.
#[derive(Debug, Clone)]
pub enum ArrowWriteFormat {
    /// A Stata `.dta` file.
    Stata { compress: bool },
    /// An SPSS `.sav` file; `compressed` selects standard SAV compression.
    Spss { compressed: bool },
    /// A SAS transport (`.xpt`) file; `version` is 5 or 8.
    Xpt {
        version: u8,
        table_name: Option<String>,
        file_label: Option<String>,
    },
    /// An SPSS portable (`.por`) file.
    Por { file_label: Option<String> },
    /// A SAS import bundle: a `.csv` (or `.csv.gz`) data file and a `.sas` script.
    SasCsvImport {
        dataset_name: Option<String>,
        library: Option<String>,
        gzip: bool,
        delete_csv_on_import: bool,
    },
}

/// Metadata that cannot be carried by the Arrow batches themselves.
#[derive(Debug, Clone, Default)]
pub struct ArrowWriteOptions {
    /// Descriptive label for each column, keyed by column name.
    pub variable_labels: HashMap<String, String>,
}

fn invalid(message: impl Into<String>) -> PolarsError {
    PolarsError::ComputeError(message.into().into())
}

/// The formats store numbers as doubles, so exact decimals (DuckDB `DECIMAL` and
/// `HUGEINT`) are converted to `f64`. Every other type is passed through.
fn decimals_to_f64(array: &dyn Array) -> Option<Box<dyn Array>> {
    let ArrowDataType::Decimal(_, scale) = array.dtype() else {
        return None;
    };
    let values = array.as_any().downcast_ref::<PrimitiveArray<i128>>()?;
    let divisor = 10f64.powi(*scale as i32);
    let converted: PrimitiveArray<f64> = values
        .iter()
        .map(|v| v.map(|v| *v as f64 / divisor))
        .collect();
    Some(Box::new(converted))
}

/// Rows merged into one chunk per column while combining batches.
const MERGE_ROWS: usize = 250_000;

/// Turn one exported struct array into a table.
fn batch_to_df(array: ArrowArray, dtype: &ArrowDataType) -> PolarsResult<DataFrame> {
    let ArrowDataType::Struct(fields) = dtype else {
        return Err(invalid("expected a struct schema for the batches"));
    };
    let array = unsafe { import_array_from_c(array, dtype.clone())? };
    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| invalid("expected struct arrays for the batches"))?;
    let columns = fields
        .iter()
        .zip(struct_array.values())
        .map(|(field, values)| {
            let values = decimals_to_f64(values.as_ref()).unwrap_or_else(|| values.clone());
            Series::from_arrow(field.name.clone(), values).map(Column::from)
        })
        .collect::<PolarsResult<Vec<_>>>()?;
    DataFrame::new(struct_array.len(), columns)
}

/// Accumulates Arrow batches into one table, converting each batch as it arrives so
/// the Arrow data of earlier batches can be released rather than held until the end.
pub struct ArrowTableBuilder {
    dtype: ArrowDataType,
    /// Merged chunks so far.
    combined: Option<DataFrame>,
    /// Batches not yet merged into a larger chunk.
    group: Option<DataFrame>,
}

impl ArrowTableBuilder {
    /// `schema` is the struct schema shared by every batch; it is consumed.
    pub fn new(schema: ArrowSchema) -> PolarsResult<Self> {
        let field = unsafe { import_field_from_c(&schema)? };
        Ok(Self { dtype: field.dtype().clone(), combined: None, group: None })
    }

    /// Add one struct-typed batch.
    pub fn push(&mut self, batch: ArrowArray) -> PolarsResult<()> {
        let df = batch_to_df(batch, &self.dtype)?;
        match self.group.as_mut() {
            Some(group) => {
                group.vstack_mut_owned(df)?;
            }
            None => self.group = Some(df),
        }
        if self.group.as_ref().is_some_and(|g| g.height() >= MERGE_ROWS) {
            self.close_group()?;
        }
        Ok(())
    }

    // The writers read cells by row index, which gets slower as the number of chunks per
    // column grows, so small batches are merged into larger chunks. Merging a group at a
    // time (rather than the whole table at the end) frees the small batches as it goes, so
    // only one extra group is in memory instead of a second copy of the whole table.
    fn close_group(&mut self) -> PolarsResult<()> {
        if let Some(mut merged) = self.group.take() {
            merged.rechunk_mut();
            match self.combined.as_mut() {
                Some(acc) => {
                    acc.vstack_mut_owned(merged)?;
                }
                None => self.combined = Some(merged),
            }
            // Hand the memory freed by the merge back to the OS; without this the process
            // keeps it, and writing a large table peaks about 1 GB higher.
            #[cfg(all(target_os = "linux", target_env = "gnu"))]
            unsafe {
                libc::malloc_trim(0);
            }
        }
        Ok(())
    }

    /// The table of everything pushed; with no batches, an empty table with the schema's columns.
    fn into_table(mut self) -> PolarsResult<DataFrame> {
        self.close_group()?;
        if let Some(df) = self.combined {
            return Ok(df);
        }
        let ArrowDataType::Struct(fields) = &self.dtype else {
            return Err(invalid("expected a struct schema for the batches"));
        };
        let columns = fields
            .iter()
            .map(|f| {
                let dtype = match &f.dtype {
                    ArrowDataType::Decimal(..) => DataType::Float64,
                    other => DataType::from_arrow_dtype(other),
                };
                Column::from(Series::new_empty(f.name.clone(), &dtype))
            })
            .collect();
        DataFrame::new(0, columns)
    }

    /// Write everything pushed to `path` in the requested format. Returns the files written:
    /// one, or the data file and the script for [`ArrowWriteFormat::SasCsvImport`].
    pub fn write(
        self,
        path: &Path,
        format: ArrowWriteFormat,
        options: ArrowWriteOptions,
    ) -> PolarsResult<Vec<PathBuf>> {
        write_table(path, format, options, self.into_table()?)
    }
}

/// Write the batches to `path` in the requested format.
///
/// `schema` is the struct schema shared by every array in `batches`. Both are
/// consumed. Returns the files written: one, or the data file and the script for
/// [`ArrowWriteFormat::SasCsvImport`].
pub fn write_arrow_ffi(
    path: &Path,
    format: ArrowWriteFormat,
    options: ArrowWriteOptions,
    schema: ArrowSchema,
    batches: Vec<ArrowArray>,
) -> PolarsResult<Vec<PathBuf>> {
    let mut builder = ArrowTableBuilder::new(schema)?;
    for batch in batches {
        builder.push(batch)?;
    }
    builder.write(path, format, options)
}

/// XPT stores numbers as IBM floating point, which reaches about 7.2e75 in size. A larger value
/// would be written as missing, so find the first such value. (A value too small to hold is
/// written as zero, which is not refused: tiny values are rounding noise far more often than
/// data.)
fn value_outside_ibm_float_range(df: &DataFrame) -> Option<(String, f64)> {
    const LARGEST: f64 = 7.237005577332262e75; // (1 - 16^-14) * 16^63
    for col in df.columns() {
        if !matches!(col.dtype(), DataType::Float32 | DataType::Float64) {
            continue;
        }
        let values = col.cast(&DataType::Float64).ok()?;
        let bad = values
            .f64()
            .ok()?
            .iter()
            .flatten()
            .find(|v| v.is_finite() && v.abs() > LARGEST);
        if let Some(value) = bad {
            return Some((col.name().to_string(), value));
        }
    }
    None
}

/// Whether the table has only text columns and its last row is blank in all of them. An XPT
/// file stores no row count, so such a row cannot be told apart from the padding after the data.
fn ends_with_blank_text_row(df: &DataFrame) -> bool {
    let height = df.height();
    if height == 0 || df.width() == 0 {
        return false;
    }
    df.columns().iter().all(|col| {
        col.str()
            .map(|strings| strings.get(height - 1).is_none_or(|s| s.trim_end().is_empty()))
            .unwrap_or(false)
    })
}

fn write_table(
    path: &Path,
    format: ArrowWriteFormat,
    options: ArrowWriteOptions,
    df: DataFrame,
) -> PolarsResult<Vec<PathBuf>> {
    let io = |e: &dyn std::fmt::Display| invalid(e.to_string());

    match format {
        ArrowWriteFormat::Stata { compress } => {
            let mut writer = StataWriter::new(path).with_compress(compress);
            if !options.variable_labels.is_empty() {
                writer = writer.with_variable_labels(options.variable_labels);
            }
            writer.write_df(&df).map_err(|e| io(&e))?;
            Ok(vec![path.to_path_buf()])
        }
        ArrowWriteFormat::Spss { compressed } => {
            let mut writer = SpssWriter::new(path).with_compression(compressed);
            if !options.variable_labels.is_empty() {
                writer = writer.with_variable_labels(options.variable_labels);
            }
            writer.write_df(&df).map_err(|e| io(&e))?;
            Ok(vec![path.to_path_buf()])
        }
        ArrowWriteFormat::Xpt { version, table_name, file_label } => {
            if let Some((column, value)) = value_outside_ibm_float_range(&df) {
                return Err(invalid(format!(
                    "column '{column}' has the value {value:e}, outside the range an XPT file can hold \
                     (up to about 7.2e75 in size); it would be written as missing"
                )));
            }
            if ends_with_blank_text_row(&df) {
                return Err(invalid(
                    "the last row is blank in every column, and an XPT file has no row count, so it would \
                     be read back as padding and lost; remove trailing blank rows or add a numeric column",
                ));
            }
            let mut writer = XptWriter::new(path).with_version(version);
            if let Some(name) = table_name {
                writer = writer.with_table_name(name);
            }
            if let Some(label) = file_label {
                writer = writer.with_file_label(label);
            }
            if !options.variable_labels.is_empty() {
                writer = writer.with_variable_labels(options.variable_labels);
            }
            writer.write_df(&df)?;
            Ok(vec![path.to_path_buf()])
        }
        ArrowWriteFormat::Por { file_label } => {
            let opts = PorWriteOptions {
                file_label,
                variable_labels: (!options.variable_labels.is_empty()).then_some(options.variable_labels),
            };
            write_por(&df, path, opts).map_err(|e| io(&e))?;
            Ok(vec![path.to_path_buf()])
        }
        ArrowWriteFormat::SasCsvImport {
            dataset_name,
            library,
            gzip,
            delete_csv_on_import,
        } => {
            let mut writer = SasWriter::new(path)
                .with_gzip(gzip)
                .with_delete_csv_on_import(delete_csv_on_import);
            if let Some(name) = dataset_name {
                writer = writer.with_dataset_name(name);
            }
            if let Some(library) = library {
                writer = writer.with_library(library);
            }
            if !options.variable_labels.is_empty() {
                writer = writer.with_variable_labels(options.variable_labels);
            }
            let (data, script) = writer.write_df(&df).map_err(|e| io(&e))?;
            Ok(vec![data, script])
        }
    }
}
