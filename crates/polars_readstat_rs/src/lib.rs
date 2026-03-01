//! Pure Rust SAS7BDAT reader with Polars and Arrow output
//!
//! This library reads SAS7BDAT files and outputs Polars DataFrames or Apache Arrow RecordBatches.
//! It supports all format variants (32/64-bit, big/little endian) and
//! all compression types (None, RLE, RDC).

pub mod sas;
pub(crate) mod scan_prefetch;
mod readstat_stream;
pub mod spss;
pub mod stata;

pub use sas::arrow_output as sas_arrow_output;
pub(crate) use sas::buffer;
pub(crate) use sas::constants;
pub(crate) use sas::data;
pub(crate) use sas::decompressor;
pub(crate) use sas::encoding;
pub(crate) use sas::error;
pub(crate) use sas::page;
pub use sas::polars_output as sas_polars_output;
pub(crate) use sas::polars_output;
pub(crate) use sas::types;
pub(crate) use sas::value;
pub use spss::arrow_output as spss_arrow_output;
pub use spss::polars_output as spss_polars_output;
pub use stata::arrow_output as stata_arrow_output;
pub use stata::polars_output as stata_polars_output;

pub use sas::header;
pub use sas::metadata;
pub use sas::reader;

pub use sas::{Compression, Endian, Format, Platform};
pub use sas::{Error, Result, Sas7bdatReader};
pub use sas::{SasValueLabelKey, SasValueLabelMap, SasValueLabels, SasVariableLabels, SasWriter};

pub use sas::scan_sas7bdat;
pub use readstat_stream::{readstat_batch_iter, ReadstatBatchIter, ReadstatBatchStream};

pub use spss::{scan_sav, Error as SpssError, Result as SpssResult, SpssReader};
pub use spss::{
    SpssValueLabelKey, SpssValueLabelMap, SpssValueLabels, SpssVariableLabels, SpssWriteColumn,
    SpssWriteSchema, SpssWriter,
};
pub use stata::{
    compress_df, pandas_make_stata_column_names, pandas_prepare_df_for_stata, pandas_rename_df,
    scan_dta, CompressOptions, Error as StataError, Result as StataResult, StataReader,
    StataWriteColumn, StataWriteSchema, StataWriter, ValueLabelMap, ValueLabels, VariableLabels,
};

use polars::prelude::DataFrame;
use polars_core::POOL;
use std::path::Path;

pub(crate) fn default_thread_count() -> usize {
    let polars_threads = POOL.current_num_threads().max(1);
    let physical = num_cpus::get_physical().max(1);
    polars_threads.min(physical)
}

/// Which columns to track informative nulls for.
#[derive(Debug, Clone)]
pub enum InformativeNullColumns {
    /// All eligible columns (numeric for Stata/SAS; numeric + declared-missing strings for SPSS).
    All,
    /// Only the named columns.
    Selected(Vec<String>),
}

/// How to represent the null indicator in the output DataFrame.
#[derive(Debug, Clone)]
pub enum InformativeNullMode {
    /// Add a separate `String` column named `<col><suffix>` right after the original column.
    SeparateColumn { suffix: String },
    /// Combine each `(col, col_null)` pair into a `Struct{value, null_indicator}` column.
    Struct,
    /// Merge into a single `String` column via `coalesce(cast(col, Str), col_null)`.
    MergedString,
}

impl Default for InformativeNullMode {
    fn default() -> Self {
        InformativeNullMode::SeparateColumn {
            suffix: "_null".to_string(),
        }
    }
}

/// Options for capturing informative (user-defined) missing value indicators.
///
/// When present in `ScanOptions`, user-defined missing values in Stata, SAS, and SPSS
/// are represented as null in the data column AND as a string indicator in a parallel column
/// (or struct/merged string depending on `mode`).
///
/// System missing (`.`) always becomes a plain null (no indicator).
#[derive(Debug, Clone)]
pub struct InformativeNullOpts {
    /// Which columns to track. Required — there is no default (you must actively opt in).
    pub columns: InformativeNullColumns,
    /// How to expose the indicator in the output. Default: `SeparateColumn { suffix: "_null" }`.
    pub mode: InformativeNullMode,
    /// If true, prefer a value label for the indicator string when one is defined (default: true).
    pub use_value_labels: bool,
}

impl InformativeNullOpts {
    pub fn new(columns: InformativeNullColumns) -> Self {
        Self {
            columns,
            mode: InformativeNullMode::default(),
            use_value_labels: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScanOptions {
    pub threads: Option<usize>,
    pub chunk_size: Option<usize>,
    pub missing_string_as_null: Option<bool>,
    pub informative_nulls: Option<InformativeNullOpts>,
    pub value_labels_as_strings: Option<bool>,
    pub preserve_order: Option<bool>,
    pub compress_opts: CompressOptionsLite,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            threads: None,
            chunk_size: None,
            missing_string_as_null: Some(true),
            informative_nulls: None,
            value_labels_as_strings: Some(true),
            preserve_order: Some(false),
            compress_opts: CompressOptionsLite::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompressOptionsLite {
    pub enabled: bool,
    pub cols: Option<Vec<String>>,
    pub compress_numeric: bool,
    pub datetime_to_date: bool,
    pub string_to_numeric: bool,
}

impl Default for CompressOptionsLite {
    fn default() -> Self {
        Self {
            enabled: false,
            cols: None,
            compress_numeric: false,
            datetime_to_date: false,
            string_to_numeric: false,
        }
    }
}

/// Check that adding informative null indicator columns won't collide with existing column names.
/// Called once before reading begins. `var_names` is the full list of column names in the file.
/// `indicator_col_names` is the set of `(main_col_name, indicator_col_name)` pairs that will be added.
pub fn check_informative_null_collisions(
    var_names: &[&str],
    indicator_col_names: &[(String, String)],
) -> polars::prelude::PolarsResult<()> {
    let existing: std::collections::HashSet<&str> = var_names.iter().copied().collect();
    for (_, indicator_name) in indicator_col_names {
        if existing.contains(indicator_name.as_str()) {
            return Err(polars::prelude::PolarsError::ComputeError(
                format!(
                    "informative null indicator column '{}' conflicts with an existing column; \
                     choose a different suffix",
                    indicator_name
                )
                .into(),
            ));
        }
    }
    Ok(())
}

/// Build the list of `(main_col_name, indicator_col_name)` pairs for the informative null feature,
/// given the variable names in the file and the user's column selection.
/// `eligible_col_names` is the subset of `var_names` that are eligible (numeric for Stata/SAS,
/// numeric+declared-missing-strings for SPSS).
pub fn informative_null_pairs(
    var_names: &[&str],
    eligible_col_names: &[&str],
    opts: &InformativeNullOpts,
) -> Vec<(String, String)> {
    let suffix = match &opts.mode {
        InformativeNullMode::SeparateColumn { suffix } => suffix.as_str(),
        InformativeNullMode::Struct | InformativeNullMode::MergedString => "_null",
    };
    let eligible_set: std::collections::HashSet<&str> = eligible_col_names.iter().copied().collect();
    match &opts.columns {
        InformativeNullColumns::All => var_names
            .iter()
            .filter(|&&n| eligible_set.contains(n))
            .map(|&n| (n.to_string(), format!("{}{}", n, suffix)))
            .collect(),
        InformativeNullColumns::Selected(selected) => selected
            .iter()
            .filter(|n| eligible_set.contains(n.as_str()))
            .map(|n| (n.clone(), format!("{}{}", n, suffix)))
            .collect(),
    }
}

/// Build a schema that incorporates indicator columns (or struct / merged-string columns) for the
/// given `pairs` of (main_column, indicator_column) according to `mode`.
/// `base` is the schema without any indicator columns.
pub(crate) fn build_indicator_schema(
    base: polars::prelude::Schema,
    pairs: &[(String, String)],
    mode: &InformativeNullMode,
) -> polars::prelude::Schema {
    use std::collections::{HashMap, HashSet};
    use polars::prelude::{DataType, Field, Schema};

    if pairs.is_empty() {
        return base;
    }

    match mode {
        InformativeNullMode::SeparateColumn { .. } => {
            let pairs_map: HashMap<&str, &str> = pairs
                .iter()
                .map(|(m, i)| (m.as_str(), i.as_str()))
                .collect();
            let indicator_set: HashSet<&str> =
                pairs.iter().map(|(_, i)| i.as_str()).collect();
            let mut schema = Schema::with_capacity(base.len() + pairs.len());
            for (name, dtype) in base.iter() {
                if indicator_set.contains(name.as_str()) {
                    continue;
                }
                schema.with_column(name.clone(), dtype.clone());
                if let Some(&ind) = pairs_map.get(name.as_str()) {
                    schema.with_column(ind.into(), DataType::String);
                }
            }
            schema
        }
        InformativeNullMode::Struct => {
            let pair_set: HashSet<&str> = pairs.iter().map(|(m, _)| m.as_str()).collect();
            let mut schema = Schema::with_capacity(base.len());
            for (name, dtype) in base.iter() {
                if pair_set.contains(name.as_str()) {
                    let struct_dtype = DataType::Struct(vec![
                        Field::new(name.clone(), dtype.clone()),
                        Field::new("null_indicator".into(), DataType::String),
                    ]);
                    schema.with_column(name.clone(), struct_dtype);
                } else {
                    schema.with_column(name.clone(), dtype.clone());
                }
            }
            schema
        }
        InformativeNullMode::MergedString => {
            let pair_set: HashSet<&str> = pairs.iter().map(|(m, _)| m.as_str()).collect();
            let mut schema = Schema::with_capacity(base.len());
            for (name, dtype) in base.iter() {
                if pair_set.contains(name.as_str()) {
                    schema.with_column(name.clone(), DataType::String);
                } else {
                    schema.with_column(name.clone(), dtype.clone());
                }
            }
            schema
        }
    }
}

/// Apply the informative null output mode to a DataFrame that already contains separate indicator
/// columns (named per `pairs`). Called at the end of each batch.
pub fn apply_informative_null_mode(
    df: polars::prelude::DataFrame,
    mode: &InformativeNullMode,
    pairs: &[(String, String)],
) -> polars::prelude::PolarsResult<polars::prelude::DataFrame> {
    use polars::prelude::*;

    if pairs.is_empty() {
        return Ok(df);
    }

    match mode {
        InformativeNullMode::SeparateColumn { .. } => {
            // Columns are already separate; just ensure indicator cols are interleaved
            // right after their main col. Build the desired column order.
            let orig_names: Vec<&PlSmallStr> = df.get_column_names();
            let indicator_set: std::collections::HashSet<&str> =
                pairs.iter().map(|(_, ind)| ind.as_str()).collect();
            let main_to_ind: std::collections::HashMap<&str, &str> = pairs
                .iter()
                .map(|(m, i)| (m.as_str(), i.as_str()))
                .collect();

            // Build interleaved order: for each col, emit it, then its indicator if any
            let mut ordered: Vec<String> = Vec::with_capacity(orig_names.len());
            for name in &orig_names {
                let name_str: &str = name.as_str();
                if indicator_set.contains(name_str) {
                    continue; // will be inserted after the main col
                }
                ordered.push(name_str.to_string());
                if let Some(&ind) = main_to_ind.get(name_str) {
                    if indicator_set.contains(ind) {
                        ordered.push(ind.to_string());
                    }
                }
            }
            let ordered_refs: Vec<&str> = ordered.iter().map(|s| s.as_str()).collect();
            df.select(ordered_refs)
        }
        InformativeNullMode::Struct => {
            // Combine each (main, indicator) pair into a Struct column, replacing the main
            let mut exprs: Vec<Expr> = Vec::with_capacity(pairs.len());
            let mut drop_cols: Vec<String> = Vec::with_capacity(pairs.len());
            for (main, ind) in pairs {
                exprs.push(
                    as_struct(vec![col(main), col(ind).alias("null_indicator")])
                        .alias(main),
                );
                drop_cols.push(ind.clone());
            }
            df.lazy()
                .with_columns(exprs)
                .drop(Selector::ByName { names: drop_cols.into_iter().map(Into::into).collect(), strict: false })
                .collect()
        }
        InformativeNullMode::MergedString => {
            // coalesce(cast(main, String), indicator) → replaces main column
            let mut exprs: Vec<Expr> = Vec::with_capacity(pairs.len());
            let mut drop_cols: Vec<String> = Vec::with_capacity(pairs.len());
            for (main, ind) in pairs {
                exprs.push(
                    polars::prelude::coalesce(&[col(main).cast(DataType::String), col(ind)])
                        .alias(main),
                );
                drop_cols.push(ind.clone());
            }
            df.lazy()
                .with_columns(exprs)
                .drop(Selector::ByName { names: drop_cols.into_iter().map(Into::into).collect(), strict: false })
                .collect()
        }
    }
}

pub fn compress_df_if_enabled(
    df: &DataFrame,
    opts: &CompressOptionsLite,
) -> std::result::Result<DataFrame, String> {
    if !opts.enabled {
        return Ok(df.clone());
    }
    let mut compress_opts = CompressOptions::default();
    compress_opts.compress_numeric = opts.compress_numeric;
    compress_opts.check_date_time = opts.datetime_to_date;
    compress_opts.check_string = opts.string_to_numeric;
    compress_opts.check_string_only = false;
    compress_opts.cast_all_null_to_boolean = true;
    compress_opts.no_boolean = false;
    compress_opts.cols = opts.cols.clone();
    compress_opts.use_stata_bounds = false;
    compress_df(df, compress_opts).map_err(|e| e.to_string())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadStatFormat {
    Sas,
    Stata,
    Spss,
}

fn detect_format(path: &Path) -> Option<ReadStatFormat> {
    let ext = path
        .extension()
        .and_then(|s| s.to_str())?
        .to_ascii_lowercase();
    match ext.as_str() {
        "sas7bdat" | "sas7bcat" => Some(ReadStatFormat::Sas),
        "dta" => Some(ReadStatFormat::Stata),
        "sav" | "zsav" => Some(ReadStatFormat::Spss),
        _ => None,
    }
}

/// Format-agnostic scan that dispatches by file extension.
pub fn readstat_scan(
    path: impl AsRef<Path>,
    opts: Option<ScanOptions>,
    format: Option<ReadStatFormat>,
) -> polars::prelude::PolarsResult<polars::prelude::LazyFrame> {
    let path = path.as_ref();
    let opts = opts.unwrap_or_default();
    let format = format.or_else(|| detect_format(path)).ok_or_else(|| {
        polars::prelude::PolarsError::ComputeError("unknown file extension".into())
    })?;

    match format {
        ReadStatFormat::Sas => sas::scan_sas7bdat(path, opts),
        ReadStatFormat::Stata => stata::scan_dta(path, opts),
        ReadStatFormat::Spss => spss::scan_sav(path, opts),
    }
}

/// Format-agnostic schema (delegates to AnonymousScan).
pub fn readstat_schema(
    path: impl AsRef<Path>,
    opts: Option<ScanOptions>,
    format: Option<ReadStatFormat>,
) -> polars::prelude::PolarsResult<polars::prelude::SchemaRef> {
    readstat_scan(path, opts, format)?.collect_schema()
}

/// Format-agnostic metadata JSON (dispatches by extension).
pub fn readstat_metadata_json(
    path: impl AsRef<Path>,
    format: Option<ReadStatFormat>,
) -> std::result::Result<String, String> {
    let path = path.as_ref();
    let format = format
        .or_else(|| detect_format(path))
        .ok_or("unknown file extension".to_string())?;
    match format {
        ReadStatFormat::Sas => sas::metadata_json(path).map_err(|e| e.to_string()),
        ReadStatFormat::Stata => stata::metadata_json(path).map_err(|e| e.to_string()),
        ReadStatFormat::Spss => spss::metadata_json(path).map_err(|e| e.to_string()),
    }
}
