use crate::sas::error::{Error, Result};
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SasValueLabelKey {
    Num(u64),
    Str(String),
}

impl From<f64> for SasValueLabelKey {
    fn from(value: f64) -> Self {
        SasValueLabelKey::Num(value.to_bits())
    }
}

impl From<&str> for SasValueLabelKey {
    fn from(value: &str) -> Self {
        SasValueLabelKey::Str(value.to_string())
    }
}

pub type SasValueLabelMap = HashMap<SasValueLabelKey, String>;
pub type SasValueLabels = HashMap<String, SasValueLabelMap>;
pub type SasVariableLabels = HashMap<String, String>;

/// Writes a CSV + SAS program pair that reconstructs a dataset with types and labels.
///
/// This does not produce a SAS7BDAT file. The output is a `.csv` data file and a
/// companion `.sas` script containing `PROC FORMAT`, a `DATA` step with `LENGTH`,
/// `FORMAT`, `LABEL`, and `INPUT` statements. Running the script in SAS loads the
/// data with the correct types and metadata.
///
/// # Type mapping
///
/// | Polars type | CSV value | SAS treatment |
/// |---|---|---|
/// | `Boolean` | `0` / `1` | `length 3` |
/// | `Date` | days since 1960-01-01 | `format yymmdd10.` |
/// | `Datetime` | seconds since 1960-01-01 (sub-second precision lost) | `format datetime19.` |
/// | `Time` | seconds since midnight (sub-second precision lost) | `format time8.` |
/// | `Int8` / `UInt8` | raw integer | `length 3` |
/// | `Int16` / `UInt16` | raw integer | `length 4` |
/// | `Int32` | raw integer | `length 5` |
/// | `UInt32` | raw integer | `length 6` |
/// | `Float32` | raw float | `length 4` |
/// | `Int64` / `UInt64` / `Float64` | raw value | SAS default (8 bytes) |
/// | `String` | as-is | `length $<max_bytes>` |
///
/// Column names are sanitized to SAS rules (alphanumeric + underscore, starts with
/// a letter, max 32 chars). Duplicates are disambiguated with a numeric suffix.
pub struct SasWriter {
    base_path: PathBuf,
    dataset_name: Option<String>,
    library: Option<String>,
    delete_csv_on_import: bool,
    value_labels: Option<SasValueLabels>,
    variable_labels: Option<SasVariableLabels>,
}

impl SasWriter {
    /// Create a new writer.
    ///
    /// `path` may be a directory or a file path (the extension is ignored).
    /// Files are written as `<dataset>.csv` and `<dataset>.sas` in that location.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            base_path: path.as_ref().to_path_buf(),
            dataset_name: None,
            library: None,
            delete_csv_on_import: false,
            value_labels: None,
            variable_labels: None,
        }
    }

    /// Set the SAS dataset name used in the generated script.
    /// If not called, the name is derived from the output file stem.
    pub fn with_dataset_name(mut self, name: impl AsRef<str>) -> Self {
        self.dataset_name = Some(name.as_ref().to_string());
        self
    }

    /// Set the SAS library for the DATA step.
    ///
    /// When set, the script emits `libname <library> "<output_dir>";` and writes
    /// `data <library>.<dataset>` so the dataset is saved permanently alongside the files.
    /// When None (default), the dataset is written to WORK.
    pub fn with_library(mut self, library: impl AsRef<str>) -> Self {
        self.library = Some(library.as_ref().to_string());
        self
    }

    /// When true, the generated SAS script deletes the CSV file after importing it.
    pub fn with_delete_csv_on_import(mut self, enabled: bool) -> Self {
        self.delete_csv_on_import = enabled;
        self
    }

    /// Attach value labels for columns (used to build `PROC FORMAT`).
    pub fn with_value_labels(mut self, labels: SasValueLabels) -> Self {
        self.value_labels = Some(labels);
        self
    }

    /// Attach variable labels (used to build a `LABEL` statement).
    pub fn with_variable_labels(mut self, labels: SasVariableLabels) -> Self {
        self.variable_labels = Some(labels);
        self
    }

    /// Write the CSV and SAS script, returning their paths.
    pub fn write_df(&self, df: &DataFrame) -> Result<(PathBuf, PathBuf)> {
        // Resolve dataset name: explicit > derived from path stem
        let raw_name = self
            .dataset_name
            .as_deref()
            .or_else(|| self.base_path.file_stem().and_then(|s| s.to_str()))
            .unwrap_or("data");
        let dataset = sanitize_sas_name(raw_name);

        let (csv_path, sas_path) = resolve_paths(&self.base_path, &dataset)?;

        let (df_renamed, name_map) = sas_rename_df(df)?;
        let value_labels = self
            .value_labels
            .as_ref()
            .map(|v| rename_value_labels(v, &name_map));
        let variable_labels = self
            .variable_labels
            .as_ref()
            .map(|v| rename_variable_labels(v, &name_map));

        let mut df_out = prepare_df_for_csv(&df_renamed)?;

        let mut file = BufWriter::new(File::create(&csv_path)?);
        CsvWriter::new(&mut file)
            .include_header(true)
            .finish(&mut df_out)
            .map_err(|e| Error::ParseError(e.to_string()))?;

        let output_dir = csv_path.parent();
        let script = build_sas_script(
            &dataset,
            &csv_path,
            &df_renamed,
            value_labels.as_ref(),
            variable_labels.as_ref(),
            self.library.as_deref(),
            output_dir,
            self.delete_csv_on_import,
        )?;
        let mut sas_file = BufWriter::new(File::create(&sas_path)?);
        sas_file.write_all(script.as_bytes())?;
        Ok((csv_path, sas_path))
    }
}

fn resolve_paths(base: &Path, dataset: &str) -> Result<(PathBuf, PathBuf)> {
    if base.is_dir() {
        let csv = base.join(format!("{dataset}.csv"));
        let sas = base.join(format!("{dataset}.sas"));
        return Ok((csv, sas));
    }
    let stem = if base.extension().and_then(|s| s.to_str()).is_some() {
        base.with_extension("")
    } else {
        base.to_path_buf()
    };
    Ok((stem.with_extension("csv"), stem.with_extension("sas")))
}

fn sanitize_sas_name(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            out.push(c);
        }
    }
    if out.is_empty() || !out.chars().next().unwrap().is_ascii_alphabetic() {
        out.insert(0, 'd');
    }
    if out.len() > 32 {
        out.truncate(32);
    }
    out
}

fn make_unique(name: &str, used: &mut HashSet<String>) -> String {
    let candidate = name.to_string();
    if !used.contains(&candidate) {
        used.insert(candidate.clone());
        return candidate;
    }
    let mut i = 1usize;
    loop {
        let suffix = format!("_{i}");
        let max_base = 32usize.saturating_sub(suffix.len());
        let mut base = name.to_string();
        if base.len() > max_base {
            base.truncate(max_base);
        }
        let cand = format!("{base}{suffix}");
        if !used.contains(&cand) {
            used.insert(cand.clone());
            return cand;
        }
        i += 1;
    }
}

fn sas_rename_df(df: &DataFrame) -> Result<(DataFrame, HashMap<String, String>)> {
    let mut used = HashSet::new();
    let names = df.get_column_names();
    let mut mapping = HashMap::new();
    let mut new_names = Vec::with_capacity(names.len());
    for name in names {
        let mut s = sanitize_sas_name(name);
        if s.is_empty() {
            s = "col".to_string();
        }
        let s = make_unique(&s, &mut used);
        mapping.insert(name.to_string(), s.clone());
        new_names.push(s);
    }
    let mut out = df.clone();
    out.set_column_names(&new_names)
        .map_err(|e| Error::ParseError(e.to_string()))?;
    Ok((out, mapping))
}

fn rename_value_labels(
    labels: &SasValueLabels,
    name_map: &HashMap<String, String>,
) -> SasValueLabels {
    let mut out = HashMap::new();
    for (name, mapping) in labels {
        let key = name_map.get(name).cloned().unwrap_or_else(|| name.clone());
        out.insert(key, mapping.clone());
    }
    out
}

fn rename_variable_labels(
    labels: &SasVariableLabels,
    name_map: &HashMap<String, String>,
) -> SasVariableLabels {
    let mut out = HashMap::new();
    for (name, label) in labels {
        let key = name_map.get(name).cloned().unwrap_or_else(|| name.clone());
        out.insert(key, label.clone());
    }
    out
}

fn prepare_df_for_csv(df: &DataFrame) -> Result<DataFrame> {
    let mut cols = Vec::with_capacity(df.width());
    for col in df.columns() {
        let series = col.as_materialized_series();
        let out = match series.dtype() {
            DataType::Boolean => series
                .cast(&DataType::Int8)
                .map_err(|e| Error::ParseError(e.to_string()))?,
            DataType::Date => {
                let casted = series
                    .cast(&DataType::Int32)
                    .map_err(|e| Error::ParseError(e.to_string()))?;
                let ca = casted.i32().map_err(|e| Error::ParseError(e.to_string()))?;
                let name = series.name().clone();
                let iter = ca.into_iter().map(|opt| opt.map(|v| v as i64 + 3653));
                Int64Chunked::from_iter_options(name, iter).into_series()
            }
            DataType::Datetime(unit, _) => {
                let casted = series
                    .cast(&DataType::Int64)
                    .map_err(|e| Error::ParseError(e.to_string()))?;
                let ca = casted.i64().map_err(|e| Error::ParseError(e.to_string()))?;
                let name = series.name().clone();
                let iter = ca.into_iter().map(|opt| {
                    opt.map(|v| {
                        let ms = match unit {
                            TimeUnit::Milliseconds => v,
                            TimeUnit::Microseconds => v / 1_000,
                            TimeUnit::Nanoseconds => v / 1_000_000,
                        };
                        let secs = ms / 1_000;
                        secs + 3653i64 * 86_400
                    })
                });
                Int64Chunked::from_iter_options(name, iter).into_series()
            }
            DataType::Time => {
                let casted = series
                    .cast(&DataType::Int64)
                    .map_err(|e| Error::ParseError(e.to_string()))?;
                let ca = casted.i64().map_err(|e| Error::ParseError(e.to_string()))?;
                let name = series.name().clone();
                let iter = ca.into_iter().map(|opt| opt.map(|v| v / 1_000_000_000));
                Int64Chunked::from_iter_options(name, iter).into_series()
            }
            _ => series.clone(),
        };
        cols.push(out.into_column());
    }
    DataFrame::new_infer_height(cols).map_err(|e| Error::ParseError(e.to_string()))
}

/// Returns the SAS numeric LENGTH (in bytes) to declare for a given Polars dtype,
/// or None if the type should use the SAS default (8 bytes).
fn sas_numeric_length(dtype: &DataType) -> Option<u8> {
    match dtype {
        // Boolean is cast to Int8 before CSV write; treat same as Int8 for LENGTH
        DataType::Boolean | DataType::Int8 | DataType::UInt8 => Some(3),
        DataType::Int16 | DataType::UInt16 => Some(4),
        DataType::Int32 => Some(5),
        DataType::UInt32 => Some(6),
        DataType::Float32 => Some(4),
        // Int64, UInt64, Float64 → SAS default 8 bytes, no statement needed
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_sas_script(
    dataset: &str,
    csv_path: &Path,
    df: &DataFrame,
    value_labels: Option<&SasValueLabels>,
    variable_labels: Option<&SasVariableLabels>,
    library: Option<&str>,
    output_dir: Option<&Path>,
    delete_csv_on_import: bool,
) -> Result<String> {
    let mut script = String::new();

    // Optional permanent libname statement
    if let (Some(lib), Some(dir)) = (library, output_dir) {
        script.push_str(&format!(
            "libname {} \"{}\";\n\n",
            lib,
            dir.display()
        ));
    }

    script.push_str("proc format;\n");
    if let Some(vlabels) = value_labels {
        for (col, mapping) in vlabels {
            if mapping.is_empty() {
                continue;
            }
            let (fmt_name, is_char) = format_name_for_column(col, df)?;
            script.push_str(&format!(
                "  value {}{}\n",
                if is_char { "$" } else { "" },
                fmt_name
            ));
            for (k, v) in mapping {
                let key = match k {
                    SasValueLabelKey::Num(n) => format!("{}", f64::from_bits(*n)),
                    SasValueLabelKey::Str(s) => format!("\"{}\"", sas_quote(s)),
                };
                script.push_str(&format!("    {} = \"{}\"\n", key, sas_quote(v)));
            }
            script.push_str("  ;\n");
        }
    }
    script.push_str("run;\n\n");

    let data_target = match library {
        Some(lib) => format!("{}.{}", lib, dataset),
        None => dataset.to_string(),
    };
    script.push_str(&format!("data {};\n", data_target));
    script.push_str(&format!(
        "  infile \"{}\" dsd dlm=',' firstobs=2 truncover encoding='utf-8';\n",
        csv_path.display()
    ));

    // Length for string columns
    for col in df.columns() {
        let series = col.as_materialized_series();
        if matches!(series.dtype(), DataType::String) {
            let width = max_string_width(series)?;
            script.push_str(&format!("  length {} ${};\n", series.name(), width));
        }
    }

    // Length for typed numeric columns (preserves storage precision in SAS)
    for col in df.columns() {
        let series = col.as_materialized_series();
        if let Some(len) = sas_numeric_length(series.dtype()) {
            script.push_str(&format!("  length {} {};\n", series.name(), len));
        }
    }

    let mut format_lines = Vec::new();
    if let Some(vlabels) = value_labels {
        for (col, mapping) in vlabels {
            if mapping.is_empty() {
                continue;
            }
            let (fmt_name, is_char) = format_name_for_column(col, df)?;
            format_lines.push(format!(
                "{} {}{}.",
                col,
                if is_char { "$" } else { "" },
                fmt_name
            ));
        }
    }
    for col in df.columns() {
        let series = col.as_materialized_series();
        let fmt = match series.dtype() {
            DataType::Date => Some("yymmdd10.".to_string()),
            DataType::Datetime(_, _) => Some("datetime19.".to_string()),
            DataType::Time => Some("time8.".to_string()),
            _ => None,
        };
        if let Some(fmt) = fmt {
            format_lines.push(format!("{} {}", series.name(), fmt));
        }
    }
    if !format_lines.is_empty() {
        script.push_str("  format ");
        script.push_str(&format_lines.join(" "));
        script.push_str(";\n");
    }

    if let Some(vlabels) = variable_labels {
        if !vlabels.is_empty() {
            script.push_str("  label ");
            let mut parts = Vec::new();
            for (col, label) in vlabels {
                parts.push(format!("{} = \"{}\"", col, sas_quote(label)));
            }
            script.push_str(&parts.join(" "));
            script.push_str(";\n");
        }
    }

    script.push_str("  input\n");
    for col in df.columns() {
        let series = col.as_materialized_series();
        let informat = match series.dtype() {
            DataType::String => {
                let width = max_string_width(series)?;
                format!("${}.", width)
            }
            _ => "best32.".to_string(),
        };
        script.push_str(&format!("    {} : {}\n", series.name(), informat));
    }
    script.push_str("  ;\nrun;\n");

    if delete_csv_on_import {
        script.push_str(&format!(
            "\nfilename _prscsv \"{}\";\n%let _prsrc = %sysfunc(fdelete(_prscsv));\nfilename _prscsv clear;\n",
            csv_path.display()
        ));
    }

    Ok(script)
}

fn format_name_for_column(col: &str, df: &DataFrame) -> Result<(String, bool)> {
    let series = df
        .column(col)
        .map_err(|e| Error::ParseError(e.to_string()))?
        .as_materialized_series();
    let is_char = matches!(series.dtype(), DataType::String);
    let mut name = format!("fmt_{}", sanitize_sas_name(col));
    if name.len() > 32 {
        name.truncate(32);
    }
    Ok((name, is_char))
}

fn sas_quote(s: &str) -> String {
    s.replace('"', "\"\"")
}

fn max_string_width(series: &Series) -> Result<usize> {
    let utf8 = series.str().map_err(|e| Error::ParseError(e.to_string()))?;
    let mut max_len = 1usize;
    for opt in utf8.into_iter() {
        if let Some(s) = opt {
            max_len = max_len.max(s.as_bytes().len());
        }
    }
    Ok(max_len)
}
