//! Writing through the Arrow C Data Interface: a table exported as Arrow batches
//! is written as Stata / SPSS / SAS-CSV and read back.

use polars::prelude::*;
use polars_arrow::array::StructArray;
use polars_arrow::datatypes::{ArrowDataType, Field as ArrowField};
use polars_arrow::ffi::{export_array_to_c, export_field_to_c, ArrowArray, ArrowSchema};
use polars_readstat_rs::arrow_write::{write_arrow_ffi, ArrowWriteFormat, ArrowWriteOptions};
use polars_readstat_rs::readstat_scan;
use std::collections::HashMap;
use std::path::PathBuf;

/// A fresh empty directory under the system temp dir, removed when dropped.
struct TempDir(PathBuf);
impl TempDir {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!("prs_arrow_write_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn sample() -> DataFrame {
    df!(
        "id" => [1i32, 2, 3, 4],
        "score" => [Some(1.5f64), None, Some(3.25), Some(-4.0)],
        "name" => [Some("alpha"), Some("beta"), None, Some("delta")],
        "flag" => [true, false, true, false],
    )
    .unwrap()
}

/// Export `df` as one schema and `n_batches` struct arrays of roughly equal size.
fn export(df: &DataFrame, n_batches: usize) -> (ArrowSchema, Vec<ArrowArray>) {
    let arrow_schema = df.schema().to_arrow(CompatLevel::newest());
    let fields: Vec<ArrowField> = arrow_schema.iter_values().cloned().collect();
    let dtype = ArrowDataType::Struct(fields);
    let field = ArrowField::new("batch".into(), dtype.clone(), true);

    let per = df.height().div_ceil(n_batches).max(1);
    let mut batches = Vec::new();
    let mut offset = 0;
    while offset < df.height() {
        let part = df.slice(offset as i64, per);
        let arrays: Vec<_> = part
            .columns()
            .iter()
            .map(|c| c.clone().rechunk_to_arrow(CompatLevel::newest()))
            .collect();
        let array = StructArray::try_new(dtype.clone(), part.height(), arrays, None).unwrap();
        batches.push(export_array_to_c(Box::new(array)));
        offset += per;
    }
    (export_field_to_c(&field), batches)
}

fn read_back(path: &std::path::Path) -> DataFrame {
    readstat_scan(path, None, None).unwrap().collect().unwrap()
}

#[test]
fn stata_roundtrip_with_several_batches() {
    let dir = TempDir::new("stata");
    let path = dir.path().join("out.dta");
    let df = sample();
    let (schema, batches) = export(&df, 3);
    let written = write_arrow_ffi(
        &path,
        ArrowWriteFormat::Stata { compress: false },
        ArrowWriteOptions::default(),
        schema,
        batches,
    )
    .unwrap();
    assert_eq!(written, vec![path.clone()]);

    let back = read_back(&path);
    assert_eq!(back.height(), 4);
    assert_eq!(back.get_column_names(), df.get_column_names());
    assert_eq!(back.column("id").unwrap().cast(&DataType::Int64).unwrap().i64().unwrap().to_vec(),
               vec![Some(1), Some(2), Some(3), Some(4)]);
    assert_eq!(back.column("score").unwrap().f64().unwrap().to_vec(),
               vec![Some(1.5), None, Some(3.25), Some(-4.0)]);
    assert_eq!(back.column("name").unwrap().str().unwrap().iter().collect::<Vec<_>>(),
               vec![Some("alpha"), Some("beta"), None, Some("delta")]);
}

#[test]
fn spss_roundtrip_with_variable_labels() {
    let dir = TempDir::new("spss");
    let path = dir.path().join("out.sav");
    let df = sample();
    let (schema, batches) = export(&df, 2);
    let mut labels = HashMap::new();
    labels.insert("score".to_string(), "Test score".to_string());
    write_arrow_ffi(
        &path,
        ArrowWriteFormat::Spss { compressed: true },
        ArrowWriteOptions { variable_labels: labels },
        schema,
        batches,
    )
    .unwrap();

    let back = read_back(&path);
    assert_eq!(back.height(), 4);
    assert_eq!(back.column("score").unwrap().f64().unwrap().to_vec(),
               vec![Some(1.5), None, Some(3.25), Some(-4.0)]);
    assert_eq!(back.column("name").unwrap().str().unwrap().iter().collect::<Vec<_>>(),
               vec![Some("alpha"), Some("beta"), None, Some("delta")]);
}

#[test]
fn sas_csv_bundle_is_written() {
    let dir = TempDir::new("bundle");
    let path = dir.path().join("bundle");
    let df = sample();
    let (schema, batches) = export(&df, 2);
    let written = write_arrow_ffi(
        &path,
        ArrowWriteFormat::SasCsvImport {
            dataset_name: Some("mydata".into()),
            library: None,
            gzip: false,
            delete_csv_on_import: false,
        },
        ArrowWriteOptions::default(),
        schema,
        batches,
    )
    .unwrap();
    assert_eq!(written.len(), 2);
    let csv = std::fs::read_to_string(&written[0]).unwrap();
    let script = std::fs::read_to_string(&written[1]).unwrap();
    assert_eq!(csv.lines().count(), 5, "header plus four rows: {csv}");
    assert!(script.contains("mydata"), "script names the dataset: {script}");
}

#[test]
fn zero_batches_writes_an_empty_table() {
    let dir = TempDir::new("empty");
    let path = dir.path().join("empty.dta");
    let df = sample();
    let (schema, _) = export(&df, 1);
    write_arrow_ffi(
        &path,
        ArrowWriteFormat::Stata { compress: false },
        ArrowWriteOptions::default(),
        schema,
        Vec::new(),
    )
    .unwrap();
    // The file has all four columns and no rows. (Collecting a zero-row scan drops the
    // columns, so check the declared schema rather than the collected frame.)
    let schema = polars_readstat_rs::readstat_schema(&path, None, None).unwrap();
    assert_eq!(schema.len(), 4);
    assert_eq!(read_back(&path).height(), 0);
}
