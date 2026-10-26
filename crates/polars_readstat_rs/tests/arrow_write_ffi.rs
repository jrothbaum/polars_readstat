//! Writing through the Arrow C Data Interface: a table exported as Arrow batches
//! is written as Stata / SPSS / SAS-CSV and read back.

use polars::prelude::*;
use polars_arrow::array::StructArray;
use polars_arrow::datatypes::{ArrowDataType, Field as ArrowField};
use polars_arrow::ffi::{export_array_to_c, export_field_to_c, ArrowArray, ArrowSchema};
use polars_readstat_rs::arrow_write::{write_arrow_ffi, ArrowWriteFormat, ArrowWriteOptions};
use polars_readstat_rs::{readstat_scan, scan_por, ScanOptions};
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

#[test]
fn xpt_roundtrip_both_versions() {
    for version in [5u8, 8u8] {
        let dir = TempDir::new(&format!("xpt{version}"));
        let path = dir.path().join("out.xpt");
        let df = sample();
        let (schema, batches) = export(&df, 2);
        let mut labels = HashMap::new();
        labels.insert("score".to_string(), "Test score".to_string());
        write_arrow_ffi(
            &path,
            ArrowWriteFormat::Xpt { version, table_name: Some("MYDATA".into()), file_label: Some("A file".into()) },
            ArrowWriteOptions { variable_labels: labels },
            schema,
            batches,
        )
        .unwrap();

        let back = read_back(&path);
        assert_eq!(back.height(), 4, "version {version}");
        let score = back.column("score").unwrap().f64().unwrap().to_vec();
        assert_eq!(score, vec![Some(1.5), None, Some(3.25), Some(-4.0)], "version {version}");
        let name = back.column("name").unwrap().str().unwrap().iter().collect::<Vec<_>>();
        assert_eq!(name, vec![Some("alpha"), Some("beta"), None, Some("delta")], "version {version}");
    }
}

#[test]
fn xpt_rejects_an_unknown_version() {
    let dir = TempDir::new("xptbad");
    let df = sample();
    let (schema, batches) = export(&df, 1);
    let result = write_arrow_ffi(
        &dir.path().join("out.xpt"),
        ArrowWriteFormat::Xpt { version: 6, table_name: None, file_label: None },
        ArrowWriteOptions::default(),
        schema,
        batches,
    );
    assert!(result.is_err());
}

#[test]
fn por_roundtrip() {
    let dir = TempDir::new("por");
    let path = dir.path().join("out.por");
    let df = sample();
    let (schema, batches) = export(&df, 2);
    write_arrow_ffi(
        &path,
        ArrowWriteFormat::Por { file_label: Some("A file".into()) },
        ArrowWriteOptions::default(),
        schema,
        batches,
    )
    .unwrap();

    let back = scan_por(&path, ScanOptions::default()).unwrap().collect().unwrap();
    assert_eq!(back.height(), 4);
    // POR names are upper case
    assert_eq!(back.column("SCORE").unwrap().f64().unwrap().to_vec(),
               vec![Some(1.5), None, Some(3.25), Some(-4.0)]);
    // a missing string is written and read back as an empty string
    assert_eq!(back.column("NAME").unwrap().str().unwrap().iter().collect::<Vec<_>>(),
               vec![Some("alpha"), Some("beta"), Some(""), Some("delta")]);
}

fn one_column(name: &str, values: Vec<Option<&str>>) -> DataFrame {
    df!(name => values).unwrap()
}

#[test]
fn por_keeps_very_large_and_small_numbers() {
    let dir = TempDir::new("porbig");
    let path = dir.path().join("big.por");
    let values = vec![1.7498005798264095e100, -2e307, 1e17, 1e18, 3e17, 5.5e19, 123.456, 0.5, -7.0, 1e-5];
    let df = df!("x" => values.clone()).unwrap();
    let (schema, batches) = export(&df, 1);
    write_arrow_ffi(&path, ArrowWriteFormat::Por { file_label: None }, ArrowWriteOptions::default(), schema, batches).unwrap();
    let back = scan_por(&path, ScanOptions::default()).unwrap().collect().unwrap();
    let got = back.column("X").unwrap().f64().unwrap().to_vec();
    for (want, got) in values.iter().zip(got) {
        let got = got.expect("not missing");
        assert!((got - want).abs() <= 1e-9 * want.abs().max(1e-300), "{want} came back as {got}");
    }
}

#[test]
fn por_all_empty_text_column_stays_text() {
    let dir = TempDir::new("porempty");
    let path = dir.path().join("empty_text.por");
    let df = one_column("T", vec![Some(""), Some(""), None]);
    let (schema, batches) = export(&df, 1);
    write_arrow_ffi(&path, ArrowWriteFormat::Por { file_label: None }, ArrowWriteOptions::default(), schema, batches).unwrap();
    let back = scan_por(&path, ScanOptions::default()).unwrap().collect().unwrap();
    assert_eq!(back.column("T").unwrap().dtype(), &DataType::String);
    assert_eq!(back.height(), 3);
}

#[test]
fn por_refuses_characters_it_cannot_store() {
    let dir = TempDir::new("pornonascii");
    for text in ["caf\u{e9}", "line\nbreak", "\u{6211}"] {
        let path = dir.path().join("t.por");
        let df = one_column("T", vec![Some("fine"), Some(text)]);
        let (schema, batches) = export(&df, 1);
        let result = write_arrow_ffi(&path, ArrowWriteFormat::Por { file_label: None }, ArrowWriteOptions::default(), schema, batches);
        let message = result.expect_err("must be refused").to_string();
        assert!(message.contains("cannot be stored in an SPSS portable file"), "{message}");
    }
}

#[test]
fn xpt_refuses_a_blank_last_row_in_an_all_text_table() {
    let dir = TempDir::new("xptblank");
    let path = dir.path().join("t.xpt");
    let df = df!("a" => ["x", "y", ""], "b" => ["p", "q", ""]).unwrap();
    let (schema, batches) = export(&df, 1);
    let message = write_arrow_ffi(
        &path, ArrowWriteFormat::Xpt { version: 8, table_name: None, file_label: None },
        ArrowWriteOptions::default(), schema, batches,
    )
    .expect_err("must be refused")
    .to_string();
    assert!(message.contains("would be read back as padding"), "{message}");

    // a numeric column makes the row distinguishable, and a blank row in the middle is fine
    for df in [
        df!("a" => ["x", "y", ""], "n" => [Some(1.0), Some(2.0), None]).unwrap(),
        df!("a" => ["x", "", "z"], "b" => ["p", "", "r"]).unwrap(),
    ] {
        let (schema, batches) = export(&df, 1);
        write_arrow_ffi(
            &path, ArrowWriteFormat::Xpt { version: 8, table_name: None, file_label: None },
            ArrowWriteOptions::default(), schema, batches,
        )
        .unwrap();
        assert_eq!(read_back(&path).height(), 3);
    }
}

#[test]
fn por_date_times_in_every_unit() {
    // 2018-03-17 12:34:56 UTC in milliseconds, microseconds and nanoseconds
    let ms: i64 = 1_521_290_096_000;
    for (unit, scale) in [(TimeUnit::Milliseconds, 1i64), (TimeUnit::Microseconds, 1_000), (TimeUnit::Nanoseconds, 1_000_000)] {
        let dir = TempDir::new("pordt");
        let path = dir.path().join("dt.por");
        let col = Series::new("T".into(), [ms * scale]).cast(&DataType::Datetime(unit, None)).unwrap();
        let df = DataFrame::new(1, vec![col.into_column()]).unwrap();
        let (schema, batches) = export(&df, 1);
        write_arrow_ffi(&path, ArrowWriteFormat::Por { file_label: None }, ArrowWriteOptions::default(), schema, batches).unwrap();
        let back = scan_por(&path, ScanOptions::default()).unwrap().collect().unwrap();
        let got = back.column("T").unwrap().cast(&DataType::Int64).unwrap().i64().unwrap().get(0).unwrap();
        assert_eq!(got, ms, "{unit:?}: stored {got} ms, expected {ms} ms");
    }
}

#[test]
fn xpt_refuses_numbers_outside_its_range() {
    let dir = TempDir::new("xptrange");
    for bad in [1.7e100, -2e307, 7.3e75] {
        let path = dir.path().join("t.xpt");
        let df = df!("x" => [1.0, bad]).unwrap();
        let (schema, batches) = export(&df, 1);
        let message = write_arrow_ffi(
            &path, ArrowWriteFormat::Xpt { version: 8, table_name: None, file_label: None },
            ArrowWriteOptions::default(), schema, batches,
        )
        .expect_err("must be refused")
        .to_string();
        assert!(message.contains("outside the range an XPT file can hold"), "{message}");
    }
    // the largest values it can hold are fine, and tiny ones are written as zero
    let path = dir.path().join("ok.xpt");
    let df = df!("x" => [7.0e75, -7.0e75, 1e-300, 0.0, 123.456]).unwrap();
    let (schema, batches) = export(&df, 1);
    write_arrow_ffi(
        &path, ArrowWriteFormat::Xpt { version: 8, table_name: None, file_label: None },
        ArrowWriteOptions::default(), schema, batches,
    )
    .unwrap();
    assert_eq!(read_back(&path).height(), 5);
}

