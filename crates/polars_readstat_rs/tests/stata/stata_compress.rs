use polars::prelude::*;
use polars_readstat_rs::{compress_df, CompressOptions};

#[test]
fn test_compress_stata_bounds() {
    let df = DataFrame::new_infer_height(vec![
        Series::new("i8_ok".into(), &[1i64, 2, 3]).into_column(),
        Series::new("i16_ok".into(), &[100i64, 200, 300]).into_column(),
        Series::new("i32_ok".into(), &[100_000i64, 200_000, 300_000]).into_column(),
        Series::new(
            "i32_over".into(),
            &[2_147_483_621i64, 2_147_483_622, 2_147_483_623],
        )
        .into_column(),
        Series::new(
            "i32_under".into(),
            &[-2_147_483_648i64, -2_147_483_649, -2_147_483_650],
        )
        .into_column(),
        Series::new(
            "u_over".into(),
            &[2_147_483_621u64, 2_147_483_622, 2_147_483_623],
        )
        .into_column(),
        Series::new("f64_int_ok".into(), &[1.0f64, 2.0, 3.0]).into_column(),
        Series::new(
            "f64_int_over".into(),
            &[2_147_483_621.0f64, 2_147_483_622.0, 2_147_483_623.0],
        )
        .into_column(),
        Series::new(
            "f32_int_over".into(),
            &[2_147_483_621.0f32, 2_147_483_622.0, 2_147_483_623.0],
        )
        .into_column(),
        Series::new("f64_non_int".into(), &[1.25f64, 2.5, 3.75]).into_column(),
    ])
    .unwrap();

    let mut opts = CompressOptions::default();
    opts.check_string = false;
    opts.compress_numeric = true;
    let out = compress_df(&df, opts).unwrap();

    assert_eq!(out.column("i8_ok").unwrap().dtype(), &DataType::Int8);
    assert_eq!(out.column("i16_ok").unwrap().dtype(), &DataType::Int16);
    assert_eq!(out.column("i32_ok").unwrap().dtype(), &DataType::Int32);
    assert_eq!(out.column("i32_over").unwrap().dtype(), &DataType::Float64);
    assert_eq!(out.column("i32_under").unwrap().dtype(), &DataType::Float64);
    assert_eq!(out.column("u_over").unwrap().dtype(), &DataType::Float64);
    assert_eq!(out.column("f64_int_ok").unwrap().dtype(), &DataType::Int8);
    assert_eq!(
        out.column("f64_int_over").unwrap().dtype(),
        &DataType::Float64
    );
    assert_eq!(
        out.column("f32_int_over").unwrap().dtype(),
        &DataType::Float64
    );
    assert_eq!(
        out.column("f64_non_int").unwrap().dtype(),
        &DataType::Float64
    );
}

#[test]
fn test_compress_string_to_numeric() {
    let df = DataFrame::new_infer_height(vec![
        Series::new("num_str".into(), &[" 1.0 ", "2", "3.5", ""]).into_column(),
        Series::new("keep_str".into(), &["a", "b", "c", ""]).into_column(),
    ])
    .unwrap();

    let mut opts = CompressOptions::default();
    opts.check_string = true;
    let out = compress_df(&df, opts).unwrap();

    assert_eq!(out.column("num_str").unwrap().dtype(), &DataType::Float64);
    assert_eq!(out.column("keep_str").unwrap().dtype(), &DataType::String);
}

#[test]
fn test_compress_string_non_numeric_stays_string() {
    let df = DataFrame::new_infer_height(vec![
        Series::new("mixed".into(), &["1", "2", "x", "4"]).into_column()
    ])
    .unwrap();

    let mut opts = CompressOptions::default();
    opts.check_string = true;
    let out = compress_df(&df, opts).unwrap();

    assert_eq!(out.column("mixed").unwrap().dtype(), &DataType::String);
}
