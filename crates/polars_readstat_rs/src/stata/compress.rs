use crate::stata::error::{Error, Result};
use polars::prelude::*;
use std::collections::HashSet;

// Stata DTA 113+ bounds (reserves sentinel values for missing data)
const DTA_113_MAX_INT8: i8 = 0x64; // 100
const DTA_113_MAX_INT16: i16 = 0x7fe4; // 32740
const DTA_113_MAX_INT32: i32 = 0x7fffffe4; // 2147483620
const DTA_113_MIN_INT8: i8 = -0x7f; // -127
const DTA_113_MIN_INT16: i16 = -0x7fff; // -32767
const DTA_113_MIN_INT32: i32 = -0x7fffffff; // -2147483647

// Standard integer bounds (full range)
const STD_MAX_INT8: i8 = i8::MAX; // 127
const STD_MAX_INT16: i16 = i16::MAX; // 32767
const STD_MAX_INT32: i32 = i32::MAX; // 2147483647
const STD_MIN_INT8: i8 = i8::MIN; // -128
const STD_MIN_INT16: i16 = i16::MIN; // -32768
const STD_MIN_INT32: i32 = i32::MIN; // -2147483648

#[derive(Debug, Clone)]
pub struct IntBounds {
    pub min_i8: i64,
    pub max_i8: i64,
    pub min_i16: i64,
    pub max_i16: i64,
    pub min_i32: i64,
    pub max_i32: i64,
}

impl IntBounds {
    pub fn stata() -> Self {
        Self {
            min_i8: DTA_113_MIN_INT8 as i64,
            max_i8: DTA_113_MAX_INT8 as i64,
            min_i16: DTA_113_MIN_INT16 as i64,
            max_i16: DTA_113_MAX_INT16 as i64,
            min_i32: DTA_113_MIN_INT32 as i64,
            max_i32: DTA_113_MAX_INT32 as i64,
        }
    }

    pub fn standard() -> Self {
        Self {
            min_i8: STD_MIN_INT8 as i64,
            max_i8: STD_MAX_INT8 as i64,
            min_i16: STD_MIN_INT16 as i64,
            max_i16: STD_MAX_INT16 as i64,
            min_i32: STD_MIN_INT32 as i64,
            max_i32: STD_MAX_INT32 as i64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompressOptions {
    pub cols: Option<Vec<String>>,
    pub compress_numeric: bool,
    pub check_string: bool,
    pub check_string_only: bool,
    pub cast_all_null_to_boolean: bool,
    pub check_date_time: bool,
    pub no_boolean: bool,
    pub use_stata_bounds: bool,
}

impl Default for CompressOptions {
    fn default() -> Self {
        Self {
            cols: None,
            compress_numeric: true,
            check_string: false,
            check_string_only: false,
            cast_all_null_to_boolean: true,
            check_date_time: true,
            no_boolean: false,
            use_stata_bounds: true,
        }
    }
}

pub fn compress_df(df: &DataFrame, opts: CompressOptions) -> Result<DataFrame> {
    let target_cols = match &opts.cols {
        Some(cols) => cols.iter().cloned().collect::<HashSet<_>>(),
        None => df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect(),
    };

    let mut out_cols: Vec<Column> = Vec::with_capacity(df.width());
    for col in df.columns() {
        let series = col.as_materialized_series();
        let name = series.name().to_string();
        if !target_cols.contains(&name) {
            out_cols.push(col.clone());
            continue;
        }

        let mut s = series.clone();

        if opts.check_date_time {
            if let DataType::Datetime(unit, _) = s.dtype() {
                if is_all_midnight(&s, *unit)? {
                    s = s.cast(&DataType::Date).map_err(Error::Polars)?;
                }
            }
        }

        if opts.check_string && matches!(s.dtype(), DataType::String) {
            if let Some(casted) = try_parse_string_to_f64(&s)? {
                s = casted;
            }
        }

        if opts.check_string_only {
            out_cols.push(s.into_column());
            continue;
        }

        if s.len() > 0 && s.null_count() == s.len() && opts.cast_all_null_to_boolean {
            let casted = s.cast(&DataType::Boolean).map_err(Error::Polars)?;
            out_cols.push(casted.into_column());
            continue;
        }

        if opts.compress_numeric {
            let bounds = if opts.use_stata_bounds {
                IntBounds::stata()
            } else {
                IntBounds::standard()
            };
            let compressed = compress_numeric_series(&s, opts.no_boolean, &bounds)?;
            out_cols.push(compressed.into_column());
        } else {
            out_cols.push(s.into_column());
        }
    }

    DataFrame::new_infer_height(out_cols).map_err(Error::Polars)
}

fn compress_numeric_series(
    series: &Series,
    no_boolean: bool,
    bounds: &IntBounds,
) -> Result<Series> {
    match series.dtype() {
        DataType::Float64 | DataType::Float32 => {
            if all_integers(series)? {
                let (min, max) = min_max_f64(series)?;
                if let Some(min) = min {
                    let max = max.unwrap_or(min);
                    if !no_boolean && min >= 0.0 && max <= 1.0 {
                        return series.cast(&DataType::Boolean).map_err(Error::Polars);
                    }
                    if min >= bounds.min_i8 as f64 && max <= bounds.max_i8 as f64 {
                        return series.cast(&DataType::Int8).map_err(Error::Polars);
                    }
                    if min >= bounds.min_i16 as f64 && max <= bounds.max_i16 as f64 {
                        return series.cast(&DataType::Int16).map_err(Error::Polars);
                    }
                    if min >= bounds.min_i32 as f64 && max <= bounds.max_i32 as f64 {
                        return series.cast(&DataType::Int32).map_err(Error::Polars);
                    }
                    return series.cast(&DataType::Float64).map_err(Error::Polars);
                }
            }
            Ok(series.clone())
        }
        DataType::Int64
        | DataType::Int32
        | DataType::Int16
        | DataType::Int8
        | DataType::UInt64
        | DataType::UInt32
        | DataType::UInt16
        | DataType::UInt8
        | DataType::Boolean => {
            let (min, max) = min_max_i64(series)?;
            if let Some(min) = min {
                let max = max.unwrap_or(min);
                if !no_boolean && min >= 0 && max <= 1 {
                    return series.cast(&DataType::Boolean).map_err(Error::Polars);
                }
                if min >= bounds.min_i8 && max <= bounds.max_i8 {
                    return series.cast(&DataType::Int8).map_err(Error::Polars);
                }
                if min >= bounds.min_i16 && max <= bounds.max_i16 {
                    return series.cast(&DataType::Int16).map_err(Error::Polars);
                }
                if min >= bounds.min_i32 && max <= bounds.max_i32 {
                    return series.cast(&DataType::Int32).map_err(Error::Polars);
                }
                return series.cast(&DataType::Float64).map_err(Error::Polars);
            }
            Ok(series.clone())
        }
        _ => Ok(series.clone()),
    }
}

fn try_parse_string_to_f64(series: &Series) -> Result<Option<Series>> {
    let utf8 = series.str().map_err(Error::Polars)?;
    let mut values: Vec<Option<f64>> = Vec::with_capacity(series.len());
    for opt in utf8.into_iter() {
        match opt {
            None => values.push(None),
            Some(s) => {
                let t = s.trim();
                if t.is_empty() {
                    values.push(None);
                } else if let Ok(v) = t.parse::<f64>() {
                    values.push(Some(v));
                } else {
                    return Ok(None);
                }
            }
        }
    }
    let mut out = Float64Chunked::from_iter(values).into_series();
    out.rename(series.name().clone());
    Ok(Some(out))
}

fn all_integers(series: &Series) -> Result<bool> {
    match series.dtype() {
        DataType::Float32 => {
            let ca = series.f32().map_err(Error::Polars)?;
            for v in ca.into_iter() {
                if let Some(v) = v {
                    if v.fract() != 0.0 {
                        return Ok(false);
                    }
                }
            }
            Ok(true)
        }
        DataType::Float64 => {
            let ca = series.f64().map_err(Error::Polars)?;
            for v in ca.into_iter() {
                if let Some(v) = v {
                    if v.fract() != 0.0 {
                        return Ok(false);
                    }
                }
            }
            Ok(true)
        }
        _ => Ok(true),
    }
}

fn min_max_f64(series: &Series) -> Result<(Option<f64>, Option<f64>)> {
    let mut min = f64::MAX;
    let mut max = f64::MIN;
    let mut any = false;
    match series.dtype() {
        DataType::Float32 => {
            let ca = series.f32().map_err(Error::Polars)?;
            for v in ca.into_iter() {
                if let Some(v) = v {
                    let v = v as f64;
                    any = true;
                    if v < min {
                        min = v;
                    }
                    if v > max {
                        max = v;
                    }
                }
            }
        }
        _ => {
            let ca = series.f64().map_err(Error::Polars)?;
            for v in ca.into_iter() {
                if let Some(v) = v {
                    any = true;
                    if v < min {
                        min = v;
                    }
                    if v > max {
                        max = v;
                    }
                }
            }
        }
    }
    if any {
        Ok((Some(min), Some(max)))
    } else {
        Ok((None, None))
    }
}

fn min_max_i64(series: &Series) -> Result<(Option<i64>, Option<i64>)> {
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    let mut any = false;
    for i in 0..series.len() {
        let value = series.get(i)?;
        if matches!(value, AnyValue::Null) {
            continue;
        }
        let v = match value {
            AnyValue::Int8(v) => v as i64,
            AnyValue::Int16(v) => v as i64,
            AnyValue::Int32(v) => v as i64,
            AnyValue::Int64(v) => v,
            AnyValue::UInt8(v) => v as i64,
            AnyValue::UInt16(v) => v as i64,
            AnyValue::UInt32(v) => v as i64,
            AnyValue::UInt64(v) if v <= i64::MAX as u64 => v as i64,
            AnyValue::Boolean(v) => {
                if v {
                    1
                } else {
                    0
                }
            }
            _ => continue,
        };
        any = true;
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
    }
    if any {
        Ok((Some(min), Some(max)))
    } else {
        Ok((None, None))
    }
}

fn is_all_midnight(series: &Series, unit: TimeUnit) -> Result<bool> {
    let day_ms = 86_400_000i64;
    let ca = series.datetime().map_err(Error::Polars)?;
    for v in ca.phys.iter() {
        if let Some(v) = v {
            let millis = match unit {
                TimeUnit::Milliseconds => v,
                TimeUnit::Microseconds => v / 1_000,
                TimeUnit::Nanoseconds => v / 1_000_000,
            };
            if millis % day_ms != 0 {
                return Ok(false);
            }
        }
    }
    Ok(true)
}
