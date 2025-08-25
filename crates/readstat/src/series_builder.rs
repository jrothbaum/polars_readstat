use polars::prelude::*;


pub enum SeriesBuilder {
    String {
        name: PlSmallStr,
        data: Vec<Option<String>>,
    },
    I8 {
        name: PlSmallStr,
        data: Vec<Option<i8>>,
    },
    I16 {
        name: PlSmallStr,
        data: Vec<Option<i16>>,
    },
    I32 {
        name: PlSmallStr,
        data: Vec<Option<i32>>,
    },
    I64 {
        name: PlSmallStr,
        data: Vec<Option<i64>>,
    },
    F32 {
        name: PlSmallStr,
        data: Vec<Option<f32>>,
    },
    F64 {
        name: PlSmallStr,
        data: Vec<Option<f64>>,
    },
    Date {
        name: PlSmallStr,
        data: Vec<Option<i32>>,
    },
    Time {
        name: PlSmallStr,
        data: Vec<Option<i64>>,
    },
    DateTime {
        name: PlSmallStr,
        data: Vec<Option<i64>>,
    },
}

// Macro to generate all setter methods succinctly
macro_rules! impl_series_setters {
    ($(($variant:ident, $type:ty, $method:ident)),* $(,)?) => {
        impl SeriesBuilder {
            $(
                pub fn $method(&mut self, index: usize, value: $type) {
                    if let Self::$variant { data, .. } = self {
                        data[index] = Some(value);
                    }
                }
            )*
        }
    };
}

// Generate all setter methods with a single macro call
impl_series_setters!(
    (String, String, set_string_at_index),
    (I8, i8, set_i8_at_index),
    (I16, i16, set_i16_at_index),
    (I32, i32, set_i32_at_index),
    (I64, i64, set_i64_at_index),
    (F32, f32, set_f32_at_index),
    (F64, f64, set_f64_at_index),
    (Date, i32, set_date_at_index),
    (Time, i64, set_time_at_index),
    (DateTime, i64, set_datetime_at_index),
);

impl SeriesBuilder {
    pub fn new_with_nulls(name: PlSmallStr, dtype: &DataType, capacity: usize) -> Self {
        match dtype {
            DataType::String => Self::String {
                name,
                data: vec![None; capacity], // Pre-allocate with nulls (your preference)
            },
            DataType::Int8 => Self::I8 {
                name,
                data: vec![None; capacity],
            },
            DataType::Int16 => Self::I16 {
                name,
                data: vec![None; capacity],
            },
            DataType::Int32 => Self::I32 {
                name,
                data: vec![None; capacity],
            },
            DataType::Int64 => Self::I64 {
                name,
                data: vec![None; capacity],
            },
            DataType::Float32 => Self::F32 {
                name,
                data: vec![None; capacity],
            },
            DataType::Float64 => Self::F64 {
                name,
                data: vec![None; capacity],
            },
            DataType::Date => Self::Date {
                name,
                data: vec![None; capacity],
            },
            DataType::Time => Self::Time {
                name,
                data: vec![None; capacity],
            },
            DataType::Datetime(_, _) => Self::DateTime {
                name,
                data: vec![None; capacity],
            },
            _ => Self::String {
                name,
                data: vec![None; capacity],
            },
        }
    }

    // Convert to final Series (consuming the builder)
    pub fn into_series(self) -> PolarsResult<Series> {
        match self {
            Self::String { name, data } => {
                Ok(Series::new(name, data))
            },
            Self::I8 { name, data } => {
                // Convert i8 to i32 for Polars compatibility
                let converted_data: Vec<Option<i32>> = data.into_iter()
                    .map(|opt| opt.map(|v| v as i32))
                    .collect();
                let series = Series::new(name, converted_data);
                series.cast(&DataType::Int8)
            },
            Self::I16 { name, data } => {
                // Convert i16 to i32 for Polars compatibility
                let converted_data: Vec<Option<i32>> = data.into_iter()
                    .map(|opt| opt.map(|v| v as i32))
                    .collect();
                let series = Series::new(name, converted_data);
                series.cast(&DataType::Int16)
            },
            Self::I32 { name, data } => {
                Ok(Series::new(name, data))
            },
            Self::I64 { name, data } => {
                Ok(Series::new(name, data))
            },
            Self::F32 { name, data } => {
                Ok(Series::new(name, data))
            },
            Self::F64 { name, data } => {
                Ok(Series::new(name, data))
            },
            Self::Date { name, data } => {
                let series = Series::new(name, data);
                series.cast(&DataType::Date)
            },
            Self::Time { name, data } => {
                let series = Series::new(name, data);
                series.cast(&DataType::Time)
            },
            Self::DateTime { name, data } => {
                let series = Series::new(name, data);
                series.cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
            },
        }
    }
}