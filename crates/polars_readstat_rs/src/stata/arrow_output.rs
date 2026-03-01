use crate::stata::polars_output::{scan_dta, stata_batch_iter};
use polars::prelude::*;
use polars_arrow::array::StructArray;
use polars_arrow::datatypes::{ArrowDataType, Field as ArrowField};
use polars_arrow::ffi::{
    export_array_to_c, export_field_to_c, export_iterator, ArrowArray, ArrowArrayStream,
    ArrowSchema,
};
use std::path::Path;

fn build_struct_field(schema: &SchemaRef) -> ArrowField {
    let arrow_schema = schema.to_arrow(CompatLevel::newest());
    let arrow_fields: Vec<ArrowField> = arrow_schema.iter_values().cloned().collect();
    let struct_dtype = ArrowDataType::Struct(arrow_fields);
    ArrowField::new("stata_data".into(), struct_dtype, true)
}

fn df_to_struct_array(df: &DataFrame, field: &ArrowField) -> PolarsResult<StructArray> {
    let struct_dtype = field.dtype().clone();
    let len = df.height();
    let arrays: Vec<Box<dyn polars_arrow::array::Array>> = df
        .columns()
        .iter()
        .map(|col| col.clone().rechunk_to_arrow(CompatLevel::newest()))
        .collect();
    Ok(StructArray::new(struct_dtype, len, arrays, None))
}

pub fn read_to_arrow_schema_ffi(
    path: &Path,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: Option<bool>,
    chunk_size: Option<usize>,
) -> PolarsResult<*mut ArrowSchema> {
    let opts = crate::ScanOptions {
        threads,
        chunk_size,
        missing_string_as_null: Some(missing_string_as_null),
        informative_nulls: None,
        value_labels_as_strings,
        preserve_order: Some(true),
        compress_opts: crate::CompressOptionsLite::default(),
    };
    let schema = scan_dta(path.to_path_buf(), opts)?.collect_schema()?;
    let field = build_struct_field(&schema);
    Ok(Box::into_raw(Box::new(export_field_to_c(&field))))
}

/// Returns a pair of raw pointers: (ArrowSchema, ArrowArray)
pub fn read_to_arrow_array_ffi(
    path: &Path,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: Option<bool>,
    chunk_size: Option<usize>,
) -> PolarsResult<(*mut ArrowSchema, *mut ArrowArray)> {
    let opts = crate::ScanOptions {
        threads,
        chunk_size,
        missing_string_as_null: Some(missing_string_as_null),
        informative_nulls: None,
        value_labels_as_strings,
        preserve_order: Some(true),
        compress_opts: crate::CompressOptionsLite::default(),
    };
    let df = scan_dta(path.to_path_buf(), opts)?.collect()?;
    let field = build_struct_field(&df.schema());
    let struct_array = df_to_struct_array(&df, &field)?;
    let c_schema = Box::into_raw(Box::new(export_field_to_c(&field)));
    let c_array = Box::into_raw(Box::new(export_array_to_c(Box::new(struct_array))));
    Ok((c_schema, c_array))
}

/// Returns a C ArrowArrayStream pointer for batch iteration.
pub fn read_to_arrow_stream_ffi(
    path: &Path,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: Option<bool>,
    chunk_size: Option<usize>,
    batch_size: usize,
) -> PolarsResult<*mut ArrowArrayStream> {
    let opts = crate::ScanOptions {
        threads,
        chunk_size,
        missing_string_as_null: Some(missing_string_as_null),
        informative_nulls: None,
        value_labels_as_strings,
        preserve_order: Some(true),
        compress_opts: crate::CompressOptionsLite::default(),
    };
    let mut lf = scan_dta(path.to_path_buf(), opts.clone())?;
    let schema = lf.collect_schema()?;
    let field = build_struct_field(&schema);
    let field_for_iter = field.clone();

    let mut iter = stata_batch_iter(
        path.to_path_buf(),
        opts.threads,
        missing_string_as_null,
        value_labels_as_strings.unwrap_or(true),
        Some(batch_size),
        true,
        None,
        0,
        None,
        None,
    )?;
    let iter = Box::new(std::iter::from_fn(move || {
        let next = iter.next()?;
        match next {
            Ok(df) => match df_to_struct_array(&df, &field_for_iter) {
                Ok(array) => Some(Ok(Box::new(array) as Box<dyn polars_arrow::array::Array>)),
                Err(e) => Some(Err(e)),
            },
            Err(e) => Some(Err(e)),
        }
    }));

    let stream = export_iterator(iter, field);
    Ok(Box::into_raw(Box::new(stream)))
}

/// Backwards-compatible: single full array export with defaults.
pub fn read_to_arrow_ffi(path: &Path) -> PolarsResult<(*mut ArrowSchema, *mut ArrowArray)> {
    read_to_arrow_array_ffi(path, None, true, Some(false), None)
}
