use crate::source::ReadSource;
use crate::spss::polars_output::{
    scan_sav, schema_from_spss_metadata, spss_batch_iter, spss_batch_iter_with_reader,
};
use crate::spss::reader::SpssReader;
use polars::prelude::*;
use polars_arrow::array::StructArray;
use polars_arrow::datatypes::{ArrowDataType, Field as ArrowField};
use polars_arrow::ffi::{
    export_array_to_c, export_field_to_c, export_iterator, ArrowArray, ArrowArrayStream,
    ArrowSchema,
};
use std::path::Path;
use std::sync::Arc;

fn build_struct_field(schema: &SchemaRef) -> ArrowField {
    let arrow_schema = schema.to_arrow(CompatLevel::newest());
    let arrow_fields: Vec<ArrowField> = arrow_schema.iter_values().cloned().collect();
    let struct_dtype = ArrowDataType::Struct(arrow_fields);
    ArrowField::new("spss_data".into(), struct_dtype, true)
}

fn df_to_struct_array(df: &DataFrame, field: &ArrowField) -> PolarsResult<StructArray> {
    let struct_dtype = field.dtype().clone();
    let len = df.height();
    let ArrowDataType::Struct(fields) = &struct_dtype else {
        return Err(PolarsError::ComputeError(
            "expected a struct field for the batch schema".into(),
        ));
    };
    // Match columns to the schema by name rather than position: a row-index
    // column is added at the front of each batch but declared last in the schema.
    let arrays = fields
        .iter()
        .map(|f| {
            df.column(f.name.as_str())
                .map(|col| col.clone().rechunk_to_arrow(CompatLevel::newest()))
        })
        .collect::<PolarsResult<Vec<Box<dyn polars_arrow::array::Array>>>>()?;
    StructArray::try_new(struct_dtype, len, arrays, None)
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
        row_index_name: None,
        compress_opts: crate::CompressOptionsLite::default(),
    };
    let schema = scan_sav(path.to_path_buf(), opts)?.collect_schema()?;
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
        row_index_name: None,
        compress_opts: crate::CompressOptionsLite::default(),
    };
    let df = scan_sav(path.to_path_buf(), opts)?.collect()?;
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
    offset: usize,
    n_rows: Option<usize>,
) -> PolarsResult<*mut ArrowArrayStream> {
    let opts = crate::ScanOptions {
        threads,
        chunk_size,
        missing_string_as_null: Some(missing_string_as_null),
        informative_nulls: None,
        value_labels_as_strings,
        preserve_order: Some(true),
        row_index_name: None,
        compress_opts: crate::CompressOptionsLite::default(),
    };
    let mut lf = scan_sav(path.to_path_buf(), opts.clone())?;
    let schema = lf.collect_schema()?;
    let field = build_struct_field(&schema);
    let field_for_iter = field.clone();

    let mut iter = spss_batch_iter(
        path.to_path_buf(),
        opts.threads,
        missing_string_as_null,
        value_labels_as_strings.unwrap_or(true),
        chunk_size,
        true,
        None,
        None,
        offset,
        n_rows,
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
    read_to_arrow_array_ffi(path, None, true, Some(true), None)
}

/// Same as [`read_to_arrow_schema_ffi`], but reads through a caller-supplied
/// [`ReadSource`] instead of opening `path` directly.
pub fn read_to_arrow_schema_ffi_from_source(
    source: Arc<dyn ReadSource>,
    value_labels_as_strings: Option<bool>,
    row_index_name: Option<&str>,
) -> PolarsResult<*mut ArrowSchema> {
    let reader = SpssReader::open_source(source)
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let schema = schema_from_spss_metadata(
        reader.metadata(),
        value_labels_as_strings.unwrap_or(true),
        None,
        row_index_name,
    )?;
    let field = build_struct_field(&Arc::new(schema));
    Ok(Box::into_raw(Box::new(export_field_to_c(&field))))
}

/// Same as [`read_to_arrow_stream_ffi`], but reads through a caller-supplied
/// [`ReadSource`] instead of opening `path` directly.
pub fn read_to_arrow_stream_ffi_from_source(
    source: Arc<dyn ReadSource>,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: Option<bool>,
    chunk_size: Option<usize>,
    offset: usize,
    n_rows: Option<usize>,
    preserve_order: bool,
    row_index_name: Option<String>,
    columns: Option<Vec<usize>>,
) -> PolarsResult<*mut ArrowArrayStream> {
    let reader = SpssReader::open_source(source.clone())
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let value_labels_as_strings = value_labels_as_strings.unwrap_or(true);
    let full_schema = schema_from_spss_metadata(reader.metadata(), value_labels_as_strings, None, row_index_name.as_deref())?;
    let subset = crate::resolve_column_subset(full_schema, columns.as_deref(), row_index_name.is_some())?;
    let schema = subset.schema.clone();
    let field = build_struct_field(&Arc::new(schema));
    let field_for_iter = field.clone();

    let mut iter = spss_batch_iter_with_reader(
        &reader,
        source,
        threads,
        missing_string_as_null,
        value_labels_as_strings,
        chunk_size,
        preserve_order,
        if subset.want_row_index { row_index_name } else { None },
        subset.decode_names.clone(),
        offset,
        n_rows,
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

fn por_error(e: impl std::fmt::Display) -> PolarsError {
    PolarsError::ComputeError(e.to_string().into())
}

/// Schema of an SPSS portable (`.por`) file, read through a caller-supplied [`ReadSource`].
pub fn read_por_to_arrow_schema_ffi_from_source(
    source: Arc<dyn ReadSource>,
    row_index_name: Option<&str>,
) -> PolarsResult<*mut ArrowSchema> {
    let meta = crate::spss::por::metadata_por_from_source(source.as_ref()).map_err(por_error)?;
    let mut schema = crate::spss::por::schema_from_por_metadata(&meta);
    if let Some(name) = row_index_name {
        schema = crate::append_row_index_schema(schema, name)?;
    }
    let field = build_struct_field(&Arc::new(schema));
    Ok(Box::into_raw(Box::new(export_field_to_c(&field))))
}

/// Stream of an SPSS portable (`.por`) file, read through a caller-supplied [`ReadSource`].
///
/// A `.por` file is read in one sequential pass into a table, which is then handed out in
/// batches of `chunk_size` rows (default 65,536), so memory is the size of the table.
/// `columns` and `row_index_name` behave as for [`read_to_arrow_stream_ffi_from_source`];
/// order is always the file's order.
pub fn read_por_to_arrow_stream_ffi_from_source(
    source: Arc<dyn ReadSource>,
    missing_string_as_null: bool,
    chunk_size: Option<usize>,
    offset: usize,
    n_rows: Option<usize>,
    row_index_name: Option<String>,
    columns: Option<Vec<usize>>,
) -> PolarsResult<*mut ArrowArrayStream> {
    let (meta, mut df) = crate::spss::por::read_por_from_source(source.as_ref()).map_err(por_error)?;

    let mut full_schema = crate::spss::por::schema_from_por_metadata(&meta);
    if let Some(name) = row_index_name.as_deref() {
        full_schema = crate::append_row_index_schema(full_schema, name)?;
    }
    let subset = crate::resolve_column_subset(full_schema, columns.as_deref(), row_index_name.is_some())?;

    if offset > 0 || n_rows.is_some() {
        df = df.slice(offset as i64, n_rows.unwrap_or(usize::MAX));
    }
    if missing_string_as_null {
        let names: Vec<PlSmallStr> = df.get_column_names().into_iter().cloned().collect();
        for name in names {
            if let Ok(strings) = df.column(name.as_str())?.str() {
                let blanked: StringChunked = strings
                    .iter()
                    .map(|v| v.filter(|s| !s.is_empty()))
                    .collect_ca(name.clone());
                df.with_column(blanked.into_column())?;
            }
        }
    }
    if let (true, Some(name)) = (subset.want_row_index, row_index_name.as_deref()) {
        df = crate::append_row_index(df, name, offset)?;
    }
    if let Some(names) = &subset.decode_names {
        let mut keep: Vec<&str> = names.iter().map(String::as_str).collect();
        if let (true, Some(name)) = (subset.want_row_index, row_index_name.as_deref()) {
            keep.push(name);
        }
        df = df.select(keep)?;
    }

    let field = build_struct_field(&Arc::new(subset.schema.clone()));
    let field_for_iter = field.clone();
    let batch_rows = chunk_size.unwrap_or(65_536).max(1);
    let total = df.height();
    let mut start = 0usize;
    let iter = Box::new(std::iter::from_fn(move || {
        if start >= total {
            return None;
        }
        let batch = df.slice(start as i64, batch_rows);
        start += batch_rows;
        match df_to_struct_array(&batch, &field_for_iter) {
            Ok(array) => Some(Ok(Box::new(array) as Box<dyn polars_arrow::array::Array>)),
            Err(e) => Some(Err(e)),
        }
    }));

    let stream = export_iterator(iter, field);
    Ok(Box::into_raw(Box::new(stream)))
}
