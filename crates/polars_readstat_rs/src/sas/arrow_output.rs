use crate::polars_output::{
    sas_batch_iter, sas_batch_iter_with_reader, scan_sas7bdat, schema_from_sas_metadata,
};
use crate::reader::Sas7bdatReader;
use crate::source::ReadSource;
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
    ArrowField::new("sas_data".into(), struct_dtype, true)
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
    chunk_size: Option<usize>,
) -> PolarsResult<*mut ArrowSchema> {
    let opts = crate::ScanOptions {
        threads,
        chunk_size,
        missing_string_as_null: Some(missing_string_as_null),
        informative_nulls: None,
        value_labels_as_strings: None,
        preserve_order: Some(true),
        row_index_name: None,
        compress_opts: crate::CompressOptionsLite::default(),
    };
    let schema = scan_sas7bdat(path.to_path_buf(), opts)?.collect_schema()?;
    let field = build_struct_field(&schema);
    Ok(Box::into_raw(Box::new(export_field_to_c(&field))))
}

/// Returns a pair of raw pointers: (ArrowSchema, ArrowArray)
/// This matches your releaseArrowArray and releaseArrowSchema cleanup logic.
pub fn read_to_arrow_array_ffi(
    path: &Path,
    threads: Option<usize>,
    missing_string_as_null: bool,
    chunk_size: Option<usize>,
) -> PolarsResult<(*mut ArrowSchema, *mut ArrowArray)> {
    let opts = crate::ScanOptions {
        threads,
        chunk_size,
        missing_string_as_null: Some(missing_string_as_null),
        informative_nulls: None,
        value_labels_as_strings: None,
        preserve_order: Some(true),
        row_index_name: None,
        compress_opts: crate::CompressOptionsLite::default(),
    };
    let df = scan_sas7bdat(path.to_path_buf(), opts)?.collect()?;
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
    chunk_size: Option<usize>,
    offset: usize,
    n_rows: Option<usize>,
) -> PolarsResult<*mut ArrowArrayStream> {
    let opts = crate::ScanOptions {
        threads,
        chunk_size,
        missing_string_as_null: Some(missing_string_as_null),
        informative_nulls: None,
        value_labels_as_strings: None,
        preserve_order: Some(true),
        row_index_name: None,
        compress_opts: crate::CompressOptionsLite::default(),
    };
    let mut lf = scan_sas7bdat(path.to_path_buf(), opts.clone())?;
    let schema = lf.collect_schema()?;
    let field = build_struct_field(&schema);
    let field_for_iter = field.clone();

    let mut iter = sas_batch_iter(
        path.to_path_buf(),
        opts.threads,
        missing_string_as_null,
        chunk_size,
        None,
        offset,
        n_rows,
        opts.preserve_order.unwrap_or(false),
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
    read_to_arrow_array_ffi(path, None, true, None)
}

/// Same as [`read_to_arrow_schema_ffi`], but reads through a caller-supplied
/// [`ReadSource`] instead of opening `path` directly — e.g. a source backed
/// by a host application's own file/VFS layer rather than `std::fs::File`.
pub fn read_to_arrow_schema_ffi_from_source(
    source: Arc<dyn ReadSource>,
    row_index_name: Option<&str>,
) -> PolarsResult<*mut ArrowSchema> {
    let reader = Sas7bdatReader::open_source(source)
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let schema = schema_from_sas_metadata(reader.metadata(), None, row_index_name)?;
    let field = build_struct_field(&Arc::new(schema));
    Ok(Box::into_raw(Box::new(export_field_to_c(&field))))
}

/// Same as [`read_to_arrow_stream_ffi`], but reads through a caller-supplied
/// [`ReadSource`] instead of opening `path` directly.
pub fn read_to_arrow_stream_ffi_from_source(
    source: Arc<dyn ReadSource>,
    threads: Option<usize>,
    missing_string_as_null: bool,
    chunk_size: Option<usize>,
    offset: usize,
    n_rows: Option<usize>,
    preserve_order: bool,
    row_index_name: Option<String>,
    columns: Option<Vec<usize>>,
) -> PolarsResult<*mut ArrowArrayStream> {
    let reader = Sas7bdatReader::open_source(source.clone())
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let full_schema = schema_from_sas_metadata(reader.metadata(), None, row_index_name.as_deref())?;
    let subset = crate::resolve_column_subset(full_schema, columns.as_deref(), row_index_name.is_some())?;
    let schema = subset.schema.clone();
    let field = build_struct_field(&Arc::new(schema));
    let field_for_iter = field.clone();

    let mut iter = sas_batch_iter_with_reader(
        &reader,
        source,
        threads,
        missing_string_as_null,
        chunk_size,
        subset.decode_indices.clone(),
        offset,
        n_rows,
        preserve_order,
        if subset.want_row_index { row_index_name } else { None },
        None,
        false,
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

/// Schema of an XPT (SAS transport) file, read through a caller-supplied [`ReadSource`].
pub fn read_xpt_to_arrow_schema_ffi_from_source(
    source: Arc<dyn ReadSource>,
    row_index_name: Option<&str>,
) -> PolarsResult<*mut ArrowSchema> {
    let meta = crate::sas::xpt::read_xpt_metadata_from_source(source.as_ref())?;
    let schema = crate::sas::xpt::schema_from_xpt_metadata(&meta, None, row_index_name)?;
    let field = build_struct_field(&Arc::new(schema));
    Ok(Box::into_raw(Box::new(export_field_to_c(&field))))
}

/// Stream of an XPT (SAS transport) file, read through a caller-supplied [`ReadSource`].
///
/// `columns`, `preserve_order` and `row_index_name` behave as for
/// [`read_to_arrow_stream_ffi_from_source`].
pub fn read_xpt_to_arrow_stream_ffi_from_source(
    source: Arc<dyn ReadSource>,
    threads: Option<usize>,
    missing_string_as_null: bool,
    chunk_size: Option<usize>,
    offset: usize,
    n_rows: Option<usize>,
    preserve_order: bool,
    row_index_name: Option<String>,
    columns: Option<Vec<usize>>,
) -> PolarsResult<*mut ArrowArrayStream> {
    let meta = crate::sas::xpt::read_xpt_metadata_from_source(source.as_ref())?;
    let full_schema = crate::sas::xpt::schema_from_xpt_metadata(&meta, None, row_index_name.as_deref())?;
    let subset = crate::resolve_column_subset(full_schema, columns.as_deref(), row_index_name.is_some())?;
    let field = build_struct_field(&Arc::new(subset.schema.clone()));
    let field_for_iter = field.clone();

    let mut iter = crate::sas::xpt::xpt_batch_iter(
        source,
        threads,
        missing_string_as_null,
        chunk_size,
        preserve_order,
        if subset.want_row_index { row_index_name } else { None },
        subset.decode_names.clone(),
        offset,
        n_rows,
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
