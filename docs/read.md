# Read

## `scan_readstat(path, **kwargs)`

Returns a Polars `LazyFrame` for SAS, Stata, and SPSS files.

Key parameters:

| Parameter | Default | Notes |
| --- | --- | --- |
| `preserve_order` | `False` | Preserve row order or expose a row index (see below). |
| `missing_string_as_null` | `False` | Convert empty strings to `null`. |
| `value_labels_as_strings` | `False` | For labeled numeric columns, return label strings. |
| `schema_overrides` | `None` | Dict of `{column: polars_dtype}`. |
| `batch_size` | `100_000` | Rows per internal chunk during collect. |
| `informative_nulls` | `None` | Capture user-defined missing indicators. |
| `columns` | `None` | Optional column subset (projection pushdown). |
| `threads` | `None` | Defaults to the Polars thread pool size. |
| `compress` | `None` | Optional type compression after scan. |

## Compression options

`compress` accepts a `CompressOptions` object or a plain dict with the same fields.
This is read-side type compression (not the Stata writer `compress` flag).

Fields:

| Field | Default | Description |
| --- | --- | --- |
| `enabled` | `False` | Enable compression after scan. |
| `cols` | `None` | Restrict compression to a list of column names. |
| `compress_numeric` | `False` | Downcast numeric types where safe. |
| `datetime_to_date` | `False` | Convert datetime columns to date when possible. |
| `string_to_numeric` | `False` | Convert strings to numeric when safe. |
| `infer_compress_length` | `None` | Limit compression inference to the first N rows. |

### Informative Nulls

SAS, Stata, and SPSS files support user-defined missing value codes (SAS `.A`–`.Z`, Stata `.a`–`.z`, SPSS discrete/range missings). By default these are read as `null`.

Use `informative_nulls` to capture the missing-value indicator alongside the data value.

```python
from polars_readstat import scan_readstat, InformativeNullOpts

lf = scan_readstat(
    "file.dta",
    informative_nulls=InformativeNullOpts(columns="all"),
)
```

`informative_nulls` accepts either an `InformativeNullOpts` dataclass or a plain dict.
The default indicator suffix is `_null` (used in `separate_column` mode).

### Modes

| Mode | Description |
| --- | --- |
| `"separate_column"` (default) | Adds a parallel `String` column `<col><suffix>` after each tracked column. |
| `"struct"` | Wraps each `(value, indicator)` pair into a `Struct` column. |
| `"merged_string"` | Merges into a single `String` column (value as string, or the indicator code). |

```python
from polars_readstat import InformativeNullOpts

opts = InformativeNullOpts(
    columns=["income", "age"],
    mode="separate_column",
    suffix="_missing",
    use_value_labels=True,
)
```

## Preserve order options

`preserve_order` accepts:

| Value | Behavior |
| --- | --- |
| `False` | Allow out-of-order batches for higher throughput. |
| `True` | Current behavior: buffer batches to preserve order (more memory). |
| `PreserveOrderOpts(...)` or `dict` | Row-index-based modes (less buffering). |

`PreserveOrderOpts` fields:

| Field | Default | Description |
| --- | --- | --- |
| `mode` | `"buffered"` | `"buffered"`, `"row_index"`, or `"sort"`. |
| `row_index_name` | `"row_index"` | Column name when mode is `"row_index"` or `"sort"`. |

Modes:

| Mode | Description |
| --- | --- |
| `"buffered"` | Keep original row order by buffering batches in Rust.  This can result in higher RAM spikes as it affects the streaming of results to polars. |
| `"row_index"` | Add a row index column in Rust, but return unsorted batches (so you can sort on the row_index later). |
| `"sort"` | Add a row index in Rust, then sort and drop it in Python. |

Example:

```python
from polars_readstat import scan_readstat, PreserveOrderOpts

lf = scan_readstat(
    "file.sas7bdat",
    preserve_order=PreserveOrderOpts(mode="row_index", row_index_name="__row_idx"),
)
```

## `read_readstat(path, **kwargs)`

Eager version of `scan_readstat` returning a `DataFrame`. Accepts the same parameters.

## `ScanReadstat(path, **kwargs)`

Reader object that exposes:

- `schema`: a `polars.Schema`
- `metadata`: a dict with file info and per-column details
- `df`: a `LazyFrame`, same as `scan_readstat(path)`

Metadata includes:

- `columns[].name`
- `columns[].label`
- `columns[].value_labels`

## `write_readstat(df, path, **kwargs)`

Write Stata (`.dta`) and SPSS (`.sav`, `.zsav`) files. See [Writing](write.md) for details.
