# Read

## `scan_readstat(path, **kwargs)`

Returns a Polars `LazyFrame` for SAS, Stata, and SPSS files.

```python
from polars_readstat import scan_readstat

lf = scan_readstat("file.sas7bdat")
df = lf.collect()
```

Key parameters:

| Parameter | Default | Notes |
| --- | --- | --- |
| `preserve_order` | `False` | Preserve row order or expose a row index (see below). |
| `missing_string_as_null` | `False` | Convert empty strings to `null`. |
| `value_labels_as_strings` | `False` | For labeled numeric columns, return label strings. |
| `schema_overrides` | `None` | Dict of `{column: polars_dtype}`. |
| `batch_size` | `None` | Rows per internal chunk during collect. Auto-inferred if `None`. |
| `informative_nulls` | `None` | Capture user-defined missing indicators. |
| `threads` | `None` | Number of threads. Defaults to the Polars thread pool size. |
| `compress` | `None` | Optional type compression after scan. |

## Compression options

`compress` accepts:

- `True`: enable all read-side compression transforms
- `False` / `None`: disable read-side compression
- a `CompressOptions` object
- a plain dict with the same fields

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

Column projection should use standard Polars lazy syntax:

```python
lf = scan_readstat("file.sas7bdat").select(["income", "age"])
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

## `ScanReadstat(path, **kwargs)`

Reader object that exposes `schema`, `metadata`, and `df`. Useful when you need file metadata before collecting data. Accepts the same parameters as `scan_readstat`.

```python
from polars_readstat import ScanReadstat

reader = ScanReadstat("file.dta")

print(reader.schema)
print(reader.metadata["row_count"])

df = reader.df.collect()
```

Metadata structure varies by format.

### SAS (`.sas7bdat`)

Per-variable info is under the `columns` key.

```python
{
  "row_count": 10,
  "column_count": 100,
  "table_name": "TEST1",
  "file_encoding": "WINDOWS-1252",
  "sas_release": "9.0401M1",
  "compression": "None",
  "creator_proc": "DATASTEP",
  "columns": [
    {
      "name": "Column1",
      "label": null,
      "format": "BEST",
      "type": "Numeric",
      "length": 8,
      "offset": 0
    },
    {
      "name": "Column4",
      "label": null,
      "format": "MMDDYY",
      "type": "Numeric",
      "length": 8,
      "offset": 16
    },
    ...
  ]
}
```

SAS `.sas7bdat` files do not contain value labels.

### Stata (`.dta`)

Per-variable info is under the `variables` key.

```python
{
  "row_count": 30,
  "version": 118,
  "byte_order": "Little",
  "encoding": "UTF-8",
  "data_label": null,
  "timestamp": " 8 Aug 2016 15:21",
  "variables": [
    {
      "name": "ethnicsn",
      "label": "ethnicity, senegal",
      "format": "%8.0g",
      "type": "Numeric(Int)",
      "value_label_name": "ETHNICSN",
      "value_labels": {
        "101": "bainouk",
        "102": "badiaranke",
        ...
      }
    },
    ...
  ]
}
```

### SPSS (`.sav` / `.zsav`)

Per-variable info is under the `variables` key. SPSS exposes the richest variable-level metadata.

```python
{
  "row_count": 5,
  "version": 2,
  "compression": "RLE",
  "encoding": "windows-1252",
  "file_label": null,
  "variables": [
    {
      "name": "mylabl",
      "label": "labeled",
      "type": "Numeric",
      "measure": "Scale",
      "alignment": "Right",
      "display_width": 8,
      "decimal_places": 2,
      "format_type": 5,
      "format_width": 8,
      "format_decimals": 2,
      "format_class": null,
      "value_label": "labels0",
      "value_labels": {
        "1": "Male",
        "2": "Female"
      },
      "missing_doubles": [],
      "missing_strings": [],
      "missing_range": false,
      ...
    },
    ...
  ]
}
```

