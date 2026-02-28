# Read

## `scan_readstat(path, **kwargs)`

Returns a Polars `LazyFrame` for SAS, Stata, and SPSS files.

Key parameters:

| Parameter | Default | Notes |
| --- | --- | --- |
| `preserve_order` | `False` | Keep original row order; may be slower with multiple threads. |
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
