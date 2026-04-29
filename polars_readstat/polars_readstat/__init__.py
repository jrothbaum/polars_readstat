from __future__ import annotations
from typing import Any, Dict, Iterator, Literal, Mapping
from pathlib import Path
import polars as pl
from dataclasses import dataclass
from polars.io.plugins import register_io_source
from polars_readstat.polars_readstat_bindings import (
    PyPolarsReadstat,
    read_readstat_rs,
    sink_stata,
    write_sas_csv_import as _write_sas_csv_import_rs,
    write_stata,
    write_spss as _write_spss_rs,
)
import warnings


def _warn_engine_deprecated() -> None:
    warnings.warn(
        "Engine is deprecated as all calls use the new polars_readstat_rs rust engine.",
        UserWarning,
        stacklevel=3,
    )


def _warn_use_mmap_deprecated() -> None:
    warnings.warn(
        "use_mmap is deprecated as it has not been implemented in the polars_readstat_rs rust engine.",
        UserWarning,
        stacklevel=3,
    )


class ScanReadstat:
    def __init__(
        self,
        path: str,
        engine: str = "",
        use_mmap: bool = False,
        threads: int | None = None,
        missing_string_as_null: bool = False,
        value_labels_as_strings: bool = False,
        preserve_order: bool | "PreserveOrderOpts" | dict = False,
        compress: "bool | CompressOptions | dict | None" = None,
        schema_overrides: Dict[Any, Any] | None = None,
        batch_size: int | None = None,
        informative_nulls: "InformativeNullOpts | dict | None" = None,
    ):
        self.path = str(path)
        if engine != "":
            _warn_engine_deprecated()

        if use_mmap:
            _warn_use_mmap_deprecated()

        self.engine = engine
        self.use_mmap = use_mmap
        self._validation_check(self.path)

        self.threads = threads

        self._metadata = None
        self._schema = None
        self.missing_string_as_null = missing_string_as_null
        self.value_labels_as_strings = value_labels_as_strings
        self.preserve_order = preserve_order
        self._preserve_order_opts = _normalize_preserve_order_opts(preserve_order)
        self.compress = _normalize_compress_opts(compress)
        self.schema_overrides = schema_overrides
        self.batch_size = batch_size
        self.informative_nulls = _normalize_informative_null_opts(informative_nulls)

    @property
    def schema(self) -> pl.Schema:
        if self._schema is None:
            self._get_schema()
        return self._schema
    
    @property
    def metadata(self) -> dict:
        if self._schema is None:
            self._get_schema()
        return self._metadata
    
    @property
    def df(self) -> pl.LazyFrame:
        return scan_readstat(
            self.path,
            engine=self.engine,
            use_mmap=self.use_mmap,
            missing_string_as_null=self.missing_string_as_null,
            value_labels_as_strings=self.value_labels_as_strings,
            preserve_order=self.preserve_order,
            compress=self.compress,
            schema_overrides=self.schema_overrides,
            batch_size=self.batch_size,
            informative_nulls=self.informative_nulls,
        )

    # def iter_batches(
    #     self,
    #     batch_size: int | None = None,
    #     columns: list[str] | None = None,
    #     n_rows: int | None = None,
    #     predicate: pl.Expr | None = None,
    # ) -> Iterator[pl.DataFrame]:
    #     warnings.warn(
    #         "ScanReadstat.iter_batches is deprecated; use scan_readstat(..., batch_size=...) and collect on the LazyFrame.",
    #         DeprecationWarning,
    #         stacklevel=2,
    #     )
    #     return scan_readstat(
    #         path=self.path,
    #         threads=self.threads,
    #         engine=self.engine,
    #         use_mmap=self.use_mmap,
    #         missing_string_as_null=self.missing_string_as_null,
    #         value_labels_as_strings=self.value_labels_as_strings,
    #         columns=columns,
    #         preserve_order=self.preserve_order,
    #         compress=self.compress,
    #         reader=self,
    #         schema_overrides=self.schema_overrides,
    #         batch_size=batch_size,
    #         return_batches=True,
    #     )
        
    def _get_schema(self) -> None:
        preserve_order, row_index_name, _ = _resolve_preserve_order_opts(self._preserve_order_opts)
        src = PyPolarsReadstat(
            path=self.path,
            size_hint=10_000,
            n_rows=None,
            threads=self.threads,
            missing_string_as_null=self.missing_string_as_null,
            value_labels_as_strings=self.value_labels_as_strings,
            preserve_order=preserve_order,
            compress=self.compress.to_dict() if self.compress is not None else None,
            informative_nulls=self.informative_nulls.to_dict() if self.informative_nulls is not None else None,
            row_index_name=row_index_name,
        )
        self._schema = src.schema()
        self._metadata = src.get_metadata()

    def _validation_check(self, path: str) -> None:
        valid_files = [".sas7bdat", ".dta", ".sav", ".zsav"]
        is_valid = False
        for fi in valid_files:
            is_valid = is_valid or path.endswith(fi)

        if not is_valid:
            message = f"{path} is not a valid file for polars_readstat. It must be one of these: {valid_files}"
            raise Exception(message)
        
        
@dataclass
class CompressOptions:
    enabled: bool = False
    cols: list[str] | None = None
    compress_numeric: bool = False
    datetime_to_date: bool = False
    string_to_numeric: bool = False
    infer_compress_length: int | None = None

    def to_dict(self) -> dict:
        return {
            "enabled": self.enabled,
            "cols": self.cols,
            "compress_numeric": self.compress_numeric,
            "datetime_to_date": self.datetime_to_date,
            "string_to_numeric": self.string_to_numeric,
            "infer_compress_length": self.infer_compress_length,
        }


def _normalize_compress_opts(
    compress: "bool | CompressOptions | Dict[str, Any] | None",
) -> CompressOptions | None:
    if compress is None:
        return None
    if isinstance(compress, bool):
        if not compress:
            return None
        return CompressOptions(
            enabled=True,
            compress_numeric=True,
            datetime_to_date=True,
            string_to_numeric=True,
        )
    if isinstance(compress, CompressOptions):
        return compress
    if isinstance(compress, dict):
        return CompressOptions(**compress)
    raise TypeError(
        f"compress must be bool, CompressOptions, dict, or None, got {type(compress)}"
    )


PreserveOrderMode = Literal["buffered", "row_index", "sort"]


@dataclass
class PreserveOrderOpts:
    """Options for preserving row order or exposing a row index during scan.

    Parameters
    ----------
    mode : PreserveOrderMode
        - ``"buffered"`` (default): current behavior; buffer batches to preserve order.
        - ``"row_index"``: add a row index column and return unsorted batches.
        - ``"sort"``: add a row index column, sort by it, then drop it in Python.
    row_index_name : str
        Name for the row index column when ``mode`` is ``"row_index"`` or ``"sort"``.
    """

    mode: PreserveOrderMode = "buffered"
    row_index_name: str = "row_index"

    def __post_init__(self) -> None:
        valid_modes = ("buffered", "row_index", "sort")
        if self.mode not in valid_modes:
            raise ValueError(
                f"preserve_order mode must be one of {valid_modes!r}, got {self.mode!r}"
            )
        if not isinstance(self.row_index_name, str) or not self.row_index_name:
            raise ValueError("row_index_name must be a non-empty string")


def _normalize_preserve_order_opts(
    preserve_order: bool | PreserveOrderOpts | Dict[str, Any] | None,
) -> PreserveOrderOpts | None:
    if preserve_order is None or preserve_order is False:
        return None
    if isinstance(preserve_order, PreserveOrderOpts):
        return preserve_order
    if isinstance(preserve_order, dict):
        return PreserveOrderOpts(**preserve_order)
    if isinstance(preserve_order, bool):
        return PreserveOrderOpts(mode="buffered")
    raise TypeError(
        "preserve_order must be bool, PreserveOrderOpts, dict, or None, "
        f"got {type(preserve_order)}"
    )


def _infer_default_batch_size(
    reader: ScanReadstat,
    n_rows: int | None,
    with_columns: list[str] | None,
) -> int:
    min_rows_floor = 1_000
    min_rows_cap = 10_000
    max_rows = 100_000
    target_cells = 2_000_000
    min_cells = 250_000

    if with_columns:
        n_cols = max(1, len(with_columns))
    else:
        n_cols = max(1, len(reader.schema))

    by_cols = max(1, target_cells // n_cols)
    batch = min(max_rows, by_cols)

    total_rows = n_rows
    if total_rows is None:
        try:
            total_rows = reader.metadata.get("row_count")
        except Exception:
            total_rows = None

    if isinstance(total_rows, int) and total_rows > 0:
        batch = min(batch, total_rows)
        min_rows_by_rows = max(min_rows_floor, min(min_rows_cap, total_rows // 10))
    else:
        min_rows_by_rows = min_rows_cap

    min_rows_by_cols = max(min_rows_floor, min(min_rows_cap, (min_cells + n_cols - 1) // n_cols))
    min_rows = min(min_rows_by_rows, min_rows_by_cols)

    return max(min_rows, batch)


def _resolve_preserve_order_opts(
    preserve_order: PreserveOrderOpts | None,
) -> tuple[bool, str | None, bool]:
    if preserve_order is None:
        return False, None, False
    if preserve_order.mode == "buffered":
        return True, None, False
    if preserve_order.mode == "row_index":
        return False, preserve_order.row_index_name, False
    if preserve_order.mode == "sort":
        return False, preserve_order.row_index_name, True
    raise ValueError(f"Unknown preserve_order mode: {preserve_order.mode!r}")


InformativeNullMode = Literal["separate_column", "struct", "merged_string"]


@dataclass
class InformativeNullOpts:
    """Options for capturing informative (user-defined) missing value indicators.

    Parameters
    ----------
    columns : "all" or list of str
        ``"all"`` to track every eligible column, or a list of column names.
        Eligible columns are numeric for SAS/Stata; numeric or string-with-declared-
        missings for SPSS.
    mode : InformativeNullMode
        How to expose the indicator in the output DataFrame:
        - ``"separate_column"`` (default): a parallel ``String`` column named
          ``<col><suffix>`` is inserted right after the original column.
        - ``"struct"``: each ``(col, col_null)`` pair becomes a ``Struct`` column
          with fields ``value`` and ``null_indicator``.
        - ``"merged_string"``: merge into a single ``String`` column via
          ``coalesce(cast(col, Str), col_null)``.
    suffix : str
        Suffix for the indicator column name when ``mode="separate_column"``.
        Defaults to ``"_null"``.
    use_value_labels : bool
        If ``True`` (default), prefer a value label for the indicator string when
        one is defined for that missing code.
    """

    columns: str | list[str] = "all"
    mode: InformativeNullMode = "separate_column"
    suffix: str = "_null"
    use_value_labels: bool = True

    def __post_init__(self) -> None:
        valid_modes = ("separate_column", "struct", "merged_string")
        if self.mode not in valid_modes:
            raise ValueError(
                f"informative_nulls mode must be one of {valid_modes!r}, got {self.mode!r}"
            )
        if isinstance(self.columns, str) and self.columns != "all":
            raise ValueError(
                f"informative_nulls columns must be 'all' or a list of column names, got {self.columns!r}"
            )

    def to_dict(self) -> dict:
        return {
            "columns": self.columns,
            "mode": self.mode,
            "suffix": self.suffix,
            "use_value_labels": self.use_value_labels,
        }


def _resolve_struct_mode_overrides(
    schema_overrides: "Dict[str, Any] | None",
    base_schema: "pl.Schema",
    informative_nulls: "InformativeNullOpts | None",
) -> "Dict[str, Any] | None":
    """Auto-wrap non-struct schema_overrides into the Struct produced by informative_nulls
    struct mode, so users can write ``schema_overrides={"x": pl.Int64}`` instead of
    ``{"x": pl.Struct({"x": pl.Int64, "null_indicator": pl.String})}``.
    The value field in the struct is always named after the column itself.
    """
    if not schema_overrides or informative_nulls is None or informative_nulls.mode != "struct":
        return schema_overrides
    resolved = {}
    for col, dtype in schema_overrides.items():
        base_dtype = base_schema.get(col)
        if base_dtype is not None and isinstance(base_dtype, pl.Struct) and not isinstance(dtype, pl.Struct):
            resolved[col] = pl.Struct({col: dtype, "null_indicator": pl.String})
        else:
            resolved[col] = dtype
    return resolved


def _normalize_informative_null_opts(
    informative_nulls: "InformativeNullOpts | Dict[str, Any] | None",
) -> InformativeNullOpts | None:
    if informative_nulls is None:
        return None
    if isinstance(informative_nulls, InformativeNullOpts):
        return informative_nulls
    if isinstance(informative_nulls, dict):
        return InformativeNullOpts(**informative_nulls)
    raise TypeError(
        f"informative_nulls must be InformativeNullOpts, dict, or None, got {type(informative_nulls)}"
    )


def scan_readstat(
    path: Any,
    threads: int | None = None,
    engine: str = "",
    use_mmap: bool = False,
    missing_string_as_null: bool = False,
    value_labels_as_strings: bool = False,
    preserve_order: bool | PreserveOrderOpts | Dict[str, Any] = False,
    compress: bool | CompressOptions | Dict[str, Any] | None = None,
    reader: ScanReadstat | None = None,
    schema_overrides: Dict[Any, Any] | None = None,
    batch_size: int | None = None,
    informative_nulls: "InformativeNullOpts | dict | None" = None,
    # return_batches: bool = False,
) -> pl.LazyFrame:
    """
    Scans a ReadStat file (SAS, SPSS, Stata) into a Polars LazyFrame.
    
    Parameters
    ----------
    path : str
        Path to the file.
    engine : str, optional
        DEPRECATED.
    threads : int, optional
        Number of threads to use.
    use_mmap : bool, optional
        DEPRECATED.
    missing_string_as_null : bool, optional
        Convert empty strings to nulls.
    value_labels_as_strings : bool, optional
        Use value labels as strings for labeled numeric columns (Stata/SPSS).
    preserve_order : bool, PreserveOrderOpts, or dict, optional
        ``False`` (default) allows out-of-order batches for higher throughput.
        ``True`` maps to ``PreserveOrderOpts(mode="buffered")`` (current behavior).
        ``PreserveOrderOpts`` or a dict with ``mode``/``row_index_name`` allows
        row-index-based ordering without buffering.
    compress : bool, CompressOptions, or dict, optional
        Read-side type compression after scan.
        ``True`` enables all transforms (numeric downcast, datetime-to-date,
        and string-to-numeric). ``False``/``None`` disables compression.
    reader : ScanReadstat, optional
        Internal use.
    schema_overrides : dict, optional
        A dictionary mapping column names to Polars DataTypes. 
        Used to force specific types (e.g., Int64) to prevent overflow errors 
        when the schema inferred from the header differs from data in the file body.
    batch_size : int, optional
        Number of rows per batch used by the scan source.
    """
    path = str(path)
    compress = _normalize_compress_opts(compress)
    informative_nulls = _normalize_informative_null_opts(informative_nulls)
    preserve_order_opts = _normalize_preserve_order_opts(preserve_order)

    if engine != "":
        _warn_engine_deprecated()
    if use_mmap:
        _warn_use_mmap_deprecated()

    if reader is None:
        reader = ScanReadstat(
            path=path,
            threads=threads,
            missing_string_as_null=missing_string_as_null,
            value_labels_as_strings=value_labels_as_strings,
            preserve_order=preserve_order,
            compress=compress,
            schema_overrides=schema_overrides,
            batch_size=batch_size,
            informative_nulls=informative_nulls,
        )
    else:
        path = reader.path
        threads = reader.threads
        missing_string_as_null = reader.missing_string_as_null
        value_labels_as_strings = reader.value_labels_as_strings
        preserve_order = reader.preserve_order
        preserve_order_opts = _normalize_preserve_order_opts(preserve_order)
        compress = reader.compress
        batch_size = reader.batch_size
        informative_nulls = reader.informative_nulls

    preserve_order_flag, row_index_name, sort_in_python = _resolve_preserve_order_opts(
        preserve_order_opts
    )

    # Resolve schema_overrides: if informative_nulls mode is "struct", auto-wrap
    # non-struct overrides so users can write schema_overrides={"x": pl.Int64}
    # instead of {"x": pl.Struct({"x": pl.Int64, "null_indicator": pl.String})}.
    effective_overrides = _resolve_struct_mode_overrides(
        schema_overrides, reader.schema, informative_nulls
    )

    # if return_batches:
    #     warnings.warn(
    #         "scan_readstat(..., return_batches=True) is deprecated; return_batches is for internal/backward-compat use only.",
    #         DeprecationWarning,
    #         stacklevel=2,
    #     )
    #     def source_generator_batches() -> Iterator[pl.DataFrame]:
    #         if batch_size is None:
    #             bs = 100_000
    #         else:
    #             bs = batch_size
    #         if bs <= 0:
    #             raise ValueError("batch_size must be > 0")

    #         src = PyPolarsReadstat(
    #             path=path,
    #             size_hint=bs,
    #             n_rows=None,
    #             threads=reader.threads,
    #             missing_string_as_null=reader.missing_string_as_null,
    #             value_labels_as_strings=reader.value_labels_as_strings,
    #             preserve_order=reader.preserve_order,
    #             compress=compress.to_dict() if compress is not None else None,
    #         )
    #         if columns is not None:
    #             cols = [c for c in columns if c]
    #             if cols:
    #                 src.set_with_columns(cols)

    #         while (out := src.next()) is not None:
    #             if schema_overrides:
    #                 cols_to_cast = {
    #                     col: dtype
    #                     for col, dtype in schema_overrides.items()
    #                     if col in out.columns
    #                 }
    #                 if cols_to_cast:
    #                     out = out.cast(cols_to_cast)
    #             yield out
    #     return source_generator_batches()

    def schema_generator() -> pl.Schema:
        return reader.schema

    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None = None,
    ) -> Iterator[pl.DataFrame]:
        if batch_size is None:
            batch_size = reader.batch_size
        if batch_size is None:
            batch_size = _infer_default_batch_size(reader, n_rows, with_columns)

        src = PyPolarsReadstat(
            path=path,
            size_hint=batch_size,
            n_rows=n_rows,
            threads=reader.threads,
            missing_string_as_null=reader.missing_string_as_null,
            value_labels_as_strings=reader.value_labels_as_strings,
            preserve_order=preserve_order_flag,
            compress=compress.to_dict() if compress is not None else None,
            informative_nulls=informative_nulls.to_dict() if informative_nulls is not None else None,
            row_index_name=row_index_name,
        )
        if with_columns is not None:
            src.set_with_columns(with_columns)

        while (out := src.next()) is not None:
            if predicate is not None:
                out = out.filter(predicate)

            yield out

    lf = register_io_source(io_source=source_generator, schema=schema_generator())
    if effective_overrides:
        lf = lf.with_columns([
            pl.col(col).cast(dtype)
            for col, dtype in effective_overrides.items()
            if col in reader.schema
        ])
    if sort_in_python and row_index_name is not None:
        lf = lf.sort(row_index_name).drop([row_index_name])
    return lf


def read_readstat(
    path: Any,
    threads: int | None = None,
    engine: str = "",
    use_mmap: bool = False,
    missing_string_as_null: bool = False,
    value_labels_as_strings: bool = False,
    columns: list[str] | None = None,
    compress: bool | CompressOptions | Dict[str, Any] | None = None,
    informative_nulls: "InformativeNullOpts | dict | None" = None,
) -> pl.DataFrame:
    """
    Read a ReadStat file (SAS, SPSS, Stata) into a Polars DataFrame using the
    Rust streaming read path.

    Parameters
    ----------
    compress : bool, CompressOptions, or dict, optional
        Apply Rust-side dual-pass compression (schema probe + cast) while reading.
        ``True`` enables all transforms; ``False``/``None`` disables compression.
    informative_nulls : InformativeNullOpts or dict, optional
        Capture user-defined missing value indicators as parallel indicator columns.
    """
    path = str(path)
    if engine != "":
        _warn_engine_deprecated()
    if use_mmap:
        _warn_use_mmap_deprecated()

    if threads is None:
        threads = pl.thread_pool_size()

    compress_opts = _normalize_compress_opts(compress)
    informative_null_opts = _normalize_informative_null_opts(informative_nulls)
    return read_readstat_rs(
        path=path,
        threads=threads,
        missing_string_as_null=missing_string_as_null,
        value_labels_as_strings=value_labels_as_strings,
        columns=columns,
        compress=compress_opts.to_dict() if compress_opts is not None else None,
        informative_nulls=informative_null_opts.to_dict() if informative_null_opts is not None else None,
    )




def write_readstat(
    df: pl.DataFrame | pl.LazyFrame,
    path: Any,
    *,
    format: str | None = None,
    metadata: dict | None = None,
    **kwargs: Any,
) -> None:
    """
    Write a DataFrame or LazyFrame to a ReadStat-supported file.

    Parameters
    ----------
    df : polars.DataFrame or polars.LazyFrame
        Data to write.
    path : str
        Output path.
    format : str, optional
        One of "dta" (Stata) or "sav"/"zsav" (SPSS). If omitted, inferred
        from the file extension.
    metadata : dict, optional
        Metadata dict returned by ``ScanReadstat(...).metadata``. Variable
        labels, value labels, formats, and (for SPSS) measure/alignment/width
        are extracted automatically. Explicit kwargs take precedence over
        anything derived from metadata.
    **kwargs : Any
        Stata supports `compress` (bool), `threads` (int),
        `value_labels` (dict[str, dict[int, str]]), `variable_labels` (dict[str, str]),
        and `variable_format` (dict[str, str]).
        SPSS supports `value_labels` (dict[str, dict[float|int, str]]),
        `variable_labels` (dict[str, str]), `variable_measure`,
        `variable_display_width`, `variable_alignment`, and `variable_format`.
        SAS binary writing is not supported; use `write_sas_csv_import`.
    """
    path = str(path)
    fmt = (format or Path(path).suffix.lstrip(".")).lower()
    df = _prepare_write_df(df)

    if fmt in ("dta", "stata"):
        base: dict[str, Any] = {}
        if metadata is not None:
            variables = [v for v in (metadata.get("variables") or []) if v.get("name") in df.columns]
            base = stata_variable_metadata_to_write_kwargs(variables)
        compress = kwargs.pop("compress", base.get("compress"))
        threads = kwargs.pop("threads", base.get("threads"))
        value_labels = kwargs.pop("value_labels", base.get("value_labels"))
        variable_labels = kwargs.pop("variable_labels", base.get("variable_labels"))
        variable_format = kwargs.pop("variable_format", base.get("variable_format"))
        if kwargs:
            raise TypeError(f"Unsupported kwargs for Stata writer: {sorted(kwargs.keys())}")
        write_stata(
            df,
            path,
            compress=compress,
            threads=threads,
            value_labels=value_labels,
            variable_labels=variable_labels,
            variable_format=variable_format,
        )
        return
    if fmt in ("sav", "zsav", "spss"):
        base = {}
        if metadata is not None:
            variables = [v for v in (metadata.get("variables") or []) if v.get("name") in df.columns]
            base = spss_variable_metadata_to_write_kwargs(variables)
        compress = kwargs.pop("compress", None)
        if compress is not None:
            warnings.warn(
                "compress has no effect for SPSS outputs.",
                UserWarning,
                stacklevel=2,
            )
        value_labels = kwargs.pop("value_labels", base.get("value_labels"))
        variable_labels = kwargs.pop("variable_labels", base.get("variable_labels"))
        variable_measure = kwargs.pop("variable_measure", base.get("variable_measure"))
        variable_display_width = kwargs.pop("variable_display_width", base.get("variable_display_width"))
        variable_alignment = kwargs.pop("variable_alignment", base.get("variable_alignment"))
        variable_format = kwargs.pop("variable_format", base.get("variable_format"))
        if kwargs:
            raise TypeError(f"Unsupported kwargs for SPSS writer: {sorted(kwargs.keys())}")
        write_spss(
            df,
            path,
            value_labels=value_labels,
            variable_labels=variable_labels,
            variable_measure=variable_measure,
            variable_display_width=variable_display_width,
            variable_alignment=variable_alignment,
            variable_format=variable_format,
        )
        return
    if fmt in ("sas7bdat", "sas"):
        raise NotImplementedError(
            "SAS binary (.sas7bdat) writing is not supported; "
            "use write_sas_csv_import(...) to emit a CSV + SAS import script."
        )

    raise ValueError(f"Unsupported output format: {fmt}")


def write_spss(
    df: pl.DataFrame | pl.LazyFrame,
    path: Any,
    value_labels: dict[str, dict[float | int | str, str]] | None = None,
    variable_labels: dict[str, str] | None = None,
    *,
    variable_measure: dict[str, str] | None = None,
    variable_display_width: dict[str, int] | None = None,
    variable_alignment: dict[str, str] | None = None,
    variable_format: dict[str, str | dict[str, int]] | None = None,
) -> None:
    """
    Write an SPSS `.sav`/`.zsav` file.

    `variable_format` string values currently support simple SPSS `F` and `A`
    formats such as `"F8.2"` and `"A20"`. Dict values may contain
    `format_type`, `width`, and `decimals`/`decimal_places`.
    """
    df = _prepare_write_df(df)
    _write_spss_rs(
        df,
        str(path),
        value_labels=value_labels,
        variable_labels=variable_labels,
        variable_measure=variable_measure,
        variable_display_width=variable_display_width,
        variable_alignment=variable_alignment,
        variable_format=variable_format,
    )


def spss_variable_metadata_to_write_kwargs(
    variables: Mapping[str, Mapping[str, Any]] | list[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """
    Convert SPSS per-variable metadata into kwargs accepted by `write_spss`.

    Accepts either `{name: variable_metadata}` or the `metadata["variables"]`
    list returned by `ScanReadstat(...).metadata`.
    """
    variable_items = _normalize_variable_metadata_items(variables)
    value_labels: dict[str, dict[Any, str]] = {}
    variable_labels: dict[str, str] = {}
    variable_measure: dict[str, str] = {}
    variable_display_width: dict[str, int] = {}
    variable_alignment: dict[str, str] = {}
    variable_format: dict[str, str] = {}

    for name, meta in variable_items:
        label = meta.get("label")
        if label is not None:
            variable_labels[name] = str(label)

        labels = meta.get("value_labels")
        if labels:
            value_labels[name] = {
                _coerce_spss_value_label_key(key): str(value)
                for key, value in labels.items()
            }

        measure = meta.get("measure")
        if measure is not None:
            variable_measure[name] = str(measure).lower()

        display_width = meta.get("display_width")
        if display_width is not None:
            variable_display_width[name] = int(display_width)

        alignment = meta.get("alignment")
        if alignment is not None:
            variable_alignment[name] = str(alignment).lower()

        fmt = _spss_variable_format_from_metadata(meta)
        if fmt is not None:
            variable_format[name] = fmt

    out: dict[str, dict[str, Any]] = {}
    if value_labels:
        out["value_labels"] = value_labels
    if variable_labels:
        out["variable_labels"] = variable_labels
    if variable_measure:
        out["variable_measure"] = variable_measure
    if variable_display_width:
        out["variable_display_width"] = variable_display_width
    if variable_alignment:
        out["variable_alignment"] = variable_alignment
    if variable_format:
        out["variable_format"] = variable_format
    return out


def _normalize_variable_metadata_items(
    variables: Mapping[str, Mapping[str, Any]] | list[Mapping[str, Any]],
) -> list[tuple[str, Mapping[str, Any]]]:
    if isinstance(variables, Mapping):
        return [(str(name), meta) for name, meta in variables.items()]

    out = []
    for meta in variables:
        name = meta.get("name")
        if name is None:
            raise ValueError("SPSS variable metadata entries must include a 'name'")
        out.append((str(name), meta))
    return out


def _spss_variable_format_from_metadata(meta: Mapping[str, Any]) -> str | None:
    format_width = meta.get("format_width")
    if format_width is None:
        format_width = meta.get("width")
    if format_width is None:
        return None

    decimals = meta.get("format_decimals")
    if decimals is None:
        decimals = meta.get("decimal_places")
    if decimals is None:
        decimals = 0

    format_type = meta.get("format_type")
    if format_type == 1 or str(format_type).upper() == "A":
        return f"A{int(format_width)}"
    if format_type is None or format_type == 5 or str(format_type).upper() == "F":
        return f"F{int(format_width)}.{int(decimals)}"
    return None


def _coerce_spss_value_label_key(key: Any) -> Any:
    if isinstance(key, (int, float)):
        return key
    if not isinstance(key, str):
        return key
    try:
        value = float(key)
    except ValueError:
        return key
    if value.is_integer():
        return int(value)
    return value


def stata_variable_metadata_to_write_kwargs(
    variables: Mapping[str, Mapping[str, Any]] | list[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """
    Convert Stata per-variable metadata into kwargs accepted by `write_readstat`.

    Accepts either `{name: variable_metadata}` or the `metadata["variables"]`
    list returned by `ScanReadstat(...).metadata`.
    """
    variable_items = _normalize_variable_metadata_items(variables)
    value_labels: dict[str, dict[Any, str]] = {}
    variable_labels: dict[str, str] = {}
    variable_format: dict[str, str] = {}

    for name, meta in variable_items:
        label = meta.get("label")
        if label is not None:
            variable_labels[name] = str(label)

        labels = meta.get("value_labels")
        if labels:
            value_labels[name] = {
                _coerce_stata_value_label_key(key): str(value)
                for key, value in labels.items()
            }

        fmt = meta.get("format")
        if fmt:
            variable_format[name] = str(fmt)

    out: dict[str, dict[str, Any]] = {}
    if value_labels:
        out["value_labels"] = value_labels
    if variable_labels:
        out["variable_labels"] = variable_labels
    if variable_format:
        out["variable_format"] = variable_format
    return out


def _coerce_stata_value_label_key(key: Any) -> Any:
    if isinstance(key, int):
        return key
    if isinstance(key, float) and key.is_integer():
        return int(key)
    if not isinstance(key, str):
        return key
    try:
        value = float(key)
    except ValueError:
        return key
    if value.is_integer():
        return int(value)
    return key


def write_sas_csv_import(
    df: pl.DataFrame | pl.LazyFrame,
    path: Any,
    *,
    dataset_name: str | None = None,
    value_labels: dict[str, dict[float | int | str, str]] | None = None,
    variable_labels: dict[str, str] | None = None,
) -> tuple[str, str]:
    """
    Write a SAS-import bundle as `CSV + .sas` script.

    This does not create a binary `.sas7bdat` file.

    Parameters
    ----------
    df : polars.DataFrame or polars.LazyFrame
        Data to write.
    path : str
        Output directory or file stem for the generated bundle.
    dataset_name : str, optional
        SAS dataset name used in the generated script.
    value_labels : dict, optional
        Mapping of column name to `{coded_value: label}`.
        Keys may be numeric or string.
    variable_labels : dict, optional
        Mapping of column name to variable label text.

    Returns
    -------
    tuple[str, str]
        `(csv_path, sas_script_path)`
    """
    df = _prepare_write_df(df)

    return _write_sas_csv_import_rs(
        df,
        str(path),
        dataset_name=dataset_name,
        value_labels=value_labels,
        variable_labels=variable_labels,
    )


def _prepare_write_df(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    if not isinstance(df, pl.DataFrame):
        raise TypeError("df must be a polars DataFrame or LazyFrame")

    # pyo3-polars currently fails on narrow unsigned write inputs.
    # Fast path: avoid materializing a new frame when no narrow unsigned columns are present.
    has_u8 = False
    has_u16 = False
    for dtype in df.schema.values():
        if dtype == pl.UInt8:
            has_u8 = True
        elif dtype == pl.UInt16:
            has_u16 = True
        if has_u8 and has_u16:
            break
    if not has_u8 and not has_u16:
        return df

    exprs: list[pl.Expr] = []
    if has_u8:
        exprs.append(pl.col(pl.UInt8).cast(pl.Int16))
    if has_u16:
        exprs.append(pl.col(pl.UInt16).cast(pl.Int32))
    return df.with_columns(exprs)
