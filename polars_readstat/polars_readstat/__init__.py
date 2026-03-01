from __future__ import annotations
from typing import Any, Dict, Iterator, Literal
from pathlib import Path
import polars as pl
from dataclasses import dataclass
from polars.io.plugins import register_io_source
from polars_readstat.polars_readstat_bindings import (
    PyPolarsReadstat,
    read_readstat_rs,
    sink_stata,
    write_stata,
    write_spss,
)
import warnings


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
        compress: "CompressOptions | dict | None" = None,
        schema_overrides: Dict[Any, Any] | None = None,
        batch_size: int | None = None,
        informative_nulls: "InformativeNullOpts | dict | None" = None,
    ):
        self.path = str(path)
        if engine != "":
            print(f"Engine is deprecated as all calls use the new polars_readstat_rs rust engine.", flush=True)

        if use_mmap:
            print(f"use_mmap is deprecated as it has not been implemented in the polars_readstat_rs rust engine.", flush=True)

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
    compress: "CompressOptions | Dict[str, Any] | None",
) -> CompressOptions | None:
    if compress is None:
        return None
    if isinstance(compress, CompressOptions):
        return compress
    if isinstance(compress, dict):
        return CompressOptions(**compress)
    raise TypeError(f"compress must be CompressOptions, dict, or None, got {type(compress)}")


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
    columns: list[str] | None = None,
    preserve_order: bool | PreserveOrderOpts | Dict[str, Any] = False,
    compress: CompressOptions | Dict[str, Any] | None = None,
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
    compress : CompressOptions or dict, optional
        Apply type compression after scan (narrow numeric, date/datetime, strings).
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
        print(f"Engine is deprecated as all calls use the new polars_readstat_rs rust engine.", flush=True)
    if use_mmap:
        print(f"use_mmap is deprecated as it has not been implemented in the polars_readstat_rs rust engine.", flush=True)

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
        base_schema = reader.schema
        if schema_overrides:
            new_schema = dict(base_schema)
            for col, dtype in schema_overrides.items():
                if col in new_schema:
                    new_schema[col] = dtype
            return pl.Schema(new_schema)
        return base_schema

    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None = None,
    ) -> Iterator[pl.DataFrame]:
        if batch_size is None:
            batch_size = reader.batch_size
        if batch_size is None:
            batch_size = 100_000

        # Prefer explicit columns arg, otherwise use Polars pushdown.
        use_columns = columns if columns is not None else with_columns

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
        if use_columns is not None:
            src.set_with_columns(use_columns)

        while (out := src.next()) is not None:
            if predicate is not None:
                out = out.filter(predicate)

            if schema_overrides:
                cols_to_cast = {}
                for col, dtype in schema_overrides.items():
                    if col in out.columns:
                        cols_to_cast[col] = dtype
                if cols_to_cast:
                    out = out.cast(cols_to_cast)

            yield out

    lf = register_io_source(io_source=source_generator, schema=schema_generator())
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
    compress: CompressOptions | Dict[str, Any] | None = None,
    informative_nulls: "InformativeNullOpts | dict | None" = None,
) -> pl.DataFrame:
    """
    Read a ReadStat file (SAS, SPSS, Stata) into a Polars DataFrame using the
    Rust streaming read path.

    Parameters
    ----------
    compress : CompressOptions or dict, optional
        Apply Rust-side dual-pass compression (schema probe + cast) while reading.
    informative_nulls : InformativeNullOpts or dict, optional
        Capture user-defined missing value indicators as parallel indicator columns.
    """
    path = str(path)
    if engine != "":
        print(f"Engine is deprecated as all calls use the new polars_readstat_rs rust engine.", flush=True)
    if use_mmap:
        print(f"use_mmap is deprecated as it has not been implemented in the polars_readstat_rs rust engine.", flush=True)

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
    **kwargs : Any
        Stata supports `compress` (bool), `threads` (int),
        `value_labels` (dict[str, dict[int, str]]) and `variable_labels` (dict[str, str]).
        SPSS supports `value_labels` (dict[str, dict[float|int, str]]) and
        `variable_labels` (dict[str, str]).
    """
    path = str(path)
    fmt = (format or Path(path).suffix.lstrip(".")).lower()

    if isinstance(df, pl.LazyFrame):
        # if fmt in ("dta", "stata"):
        #     sink_readstat(df, path, format=fmt, **kwargs)
        #     return
        df = df.collect()
    if not isinstance(df, pl.DataFrame):
        raise TypeError("df must be a polars DataFrame or LazyFrame")

    # pyo3-polars currently fails on narrow unsigned write inputs.
    df = df.with_columns(
        pl.col(pl.UInt8).cast(pl.Int16),
        pl.col(pl.UInt16).cast(pl.Int32),
    )

    if fmt in ("dta", "stata"):
        compress = kwargs.pop("compress", None)
        threads = kwargs.pop("threads", None)
        value_labels = kwargs.pop("value_labels", None)
        variable_labels = kwargs.pop("variable_labels", None)
        if kwargs:
            raise TypeError(f"Unsupported kwargs for Stata writer: {sorted(kwargs.keys())}")
        write_stata(
            df,
            path,
            compress=compress,
            threads=threads,
            value_labels=value_labels,
            variable_labels=variable_labels,
        )
        return
    if fmt in ("sav", "zsav", "spss"):
        compress = kwargs.pop("compress", None)
        if compress is not None:
            warnings.warn(
                "compress has no effect for SPSS outputs.",
                UserWarning,
                stacklevel=2,
            )
        value_labels = kwargs.pop("value_labels", None)
        variable_labels = kwargs.pop("variable_labels", None)
        if kwargs:
            raise TypeError(f"Unsupported kwargs for SPSS writer: {sorted(kwargs.keys())}")
        write_spss(df, path, value_labels=value_labels, variable_labels=variable_labels)
        return
    if fmt in ("sas7bdat", "sas"):
        raise NotImplementedError("SAS writing is not supported yet")

    raise ValueError(f"Unsupported output format: {fmt}")


# def sink_readstat(
#     lf: pl.LazyFrame,
#     path: Any,
#     *,
#     format: str | None = None,
#     **kwargs: Any,
# ) -> None:
#     """
#     Sink a LazyFrame to a ReadStat-supported file.

#     Notes
#     -----
#     - Stata (`.dta`) uses Rust batch streaming via `sink_batches`.
#     - SPSS (`.sav`/`.zsav`) currently materializes and writes non-streaming.
#     """
#     if not isinstance(lf, pl.LazyFrame):
#         raise TypeError("lf must be a polars LazyFrame")

#     path = str(path)
#     fmt = (format or Path(path).suffix.lstrip(".")).lower()

#     if fmt in ("dta", "stata"):
#         compress = kwargs.pop("compress", None)
#         threads = kwargs.pop("threads", None)
#         value_labels = kwargs.pop("value_labels", None)
#         variable_labels = kwargs.pop("variable_labels", None)
#         batch_size = kwargs.pop("batch_size", None)
#         preserve_order = kwargs.pop("preserve_order", True)
#         if kwargs:
#             raise TypeError(f"Unsupported kwargs for Stata sink: {sorted(kwargs.keys())}")
#         warnings.warn(
#             "Stata sink streaming is currently disabled in Python; "
#             "falling back to materialized write.",
#             RuntimeWarning,
#             stacklevel=2,
#         )
#         try:
#             df = lf.collect(engine="streaming")
#         except Exception:
#             df = lf.collect()
#         write_readstat(
#             df,
#             path,
#             format="dta",
#             compress=compress,
#             threads=threads,
#             value_labels=value_labels,
#             variable_labels=variable_labels,
#         )
#         return

#     if fmt in ("sav", "zsav", "spss"):
#         warnings.warn(
#             "SPSS sink currently materializes LazyFrame before writing; output is not streamed.",
#             RuntimeWarning,
#             stacklevel=2,
#         )
#         write_readstat(lf.collect(engine="streaming"), path, format=fmt, **kwargs)
#         return

#     if fmt in ("sas7bdat", "sas"):
#         raise NotImplementedError("SAS writing is not supported yet")

#     raise ValueError(f"Unsupported output format: {fmt}")
