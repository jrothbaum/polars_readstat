from __future__ import annotations
from typing import Any, Iterator, Dict
from pathlib import Path
from polars.io.plugins import register_io_source
import polars as pl
from polars_readstat.polars_readstat_rs import PyPolarsReadstat

class ScanReadstat:
    def __init__(
        self,
        path: str,
        engine: str = "cpp",
        use_mmap: bool = False,
        threads: int | None = None,
        schema_overrides: Dict[Any, Any] | None = None,
    ):
        self.path = str(path)
        self.engine = self._validation_check(self.path, engine)
        
        if threads is None:
            threads = pl.thread_pool_size()
        self.threads = threads

        self._metadata = None
        self._schema = None
        self.use_mmap = use_mmap
        self.schema_overrides = schema_overrides

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
        return scan_readstat(self.path, engine=self.engine, schema_overrides=self.schema_overrides)
        
    def _get_schema(self) -> None:
        src = PyPolarsReadstat(
            path=self.path,
            size_hint=10_000,
            n_rows=1,
            threads=self.threads,
            engine=self.engine,
            use_mmap=self.use_mmap
        )
        self._schema = src.schema()
        self._metadata = src.get_metadata()

    def _validation_check(self, path: str, engine: str) -> str:
        valid_files = [".sas7bdat", ".dta", ".sav", ".zsav"]
        is_valid = False
        for fi in valid_files:
            is_valid = is_valid or path.endswith(fi)

        if not is_valid:
            message = f"{path} is not a valid file for polars_readstat. It must be one of these: {valid_files}"
            raise Exception(message)
        
        if path.endswith(".sas7bdat") and engine not in ["cpp", "readstat"]:
            if engine == "":
                pass
                # print("Defaulting to cpp engine for reading sas file")
            else:
                print(f"{engine} is not a valid reader for sas7bdat files. Defaulting to cpp.", flush=True)
            engine = "cpp"
        
        if not path.endswith(".sas7bdat") and engine == "cpp":
            print(f"{engine} is not a valid reader for anything but sas7bdat files. Defaulting to readstat.", flush=True)
            engine = "readstat"
        if not path.endswith(".sas7bdat") and engine == "":
            # print("Defaulting to readstat engine")
            engine = "readstat"

        return engine

def scan_readstat(
    path: Any,
    engine: str = "",
    threads: int | None = None,
    use_mmap: bool = False,
    reader: ScanReadstat | None = None,
    schema_overrides: Dict[Any, Any] | None = None
) -> pl.LazyFrame:
    """
    Scans a ReadStat file (SAS, SPSS, Stata) into a Polars LazyFrame.
    
    Parameters
    ----------
    path : str
        Path to the file.
    engine : str, optional
        'readstat' or 'cpp' (for sas7bdat).
    threads : int, optional
        Number of threads to use.
    use_mmap : bool, optional
        Use memory mapping for file reading.
    reader : ScanReadstat, optional
        Internal use.
    schema_overrides : dict, optional
        A dictionary mapping column names to Polars DataTypes. 
        Used to force specific types (e.g., Int64) to prevent overflow errors 
        when the schema inferred from the header differs from data in the file body.
    """
    path = str(path)

    if reader is None:
        reader = ScanReadstat(
            path=path,
            engine=engine,
            threads=threads,
            use_mmap=use_mmap,
            schema_overrides=schema_overrides
        )
        engine = reader.engine

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
            if engine == "cpp":
                batch_size = 100_000
            else:
                batch_size = 10_000

        src = PyPolarsReadstat(
            path=path,
            size_hint=batch_size,
            n_rows=n_rows,
            threads=reader.threads,
            engine=engine,
            use_mmap=use_mmap
        )
        
        if with_columns is not None: 
            src.set_with_columns(with_columns)
            
        while (out := src.next()) is not None:
            if predicate is not None:
                out = out.filter(predicate)
            
            # Apply schema overrides (cast) immediately on the processed chunk
            if schema_overrides:
                cols_to_cast = {}
                for col, dtype in schema_overrides.items():
                    if col in out.columns:
                        cols_to_cast[col] = dtype
                
                if cols_to_cast:
                    out = out.cast(cols_to_cast)

            yield out
        
    return register_io_source(io_source=source_generator, schema=schema_generator())