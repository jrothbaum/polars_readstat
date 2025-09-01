from __future__ import annotations
from typing import Any, Iterator, Optional
from polars.io.plugins import register_io_source
import polars as pl
from polars_readstat.polars_readstat_rs import PyPolarsReadstat

class ScanReadstat:
    def __init__(self,
                 path:str,
                 engine:str="readstat",
                 threads:int | None=None):
        self.path = path
        self.engine = self._validation_check(path,
                                             engine)
        if threads is None:
            threads = pl.thread_pool_size()

        self.threads = threads

        self._metadata = None
        self._df = None
        self._schema = None

    @property
    def df(self) -> pl.LazyFrame:
        return scan_readstat(self.path,
                             self.df)

    @property
    def schema(self) -> pl.Schema:
        if self._schema is None:
            self._get_schema()

        return self._schema
    

    @property
    def df(self) -> pl.LazyFrame:
        return scan_readstat(self.path,
                             self.engine)
    
    @property
    def metadata(self) -> dict:
        if self._schema is None:
            self._get_schema()

        return self._metadata
        
    
    def _get_schema(self) -> None:
        src = PyPolarsReadstat(path=self.path,
                               size_hint=10_000,
                               n_rows=1,
                               threads=self.threads,
                               engine=self.engine)

        self._schema = src.schema()
        self._metadata = src.get_metadata()

    def _validation_check(self,
                          path:str,
                          engine:str) -> str:
        valid_files = [".sas7bdat",
                        ".dta",
                        ".sav"]
        is_valid = False
        for fi in valid_files:
            is_valid = is_valid or path.endswith(fi)

        if not is_valid:
            message = f"{path} is not a valid file for polars_readstat.  It must be one of these: {valid_files} ( is not a valid file )"
            raise Exception(message)
        
        if path.endswith(".sas7bdat") and engine not in ["cpp","readstat"]:
            print(f"{engine} is not a valid reader for sas7bdat files.  Defaulting to cpp.",
                    flush=True)
            engine = "cpp"
        if not path.endswith(".sas7bdat") and engine == "cpp":
            print(f"{engine} is not a valid reader for anything but sas7bdat files.  Defaulting to readstat.",
                    flush=True)
            engine = "readstat"

        return engine
def scan_readstat(path:str,
                  engine:str="readstat",
                  threads:int|None=None,
                  reader:PyPolarsReadstat | None=None) -> pl.LazyFrame:
    if reader is None:
        reader = ScanReadstat(path=path,
                            engine=engine,
                            threads=threads)

    def schema() -> pl.Schema:
        return reader.schema
        
        
    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None=None,
    ) -> Iterator[pl.DataFrame]:
        if batch_size is None:
            if engine == "cpp":
                batch_size = 100_000
            else:
                batch_size = 10_000


        src = PyPolarsReadstat(path=path,
                               size_hint=batch_size,
                               n_rows=n_rows,
                               threads=reader.threads,
                               engine=engine)
        
        if with_columns is not None: 
            src.set_with_columns(with_columns)
            
        schema = src.schema()

        
        
        while (out := src.next()) is not None:
            if predicate is not None:
                out = out.filter(predicate)
            yield out
        
    out = register_io_source(io_source=source_generator, schema=schema())
    
    return out

def _validation_check(path:str,
                      engine:str) -> str:
    valid_files = [".sas7bdat",
                   ".dta",
                   ".sav"]
    is_valid = False
    for fi in valid_files:
        is_valid = is_valid or path.endswith(fi)

    if not is_valid:
        message = f"{path} is not a valid file for polars_readstat.  It must be one of these: {valid_files} ( is not a valid file )"
        raise Exception(message)
    
    if path.endswith(".sas7bdat") and engine not in ["cpp","readstat"]:
        print(f"{engine} is not a valid reader for sas7bdat files.  Defaulting to cpp.",
                flush=True)
        engine = "cpp"
    if not path.endswith(".sas7bdat") and engine == "cpp":
        print(f"{engine} is not a valid reader for anything but sas7bdat files.  Defaulting to readstat.",
                flush=True)
        engine = "readstat"

    return engine