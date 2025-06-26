from __future__ import annotations
from typing import Any, Iterator, Optional
from polars.io.plugins import register_io_source
import polars as pl
from polars_readstat.polars_readstat_rs import (read_readstat,
                                                read_cppsas_py)

def scan_readstat(path:str,
                  engine:str="cpp") -> pl.LazyFrame:
    if path.endswith(".sas7bdat") and engine not in ["cpp","readstat"]:
        engine = "cpp"
        print(f"{engine} is not a valid reader for sas7bdat files.  Defaulting to cpp.",
                flush=True)

    def schema() -> pl.Schema:
        if path.endswith(".sas7bdat") and engine == "cpp":
            src = read_cppsas_py(path,
                                 1, 
                                 1, 
                                 None)
            return src.schema()
        else:
            src = read_readstat(path,
                                0, 
                                0,
                                1)
            return src.schema()
    
    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None=1_000_000,
    ) -> Iterator[pl.DataFrame]:
        if path.endswith(".sas7bdat") and engine == "cpp":
            if with_columns is not None: 
                print(with_columns)
            src = read_cppsas_py(path,
                                 batch_size, 
                                 n_rows, 
                                 with_columns)
            schema = src.schema()

            
            
            while (out := src.next()) is not None:
                if predicate is not None:
                    out = out.filter(predicate)
                yield out
        else:
            src = read_readstat(path,
                                batch_size,
                                n_rows,
                                threads=pl.thread_pool_size())
            
            schema = src.schema()

            if with_columns is not None: 
                src.set_with_columns(with_columns)
            
            while (out := src.next()) is not None:
                yield out



    return register_io_source(io_source=source_generator, schema=schema())


