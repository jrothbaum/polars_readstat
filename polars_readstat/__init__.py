
from typing import Any, Iterator
from polars.io.plugins import register_io_source
import polars as pl


from polars_readstat import read_readstat 

def scan_readstat(path:str) -> pl.LazyFrame:
    
    def schema() -> pl.Schema:
        src = read_readstat(path,
                            0, 
                            0)
        return src.schema()
    
    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        
        src = read_readstat(path,
                            batch_size,
                            n_rows)
        

        if with_columns is not None:
            src.set_with_columns(with_columns)
        # else:
        #     src.set_with_columns(list(schema().keys()))


        # Set the predicate.
        predicate_set = True
        if predicate is not None:
            try:
                src.try_set_predicate(predicate)
            except pl.exceptions.ComputeError:
                predicate_set = False

        while (out := src.next()) is not None:
            # If the source could not apply the predicate
            # (because it wasn't able to deserialize it), we do it here.
            if not predicate_set and predicate is not None:
                out = out.filter(predicate)

            yield out



    return register_io_source(io_source=source_generator, schema=schema())



if __name__ == "__main__":
    def read_test(path:str) -> None:
        print(path)
        df = scan_readstat(path)
        print(df.collect_schema())
        print(df.collect())
        df = df.head(2)
        print(df.collect())
        
        df = df.select("mychar",
                    "mynum",
                    "myord")
        
        print(df.collect_schema())
        df = df.collect()
        print(df)
        print("\n\n\n")
        
    read_test("/home/jrothbaum/python/polars_readstat/crates/polars_readstat/tests/data/sample.sas7bdat")

    read_test("/home/jrothbaum/python/polars_readstat/crates/polars_readstat/tests/data/sample.dta")