# polars-readstat

Polars plugin for SAS (`.sas7bdat`), Stata (`.dta`), and SPSS (`.sav`/`.zsav`) files.

The Python package wraps the Rust core in `polars_readstat_rs` and exposes a Polars-first API.

## Install

```bash
pip install polars-readstat
```

## Quick start

### Reading a SAS, Stata, or SPSS file
```
import polars as pl
from polars_readstat import scan_readstat 

lf = scan_readstat("/path/file.sas7bdat")
df = lf.select(["SERIALNO", "AGEP"]).filter(pl.col("AGEP") >= 18).collect()
```


### Getting metadata from the file

```python
import polars as pl
from polars_readstat import ScanReadstat

sr = ScanReadstat("/path/file.sas7bdat")
metadata = sr.metadata
#   You can also get the lazyframe/dataframe from it to avoid having to re-scan the file 
df = sr.df.collect()
```

### Writing output files

```python
import polars as pl
from polars_readstat import write_readstat, write_sas_csv_import

write_readstat(df, "/path/out.dta")
write_readstat(df, "/path/out.sav")

# Writes two files: /path/out/sas_bundle.csv and /path/out/sas_bundle.sas
write_sas_csv_import(df, "/path/out/sas_bundle", dataset_name="my_data")
```

See [Read](read.md) for the full read API and [Write](write.md) for output options.
