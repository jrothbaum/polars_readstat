# polars_readstat
Polars IO plugin to read SAS (sas7bdat), Stata (dta), and SPSS (sav) files

## Basic usage
```
import polars as pl
from polars_readstat import scan_readstat
df_stata = scan_readstat("/path/file.dta")
df_sas = scan_readstat("/path/file.sas7bdat")
df_spss = scan_readstat("/path/file.sav")


# Then do any normal thing you'd do in polars
df_stata = (df_stata.head(1000)
                    .filter(pl.col("a") > 0.5)
                    .select(["b","c"]))
...
df_stata = df_stata.collect()


# For sas7bdat files, there are two "engines"
#   1. readstat:  generally, but not always slower, but less likely to have errors
#                 the default
#   2. cpp:       faster, but more likely to fail loading a file
#                 If it's going to throw an error, it usually does so quickly
df = scan_readstat("/path/file.sas7bdat",
                   engine="readstat")

df = scan_readstat("/path/file.sas7bdat",
                   engine="cpp")


# If you want to get the metadata, use the ScanReadstat python class:

from polars_readstat import ScanReadstat
reader = ScanReadstat(path=path)  # You can pass engine for sas7bdat files, as above
metadata = reader.metadata  # Python dictionary with metadata information
schema = reader.schema  # Polars schema
df = reader.df  # LazyFrame

# Then do any normal thing you'd do in polars

# That's it
```

## :key: Dependencies
This plugin calls rust bindings to load files in chunks, it  is only possible due to the following _**excellent**_ projects:
- The [ReadStat](https://github.com/WizardMac/ReadStat) C library developed by [Evan Miller](https://www.evanmiller.org)
- The [readstat-rs](https://github.com/curtisalexander/readstat-rs) rust bindings to that [ReadStat](https://github.com/WizardMac/ReadStat) C library developed by [Curtis Alexander](https://github.com/curtisalexander)
- The [cpp-sas7bdat](https://github.com/olivia76/cpp-sas7bdat/tree/main/src) C++ library by [Olivia Quinet](https://github.com/olivia76)
- [Polars](https://github.com/pola-rs/polars) (obviously) developed by [Ritchie Vink](https://www.ritchievink.com/) and many others

This takes a modified version of the readstat-rs bindings to readstat's C functions.  My modifications:
- Swapped out the now unmaintained [arrow2](https://github.com/jorgecarleitao/arrow2) crate for [Polars](https://github.com/pola-rs/polars)
- Removed the CLI and write capabilities
- Added read support for Stata (dta) and SPSS (sav) files
- Removed some intermediate steps that resulted in processing full vectors of data repeatedly before creating polars dataframe
- Modified the parsing of SAS and Stata data formats (particularly dates and datetimes) to provide a better (?... hopefully) mapping to polars data types

Because of concerns about the performance of readstat reading large SAS files, I have also started integrating a different engine from the cpp-sas7bdat library.  To do so, I have modified it as follows:
- Added an Arrow sink to read the sas7bdat file to Arrow arrays using the [c++ Arrow library](https://arrow.apache.org/docs/cpp/index.html)
- Updated the package build to use [UV](https://github.com/astral-sh/uv) instead of pip for loading [conan](https://conan.io/) to manage the C++ packages
- Added rust ffi bindings to the C++ code to zero-copy pass the Arrow array to rust and polars 

Other notable features
- Multithreaded using the number of pl.thread_pool_size
- Currently comparable to pandas and pyreadstat or faster (see benchmarks below)

Pending tasks:
- Write support for Stata (dta) and SPSS (sav) files.  Readstat itself cannot write SAS (sas7bdat) files that SAS can read, and I'm not fool enough to try to figure that out.  Also, any workflow that involves SAS should be one-way (SAS->something else) so you should only read SAS files, never write them.
- Unit tests on the data sets used by [pyreadstat](https://github.com/Roche/pyreadstat) to confirm that my output matches theirs

## Benchmark
This was run on my computer, with the following specs (and reading the data from an external SSD):<br>
CPU: AMD Ryzen 7 8845HS w/ Radeon 780M Graphics<br>
Cores: 16<br>
RAM: 14Gi<br>
OS: Linux Mint 22<br>
Last Run: August 31, 2025
Version: 0.6

This is not intended to be a scientific benchmark, just a test of loading realistic files.  The Stata and SAS files used are different.  One is tall and narrow (lots of rows, few columns) and the other is shorter and wider (fewer rows, many more columns).

For each file, I compared 4 different scenarios: 1) load the full file, 2) load a subset of columns, 3) filter to a subet of rows, 4) load a subset of columns and filter to a subset of rows.

All reported times are in seconds using python's time.time() (I know...).

### Compared to Pandas and Pyreadstat (using read_file_multiprocessing for parallel processing in Pyreadstat)
* SAS
  * Subset: False, Filter: False
    * polars (readstat):  5.18
    * polars (cpp):       1.31
    * pandas:             2.07
    * pyreadstat:        10.75
  * Subset: True, Filter: False
    * polars (readstat):  0.66
    * polars (cpp):       0.09
    * pandas:             2.06
    * pyreadstat:         0.46
  * Subset: False, Filter: True
    * polars (readstat):  7.88
    * polars (cpp):       1.56
    * pandas:             3.03
    * pyreadstat:        11.93
  * Subset: True, Filter: True
    * polars (readstat):  0.73
    * polars (cpp):       0.09
    * pandas:             2.09
    * pyreadstat:         0.50

* Stata
  * Subset: False, Filter: False
    * polars (readstat):  1.97
    * pandas:             1.14
    * pyreadstat:         7.46
  * Subset: True, Filter: False
    * polars (readstat):  0.95
    * pandas:             1.18
    * pyreadstat:         2.18
  * Subset: False, Filter: True
    * polars (readstat):  2.03
    * pandas:             0.99
    * pyreadstat:         7.66
  * Subset: True, Filter: True
    * polars (readstat):  0.98
    * pandas:             0.96
    * pyreadstat:         2.24



File details:
* Stata (dta) 
  * 2000 5% sample decennial census file from [ipums](https://usa.ipums.org/usa/)
  * Schema: Schema({'index': Int32, 'YEAR': Int32, 'SAMPLE': Int32, 'SERIAL': Int32, 'HHWT': Float64, 'CLUSTER': Float64, 'STRATA': Int32, 'GQ': Int32, 'PERNUM': Int32, 'PERWT': Float64, 'RELATE': Int32, 'RELATED': Int32, 'SEX': Int32, 'AGE': Int32, 'MARST': Int32, 'BIRTHYR': Int32})
  * Rows: 10,000,000 (limited to 10 million to fit in laptop memory)
* SAS (sas7bdat)
  * [American Community Survey](https://www.census.gov/programs-surveys/acs) 5-year file for Illinois, available [here](https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/) as the sas_pil.zip file.
  * Schema: Schema({'RT': String, 'SERIALNO': String, 'DIVISION': String, 'SPORDER': Float64, 'PUMA': String, 'REGION': String, 'STATE': String, 'ADJINC': String, 'PWGTP': Float64, 'AGEP': Float64, 'CIT': String, 'CITWP': Float64, 'COW': String, 'DDRS': String, 'DEAR': String, 'DEYE': String, 'DOUT': String, 'DPHY': String, 'DRAT': String, 'DRATX': String, 'DREM': String, 'ENG': String, 'FER': String, 'GCL': String, 'GCM': String, 'GCR': String, 'HINS1': String, 'HINS2': String, 'HINS3': String, 'HINS4': String, 'HINS5': String, 'HINS6': String, 'HINS7': String, 'INTP': Float64, 'JWMNP': Float64, 'JWRIP': Float64, 'JWTRNS': String, 'LANX': String, 'MAR': String, 'MARHD': String, 'MARHM': String, 'MARHT': String, 'MARHW': String, 'MARHYP': Float64, 'MIG': String, 'MIL': String, 'MLPA': String, 'MLPB': String, 'MLPCD': String, 'MLPE': String, 'MLPFG': String, 'MLPH': String, 'MLPIK': String, 'MLPJ': String, 'NWAB': String, 'NWAV': String, 'NWLA': String, 'NWLK': String, 'NWRE': String, 'OIP': Float64, 'PAP': Float64, 'RELSHIPP': String, 'RETP': Float64, 'SCH': String, 'SCHG': String, 'SCHL': String, 'SEMP': Float64, 'SEX': String, 'SSIP': Float64, 'SSP': Float64, 'WAGP': Float64, 'WKHP': Float64, 'WKL': String, 'WKWN': Float64, 'WRK': String, 'YOEP': Float64, 'ANC': String, 'ANC1P': String, 'ANC2P': String, 'DECADE': String, 'DIS': String, 'DRIVESP': String, 'ESP': String, 'ESR': String, 'FOD1P': String, 'FOD2P': String, 'HICOV': String, 'HISP': String, 'INDP': String, 'JWAP': String, 'JWDP': String, 'LANP': String, 'MIGPUMA': String, 'MIGSP': String, 'MSP': String, 'NAICSP': String, 'NATIVITY': String, 'NOP': String, 'OC': String, 'OCCP': String, 'PAOC': String, 'PERNP': Float64, 'PINCP': Float64, 'POBP': String, 'POVPIP': Float64, 'POWPUMA': String, 'POWSP': String, 'PRIVCOV': String, 'PUBCOV': String, 'QTRBIR': String, 'RAC1P': String, 'RAC2P19': String, 'RAC2P23': String, 'RAC3P': String, 'RACAIAN': String, 'RACASN': String, 'RACBLK': String, 'RACNH': String, 'RACNUM': String, 'RACPI': String, 'RACSOR': String, 'RACWHT': String, 'RC': String, 'SCIENGP': String, 'SCIENGRLP': String, 'SFN': String, 'SFR': String, 'SOCP': String, 'VPS': String, 'WAOB': String, 'FAGEP': String, 'FANCP': String, 'FCITP': String, 'FCITWP': String, 'FCOWP': String, 'FDDRSP': String, 'FDEARP': String, 'FDEYEP': String, 'FDISP': String, 'FDOUTP': String, 'FDPHYP': String, 'FDRATP': String, 'FDRATXP': String, 'FDREMP': String, 'FENGP': String, 'FESRP': String, 'FFERP': String, 'FFODP': String, 'FGCLP': String, 'FGCMP': String, 'FGCRP': String, 'FHICOVP': String, 'FHINS1P': String, 'FHINS2P': String, 'FHINS3C': String, 'FHINS3P': String, 'FHINS4C': String, 'FHINS4P': String, 'FHINS5C': String, 'FHINS5P': String, 'FHINS6P': String, 'FHINS7P': String, 'FHISP': String, 'FINDP': String, 'FINTP': String, 'FJWDP': String, 'FJWMNP': String, 'FJWRIP': String, 'FJWTRNSP': String, 'FLANP': String, 'FLANXP': String, 'FMARP': String, 'FMARHDP': String, 'FMARHMP': String, 'FMARHTP': String, 'FMARHWP': String, 'FMARHYP': String, 'FMIGP': String, 'FMIGSP': String, 'FMILPP': String, 'FMILSP': String, 'FOCCP': String, 'FOIP': String, 'FPAP': String, 'FPERNP': String, 'FPINCP': String, 'FPOBP': String, 'FPOWSP': String, 'FPRIVCOVP': String, 'FPUBCOVP': String, 'FRACP': String, 'FRELSHIPP': String, 'FRETP': String, 'FSCHGP': String, 'FSCHLP': String, 'FSCHP': String, 'FSEMP': String, 'FSEXP': String, 'FSSIP': String, 'FSSP': String, 'FWAGP': String, 'FWKHP': String, 'FWKLP': String, 'FWKWNP': String, 'FWRKP': String, 'FYOEP': String, 'PWGTP1': Float64, 'PWGTP2': Float64, 'PWGTP3': Float64, 'PWGTP4': Float64, 'PWGTP5': Float64, 'PWGTP6': Float64, 'PWGTP7': Float64, 'PWGTP8': Float64, 'PWGTP9': Float64, 'PWGTP10': Float64, 'PWGTP11': Float64, 'PWGTP12': Float64, 'PWGTP13': Float64, 'PWGTP14': Float64, 'PWGTP15': Float64, 'PWGTP16': Float64, 'PWGTP17': Float64, 'PWGTP18': Float64, 'PWGTP19': Float64, 'PWGTP20': Float64, 'PWGTP21': Float64, 'PWGTP22': Float64, 'PWGTP23': Float64, 'PWGTP24': Float64, 'PWGTP25': Float64, 'PWGTP26': Float64, 'PWGTP27': Float64, 'PWGTP28': Float64, 'PWGTP29': Float64, 'PWGTP30': Float64, 'PWGTP31': Float64, 'PWGTP32': Float64, 'PWGTP33': Float64, 'PWGTP34': Float64, 'PWGTP35': Float64, 'PWGTP36': Float64, 'PWGTP37': Float64, 'PWGTP38': Float64, 'PWGTP39': Float64, 'PWGTP40': Float64, 'PWGTP41': Float64, 'PWGTP42': Float64, 'PWGTP43': Float64, 'PWGTP44': Float64, 'PWGTP45': Float64, 'PWGTP46': Float64, 'PWGTP47': Float64, 'PWGTP48': Float64, 'PWGTP49': Float64, 'PWGTP50': Float64, 'PWGTP51': Float64, 'PWGTP52': Float64, 'PWGTP53': Float64, 'PWGTP54': Float64, 'PWGTP55': Float64, 'PWGTP56': Float64, 'PWGTP57': Float64, 'PWGTP58': Float64, 'PWGTP59': Float64, 'PWGTP60': Float64, 'PWGTP61': Float64, 'PWGTP62': Float64, 'PWGTP63': Float64, 'PWGTP64': Float64, 'PWGTP65': Float64, 'PWGTP66': Float64, 'PWGTP67': Float64, 'PWGTP68': Float64, 'PWGTP69': Float64, 'PWGTP70': Float64, 'PWGTP71': Float64, 'PWGTP72': Float64, 'PWGTP73': Float64, 'PWGTP74': Float64, 'PWGTP75': Float64, 'PWGTP76': Float64, 'PWGTP77': Float64, 'PWGTP78': Float64, 'PWGTP79': Float64, 'PWGTP80': Float64})
  * Rows: 623,757 

