# polars_readstat
Polars IO plugin to read SAS (sas7bdat), Stata (dta), and SPSS (sav) files

## :key: Dependencies
This plugin calls rust bindings to load files in chunks, it  is only possible due to the following _**excellent**_ projects:
- The [ReadStat](https://github.com/WizardMac/ReadStat) C library developed by [Evan Miller](https://www.evanmiller.org)
- The [readstat-rs](https://github.com/curtisalexander/readstat-rs) rust bindings to that [ReadStat](https://github.com/WizardMac/ReadStat) C library developed by [Curtis Alexander](https://github.com/curtisalexander)
- [Polars](https://github.com/pola-rs/polars) (obviously) developed by [Ritchie Vink](https://www.ritchievink.com/) and many others

This takes a modified version of the readstat-rs bindings to readstat's C functions.  My modifications:
- Swapped out the now unmaintained [arrow2](https://github.com/jorgecarleitao/arrow2) crate for [Polars](https://github.com/pola-rs/polars)
- Removed the CLI and write capabilities
- Added read support for Stata (dta) and SPSS (sav) files
- Removed some intermediate steps that resulted in processing full vectors of data repeatedly before creating polars dataframe
- Modified the parsing of SAS and Stata data formats (particularly dates and datetimes) to provide a better (?... hopefully) mapping to polars data types

Other notable features
- Multithreaded using the number of pl.thread_pool_size
- Currently comparable to pandas (see benchmarks below) on full reads and faster when reading subsets of columns

Pending tasks:
- Write support for Stata (dta) and SPSS (sav) files.  Readstat itself cannot write SAS (sas7bdat) files that SAS can read, and I'm not fool enough to try to figure that out.  Also, any workflow that involves SAS should be one-way (SAS->something else) so you should only read SAS files, never write them.
- Unit tests on the data sets used by [pyreadstat](https://github.com/Roche/pyreadstat) to confirm that my output matches theirs


## Benchmark
This was run on my computer, with the following specs (and reading the data from an external SSD):<br>
CPU: AMD Ryzen 7 8845HS w/ Radeon 780M Graphics<br>
Cores: 16<br>
RAM: 14Gi<br>
OS: Linux Mint 22<br>

This is not intended to be a scientific benchmark, just a test of loading realistic files.  The Stata and SAS files used are different.  One is tall and narrow (lots of rows, few columns) and the other is shorter and wider (fewer rows, many more columns).

For each file, I compared 4 different scenarios: 1) load the full file, 2) load a subset of columns, 3) filter to a subet of rows, 4) load a subset of columns and filter to a subset of rows.

All reported times are in seconds using python's time.time() (I know...).

### Compared to Pyreadstat (using read_file_multiprocessing for parallel processing)
* Stata
  * Subset: False, Filter: False
    * Polars: 2.245
    * Pyreadstat: 7.200
    * Are identical: True
  * Subset: True, Filter: False
    * Polars: 0.892
    * Pyreadstat: 2.209
    * Are identical: True
  * Subset: False, Filter: True
    * Polars: 2.250
    * Pyreadstat: 7.598
    * Are identical: True
  * Subset: True, Filter: True
    * Polars: 0.892
    * Pyreadstat: 2.361
    * Are identical: True

* SAS
  * Subset: False, Filter: False
    * Polars: 5.262
    * Pyreadstat: 13.693
    * Are identical: True
  * Subset: True, Filter: False
    * Polars: 2.575
    * Pyreadstat: 0.855
    * Are identical: True
  * Subset: False, Filter: True
    * Polars: 3.912
    * Pyreadstat: 13.485
    * Are identical: True
  * Subset: True, Filter: True
    * Polars: 1.0439
    * Pyreadstat: 1.445
    * Are identical: True




### Compared to Pandas
* Stata
  * Subset: False, Filter: False
    * Polars: 2.312
    * Pandas: 1.276
    * Are identical: True
  * Subset: True, Filter: False
    * Polars: 0.877
    * Pandas: 1.312
    * Are identical: True
  * Subset: False, Filter: True
    * Polars: 2.335
    * Pandas: 1.366
    * Are identical: True
  * Subset: True, Filter: True
    * Polars: 0.908
    * Pandas: 1.378
    * Are identical: True
* SAS
  * Subset: False, Filter: False
    * Polars: 3.843
    * Pandas: 8.432
    * Are identical: False
  * Subset: True, Filter: False
    * Polars: 1.027
    * Pandas: 4.125
    * Are identical: True
  * Subset: False, Filter: True
    * Polars: 3.848
    * Pandas: 8.534
    * Are identical: False
  * Subset: True, Filter: True
    * Polars: 1.624
    * Pandas: 4.531
    * Are identical: True



File details:
* Stata (dta) 
  * 2000 5% sample decennial census file from [ipums](https://usa.ipums.org/usa/)
  * Schema: Schema({'index': Int32, 'YEAR': Int32, 'SAMPLE': Int32, 'SERIAL': Int32, 'HHWT': Float64, 'CLUSTER': Float64, 'STRATA': Int32, 'GQ': Int32, 'PERNUM': Int32, 'PERWT': Float64, 'RELATE': Int32, 'RELATED': Int32, 'SEX': Int32, 'AGE': Int32, 'MARST': Int32, 'BIRTHYR': Int32})
  * Rows: 14,081,466
* SAS (sas7bdat)
  * [American Community Survey](https://www.census.gov/programs-surveys/acs) 5-year file for Illinois, available [here](https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/) as the sas_pil.zip file.
  * Schema: Schema({'RT': String, 'SERIALNO': String, 'DIVISION': String, 'SPORDER': Float64, 'PUMA': String, 'REGION': String, 'STATE': String, 'ADJINC': String, 'PWGTP': Float64, 'AGEP': Float64, 'CIT': String, 'CITWP': Float64, 'COW': String, 'DDRS': String, 'DEAR': String, 'DEYE': String, 'DOUT': String, 'DPHY': String, 'DRAT': String, 'DRATX': String, 'DREM': String, 'ENG': String, 'FER': String, 'GCL': String, 'GCM': String, 'GCR': String, 'HINS1': String, 'HINS2': String, 'HINS3': String, 'HINS4': String, 'HINS5': String, 'HINS6': String, 'HINS7': String, 'INTP': Float64, 'JWMNP': Float64, 'JWRIP': Float64, 'JWTRNS': String, 'LANX': String, 'MAR': String, 'MARHD': String, 'MARHM': String, 'MARHT': String, 'MARHW': String, 'MARHYP': Float64, 'MIG': String, 'MIL': String, 'MLPA': String, 'MLPB': String, 'MLPCD': String, 'MLPE': String, 'MLPFG': String, 'MLPH': String, 'MLPIK': String, 'MLPJ': String, 'NWAB': String, 'NWAV': String, 'NWLA': String, 'NWLK': String, 'NWRE': String, 'OIP': Float64, 'PAP': Float64, 'RELSHIPP': String, 'RETP': Float64, 'SCH': String, 'SCHG': String, 'SCHL': String, 'SEMP': Float64, 'SEX': String, 'SSIP': Float64, 'SSP': Float64, 'WAGP': Float64, 'WKHP': Float64, 'WKL': String, 'WKWN': Float64, 'WRK': String, 'YOEP': Float64, 'ANC': String, 'ANC1P': String, 'ANC2P': String, 'DECADE': String, 'DIS': String, 'DRIVESP': String, 'ESP': String, 'ESR': String, 'FOD1P': String, 'FOD2P': String, 'HICOV': String, 'HISP': String, 'INDP': String, 'JWAP': String, 'JWDP': String, 'LANP': String, 'MIGPUMA': String, 'MIGSP': String, 'MSP': String, 'NAICSP': String, 'NATIVITY': String, 'NOP': String, 'OC': String, 'OCCP': String, 'PAOC': String, 'PERNP': Float64, 'PINCP': Float64, 'POBP': String, 'POVPIP': Float64, 'POWPUMA': String, 'POWSP': String, 'PRIVCOV': String, 'PUBCOV': String, 'QTRBIR': String, 'RAC1P': String, 'RAC2P19': String, 'RAC2P23': String, 'RAC3P': String, 'RACAIAN': String, 'RACASN': String, 'RACBLK': String, 'RACNH': String, 'RACNUM': String, 'RACPI': String, 'RACSOR': String, 'RACWHT': String, 'RC': String, 'SCIENGP': String, 'SCIENGRLP': String, 'SFN': String, 'SFR': String, 'SOCP': String, 'VPS': String, 'WAOB': String, 'FAGEP': String, 'FANCP': String, 'FCITP': String, 'FCITWP': String, 'FCOWP': String, 'FDDRSP': String, 'FDEARP': String, 'FDEYEP': String, 'FDISP': String, 'FDOUTP': String, 'FDPHYP': String, 'FDRATP': String, 'FDRATXP': String, 'FDREMP': String, 'FENGP': String, 'FESRP': String, 'FFERP': String, 'FFODP': String, 'FGCLP': String, 'FGCMP': String, 'FGCRP': String, 'FHICOVP': String, 'FHINS1P': String, 'FHINS2P': String, 'FHINS3C': String, 'FHINS3P': String, 'FHINS4C': String, 'FHINS4P': String, 'FHINS5C': String, 'FHINS5P': String, 'FHINS6P': String, 'FHINS7P': String, 'FHISP': String, 'FINDP': String, 'FINTP': String, 'FJWDP': String, 'FJWMNP': String, 'FJWRIP': String, 'FJWTRNSP': String, 'FLANP': String, 'FLANXP': String, 'FMARP': String, 'FMARHDP': String, 'FMARHMP': String, 'FMARHTP': String, 'FMARHWP': String, 'FMARHYP': String, 'FMIGP': String, 'FMIGSP': String, 'FMILPP': String, 'FMILSP': String, 'FOCCP': String, 'FOIP': String, 'FPAP': String, 'FPERNP': String, 'FPINCP': String, 'FPOBP': String, 'FPOWSP': String, 'FPRIVCOVP': String, 'FPUBCOVP': String, 'FRACP': String, 'FRELSHIPP': String, 'FRETP': String, 'FSCHGP': String, 'FSCHLP': String, 'FSCHP': String, 'FSEMP': String, 'FSEXP': String, 'FSSIP': String, 'FSSP': String, 'FWAGP': String, 'FWKHP': String, 'FWKLP': String, 'FWKWNP': String, 'FWRKP': String, 'FYOEP': String, 'PWGTP1': Float64, 'PWGTP2': Float64, 'PWGTP3': Float64, 'PWGTP4': Float64, 'PWGTP5': Float64, 'PWGTP6': Float64, 'PWGTP7': Float64, 'PWGTP8': Float64, 'PWGTP9': Float64, 'PWGTP10': Float64, 'PWGTP11': Float64, 'PWGTP12': Float64, 'PWGTP13': Float64, 'PWGTP14': Float64, 'PWGTP15': Float64, 'PWGTP16': Float64, 'PWGTP17': Float64, 'PWGTP18': Float64, 'PWGTP19': Float64, 'PWGTP20': Float64, 'PWGTP21': Float64, 'PWGTP22': Float64, 'PWGTP23': Float64, 'PWGTP24': Float64, 'PWGTP25': Float64, 'PWGTP26': Float64, 'PWGTP27': Float64, 'PWGTP28': Float64, 'PWGTP29': Float64, 'PWGTP30': Float64, 'PWGTP31': Float64, 'PWGTP32': Float64, 'PWGTP33': Float64, 'PWGTP34': Float64, 'PWGTP35': Float64, 'PWGTP36': Float64, 'PWGTP37': Float64, 'PWGTP38': Float64, 'PWGTP39': Float64, 'PWGTP40': Float64, 'PWGTP41': Float64, 'PWGTP42': Float64, 'PWGTP43': Float64, 'PWGTP44': Float64, 'PWGTP45': Float64, 'PWGTP46': Float64, 'PWGTP47': Float64, 'PWGTP48': Float64, 'PWGTP49': Float64, 'PWGTP50': Float64, 'PWGTP51': Float64, 'PWGTP52': Float64, 'PWGTP53': Float64, 'PWGTP54': Float64, 'PWGTP55': Float64, 'PWGTP56': Float64, 'PWGTP57': Float64, 'PWGTP58': Float64, 'PWGTP59': Float64, 'PWGTP60': Float64, 'PWGTP61': Float64, 'PWGTP62': Float64, 'PWGTP63': Float64, 'PWGTP64': Float64, 'PWGTP65': Float64, 'PWGTP66': Float64, 'PWGTP67': Float64, 'PWGTP68': Float64, 'PWGTP69': Float64, 'PWGTP70': Float64, 'PWGTP71': Float64, 'PWGTP72': Float64, 'PWGTP73': Float64, 'PWGTP74': Float64, 'PWGTP75': Float64, 'PWGTP76': Float64, 'PWGTP77': Float64, 'PWGTP78': Float64, 'PWGTP79': Float64, 'PWGTP80': Float64})
  * Rows: 623,757 

