# polars_readstat
Polars IO plugin to read SAS (sas7bdat) and Stata (dta) files

## :key: Dependencies
This plugin calls rust bindings to load files in chunks, it  is only possible due to the following _**excellent**_ projects:
- The [ReadStat](https://github.com/WizardMac/ReadStat) C library developed by [Evan Miller](https://www.evanmiller.org)
- The [readstat-rs](https://github.com/curtisalexander/readstat-rs) rust bindings to that [ReadStat](https://github.com/WizardMac/ReadStat) C library developed by [Curtis Alexander](https://github.com/curtisalexander)
- [Polars](https://github.com/pola-rs/polars) (obviously) developed by [Ritchie Vink](https://www.ritchievink.com/) and many others

This takes a modified version of the readstat-rs bindings to readstat's C functions.  My modifications:
- Swapped out the now unmaintained [arrow2](https://github.com/jorgecarleitao/arrow2) crate for [Polars](https://github.com/pola-rs/polars)
- Removed the CLI and write capabilities
- Added read support for Stata (dta) files
- Removed some intermediate steps that resulted in processing full vectors of data repeatedly before creating polars dataframe
- Modified the parsing of SAS and Stata data formats (particularly dates and datetimes) to provide a better (?... hopefully) mapping to polars data types

Other notable features
- Multithreaded using the number of pl.thread_pool_size
- Currently slower than (but close-ish to) pandas on reading a large SAS and Stata file, but with speed-up for loading of subset of variables relative to pandas

Pending tasks:
- Write support for Stata (dta) files.  Readstat itself cannot write SAS (sas7bdat) files that SAS can read, and I'm not fool enough to try to figure that out.  Also, any workflow that involves SAS should be one-way (SAS->something else) so you should only read SAS files, never write them.
- Unit tests on the data sets used by [pyreadstat](https://github.com/Roche/pyreadstat) to confirm that my output matches theirs