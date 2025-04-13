# polars_readstat
Polars IO plugin to read SAS (sas7bdat) and Stata (dta) files

## :key: Dependencies
This plugin calls rust bindings to load files in chunks, it  is only possible due to the following _**excellent**_ projects:
- The [ReadStat](https://github.com/WizardMac/ReadStat) C library developed by [Evan Miller](https://www.evanmiller.org)
- The [readstat-rs](https://github.com/curtisalexander/readstat-rs) rust bindings to that [ReadStat](https://github.com/WizardMac/ReadStat) C library developed by Curtis Alexander
- [Polars](https://github.com/pola-rs/polars) (obviously) developed by Ritchie Vink and many others

This takes a modified version of the readstat-rs bindings to readstat's C functions.  I swapped out the now unmaintained arrow2 crate for polars (and removed the CLI and write capabilities) to integrate this into polars.

