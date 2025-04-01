[![readstat-rs](https://github.com/curtisalexander/readstat-rs/workflows/readstat-rs/badge.svg)](https://github.com/curtisalexander/readstat-rs/actions)
[![Gitpod ready-to-code](https://img.shields.io/badge/Gitpod-ready--to--code-908a85?logo=gitpod)](https://gitpod.io/#https://github.com/curtisalexander/readstat-rs)

# readstat-rs
Command-line tool for working with SAS binary &mdash; `sas7bdat` &mdash; files.

Get [metadata](#metadata), [preview data](#preview-data), or [convert data](#data) to [`csv`](https://en.wikipedia.org/wiki/Comma-separated_values), [`feather`](https://arrow.apache.org/docs/python/feather.html) (or the [Arrow IPC format](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc)), [`ndjson`](https://github.com/ndjson/ndjson-spec), or [`parquet`](https://parquet.apache.org/) formats.

## :key: Dependencies
The command-line tool is developed in Rust and is only possible due to the following _**excellent**_ projects:
- The [ReadStat](https://github.com/WizardMac/ReadStat) C library developed by [Evan Miller](https://www.evanmiller.org)
- The [arrow2](https://github.com/jorgecarleitao/arrow2) Rust crate developed by [Jorge Leitao](https://github.com/jorgecarleitao)

The `ReadStat` library is used to parse and read `sas7bdat` files, and the `arrow2` crate is used to convert the read `sas7bdat` data into the [Arrow](https://arrow.apache.org/) memory format. Once in the `Arrow` memory format, the data can be written to other file formats.

## Quickstart

Convert the first 50,000 rows of `example.sas7bdat` (by performing the read in parallel) to the file `example.parquet`, overwriting the file if it already exists.
```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.parquet --format parquet --rows 50000 --overwrite --parallel
```

## Install

### Download a Release
\[Mostly\] static binaries for Linux, macOS, and Windows may be found at the [Releases page](https://github.com/curtisalexander/readstat-rs/releases/).

### Setup
Move the `readstat` binary to a known directory and add the binary to the user's [PATH](https://en.wikipedia.org/wiki/PATH_(variable)).

#### Linux & macOS
Ensure the path to `readstat` is added to the appropriate shell configuration file.

#### Windows
For Windows users, path configuration may be found within the Environment Variables menu.  Executing the following from the command line opens the Environment Variables menu for the current user.

```sh
rundll32.exe sysdm.cpl,EditEnvironmentVariables
```

### Run
Run the binary.

```sh
readstat --help
```

## Build

### Clone
Ensure submodules are also cloned.

```sh
git clone --recurse-submodules https://github.com/curtisalexander/readstat-rs.git
```

The [ReadStat](https://github.com/WizardMac/ReadStat) repository is included as a [git submodule](https://git-scm.com/book/en/v2/Git-Tools-Submodules) within this repository.  In order to build and link, first a [readstat-sys](https://github.com/curtisalexander/readstat-rs/tree/main/readstat-sys) crate is created.  Then the [readstat](https://github.com/curtisalexander/readstat-rs/tree/main/readstat) binary utilizes `readstat-sys` as a dependency.

### Linux
Install developer tools

```sh
# unixodbc-dev needed for full compilation of arrow2
sudo apt install build-essential clang unixodbc-dev
```

Build
```sh
cargo build
```

### macOS
Install developer tools

```sh
xcode-select --install
```

Build
```sh
cargo build
```

### Windows
Building on Windows requires [LLVM](https://releases.llvm.org/download.html) and [Visual Studio C++ Build tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/) be downloaded and installed.

In addition, the path to `libclang` needs to be set in the environment variable `LIBCLANG_PATH`.  If `LIBCLANG_PATH` is not set then the [readstat-sys build script](crates/readstat-sys/build.rs) assumes the needed path to be `C:\Program Files\LLVM\lib`.

For details see the following.
- [Check for `LIBCLANG_PATH`](https://github.com/curtisalexander/readstat-rs/blob/main/crates/readstat-sys/build.rs#L78-L82)
- [Building in GitHub Actions](https://github.com/curtisalexander/readstat-rs/blob/main/.github/workflows/main.yml#L111-L114)

Build
```sh
cargo build
```

## Run
After either [building](#build) or [installing](#install), the binary is invoked using [subcommands](https://docs.rs/clap/latest/clap/_derive/_tutorial/index.html#subcommands).  Currently, the following subcommands have been implemented:
- `metadata` &rarr; writes the following to standard out or json
    - row count
    - variable count
    - table name
    - table label
    - file encoding
    - format version
    - bitness
    - creation time
    - modified time
    - compression
    - byte order
    - variable names
    - variable type classes
    - variable types
    - variable labels
    - variable format classes
    - variable formats
    - arrow data types
- `preview` &rarr; writes the first 10 rows (or optionally the number of rows provided by the user) of parsed data in `csv` format to standard out
- `data` &rarr; writes parsed data in `csv`, `feather`, `ndjson`, or `parquet` format to a file

### Metadata
To write metadata to standard out, invoke the following.

```sh
readstat metadata /some/dir/to/example.sas7bdat
```

To write metadata to json, invoke the following.  This is useful for reading the metadata programmatically.

```sh
readstat metadata /some/dir/to/example.sas7bdat --as-json
```

### Preview Data
To write parsed data (as a `csv`) to standard out, invoke the following (default is to write the first 10 rows).

```sh
readstat preview /some/dir/to/example.sas7bdat
```

To write the first 100 rows of parsed data (as a `csv`) to standard out, invoke the following.

```sh
readstat preview /some/dir/to/example.sas7bdat --rows 100
```

### Data
:memo: The `data` subcommand includes a parameter for `--format`, which is the file format that is to be written.  Currently, the following formats have been implemented:
- `csv`
- `feather`
- `ndjson`
- `parquet`

#### `csv`
To write parsed data (as `csv`) to a file, invoke the following (default is to write all parsed data to the specified file).

The default `--format` is `csv`.  Thus, the parameter is elided from the below examples.

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.csv
```

To write the first 100 rows of parsed data (as `csv`) to a file, invoke the following.

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.csv --rows 100
```

#### `feather`
To write parsed data (as `feather`) to a file, invoke the following (default is to write all parsed data to the specified file).

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.feather --format feather
```

To write the first 100 rows of parsed data (as `feather`) to a file, invoke the following.

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.feather --format feather --rows 100
```

#### `ndjson`
To write parsed data (as `ndjson`) to a file, invoke the following (default is to write all parsed data to the specified file).

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.ndjson --format ndjson
```

To write the first 100 rows of parsed data (as `ndjson`) to a file, invoke the following.

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.ndjson --format ndjson --rows 100
```

#### `parquet`
To write parsed data (as `parquet`) to a file, invoke the following (default is to write all parsed data to the specified file).

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.parquet --format parquet
```

To write the first 100 rows of parsed data (as `parquet`) to a file, invoke the following.

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.parquet --format parquet --rows 100
```

To write parsed data (as `parquet`) to a file with specific compression settings, invoke the following:

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.parquet --format parquet --compression zstd --compression-level 3
```

### Parallelism
The `data` subcommand includes a parameter for `--parallel` &mdash; if invoked, the _**reading**_ of a `sas7bdat` will occur in parallel.  If the total rows to process is greater than `stream-rows` (if unset, the default rows to stream is 10,000), then each chunk of rows is read in parallel.  Note that all processors on the user's machine are used with the `--parallel` option.  In the future, may consider allowing the user to throttle this number.

Note that although reading is in parallel, _**writing**_ is still sequential.  Thus, one should only anticipate moderate speed-ups as much of the time is spent writing.

:heavy_exclamation_mark: Utilizing the `--parallel` parameter will increase memory usage &mdash; there will be multiple threads simultaneously reading chunks from the `sas7bdat`.  In addition, because all processors are utilized, CPU usage may be maxed out during reading.

:warning: Also, note that utilizing the `--parallel` parameter may write rows out of order from the original `sas7bdat`.

### Reader
The `preview` and `data` subcommands include a parameter for `--reader`.  The possible values for `--reader` include the following.
- `mem` &rarr; Parse and read the entire `sas7bdat` into memory before writing to either standard out or a file
- `stream` (default) &rarr; Parse and read at most `stream-rows` into memory before writing to disk
    - `stream-rows` may be set via the command line parameter `--stream-rows` or if elided will default to 10,000 rows

**Why is this useful?**
- `mem` is useful for testing purposes
- `stream` is useful for keeping memory usage low for large datasets (and hence is the default)
- In general, users should not need to deviate from the default &mdash; `stream` &mdash; unless they have a specific need
- In addition, by enabling these options as command line parameters [hyperfine](#benchmarking) may be used to benchmark across an assortment of file sizes

### Debug
Debug information is printed to standard out by setting the environment variable `RUST_LOG=debug` before the call to `readstat`.

:warning: This is quite verbose!  If using the [preview](#preview-data) or [data](#data) subcommand, will write debug information for _every single value_!

```sh
# Linux and macOS
RUST_LOG=debug readstat ...
```

```powershell
# Windows PowerShell
$env:RUST_LOG="debug"; readstat ...
```

### Help
For full details run with `--help`.

```sh
readstat --help
readstat metadata --help
readstat preview --help
readstat data --help
```

## Floating Point Values
:warning: Decimal values are rounded to contain only 14 decimal digits!

For example, the number `1.1234567890123456` created within SAS would be returned as `1.12345678901235` within Rust.

Why does this happen?  Is this an implementation error?  No, rounding to only 14 decimal digits has been _purposely implemented_ within the Rust code.

As a specific example, when testing with the [cars.sas7bdat](crates/readstat-tests/tests/data/README.md) dataset (which was created originally on Windows), the numeric value `4.6` as observed within SAS was being returned as `4.600000000000001` (15 digits) within Rust.  Values created on Windows with an x64 processor are only accurate to 15 digits.

For comparison, the [ReadStat binary](https://github.com/WizardMac/ReadStat#command-line-usage) [truncates to 14 decimal places](https://github.com/WizardMac/ReadStat/blob/master/src/bin/write/mod_csv.c#L147) when writing to `csv`.

Finally, SAS represents all numeric values in floating-point representation which creates a challenge for **all** parsed numerics!

### Sources
- [How SAS Stores Numeric Values](https://documentation.sas.com/?cdcId=pgmsascdc&cdcVersion=9.4_3.5&docsetId=lrcon&docsetTarget=p0ji1unv6thm0dn1gp4t01a1u0g6.htm&locale=en#n00dmtao82eizen1e6yziw3s31da)
- [Accuracy on x64 Windows Processors](https://documentation.sas.com/?cdcId=pgmsascdc&cdcVersion=9.4_3.5&docsetId=lrcon&docsetTarget=p0ji1unv6thm0dn1gp4t01a1u0g6.htm&locale=en#n0pd8l179ai8odn17nncb4izqq3d)
    - SAS on Windows with x64 processors can only represent 15 digits
- [Floating-point arithmetic may give inaccurate results in Excel](https://docs.microsoft.com/en-us/office/troubleshoot/excel/floating-point-arithmetic-inaccurate-result)

## Date, Time, and Datetimes
Currently any dates, times, or datetimes in the following SAS formats are parsed and read as dates, times, or datetimes.
- Dates
    - [`DATEw.`](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/leforinforref/n16vcb736tge20n1ex3yxx49fzqa.htm)
    - [`DDMMYYw.`](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/leforinforref/n1o9q2mkgoey0sn1uegfwwnm2xwi.htm)
    - [`DDMMYYxw.`](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/leforinforref/n15jgpgn7b87scn1ugjiwnvdag3h.htm)
    - [`MMDDYYw.`](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/leforinforref/n08s3dzq3m0shgn12gtwgdralakv.htm)
    - [`MMDDYYxw.`](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/leforinforref/p1og22ny80wqj5n1a1oxayemz4bt.htm)
    - [`YYMMDDw.`](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/leforinforref/n00fxkkwqijasxn1580tkw8mh5ob.htm)
    - [`YYMMDDxw.`](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/leforinforref/p0iptsg6780kzfn1k5f0b8s7k7dq.htm)
- Times
    - [`TIMEw.d`](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/leforinforref/p0b2xn5ovzhtjnn1db5g1gg64yhf.htm)
- Datetimes
    - [`DATETIMEw.d`](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/leforinforref/n0av4h8lmnktm4n1i33et4wyz5yy.htm)

:warning: If the format does not match one of the above SAS formats, or if the value does not have a format applied, then the value will be parsed and read as a numeric value!

### Details
SAS stores [dates, times, and datetimes](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/lrcon/p1wj0wt2ebe2a0n1lv4lem9hdc0v.htm) internally as numeric values.  To distinguish among dates, times, datetimes, or numeric values, a SAS format is read from the variable metadata.  If the format matches one of the above SAS formats then the numeric value is converted and read into memory using one of the Arrow types:
- [Date32Type](https://docs.rs/arrow/latest/arrow/datatypes/struct.Date32Type.html)
- [Time32SecondType](https://docs.rs/arrow/latest/arrow/datatypes/struct.Time32SecondType.html)
- [TimestampSecondType](https://docs.rs/arrow/latest/arrow/datatypes/struct.TimestampSecondType.html)

If values are read into memory as Arrow date, time, or datetime types, then when they are written &mdash; from an [arrow2 `Chunk`](https://docs.rs/arrow2/latest/arrow2/chunk/struct.Chunk.html) to `csv`, `feather`, `ndjson`, or `parquet` &mdash; they are treated as dates, times, or datetimes and not as numeric values.

Finally, [more work is planned](https://github.com/curtisalexander/readstat-rs/issues/21) to handle other SAS dates, times, and datetimes that have SAS formats other than those listed above.

## Testing
To perform unit / integration tests, run the following.

```
cargo test
```

### Datasets
Formally tested (via integration tests) against the following datasets.  See the [README.md](crates/readstat-tests/tests/data/README.md) for data sources.
- [ ] `ahs2019n.sas7bdat` &rarr; US Census data
- [X] `all_types.sas7bdat` &rarr; SAS dataset containing all SAS types
- [X] `cars.sas7bdat` &rarr; SAS cars dataset
- [X] `hasmissing.sas7bdat` &rarr; SAS dataset containing missing values
- [ ] `intel.sas7bdat`
- [ ] `messydata.sas7bdat`
- [ ] `rand_ds.sas7bdat` &rarr; Created using [create_rand_ds.sas](crates/readstat-tests/util/create_rand_ds.sas)
- [X] `rand_ds_largepage_err.sas7bdat` &rarr; Created using [create_rand_ds.sas](crates/readstat-tests/util/create_rand_ds.sas) with [BUFSIZE](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/ledsoptsref/n0pw7cnugsttken1voc6qo0ye3cg.htm) set to `2M`
- [X] `rand_ds_largepage_ok.sas7bdat` &rarr; Created using [create_rand_ds.sas](crates/readstat-tests/util/create_rand_ds.sas) with [BUFSIZE](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/ledsoptsref/n0pw7cnugsttken1voc6qo0ye3cg.htm) set to `1M`
- [X] `scientific_notation.sas7bdat` &rarr; Used to test float parsing
- [ ] `somedata.sas7bdat`
- [ ] `somemiss.sas7bdat`

### Valgrind
To ensure no memory leaks, [valgrind](https://valgrind.org/) may be utilized.  For example, to ensure no memory leaks for the test `parse_file_metadata_test`, run the following from within the `readstat` directory.

```
valgrind ./target/debug/deps/parse_file_metadata_test-<hash>
```

## [Platform Support](https://doc.rust-lang.org/rustc/platform-support.html)
- :heavy_check_mark: Linux   &rarr; successfully builds and runs
    - [glibc](https://www.gnu.org/software/libc/)
    - [musl](https://www.musl-libc.org/)
- :heavy_check_mark: macOS   &rarr; successfully builds and runs
- :heavy_check_mark: Windows &rarr; successfully builds and runs
    - As of [ReadStat](https://github.com/WizardMac/ReadStat) `1.1.5`, able to build using MSVC in lieu of setting up an msys2 environment
    - [Requires `libclang`](#windows) in order to build as `libclang` is [required by bindgen](https://rust-lang.github.io/rust-bindgen/requirements.html#clang)

## Benchmarking
Benchmarking performed with [hyperfine](https://github.com/sharkdp/hyperfine).

This example compares the performance of the Rust binary with the performance of the C binary built from the `ReadStat` repository.  In general, hope that performance is fairly close to that of the C binary.

To run, execute the following from within the `readstat` directory.

```powershell
# Windows
hyperfine --warmup 5 "ReadStat_App.exe -f crates\readstat-tests\tests\data\cars.sas7bdat tests\data\cars_c.csv" ".\target\release\readstat.exe data crates\readstat-tests\tests\data\cars.sas7bdat --output crates\readstat-tests\tests\data\cars_rust.csv"
```

:memo: First experiments on Windows are challenging to interpret due to file caching.  Need further research into utilizing the `--prepare` option provided by `hyperfine` on Windows.

```sh
# Linux and macOS
hyperfine --prepare "sync; echo 3 | sudo tee /proc/sys/vm/drop_caches" "readstat -f crates/readstat-tests/tests/data/cars.sas7bdat crates/readstat-tests/tests/data/cars_c.csv" "./target/release/readstat data tests/data/cars.sas7bdat --output crates/readstat-tests/tests/data/cars_rust.csv"
```

Other, future, benchmarking may be performed now that [channels and threads](https://github.com/curtisalexander/readstat-rs/issues/28) have been developed.

## Profiling
Profiling performed with [cargo flamegraph](https://github.com/flamegraph-rs/flamegraph).

To run, execute the following from within the `readstat` directory.
```sh
cargo flamegraph --bin readstat -- data tests/data/_ahs2019n.sas7bdat --output tests/data/_ahs2019n.csv
```

Flamegraph is written to `readstat/flamegraph.svg`.

:memo: Have yet to utilize flamegraphs in order to improve performance.

## GitHub Actions
Below is the rough `git tag` dance to delete and/or add tags to [trigger GitHub Actions](https://github.com/curtisalexander/readstat-rs/blob/main/.github/workflows/main.yml#L7-L10).

```sh
# delete local tag
git tag --delete v0.1.0

# delete remote tag
git push origin --delete v0.1.0

# add and commit local changes
git add .
git commit -m "commit msg"

# push local changes to remote
git push

# add local tag
git tag -a v0.1.0 -m "v0.1.0"

# push local tag to remote
git push origin --tags
```

## Goals

### Short Term
Short term, developing the command-line tool was a helpful exercise in binding to a C library using [bindgen](https://rust-lang.github.io/rust-bindgen/) and the [Rust FFI](https://doc.rust-lang.org/nomicon/ffi.html).  It definitely required a review of C pointers (and for which I claim no expertise)!

### Long Term
The long term goals of this repository are uncertain.  Possibilities include:
- :heavy_check_mark: Developing a command line tool that performs transformations from `sas7bdat` to other file types
    - [X] text
        - [X] `csv`
        - [X] `ndjson`
    - [X] binary
        - [X] `feather`
        - [X] `parquet`
- :heavy_check_mark: Developing a command line tool that expands the functionality made available by the [readstat](https://github.com/WizardMac/ReadStat#command-line-usage) command line tool
- Completing and publishing the `readstat-sys` crate that binds to [ReadStat](https://github.com/WizardMac/ReadStat)
- Developing and publishing a Rust library &mdash; `readstat` &mdash; that allows Rust programmers to work with `sas7bdat` files
    - Implementing a custom [serde data format](https://serde.rs/data-format.html) for `sas7bdat` files (implement serialize first and deserialize later (if possible))

## Resources
The following have been **_incredibly_** helpful while developing!
- [How to not RiiR](http://adventures.michaelfbryan.com/posts/how-not-to-riir/#building-chmlib-sys)
- [Making a *-sys crate](https://kornel.ski/rust-sys-crate)
- [Rust Closures in FFI](https://adventures.michaelfbryan.com/posts/rust-closures-in-ffi/)
- Rust FFI: Microsoft Flight Simulator SDK
    - [Part 1](https://youtu.be/jNNz4h3iIlw)
    - [Part 2](https://youtu.be/ugiR9M16fwg)
- Stack Overflow answers by [Jake Goulding](https://stackoverflow.com/users/155423/shepmaster)
- ReadStat pull request to add [MSVC/Windows support](https://github.com/WizardMac/ReadStat/pull/214)
- [jamovi-readstat](https://github.com/jamovi/jamovi-readstat) [appveyor.yml](https://github.com/jamovi/jamovi-readstat/blob/master/appveyor.yml) file to build ReadStat on Windows
- [Arrow documentation for utilizing ArrayBuilders](https://docs.rs/arrow/latest/arrow/array/trait.ArrayBuilder.html#example)
