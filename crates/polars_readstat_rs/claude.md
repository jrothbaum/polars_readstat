# SAS7BDAT Rust Port

Convert the cppsas7bdat C++ library to pure Rust, producing Polars DataFrames.

## Goal

A pure Rust SAS7BDAT reader with:
- No C/C++ dependencies
- Batch-based reading for memory efficiency
- Parallel parsing where the format allows
- Direct output to Polars DataFrames

## Architecture

### Core Pipeline

The C++ code follows a clear pipeline that should be preserved:

```
check_header → read_header → read_metadata → read_data
```

Each stage depends on the previous. In Rust:

```rust
pub struct Sas7bdatReader<R: Read + Seek> {
    source: R,
    header: Header,
    metadata: Metadata,
    decompressor: Decompressor,
    current_row: usize,
}

impl<R: Read + Seek> Sas7bdatReader<R> {
    pub fn open(source: R) -> Result<Self, Error>;
    pub fn metadata(&self) -> &Metadata;
    pub fn next_batch(&mut self, batch_size: usize) -> Result<Option<DataFrame>, Error>;
}
```

### Format Variants

The C++ uses `std::variant` with template parameters for compile-time dispatch:

```cpp
using RH = std::variant<
    READ_HEADER<DATASOURCE, Endian::big, Format::bit64>,
    READ_HEADER<DATASOURCE, Endian::big, Format::bit32>,
    READ_HEADER<DATASOURCE, Endian::little, Format::bit64>,
    READ_HEADER<DATASOURCE, Endian::little, Format::bit32>
>;
```

In Rust, use enums and runtime dispatch (simpler, branch predictor handles it):

```rust
#[derive(Clone, Copy)]
pub enum Endian { Big, Little }

#[derive(Clone, Copy)]
pub enum Format { Bit32, Bit64 }

#[derive(Clone, Copy)]
pub enum Compression { None, RLE, RDC }
```

### Decompression

Three decompressor types (from C++ `DECOMPRESSOR` namespace):
- `None` - uncompressed data
- `RLE` - Run Length Encoding
- `RDC` - Ross Data Compression

```rust
enum Decompressor {
    None,
    Rle(RleState),
    Rdc(RdcState),
}

impl Decompressor {
    fn decompress(&mut self, input: &[u8], output: &mut [u8]) -> Result<usize, Error>;
}
```

## Key C++ Files to Port

Reference these files in order:

1. **header.hpp** - Magic number check, alignment, endianness detection
2. **metadata.hpp** - Column definitions, compression type, row count
3. **data.hpp** - Page reading, row parsing
4. **decompressors.hpp** - RLE and RDC implementations

The main orchestration is in **sas7bdat-impl.hpp**.

## Memory-Efficient Batch Reading

### Two-Stage Buffering

Read in cache-friendly chunks, fill larger Arrow arrays:

```rust
fn next_batch(&mut self, batch_size: usize) -> Result<Option<DataFrame>> {
    // Allocate builders for full batch
    let mut builders: Vec<ColumnBuilder> = self.metadata.columns
        .iter()
        .map(|col| ColumnBuilder::with_capacity(col.dtype, batch_size))
        .collect();
    
    const READ_CHUNK: usize = 10_000;  // Cache-friendly
    let mut rows_read = 0;
    
    while rows_read < batch_size {
        // Read one page or chunk
        let page = self.read_next_page()?;
        let decompressed = self.decompressor.decompress(&page)?;
        
        // Parse rows from decompressed data
        for row_bytes in decompressed.chunks_exact(self.metadata.row_length) {
            for (col_idx, col) in self.metadata.columns.iter().enumerate() {
                let value = parse_value(&row_bytes[col.offset..], col);
                builders[col_idx].push(value);
            }
            rows_read += 1;
            if rows_read >= batch_size {
                break;
            }
        }
    }
    
    // Convert to DataFrame
    let columns: Vec<Series> = builders
        .into_iter()
        .zip(&self.metadata.columns)
        .map(|(b, col)| b.into_series(&col.name))
        .collect();
    
    Ok(Some(DataFrame::new(columns)?))
}
```

### Parallel Reading (Uncompressed Files Only)

For uncompressed files, pages can be read in parallel:

```rust
fn read_batch_parallel(&mut self, batch_size: usize) -> Result<Option<DataFrame>> {
    if self.metadata.compression != Compression::None {
        return self.read_batch_sequential(batch_size);
    }
    
    // Calculate page ranges
    let pages_needed = (batch_size + self.rows_per_page - 1) / self.rows_per_page;
    
    // Parallel page reads
    let page_data: Vec<Vec<u8>> = (0..pages_needed)
        .into_par_iter()
        .map(|page_idx| self.read_page(self.current_page + page_idx))
        .collect::<Result<Vec<_>>>()?;
    
    // Parallel row parsing by page
    let chunks: Vec<DataFrame> = page_data
        .into_par_iter()
        .map(|page| parse_page_to_dataframe(&page, &self.metadata))
        .collect::<Result<Vec<_>>>()?;
    
    Ok(Some(concat(&chunks)?))
}
```

## Type Mapping

### SAS to Polars

| SAS Type | Polars Type |
|----------|-------------|
| Numeric (8 bytes) | Float64 |
| Numeric with format | Float64 or Date/DateTime |
| Character | String |
| Missing numeric | null |
| Missing character | null or empty string |

### Date/Time Handling

SAS stores dates as days since 1960-01-01, datetimes as seconds since 1960-01-01 00:00:00.

```rust
const SAS_EPOCH_OFFSET_DAYS: i32 = -3653;  // 1960-01-01 relative to Unix epoch

fn sas_date_to_polars(sas_days: f64) -> Option<i32> {
    if sas_days.is_nan() {
        None
    } else {
        Some((sas_days as i32) + SAS_EPOCH_OFFSET_DAYS)
    }
}
```

## Testing Strategy

### Test Harness

Compare output against cppsas7bdat:

```rust
#[test]
fn test_matches_reference() {
    let test_files = glob("testdata/*.sas7bdat").unwrap();
    
    for file in test_files {
        let expected = read_with_cppsas7bdat(&file);
        let actual = read_with_rust(&file);
        
        assert_eq!(expected.shape(), actual.shape(), "Shape mismatch: {file:?}");
        
        for (exp_col, act_col) in expected.columns().zip(actual.columns()) {
            assert_series_approx_equal(exp_col, act_col, 1e-10);
        }
    }
}
```

### Test File Categories

Create or collect test files covering:

1. **Format variants**
   - 32-bit vs 64-bit
   - Big-endian vs little-endian
   - Windows vs Unix created

2. **Compression**
   - Uncompressed
   - RLE compressed
   - RDC compressed

3. **Data types**
   - Numeric columns
   - Character columns (various lengths)
   - Date/datetime columns
   - Missing values

4. **Edge cases**
   - Empty dataset
   - Single row
   - Very wide (many columns)
   - Very long (many rows)
   - Maximum length strings

## Implementation Order

### Phase 1: Core Reading (Week 1-2)

1. Header parsing
   - Magic number validation
   - Endianness detection
   - Format (32/64-bit) detection
   
2. Metadata parsing
   - Column definitions
   - Row count and length
   - Page size

3. Uncompressed data reading
   - Page iteration
   - Row parsing
   - Type conversion

### Phase 2: Compression (Week 2-3)

1. RLE decompression
2. RDC decompression
3. Integration with page reading

### Phase 3: Polish (Week 3-4)

1. Parallel reading for uncompressed files
2. Memory optimization
3. Error handling and edge cases
4. Documentation

## Dependencies

```toml
[dependencies]
polars = { version = "0.37", features = ["lazy"] }
byteorder = "1.5"
encoding_rs = "0.8"  # For character encoding
rayon = "1.8"  # For parallel iteration
thiserror = "1.0"

[dev-dependencies]
glob = "0.3"
```

## Error Handling

```rust
#[derive(Debug, thiserror::Error)]
pub enum Sas7bdatError {
    #[error("Invalid magic number")]
    InvalidMagicNumber,
    
    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),
    
    #[error("Decompression failed: {0}")]
    DecompressionError(String),
    
    #[error("Invalid page type: {0}")]
    InvalidPageType(u8),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),
}
```

## Notes

- The C++ code is well-structured; follow its logic rather than doing a mechanical line-by-line translation
- Focus on correctness first, then optimize
- The decompression algorithms (especially RDC) are the trickiest part
- Test against real-world files from your actual use cases, not just synthetic test data