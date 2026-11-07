# Parallelization

SAS7BDAT, XPT, Stata and SPSS share one scheduler, `ParallelBatches` in `src/parallel.rs`.

## How it works

A read is split into numbered jobs, each decoding a fixed range of the file into one or more batches. A central scheduler hands jobs to Polars' global rayon pool, so the number of decode threads stays bounded however many scans run at once.

- **Dispatch:** the consumer submits job *i+1* only when fewer than `2 × threads` jobs are outstanding (submitted but not yet handed out).
- **Memory:** that window caps buffered results at `2 × threads` jobs, ordered or not. A job is about one batch (roughly 4MB of raw row data, at most 100k rows).
- **Ordering:** with `preserve_order=true`, finished jobs wait in a reorder buffer and are handed out in file order. Otherwise they are handed out as they finish. Both stay within the same window and cost the same on the files tested (SAS, Stata, SPSS).
- **Threads:** jobs never block, an error or panic in a job is returned as an error, and dropping the iterator cancels the remaining jobs without waiting for them.

## Job ranges per format

| Format | Job = |
| --- | --- |
| Stata, SPSS, XPT | A range of rows |
| SAS7BDAT | A range of pages (about one batch of rows) |

SAS rows per page vary, so row counts aren't known up front. That is why SAS adds a few pieces around the scheduler:

- Rows on MIX pages (at the start of the file) are read serially first.
- A row index (which forces file order), `n_rows` and the compressed-file row cap are applied to the output as it is produced.
- `offset > 0` reads `[0, offset + n)` in parallel and drops the first `offset` rows.
- Compressed files (RLE/RDC) always run in file order. Each row decompresses independently, so they parallelize like uncompressed files.
- `informative_nulls` reads serially.

## Thread count

- Rust default: `min(Polars pool size, physical cores)`.
- Python default: half the Polars pool size (leaving room for Polars' own work), capped at physical cores.
- Pass `threads` to override. `threads=1` reads serially.

## Batch size

`default_batch_rows` targets about 4MB of raw row data per batch, at most 100k rows. Very wide rows get larger batches (up to about 32MB) so per-column overhead doesn't dominate. The Python `scan_readstat` ignores Polars' `batch_size` hint; pass `batch_size` to override.

## Guidance

- Use `preserve_order=true` when you need file order; it doesn't add RAM or time.
- `threads` above the physical core count doesn't help. Uncompressed SAS is mostly I/O-bound.
- Wide files (thousands of columns) use the most memory per batch. Peak RAM on a 5,200-column SAS file was about 6.5GB when writing Parquet, in every ordering mode.
