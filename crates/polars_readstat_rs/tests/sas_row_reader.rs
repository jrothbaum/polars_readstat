/// Verify that SasRowReader produces the same row count and numeric values as
/// readstat_scan for a set of representative files.

#[cfg(feature = "row_reader")]
mod tests {
    use polars::prelude::*;
    use polars_readstat_rs::{sas_row_readers, SasColumnKind, ScanOptions, readstat_scan};
    use std::path::PathBuf;

    fn test_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/sas/data")
    }

    /// Drain all readers and return (total_rows, f64_sum_of_all_numeric_columns).
    fn drain_readers(path: &std::path::Path) -> (usize, f64) {
        let opts = ScanOptions::default();
        let mut readers = sas_row_readers(path, &opts).expect("sas_row_readers");

        let schema = readers[0].schema.clone();
        let n_cols = schema.len();

        let mut total_rows = 0usize;
        let mut f64_sum = 0.0f64;

        let chunk = 2048;
        let mut f64_vals = vec![0.0f64; chunk];
        let mut valid = vec![false; chunk];
        let mut i32_vals = vec![0i32; chunk];
        let mut i64_vals = vec![0i64; chunk];
        let mut str_vals: Vec<Option<String>> = Vec::new();

        for reader in &mut readers {
            loop {
                let n = reader.next_chunk(chunk).unwrap();
                if n == 0 { break; }

                for col in 0..n_cols {
                    match schema[col].kind {
                        SasColumnKind::F64 => {
                            reader.fill_f64(col, n, &mut f64_vals, &mut valid);
                            for i in 0..n {
                                if valid[i] { f64_sum += f64_vals[i]; }
                            }
                        }
                        SasColumnKind::DateI32 => {
                            reader.fill_date_i32(col, n, &mut i32_vals, &mut valid);
                        }
                        SasColumnKind::DateTimeI64 => {
                            reader.fill_datetime_i64(col, n, &mut i64_vals, &mut valid);
                        }
                        SasColumnKind::TimeI64 => {
                            reader.fill_time_i64(col, n, &mut i64_vals, &mut valid);
                        }
                        SasColumnKind::Str => {
                            reader.fill_str(col, n, &mut str_vals);
                        }
                    }
                }

                // Only count rows after reading all columns (first col drives the count).
                total_rows += n;
                reader.commit(n);
            }
        }

        (total_rows, f64_sum)
    }

    /// Compute (row_count, f64_sum) via the existing readstat_scan path.
    fn scan_checksum(path: &std::path::Path) -> (usize, f64) {
        let df = readstat_scan(path, None, None)
            .expect("scan")
            .collect()
            .expect("collect");

        let mut sum = 0.0f64;
        for col in df.columns() {
            if *col.dtype() == DataType::Float64 {
                if let Ok(ca) = col.f64() {
                    sum += ca.into_iter().flatten().sum::<f64>();
                }
            }
        }
        (df.height(), sum)
    }

    fn check_file(rel: &str) {
        let path = test_dir().join(rel);
        if !path.exists() {
            eprintln!("skip (not found): {rel}");
            return;
        }
        let (rr_rows, rr_sum) = drain_readers(&path);
        let (sc_rows, sc_sum) = scan_checksum(&path);

        assert_eq!(rr_rows, sc_rows, "{rel}: row count mismatch");
        assert!(
            (rr_sum - sc_sum).abs() < 1e-3,
            "{rel}: f64 sum mismatch: row_reader={rr_sum} scan={sc_sum}"
        );
    }

    #[test]
    fn test_airline_uncompressed() {
        check_file("data_pandas/airline.sas7bdat");
    }

    #[test]
    fn test_datetime_columns() {
        check_file("data_pandas/datetime.sas7bdat");
    }

    #[test]
    fn test_productsales() {
        check_file("data_pandas/productsales.sas7bdat");
    }

    #[test]
    fn test_rle_compressed() {
        check_file("data_pandas/test2.sas7bdat");
    }

    #[test]
    fn test_rdc_compressed() {
        check_file("data_pandas/test3.sas7bdat");
    }

    #[test]
    fn test_mixed_types() {
        check_file("data_pandas/test10.sas7bdat");
    }

    #[test]
    fn test_larger_file() {
        // drugprob has many rows and string columns
        check_file("sas_to_csv/drugprob.sas7bdat");
    }

    // ── timing comparison ─────────────────────────────────────────────────────

    fn time_row_reader(path: &std::path::Path, runs: usize) -> std::time::Duration {
        let opts = ScanOptions::default();
        let schema = sas_row_readers(path, &opts).unwrap()[0].schema.clone();
        let n_cols = schema.len();
        let chunk = 2048;
        let mut f64_vals = vec![0.0f64; chunk];
        let mut valid = vec![false; chunk];
        let mut i32_vals = vec![0i32; chunk];
        let mut i64_vals = vec![0i64; chunk];
        let mut str_vals: Vec<Option<String>> = Vec::new();

        let mut times = Vec::with_capacity(runs);
        for _ in 0..runs {
            let t = std::time::Instant::now();
            let mut readers = sas_row_readers(path, &opts).unwrap();
            for reader in &mut readers {
                loop {
                    let n = reader.next_chunk(chunk).unwrap();
                    if n == 0 { break; }
                    for col in 0..n_cols {
                        match schema[col].kind {
                            SasColumnKind::F64         => { reader.fill_f64(col, n, &mut f64_vals, &mut valid); }
                            SasColumnKind::DateI32     => { reader.fill_date_i32(col, n, &mut i32_vals, &mut valid); }
                            SasColumnKind::DateTimeI64 => { reader.fill_datetime_i64(col, n, &mut i64_vals, &mut valid); }
                            SasColumnKind::TimeI64     => { reader.fill_time_i64(col, n, &mut i64_vals, &mut valid); }
                            SasColumnKind::Str         => { reader.fill_str(col, n, &mut str_vals); }
                        }
                    }
                    reader.commit(n);
                }
            }
            times.push(t.elapsed());
        }
        times.sort();
        times[runs / 2]
    }

    fn time_arrow_scan(path: &std::path::Path, runs: usize) -> std::time::Duration {
        let mut times = Vec::with_capacity(runs);
        for _ in 0..runs {
            let t = std::time::Instant::now();
            readstat_scan(path, None, None).unwrap().collect().unwrap();
            times.push(t.elapsed());
        }
        times.sort();
        times[runs / 2]
    }

    #[test]
    #[ignore]
    fn bench_nls() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/sas/data/too_big/nls.sas7bdat");
        if !path.exists() { eprintln!("nls.sas7bdat not found"); return; }
        let runs = 5;
        let rr = time_row_reader(&path, runs);
        let arrow = time_arrow_scan(&path, runs);
        println!(
            "\nnls.sas7bdat  row_reader={:.1}ms  arrow_scan={:.1}ms  ratio={:.2}x",
            rr.as_secs_f64() * 1000.0,
            arrow.as_secs_f64() * 1000.0,
            arrow.as_secs_f64() / rr.as_secs_f64(),
        );
    }

    #[test]
    #[ignore]
    fn bench_topical() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/sas/data/too_big/topical.sas7bdat");
        if !path.exists() { eprintln!("topical.sas7bdat not found"); return; }
        let runs = 5;
        let rr = time_row_reader(&path, runs);
        let arrow = time_arrow_scan(&path, runs);
        println!(
            "\ntopical.sas7bdat  row_reader={:.1}ms  arrow_scan={:.1}ms  ratio={:.2}x",
            rr.as_secs_f64() * 1000.0,
            arrow.as_secs_f64() * 1000.0,
            arrow.as_secs_f64() / rr.as_secs_f64(),
        );
    }

    fn time_row_reader_opts(path: &std::path::Path, opts: &ScanOptions, runs: usize) -> std::time::Duration {
        let schema = sas_row_readers(path, opts).unwrap()[0].schema.clone();
        let n_cols = schema.len();
        let chunk = 2048;
        let mut f64_vals = vec![0.0f64; chunk];
        let mut valid = vec![false; chunk];
        let mut i32_vals = vec![0i32; chunk];
        let mut i64_vals = vec![0i64; chunk];
        let mut str_vals: Vec<Option<String>> = Vec::new();
        let mut times = Vec::with_capacity(runs);
        for _ in 0..runs {
            let t = std::time::Instant::now();
            let mut readers = sas_row_readers(path, opts).unwrap();
            for reader in &mut readers {
                loop {
                    let n = reader.next_chunk(chunk).unwrap();
                    if n == 0 { break; }
                    for col in 0..n_cols {
                        match schema[col].kind {
                            SasColumnKind::F64         => { reader.fill_f64(col, n, &mut f64_vals, &mut valid); }
                            SasColumnKind::DateI32     => { reader.fill_date_i32(col, n, &mut i32_vals, &mut valid); }
                            SasColumnKind::DateTimeI64 => { reader.fill_datetime_i64(col, n, &mut i64_vals, &mut valid); }
                            SasColumnKind::TimeI64     => { reader.fill_time_i64(col, n, &mut i64_vals, &mut valid); }
                            SasColumnKind::Str         => { reader.fill_str(col, n, &mut str_vals); }
                        }
                    }
                    reader.commit(n);
                }
            }
            times.push(t.elapsed());
        }
        times.sort();
        times[runs / 2]
    }

    fn time_row_reader_str_buf(path: &std::path::Path, opts: &ScanOptions, runs: usize) -> std::time::Duration {
        use polars_readstat_rs::sas_row_readers;
        let schema = sas_row_readers(path, opts).unwrap()[0].schema.clone();
        let n_cols = schema.len();
        let chunk = 2048;
        let mut f64_vals = vec![0.0f64; chunk];
        let mut valid = vec![false; chunk];
        let mut i32_vals = vec![0i32; chunk];
        let mut i64_vals = vec![0i64; chunk];
        let mut str_bytes: Vec<u8> = Vec::new();
        let mut str_offsets: Vec<u32> = Vec::new();
        let mut str_nulls: Vec<bool> = Vec::new();
        let mut times = Vec::with_capacity(runs);
        for _ in 0..runs {
            let t = std::time::Instant::now();
            let mut readers = sas_row_readers(path, opts).unwrap();
            for reader in &mut readers {
                loop {
                    let n = reader.next_chunk(chunk).unwrap();
                    if n == 0 { break; }
                    for col in 0..n_cols {
                        match schema[col].kind {
                            SasColumnKind::F64         => { reader.fill_f64(col, n, &mut f64_vals, &mut valid); }
                            SasColumnKind::DateI32     => { reader.fill_date_i32(col, n, &mut i32_vals, &mut valid); }
                            SasColumnKind::DateTimeI64 => { reader.fill_datetime_i64(col, n, &mut i64_vals, &mut valid); }
                            SasColumnKind::TimeI64     => { reader.fill_time_i64(col, n, &mut i64_vals, &mut valid); }
                            SasColumnKind::Str         => { reader.fill_str_buf(col, n, &mut str_bytes, &mut str_offsets, &mut str_nulls); }
                        }
                    }
                    reader.commit(n);
                }
            }
            times.push(t.elapsed());
        }
        times.sort();
        times[runs / 2]
    }

    fn time_row_reader_no_str(path: &std::path::Path, opts: &ScanOptions, runs: usize) -> std::time::Duration {
        let schema = sas_row_readers(path, opts).unwrap()[0].schema.clone();
        let n_cols = schema.len();
        let chunk = 2048;
        let mut f64_vals = vec![0.0f64; chunk];
        let mut valid = vec![false; chunk];
        let mut i32_vals = vec![0i32; chunk];
        let mut i64_vals = vec![0i64; chunk];
        let mut times = Vec::with_capacity(runs);
        for _ in 0..runs {
            let t = std::time::Instant::now();
            let mut readers = sas_row_readers(path, opts).unwrap();
            for reader in &mut readers {
                loop {
                    let n = reader.next_chunk(chunk).unwrap();
                    if n == 0 { break; }
                    for col in 0..n_cols {
                        match schema[col].kind {
                            SasColumnKind::F64         => { reader.fill_f64(col, n, &mut f64_vals, &mut valid); }
                            SasColumnKind::DateI32     => { reader.fill_date_i32(col, n, &mut i32_vals, &mut valid); }
                            SasColumnKind::DateTimeI64 => { reader.fill_datetime_i64(col, n, &mut i64_vals, &mut valid); }
                            SasColumnKind::TimeI64     => { reader.fill_time_i64(col, n, &mut i64_vals, &mut valid); }
                            SasColumnKind::Str         => { /* skip string decode */ }
                        }
                    }
                    reader.commit(n);
                }
            }
            times.push(t.elapsed());
        }
        times.sort();
        times[runs / 2]
    }

    #[test]
    #[ignore]
    fn bench_topical_threads() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/sas/data/too_big/topical.sas7bdat");
        if !path.exists() { eprintln!("topical.sas7bdat not found"); return; }
        let runs = 5;
        let opts_default = ScanOptions::default();
        let full = time_row_reader_opts(&path, &opts_default, runs);
        let buf = time_row_reader_str_buf(&path, &opts_default, runs);
        let no_str = time_row_reader_no_str(&path, &opts_default, runs);
        let arrow = time_arrow_scan(&path, runs);
        println!("topical  fill_str={:.1}ms  fill_str_buf={:.1}ms  no_str={:.1}ms  arrow={:.1}ms",
            full.as_secs_f64() * 1000.0,
            buf.as_secs_f64() * 1000.0,
            no_str.as_secs_f64() * 1000.0,
            arrow.as_secs_f64() * 1000.0,
        );
    }
}
