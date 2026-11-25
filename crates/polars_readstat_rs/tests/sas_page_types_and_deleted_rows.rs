//! Rows SAS stores outside plain DATA pages, and rows it marks deleted or moves: every read
//! path must return exactly the dataset's rows, and a file that comes up short is an error.
//!
//! `load_log` and `test_meta2_page` come from the pandas test suite. Before, rows on a DATA
//! page flagged as having deleted rows (type 0x0180) and rows continued on a META2 page
//! (0x4000) were skipped without an error, and deleted rows were returned.

use polars::prelude::*;
use polars_readstat_rs::Sas7bdatReader;

const DATA: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/sas/data/data_pandas");

fn read_modes(reader: &Sas7bdatReader) -> Vec<(&'static str, DataFrame)> {
    vec![
        ("sequential", reader.read().sequential().finish().unwrap()),
        (
            "ordered parallel",
            reader
                .read()
                .with_n_threads(4)
                .with_chunk_size(64)
                .preserve_order(true)
                .finish()
                .unwrap(),
        ),
        (
            "parallel",
            reader
                .read()
                .with_n_threads(4)
                .with_chunk_size(64)
                .finish()
                .unwrap(),
        ),
    ]
}

fn sorted(df: &DataFrame) -> DataFrame {
    let cols: Vec<String> = df.get_column_names().iter().map(|c| c.to_string()).collect();
    df.sort(cols, SortMultipleOptions::default().with_nulls_last(true)).unwrap()
}

/// Every read mode gives `expected` rows, the same ones, in file order where ordered.
fn check_modes(reader: &Sas7bdatReader, expected: usize) -> DataFrame {
    let modes = read_modes(reader);
    let reference = modes[0].1.clone();
    for (mode, df) in &modes {
        assert_eq!(df.height(), expected, "{mode}");
        if *mode == "parallel" {
            assert!(sorted(df).equals_missing(&sorted(&reference)), "{mode}");
        } else {
            assert!(df.equals_missing(&reference), "{mode}");
        }
    }
    reference
}

#[test]
fn flagged_pages_skip_deleted_rows() {
    let reader = Sas7bdatReader::open(format!("{DATA}/load_log.sas7bdat")).unwrap();
    let meta = reader.metadata();
    assert_eq!(meta.total_row_count, 2097);
    assert_eq!(meta.deleted_row_count, 9);
    assert_eq!(meta.row_count, 2088);
    // 2,088 rows, as SAS and ReadStat give; pages 0 (MIX, 0x0280) and 4 (DATA, 0x0180) carry
    // the deleted rows, and page 4's 292 rows used to be dropped entirely.
    check_modes(&reader, 2088);
}

#[test]
fn rows_on_meta2_pages_are_read() {
    let reader = Sas7bdatReader::open(format!("{DATA}/test_meta2_page.sas7bdat")).unwrap();
    let df = check_modes(&reader, 1000);
    // one row per day from 1997-01-01; 473 of them sit on the META2 page
    let dates: Vec<i32> = df
        .column("date")
        .unwrap()
        .cast(&DataType::Int32)
        .unwrap()
        .i32()
        .unwrap()
        .into_no_null_iter()
        .collect();
    for pair in dates.windows(2) {
        assert_eq!(pair[1], pair[0] + 1);
    }
}

// ─── Compressed files: deleted and moved rows ─────────────────────────────────

/// Just enough of the file layout to patch subheader pointers.
struct Layout {
    u64: bool,
    le: bool,
    header_length: usize,
    page_length: usize,
}

impl Layout {
    fn of(b: &[u8]) -> Self {
        let u64 = b[32] == 0x33;
        let le = b[37] == 0x01;
        let a1 = if b[35] == 0x33 { 4 } else { 0 };
        let rd4 = |o: usize| {
            let v: [u8; 4] = b[o..o + 4].try_into().unwrap();
            (if le { u32::from_le_bytes(v) } else { u32::from_be_bytes(v) }) as usize
        };
        Layout { u64, le, header_length: rd4(196 + a1), page_length: rd4(200 + a1) }
    }
    fn int(&self) -> usize {
        if self.u64 { 8 } else { 4 }
    }
    fn read(&self, b: &[u8], o: usize, n: usize) -> u64 {
        let mut v = 0u64;
        for k in 0..n {
            let byte = if self.le { b[o + n - 1 - k] } else { b[o + k] };
            v = (v << 8) | byte as u64;
        }
        v
    }
    fn write(&self, b: &mut [u8], o: usize, n: usize, v: u64) {
        for k in 0..n {
            let byte = (v >> (8 * k)) as u8;
            if self.le { b[o + k] = byte } else { b[o + n - 1 - k] = byte }
        }
    }
    /// (page, pointer byte offset, subheader index) of every subheader pointer.
    fn pointers(&self, b: &[u8]) -> Vec<(usize, usize, usize)> {
        let pbo = if self.u64 { 32 } else { 16 };
        let pages = (b.len() - self.header_length) / self.page_length;
        let mut out = Vec::new();
        for p in 0..pages {
            let start = self.header_length + p * self.page_length;
            let ptype = self.read(b, start + pbo, 2) as u16;
            if ptype & 0x0F00 == 0x0100 || ptype & 0x9000 != 0 {
                continue;
            }
            let count = self.read(b, start + pbo + 4, 2) as usize;
            for i in 0..count {
                out.push((p, start + pbo + 8 + i * 3 * self.int(), i));
            }
        }
        out
    }
    fn code_at(&self, b: &[u8], ptr: usize) -> u8 {
        b[ptr + 2 * self.int()]
    }
    fn set_code(&self, b: &mut [u8], ptr: usize, code: u8) {
        b[ptr + 2 * self.int()] = code;
    }
    /// Byte offset of the ROW_SIZE subheader (signature F7F7F7F7).
    fn row_size_subheader(&self, b: &[u8]) -> usize {
        for (p, ptr, _) in self.pointers(b) {
            let off = self.read(b, ptr, self.int()) as usize;
            let len = self.read(b, ptr + self.int(), self.int()) as usize;
            let at = self.header_length + p * self.page_length + off;
            if len > 0 && (b[at..at + 4] == [0xF7; 4] || b[at + 4..at + 8] == [0xF7; 4]) {
                return at;
            }
        }
        panic!("no ROW_SIZE subheader");
    }
}

/// The compressed rows' pointers, in file order.
fn row_pointers(layout: &Layout, b: &[u8]) -> Vec<(usize, usize, usize)> {
    layout
        .pointers(b)
        .into_iter()
        .filter(|&(_, ptr, _)| layout.code_at(b, ptr) == 4)
        .collect()
}

fn rows(df: &DataFrame, idx: &[u32]) -> DataFrame {
    df.take(&IdxCa::from_vec("i".into(), idx.to_vec())).unwrap()
}

fn compressed_file() -> (Vec<u8>, DataFrame) {
    let bytes = std::fs::read(format!("{DATA}/test2.sas7bdat")).unwrap();
    let original = Sas7bdatReader::open_bytes(bytes.clone())
        .unwrap()
        .read()
        .sequential()
        .finish()
        .unwrap();
    assert_eq!(original.height(), 10);
    (bytes, original)
}

#[test]
fn compressed_deleted_row_is_skipped() {
    let (mut b, original) = compressed_file();
    let layout = Layout::of(&b);
    let ptrs = row_pointers(&layout, &b);
    assert_eq!(ptrs.len(), 10);
    // delete row 3: code 5, and one more deleted row in the header
    layout.set_code(&mut b, ptrs[3].1, 5);
    let rs = layout.row_size_subheader(&b);
    let n = layout.int();
    let deleted = layout.read(&b, rs + 7 * n, n);
    layout.write(&mut b, rs + 7 * n, n, deleted + 1);

    let reader = Sas7bdatReader::open_bytes(b).unwrap();
    assert_eq!(reader.metadata().row_count, 9);
    let df = check_modes(&reader, 9);
    assert!(df.equals_missing(&rows(&original, &[0, 1, 2, 4, 5, 6, 7, 8, 9])));
}

#[test]
fn compressed_moved_row_is_read_in_place() {
    let (mut b, original) = compressed_file();
    let layout = Layout::of(&b);
    let ptrs = row_pointers(&layout, &b);
    let n = layout.int();
    // Row 3 was moved to where row 7's subheader is: its pointer becomes a reference
    // (offset = page + 1, length = subheader index + 1), and row 7's slot the moved copy.
    let (page7, ptr7, index7) = ptrs[7];
    let ptr3 = ptrs[3].1;
    layout.write(&mut b, ptr3, n, page7 as u64 + 1);
    layout.write(&mut b, ptr3 + n, n, index7 as u64 + 1);
    layout.set_code(&mut b, ptr3, 3);
    layout.set_code(&mut b, ptr7, 6);
    let rs = layout.row_size_subheader(&b);
    layout.write(&mut b, rs + 6 * n, n, 9);

    let reader = Sas7bdatReader::open_bytes(b).unwrap();
    let df = check_modes(&reader, 9);
    assert!(df.equals_missing(&rows(&original, &[0, 1, 2, 7, 4, 5, 6, 8, 9])));
}

#[test]
fn unknown_subheader_code_is_an_error() {
    let (mut b, _) = compressed_file();
    let layout = Layout::of(&b);
    let ptrs = row_pointers(&layout, &b);
    layout.set_code(&mut b, ptrs[3].1, 0x07);
    let reader = Sas7bdatReader::open_bytes(b).unwrap();
    assert!(reader.read().sequential().finish().is_err());
}

// ─── Row count check ──────────────────────────────────────────────────────────

#[test]
fn truncated_file_is_an_error() {
    let b = std::fs::read(format!("{DATA}/load_log.sas7bdat")).unwrap();
    let layout = Layout::of(&b);
    let cut = b[..b.len() - layout.page_length].to_vec();
    let reader = Sas7bdatReader::open_bytes(cut).unwrap();
    for (mode, result) in [
        ("sequential", reader.read().sequential().finish()),
        ("parallel", reader.read().with_n_threads(4).with_chunk_size(64).finish()),
        (
            "ordered parallel",
            reader.read().with_n_threads(4).with_chunk_size(64).preserve_order(true).finish(),
        ),
    ] {
        let err = result.expect_err(mode).to_string();
        assert!(err.contains("expected 2088 rows"), "{mode}: {err}");
    }
    // a limit the file can satisfy still reads
    let head = reader.read().sequential().with_limit(100).finish().unwrap();
    assert_eq!(head.height(), 100);
}

#[cfg(feature = "row_reader")]
#[test]
fn row_reader_api_matches() {
    use polars_readstat_rs::{sas_row_readers, ScanOptions};
    for (file, expected) in [("load_log", 2088usize), ("test_meta2_page", 1000)] {
        for threads in [1usize, 4] {
            let opts = ScanOptions { threads: Some(threads), ..ScanOptions::default() };
            let path = std::path::PathBuf::from(format!("{DATA}/{file}.sas7bdat"));
            let mut total = 0;
            for reader in &mut sas_row_readers(&path, &opts).unwrap() {
                loop {
                    let n = reader.next_chunk(512).unwrap();
                    if n == 0 {
                        break;
                    }
                    reader.commit(n);
                    total += n;
                }
            }
            assert_eq!(total, expected, "{file}, {threads} threads");
        }
    }
}
