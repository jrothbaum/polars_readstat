use polars_readstat_rs::sas::xpt::{read_xpt_metadata, scan_xpt, xpt_metadata_json};
use polars_readstat_rs::ScanOptions;
use std::path::Path;

fn test_file(label: &str, path: &str) {
    println!("\n=== {} ===", label);
    let meta = match read_xpt_metadata(Path::new(path)) {
        Ok(m) => m,
        Err(e) => { println!("ERROR reading metadata: {e}"); return; }
    };
    println!("version={} table={:?} row_len={} cols={}",
        meta.version, meta.table_name, meta.row_length, meta.columns.len());
    for c in &meta.columns {
        println!("  {:<20} {:?} fmt={:?} width={}", c.name, c.col_type, c.format, c.storage_width);
    }
    let lf = match scan_xpt(path, ScanOptions::default()) {
        Ok(lf) => lf,
        Err(e) => { println!("ERROR scan: {e}"); return; }
    };
    let df = match lf.collect() {
        Ok(df) => df,
        Err(e) => { println!("ERROR collect: {e}"); return; }
    };
    println!("DataFrame shape: {:?}", df.shape());
    println!("{}", df.head(Some(3)));
}

fn main() {
    let base = "crates/polars_readstat_rs/tests/sas/data/xpt";
    test_file("sample.xpt (v5)", &format!("{base}/sample.xpt"));
    test_file("sas.xpt5",        &format!("{base}/sas.xpt5"));
    test_file("sas.xpt8",        &format!("{base}/sas.xpt8"));
    test_file("dates_xpt_v8",    &format!("{base}/dates_xpt_v8.xpt"));

    println!("\n=== metadata JSON (sample.xpt) ===");
    match xpt_metadata_json(Path::new(&format!("{base}/sample.xpt"))) {
        Ok(json) => println!("{json}"),
        Err(e) => println!("ERROR: {e}"),
    }
}
