use polars_readstat_rs::{scan_sas7bdat, ScanOptions};
use std::path::PathBuf;

#[test]
fn test_mix_page_alignment_stattransfer_style() {
    let path = PathBuf::from("tests/sas/data/data_pandas/test1.sas7bdat");
    if !path.exists() {
        return;
    }

    let df = scan_sas7bdat(path, ScanOptions::default())
        .expect("scan")
        .collect()
        .expect("collect");

    let col1 = df.column("Column1").unwrap().f64().unwrap();
    let col3 = df.column("Column3").unwrap().f64().unwrap();
    let col8 = df.column("Column8").unwrap().f64().unwrap();

    assert_eq!(col1.get(7), Some(0.148));
    assert_eq!(col1.get(8), None);
    assert_eq!(col1.get(9), Some(0.663));
    assert_eq!(col3.get(7), Some(37.0));
    assert_eq!(col3.get(8), Some(15.0));
    assert_eq!(col3.get(9), None);
    assert_eq!(col8.get(7), Some(8833.0));
    assert_eq!(col8.get(8), Some(3227.0));
    assert_eq!(col8.get(9), None);
}

#[test]
fn test_mix_page_alignment_brumm_money_values() {
    let path = PathBuf::from("tests/sas/data/data_poe/brumm.sas7bdat");
    if !path.exists() {
        return;
    }

    let df = scan_sas7bdat(path, ScanOptions::default())
        .expect("scan")
        .collect()
        .expect("collect");

    let money = df.column("money").unwrap().f64().unwrap();
    assert_eq!(money.get(0), Some(356.7));
    assert_eq!(money.get(1), Some(11.5));
    assert_eq!(money.get(2), Some(7.3));
    assert_eq!(money.get(3), Some(18.0));
}
