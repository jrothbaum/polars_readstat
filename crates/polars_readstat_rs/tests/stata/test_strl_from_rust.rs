use polars::prelude::*;
use polars_readstat_rs::stata::writer::StataWriter;

#[test]
fn test_strl_write_for_stata_native_reader() {
    // Same test strings as Stata plugin test
    let s1 = "a".repeat(3000) + "hello";
    let s2 = "b".repeat(3000) + "hell";
    let s3 = "c".repeat(3000) + "hel";

    println!("Creating strings:");
    println!("  s1 length: {}, last 10: {:?}", s1.len(), &s1[s1.len()-10..]);
    println!("  s2 length: {}, last 10: {:?}", s2.len(), &s2[s2.len()-10..]);
    println!("  s3 length: {}, last 10: {:?}", s3.len(), &s3[s3.len()-10..]);

    let df = df!(
        "longstr" => &[s1.as_str(), s2.as_str(), s3.as_str()]
    ).unwrap();

    println!("\nDataFrame created, shape: {:?}", df.shape());

    let output_path = r"C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\test_rust_strl.dta";
    println!("Writing to: {}", output_path);

    StataWriter::new(output_path)
        .write_df(&df)
        .unwrap();

    println!("Done! Now check in Stata:");
    println!(r#"  use "C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\test_rust_strl.dta", clear"#);
    println!(r#"  di "Row 1 last 10: " substr(longstr[1], length(longstr[1])-9, 10)"#);
    println!(r#"  di "Row 2 last 10: " substr(longstr[2], length(longstr[2])-9, 10)"#);
    println!(r#"  di "Row 3 last 10: " substr(longstr[3], length(longstr[3])-9, 10)"#);
}
