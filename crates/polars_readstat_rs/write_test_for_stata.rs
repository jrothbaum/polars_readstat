use polars::prelude::*;
use polars_readstat_rs::stata::writer::StataWriter;

fn main() {
    let s1 = "a".repeat(3000) + "hello";
    let s2 = "b".repeat(3000) + "hell";
    let s3 = "c".repeat(3000) + "hel";

    println!("Writing strings with lengths: {}, {}, {}", s1.len(), s2.len(), s3.len());

    let df = df!(
        "longstr" => &[s1.as_str(), s2.as_str(), s3.as_str()]
    ).unwrap();

    let output_path = r"C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\test_fixed_strl.dta";
    StataWriter::new(output_path).write_df(&df).unwrap();

    println!("✓ Wrote to: {}", output_path);
    println!("\nNow test in Stata:");
    println!(r#"  use "{}", clear"#, output_path);
    println!(r#"  di "Row 1: " length(longstr[1]) " chars, last 10: " substr(longstr[1], length(longstr[1])-9, 10)"#);
    println!(r#"  di "Row 2: " length(longstr[2]) " chars, last 10: " substr(longstr[2], length(longstr[2])-9, 10)"#);
    println!(r#"  di "Row 3: " length(longstr[3]) " chars, last 10: " substr(longstr[3], length(longstr[3])-9, 10)"#);
}
