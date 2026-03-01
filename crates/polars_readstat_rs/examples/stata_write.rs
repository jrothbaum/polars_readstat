use polars::prelude::*;
use polars_readstat_rs::{StataResult, StataWriter};

/// Usage: stata_write <out_path>
fn main() -> StataResult<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: stata_write <out_path>");
        std::process::exit(1);
    }

    let df = DataFrame::new_infer_height(vec![
        Series::new("id".into(), &[1i32, 2, 3]).into_column(),
        Series::new("name".into(), &["alice", "bob", "carol"]).into_column(),
        Series::new("date".into(), &[0i32, 1, 2])
            .cast(&DataType::Date)
            .unwrap()
            .into_column(),
    ])
    .unwrap();

    StataWriter::new(&args[1]).write_df(&df)?;
    println!("Wrote {}", &args[1]);
    Ok(())
}
