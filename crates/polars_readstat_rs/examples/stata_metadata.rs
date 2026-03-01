use polars_readstat_rs::stata::metadata_json;
use std::path::PathBuf;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: stata_metadata <file>");
        std::process::exit(1);
    }
    let path = PathBuf::from(&args[1]);
    let json = metadata_json(path).expect("metadata");
    println!("{}", json);
}
