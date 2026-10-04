use polars::prelude::*;
use polars_readstat_rs::{read_por, write_por, PorWriteOptions};
fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a[1] == "write" {
        let df = DataFrame::new_infer_height(vec![
            Series::new("name".into(), &["abc", "", "x y"]).into_column(),
            Series::new("id".into(), &[1i32, 2, 3]).into_column(),
        ]).unwrap();
        write_por(&df, &a[2], PorWriteOptions::default()).unwrap();
    } else {
        let (_, df) = read_por(&a[2]).unwrap();
        println!("{:?}", df.column("NAME").unwrap().str().unwrap().iter().collect::<Vec<_>>());
    }
}
