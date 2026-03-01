use polars::prelude::*;
use polars_readstat_rs::{
    SasValueLabelKey, SasValueLabelMap, SasValueLabels, SasVariableLabels, SasWriter,
};
use std::collections::HashMap;

fn main() -> polars_readstat_rs::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: sas_write_csv <out_dir_or_stem>");
        std::process::exit(1);
    }

    let df = DataFrame::new_infer_height(vec![
        Series::new("id".into(), &[1i32, 2, 3]).into_column(),
        Series::new("name".into(), &["alice", "bob", "carol"]).into_column(),
        Series::new("date".into(), &[0i32, 1, 2])
            .cast(&DataType::Date)
            .unwrap()
            .into_column(),
    ])?;

    let mut vmap: SasValueLabelMap = HashMap::new();
    vmap.insert(SasValueLabelKey::from(1.0), "one".to_string());
    vmap.insert(SasValueLabelKey::from(2.0), "two".to_string());
    let mut vlabels: SasValueLabels = HashMap::new();
    vlabels.insert("id".to_string(), vmap);

    let var_labels = SasVariableLabels::from([
        ("id".to_string(), "Identifier".to_string()),
        ("name".to_string(), "Person name".to_string()),
    ]);

    let out = SasWriter::new(&args[1])
        .with_dataset_name("demo")
        .with_value_labels(vlabels)
        .with_variable_labels(var_labels)
        .write_df(&df)?;

    println!("Wrote CSV: {}", out.0.display());
    println!("Wrote SAS: {}", out.1.display());
    Ok(())
}
