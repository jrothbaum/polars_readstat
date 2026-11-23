use polars::prelude::*;
use polars_readstat_rs::{PorWriteOptions, read_por, write_por};

// Numbers far below 1 used to be written as a fraction capped at 50 base-30 digits, leading
// zeros included: they lost precision from about 1e-56 and were written as zero from about 4e-73.
#[test]
fn spss_por_small_numbers_roundtrip() -> PolarsResult<()> {
    let values = [
        0.5,
        1.5e-20,
        1e-21,
        1e-50,
        1e-60,
        1e-70,
        1e-72,
        1e-78,
        1e-80,
        -3e-90,
        2.5e-300,
        1e-310,
        f64::from_bits(1), // the smallest subnormal
        f64::MIN_POSITIVE,
        0.0,
    ];
    let df = DataFrame::new_infer_height(vec![Series::new("x".into(), &values).into_column()])?;
    let file_path = std::env::temp_dir().join("test_spss_por_small_numbers.por");
    let _ = std::fs::remove_file(&file_path);
    write_por(&df, &file_path, PorWriteOptions::default())
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let (_meta, out) =
        read_por(&file_path).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let got: Vec<Option<f64>> = out.column("X")?.f64()?.iter().collect();
    let _ = std::fs::remove_file(&file_path);
    assert_eq!(got.len(), values.len());
    for (want, got) in values.iter().zip(got) {
        let got = got.unwrap_or_else(|| panic!("{want:e} read back as missing"));
        if *want == 0.0 {
            assert_eq!(got, 0.0);
            continue;
        }
        assert!(got != 0.0, "{want:e} read back as 0");
        let rel = ((got - want) / want).abs();
        // a few ulps: the reader rebuilds the value with float arithmetic
        let tolerance = if want.abs() < f64::MIN_POSITIVE {
            // a subnormal has fewer significant bits
            f64::from_bits(1) / want.abs()
        } else {
            1e-15
        };
        assert!(rel <= tolerance, "{want:e} read back as {got:e}");
    }
    Ok(())
}
