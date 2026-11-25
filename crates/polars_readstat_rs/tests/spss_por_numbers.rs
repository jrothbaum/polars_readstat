use polars::prelude::*;
use polars_readstat_rs::{PorWriteOptions, read_por, write_por};

// Every finite double must come back from a .por file bit-identical. Before, numbers were
// written as a fraction of at most 50 base-30 digits counted from the point: values far below 1
// lost precision from about 1e-56 and were written as zero from about 4e-73, and ordinary
// fractions came back a few ulps off.
#[test]
fn spss_por_numbers_roundtrip_exactly() -> PolarsResult<()> {
    let mut values = vec![
        0.0,
        0.5,
        0.1,
        1.1,
        -1000.3,
        23.45,
        1.0 / 3.0,
        1e-20,
        1e-70,
        1e-80,
        -3e-90,
        2.5e-300,
        1e-310,
        f64::from_bits(1),
        f64::MIN_POSITIVE,
        1e17,
        3e17,
        -1e300,
        f64::MAX,
        11_903_760_000.0,
    ];
    let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
    for _ in 0..20_000 {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        let v = f64::from_bits(s);
        if v.is_finite() {
            values.push(v);
        }
    }

    let df = DataFrame::new_infer_height(vec![Series::new("x".into(), &values).into_column()])?;
    let file_path = std::env::temp_dir().join("test_spss_por_numbers.por");
    let _ = std::fs::remove_file(&file_path);
    write_por(&df, &file_path, PorWriteOptions::default())
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let (_meta, out) =
        read_por(&file_path).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let _ = std::fs::remove_file(&file_path);

    let got: Vec<Option<f64>> = out.column("X")?.f64()?.iter().collect();
    assert_eq!(got.len(), values.len());
    for (want, got) in values.iter().zip(got) {
        let got = got.unwrap_or_else(|| panic!("{want:e} read back as missing"));
        // -0.0 is written as 0
        let want = if *want == 0.0 { 0.0 } else { *want };
        assert_eq!(got.to_bits(), want.to_bits(), "{want:e} read back as {got:e}");
    }
    Ok(())
}
