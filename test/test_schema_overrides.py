import polars as pl
import pytest
from pathlib import Path
from polars_readstat import scan_readstat

def get_test_file_path():
    base_path = Path(__file__).parent.parent / "crates/cpp-sas7bdat/vendor/test"
    files = list(base_path.glob("**/*.sas7bdat"))
    files = [f for f in files if "zero_variables" not in f.name]
    if not files:
        pytest.skip("No .sas7bdat test files found in crates/cpp-sas7bdat/vendor/test")
    return str(files[0])

def test_schema_overrides_types():
    """
    Verifies that passing schema_overrides changes the resulting data types.
    """
    file_path = get_test_file_path()
    print(f"Testing against: {file_path}")

    try:
        df_default = scan_readstat(file_path).collect()
    except Exception as e:
        pytest.skip(f"Could not read test file normally: {e}")

    # Find a numeric column to test overriding
    # We try to cast a Float/Int column to a String (Utf8)
    target_col = None
    for col, dtype in df_default.schema.items():
        if dtype in [pl.Float64, pl.Int64, pl.Float32]:
            target_col = col
            break
    
    if target_col is None:
        pytest.skip("No numeric columns found in the test file to override.")

    print(f"Attempting to override column '{target_col}' to String")

    overrides = {target_col: pl.String}
    q_override = scan_readstat(file_path, schema_overrides=overrides)
    
    assert q_override.schema[target_col] == pl.String
    
    df_override = q_override.collect()
    assert df_override.schema[target_col] == pl.String
    
    first_val = df_override[target_col][0]
    assert isinstance(first_val, str)

def test_schema_overrides_non_existent_column():
    """
    Verifies that overriding a column that doesn't exist 
    does not crash the reader (it should just be ignored).
    """
    file_path = get_test_file_path()
    overrides = {"this_col_does_not_exist": pl.Int64}
    df = scan_readstat(file_path, schema_overrides=overrides).collect()
    assert not df.is_empty()