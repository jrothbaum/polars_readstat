//! The SAS integration tests in tests/sas. The multi-GB files in tests/sas/data/too_big are
//! left out (see `common::all_sas_files`).
//!
//! Not included: test_data.rs, test_header.rs, test_integration.rs and test_metadata.rs read
//! file1/file2.sas7bdat from the removed crates/cpp-sas7bdat vendor directory.

#[path = "sas/common/mod.rs"]
mod common;

#[path = "sas/integration_tests.rs"]
mod integration_tests;

#[path = "sas/regression_tests.rs"]
mod regression_tests;

#[path = "sas/test_all_files.rs"]
mod test_all_files;

#[path = "sas/test_parallel_reading.rs"]
mod test_parallel_reading;
