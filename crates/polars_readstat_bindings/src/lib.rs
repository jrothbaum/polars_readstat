#[cfg(feature = "python")]
mod pybindings;

#[cfg(feature = "python")]
pub use pybindings::polars_readstat_bindings;
