use std::{
    env,
    thread
};
use rayon::ThreadPoolBuilder;

pub fn set_max_threads() -> Result<(), Box<dyn std::error::Error>> {
    // Attempt to initialize the global Rayon thread pool
    // This will only succeed the first time it's called across the entire process.
    // If another part of the application (or a library) has already initialized it,
    // we'll gracefully handle that.
    match ThreadPoolBuilder::new()
        .num_threads(get_max_threads())
        .build_global()
    {
        Ok(_) => {
            Ok(())
        },
        Err(e) => {
            // Already initialized.
            Ok(())
        }
    }
}

pub fn get_max_threads() -> usize {
    let max_threads = env::var("POLARS_MAX_THREADS")
        .map(|val| val.parse::<usize>().unwrap_or_else(|_| {
            println!("Warning: POLARS_MAX_THREADS is not a valid number, using system thread count");
            thread::available_parallelism().map_or(1, |p| p.get())
        }))
        .unwrap_or_else(|_| {
            // If env var is not set, use the number of available threads
            thread::available_parallelism().map_or(1, |p| p.get())
        });

    max_threads
}
