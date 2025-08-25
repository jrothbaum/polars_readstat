use std::sync::{Arc, Mutex, Condvar};
use polars::prelude::DataFrame;

pub struct ReadStatStreamer {
    chunks: Arc<Mutex<Vec<DataFrame>>>,
    condvar: Arc<Condvar>,
    is_complete: Arc<Mutex<bool>>,
}

impl ReadStatStreamer {
    pub fn new() -> (Self, Arc<Mutex<Vec<DataFrame>>>, Arc<Condvar>, Arc<Mutex<bool>>) {
        let chunks = Arc::new(Mutex::new(Vec::new()));
        let condvar = Arc::new(Condvar::new());
        let is_complete = Arc::new(Mutex::new(false));
        
        let consumer = Self {
            chunks: chunks.clone(),
            condvar: condvar.clone(),
            is_complete: is_complete.clone(),
        };
        
        (consumer, chunks, condvar, is_complete)
    }
    
    /// Get next DataFrame - check vector first, then wait for callback
    pub fn next(&mut self) -> Option<DataFrame> {
        let mut chunks = self.chunks.lock().unwrap();
        
        loop {
            // Check vector first
            if !chunks.is_empty() {
                return Some(chunks.remove(0));
            }
            
            // Vector is empty - check if done
            if *self.is_complete.lock().unwrap() {
                return None;
            }
            
            // Vector is empty and not done - wait for callback to signal
            chunks = self.condvar.wait(chunks).unwrap();
        }
    }
}
