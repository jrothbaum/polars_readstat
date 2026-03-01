use polars::prelude::{DataFrame, PolarsResult};
use std::sync::mpsc::{sync_channel, Receiver};
use std::thread::{self, JoinHandle};

pub(crate) struct Prefetcher {
    rx: Receiver<PolarsResult<DataFrame>>,
    handle: Option<JoinHandle<()>>,
}

impl Prefetcher {
    pub(crate) fn new(
        rx: Receiver<PolarsResult<DataFrame>>,
        handle: Option<JoinHandle<()>>,
    ) -> Self {
        Self { rx, handle }
    }

    pub(crate) fn next(&self) -> PolarsResult<Option<DataFrame>> {
        match self.rx.recv() {
            Ok(Ok(df)) => Ok(Some(df)),
            Ok(Err(e)) => Err(e),
            Err(_) => Ok(None),
        }
    }
}

impl Drop for Prefetcher {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub(crate) fn spawn_prefetcher<I>(iter: I) -> Prefetcher
where
    I: Iterator<Item = PolarsResult<DataFrame>> + Send + 'static,
{
    let (tx, rx) = sync_channel::<PolarsResult<DataFrame>>(10);
    let handle = thread::spawn(move || {
        for item in iter {
            let is_err = item.is_err();
            if tx.send(item).is_err() {
                break;
            }
            if is_err {
                break;
            }
        }
    });
    Prefetcher::new(rx, Some(handle))
}
