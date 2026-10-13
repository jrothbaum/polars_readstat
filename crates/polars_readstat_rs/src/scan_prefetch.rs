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
        crate::disconnect_receiver(&mut self.rx);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn drop_before_exhausted_does_not_hang() {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            // Endless producer: it is always blocked on the full channel when we drop.
            let iter = std::iter::repeat_with(|| Ok(DataFrame::empty()));
            let prefetch = spawn_prefetcher(iter);
            prefetch.next().unwrap();
            drop(prefetch);
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("dropping a partially consumed Prefetcher hung");
    }
}
