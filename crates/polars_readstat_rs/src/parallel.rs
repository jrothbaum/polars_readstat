//! Shared scheduler for the parallel readers.
//!
//! A read is split into numbered jobs (a row range or page range each). Jobs
//! run on Polars' global rayon pool instead of a private pool per scan, so the
//! number of decode threads stays bounded no matter how many scans run at once.
//!
//! Rules that keep this safe on a shared pool:
//! - A job never blocks: it decodes its range and pushes the result into an
//!   unbounded channel. Backpressure comes from the consumer, which only
//!   submits a new job when fewer than `window` jobs are outstanding
//!   (submitted but not yet handed out). That also caps the reorder buffer in
//!   ordered mode at `window` jobs.
//! - When the consumer itself runs on a pool thread, it helps run queued pool
//!   work while it waits instead of parking that thread.
//! - Dropping the iterator just flags the remaining jobs to skip; nothing is
//!   joined, so an early drop can never wait on a worker.

use polars::prelude::{DataFrame, PolarsError, PolarsResult};
use polars_core::runtime::THREAD_POOL as POOL;
use std::collections::{BTreeMap, VecDeque};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::Arc;
use std::time::Duration;

/// Decodes job `i` into zero or more batches, in file order.
pub(crate) type BatchJob = dyn Fn(usize) -> PolarsResult<Vec<DataFrame>> + Send + Sync;

type JobResult = (usize, PolarsResult<Vec<DataFrame>>);

pub(crate) struct ParallelBatches {
    job: Arc<BatchJob>,
    n_jobs: usize,
    ordered: bool,
    max_running: usize,
    window: usize,
    next_submit: usize,
    /// Ordered mode: index of the next job to hand out.
    next_yield: usize,
    running: usize,
    /// Submitted but not yet handed out (running + finished and buffered).
    outstanding: usize,
    /// Ordered mode: finished jobs keyed by index.
    done: BTreeMap<usize, PolarsResult<Vec<DataFrame>>>,
    /// Unordered mode: finished jobs in completion order.
    ready: VecDeque<PolarsResult<Vec<DataFrame>>>,
    current: VecDeque<DataFrame>,
    tx: Sender<JobResult>,
    rx: Receiver<JobResult>,
    cancelled: Arc<AtomicBool>,
    finished: bool,
}

impl ParallelBatches {
    /// Run `n_jobs` jobs with at most `max_running` executing at once.
    /// `ordered` yields batches in job order; otherwise in completion order.
    pub(crate) fn new(n_jobs: usize, max_running: usize, ordered: bool, job: Arc<BatchJob>) -> Self {
        let max_running = max_running.max(1);
        let (tx, rx) = channel();
        Self {
            job,
            n_jobs,
            ordered,
            max_running,
            window: max_running * 2,
            next_submit: 0,
            next_yield: 0,
            running: 0,
            outstanding: 0,
            done: BTreeMap::new(),
            ready: VecDeque::new(),
            current: VecDeque::new(),
            tx,
            rx,
            cancelled: Arc::new(AtomicBool::new(false)),
            finished: false,
        }
    }

    fn fill(&mut self) {
        while self.next_submit < self.n_jobs
            && self.running < self.max_running
            && self.outstanding < self.window
        {
            let idx = self.next_submit;
            self.next_submit += 1;
            self.running += 1;
            self.outstanding += 1;
            let job = Arc::clone(&self.job);
            let tx = self.tx.clone();
            let cancelled = Arc::clone(&self.cancelled);
            POOL.spawn(move || {
                if cancelled.load(Ordering::Relaxed) {
                    return;
                }
                // A panic escaping a rayon spawn aborts the process, and a job that
                // never reports back would leave the consumer waiting forever.
                let result = catch_unwind(AssertUnwindSafe(|| job(idx))).unwrap_or_else(|p| {
                    let msg = p
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| p.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic".to_string());
                    Err(PolarsError::ComputeError(
                        format!("parallel read worker panicked: {msg}").into(),
                    ))
                });
                let _ = tx.send((idx, result));
            });
        }
    }

    fn accept(&mut self, (idx, result): JobResult) {
        self.running -= 1;
        if self.ordered {
            self.done.insert(idx, result);
        } else {
            self.ready.push_back(result);
        }
    }

    fn take_ready(&mut self) -> Option<PolarsResult<Vec<DataFrame>>> {
        let result = if self.ordered {
            let r = self.done.remove(&self.next_yield)?;
            self.next_yield += 1;
            r
        } else {
            self.ready.pop_front()?
        };
        self.outstanding -= 1;
        Some(result)
    }

    fn wait(&mut self) -> JobResult {
        if POOL.current_thread_index().is_none() {
            return self.rx.recv().expect("scheduler keeps a sender alive");
        }
        // On a pool thread: run other queued work (possibly our own jobs) while waiting.
        loop {
            match self.rx.try_recv() {
                Ok(msg) => return msg,
                Err(TryRecvError::Empty) => match rayon::yield_now() {
                    Some(rayon::Yield::Executed) => {}
                    _ => match self.rx.recv_timeout(Duration::from_micros(200)) {
                        Ok(msg) => return msg,
                        Err(RecvTimeoutError::Timeout) => {}
                        Err(RecvTimeoutError::Disconnected) => {
                            unreachable!("scheduler keeps a sender alive")
                        }
                    },
                },
                Err(TryRecvError::Disconnected) => unreachable!("scheduler keeps a sender alive"),
            }
        }
    }
}

impl Iterator for ParallelBatches {
    type Item = PolarsResult<DataFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(df) = self.current.pop_front() {
                return Some(Ok(df));
            }
            if self.finished {
                return None;
            }
            while let Ok(msg) = self.rx.try_recv() {
                self.accept(msg);
            }
            if let Some(result) = self.take_ready() {
                // Queue the next job before handing this batch to the caller.
                self.fill();
                match result {
                    Ok(batches) => {
                        self.current = batches.into();
                        continue;
                    }
                    Err(e) => {
                        self.finished = true;
                        self.cancelled.store(true, Ordering::Relaxed);
                        return Some(Err(e));
                    }
                }
            }
            if self.outstanding == 0 && self.next_submit >= self.n_jobs {
                self.finished = true;
                return None;
            }
            self.fill();
            let msg = self.wait();
            self.accept(msg);
        }
    }
}

impl Drop for ParallelBatches {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;
    use std::sync::atomic::AtomicUsize;

    fn job_df(i: usize) -> DataFrame {
        df!("i" => [i as u32]).unwrap()
    }

    fn values(iter: ParallelBatches) -> Vec<u32> {
        iter.map(|df| df.unwrap().column("i").unwrap().u32().unwrap().get(0).unwrap())
            .collect()
    }

    #[test]
    fn ordered_yields_in_job_order() {
        // Later jobs finish first.
        let job: Arc<BatchJob> = Arc::new(|i| {
            std::thread::sleep(Duration::from_micros(((50 - i) * 100) as u64));
            Ok(vec![job_df(i)])
        });
        let got = values(ParallelBatches::new(50, 4, true, job));
        assert_eq!(got, (0..50).collect::<Vec<u32>>());
    }

    #[test]
    fn unordered_yields_every_job_once() {
        let job: Arc<BatchJob> = Arc::new(|i| Ok(vec![job_df(i)]));
        let mut got = values(ParallelBatches::new(200, 8, false, job));
        got.sort();
        assert_eq!(got, (0..200).collect::<Vec<u32>>());
    }

    #[test]
    fn empty_jobs_and_multi_batch_jobs() {
        let job: Arc<BatchJob> = Arc::new(|i| Ok((0..i % 3).map(|_| job_df(i)).collect()));
        let got = values(ParallelBatches::new(9, 3, true, job));
        assert_eq!(got, vec![1, 2, 2, 4, 5, 5, 7, 8, 8]);
    }

    #[test]
    fn bounded_outstanding_jobs() {
        let started = Arc::new(AtomicUsize::new(0));
        let s = Arc::clone(&started);
        let job: Arc<BatchJob> = Arc::new(move |i| {
            s.fetch_add(1, Ordering::SeqCst);
            Ok(vec![job_df(i)])
        });
        let mut iter = ParallelBatches::new(1000, 4, true, job);
        iter.next().unwrap().unwrap();
        std::thread::sleep(Duration::from_millis(50));
        // At most `window` (2 × max_running) jobs submitted ahead of the consumer.
        assert!(started.load(Ordering::SeqCst) <= 9);
    }

    #[test]
    fn error_stops_iteration() {
        let job: Arc<BatchJob> = Arc::new(|i| {
            if i == 3 {
                Err(PolarsError::ComputeError("boom".into()))
            } else {
                Ok(vec![job_df(i)])
            }
        });
        let results: Vec<_> = ParallelBatches::new(10, 2, true, job).collect();
        assert_eq!(results.len(), 4);
        assert!(results[3].is_err());
    }

    #[test]
    fn panic_becomes_error() {
        let job: Arc<BatchJob> = Arc::new(|i| {
            if i == 1 {
                panic!("bad page");
            }
            Ok(vec![job_df(i)])
        });
        let results: Vec<_> = ParallelBatches::new(4, 2, true, job).collect();
        let err = results.into_iter().find_map(|r| r.err()).expect("an error");
        assert!(err.to_string().contains("bad page"));
    }

    #[test]
    fn consumer_on_pool_thread_does_not_deadlock() {
        // Every pool thread is a consumer; their jobs need those same threads.
        let n = POOL.current_num_threads() + 2;
        let totals: Vec<usize> = POOL.install(|| {
            use rayon::prelude::*;
            (0..n)
                .into_par_iter()
                .map(|_| {
                    let job: Arc<BatchJob> = Arc::new(|i| Ok(vec![job_df(i)]));
                    ParallelBatches::new(20, 4, true, job).count()
                })
                .collect()
        });
        assert!(totals.iter().all(|&t| t == 20));
    }
}
