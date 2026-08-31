use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use super::ServerError;

pub(crate) const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Ingress records successful submissions; the sole terminal handler acknowledges them.
/// The disruptor's consumer cursor is private in the pinned dependency.
pub(crate) struct PipelineProgress {
    submitted: AtomicI64,
    completed: AtomicI64,
}

impl PipelineProgress {
    pub(crate) fn new() -> Self {
        Self {
            submitted: AtomicI64::new(-1),
            completed: AtomicI64::new(-1),
        }
    }

    pub(crate) fn submitted(&self, sequence: i64) {
        self.submitted.fetch_max(sequence, Ordering::Release);
    }

    pub(crate) fn completed(&self, sequence: i64) {
        self.completed.store(sequence, Ordering::Release);
    }

    pub(crate) fn drain(&self, timeout: Duration) -> Result<(), ServerError> {
        let target = self.submitted.load(Ordering::Acquire);
        wait_until(timeout, || {
            Ok(self.completed.load(Ordering::Acquire) >= target)
        })
        .map_err(|error| {
            ServerError::DrainError(format!(
                "terminal consumer at {}, submitted sequence {target}: {error}",
                self.completed.load(Ordering::Acquire)
            ))
        })
    }
}

pub(crate) fn wait_until(
    timeout: Duration,
    mut complete: impl FnMut() -> Result<bool, ServerError>,
) -> Result<(), ServerError> {
    let start = Instant::now();
    loop {
        if complete()? {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(ServerError::DrainError("progress timed out".into()));
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Ingress must already be closed. A failure leaves recording resources alive.
pub(crate) fn finish_shutdown(
    drain: impl FnOnce() -> Result<(), ServerError>,
    flush: impl FnOnce() -> Result<(), ServerError>,
    close: impl FnOnce() -> Result<(), ServerError>,
) -> Result<(), ServerError> {
    drain()?;
    flush()?;
    close()
}

#[cfg(test)]
mod tests {
    use super::*;
    use disruptor::{BusySpin, Producer, build_multi_producer};
    use std::sync::{Arc, Mutex, mpsc};

    #[test]
    fn c8_2_terminal_ack_blocks_replay_completion() {
        let progress = Arc::new(PipelineProgress::new());
        let ack = Arc::clone(&progress);
        let (release, held) = mpsc::channel();
        let mut producer = build_multi_producer(64, || 0, BusySpin)
            .handle_events_with(move |_, sequence, _| {
                held.recv().unwrap();
                ack.completed(sequence);
            })
            .build();
        progress.submitted(producer.try_publish(|v| *v = 1).unwrap());
        assert!(progress.drain(Duration::from_millis(20)).is_err());
        release.send(()).unwrap();
        progress.drain(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn c7_1_queued_commands_and_lagging_archive_drain_before_close() {
        let progress = Arc::new(PipelineProgress::new());
        let pending = Arc::new(Mutex::new(Vec::new()));
        let journal = Arc::new(Mutex::new(Vec::new()));
        let ack = Arc::clone(&progress);
        let offered = Arc::clone(&pending);
        let (release, held) = mpsc::channel();
        let mut producer = build_multi_producer(64, || 0, BusySpin)
            .handle_events_with(move |cell, sequence, _| {
                held.recv().unwrap();
                // SAFETY: This test has a single consumer and reads only its current slot.
                offered.lock().unwrap().push(unsafe { *cell.get() });
                ack.completed(sequence);
            })
            .build();
        for value in 0..3 {
            progress.submitted(producer.try_publish(|v| *v = value).unwrap());
        }
        let worker = std::thread::spawn(move || {
            finish_shutdown(
                || progress.drain(Duration::from_secs(1)),
                || {
                    // The archive deliberately lags publication until this flush step.
                    assert!(journal.lock().unwrap().is_empty());
                    let target = pending.lock().unwrap().len();
                    assert_eq!(target, 3);
                    wait_until(Duration::from_secs(1), || {
                        if let Some(value) = pending.lock().unwrap().pop() {
                            journal.lock().unwrap().push(value);
                        }
                        Ok(journal.lock().unwrap().len() == target)
                    })
                },
                || {
                    let mut recovered = journal.lock().unwrap().clone();
                    recovered.sort();
                    assert_eq!(recovered, [0, 1, 2]);
                    Ok(())
                },
            )
            .unwrap();
        });
        for _ in 0..3 {
            release.send(()).unwrap();
        }
        worker.join().unwrap();
    }

    #[test]
    fn c7_1_drain_or_archive_failure_does_not_close_recording() {
        for fail_drain in [true, false] {
            let result = finish_shutdown(
                || {
                    if fail_drain {
                        Err(ServerError::DrainError("consumer stalled".into()))
                    } else {
                        Ok(())
                    }
                },
                || wait_until(Duration::ZERO, || Ok(false)),
                || panic!("recording must stay open on failure"),
            );
            assert!(result.is_err());
        }
    }
}
