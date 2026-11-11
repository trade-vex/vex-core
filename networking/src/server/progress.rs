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

    pub(crate) fn submitted_sequence(&self) -> i64 {
        self.submitted.load(Ordering::Acquire)
    }

    /// Waits until the terminal handler has completed every sequence submitted so far. Fails only
    /// once `stall` passes without `completed` advancing, as `replay::drain_replay` does:
    /// a drain that re-publishes a replay waits for Kafka, which can take as long as Kafka needs,
    /// but a pipeline that stops (Kafka down, a stuck stage) still fails.
    pub(crate) fn drain(&self, stall: Duration) -> Result<(), ServerError> {
        let target = self.submitted.load(Ordering::Acquire);
        let mut completed = self.completed.load(Ordering::Acquire);
        let mut last_progress = Instant::now();
        while completed < target {
            if last_progress.elapsed() >= stall {
                return Err(ServerError::DrainError(format!(
                    "terminal consumer stalled at {completed} for {stall:?}, submitted sequence {target}"
                )));
            }
            std::thread::sleep(Duration::from_millis(1));
            let now = self.completed.load(Ordering::Acquire);
            if now > completed {
                completed = now;
                last_progress = Instant::now();
            }
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use disruptor::{BusySpin, Producer, build_multi_producer};
    use std::sync::{Arc, mpsc};

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
    fn a_drain_that_keeps_advancing_outlasts_its_stall_timeout() {
        const STALL: Duration = Duration::from_millis(300);
        let progress = Arc::new(PipelineProgress::new());
        progress.submitted(19);
        let terminal = {
            let progress = Arc::clone(&progress);
            std::thread::spawn(move || {
                // Slower in total than the stall timeout, but never stalled that long.
                for sequence in 0..=19 {
                    std::thread::sleep(Duration::from_millis(30));
                    progress.completed(sequence);
                }
            })
        };

        let started = Instant::now();
        progress.drain(STALL).unwrap();
        assert!(started.elapsed() > STALL, "{:?}", started.elapsed());
        terminal.join().unwrap();
    }

    #[test]
    fn a_drain_that_stops_advancing_fails_after_its_stall_timeout() {
        const STALL: Duration = Duration::from_millis(50);
        let progress = PipelineProgress::new();
        progress.submitted(9);
        progress.completed(3);

        let started = Instant::now();
        let error = progress.drain(STALL).unwrap_err();
        assert!(started.elapsed() >= STALL);
        assert!(error.to_string().contains("stalled at 3"), "{error}");
    }
}
