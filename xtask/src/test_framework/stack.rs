//! The containers of the xtask docker-compose stack (`xtask/tests/docker-compose.yml`) that a
//! scenario pauses, signals or restarts, by their fixed container names.

use crate::test_framework::types::{TestError, TestResult};
use duct::cmd;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Redpanda: the Kafka broker, with its Schema Registry.
pub const BROKER: &str = "vex-redpanda";

/// The core (`test_server`), its own container's only process. It shares the network and IPC
/// namespaces of the Aeron archive's container, so the journal outlives it.
pub const CORE: &str = "vex-server";

fn docker(args: &[&str]) -> TestResult<String> {
    cmd("docker", args)
        .stderr_to_stdout()
        .read()
        .map_err(TestError::from)
}

/// The current time, as `docker logs --since` takes it.
pub fn now() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is past 1970");
    format!("{}.{:09}", now.as_secs(), now.subsec_nanos())
}

/// Starts a stopped container.
pub fn start(container: &str) -> TestResult<()> {
    docker(&["start", container]).map(drop)
}

/// Sends `signal` (`TERM`, `KILL`) to the container's main process.
pub fn signal(container: &str, signal: &str) -> TestResult<()> {
    docker(&["kill", "--signal", signal, container]).map(drop)
}

/// Whether the container is running, and the exit code it last exited with.
fn state(container: &str) -> TestResult<(bool, i64)> {
    let state = docker(&[
        "inspect",
        "--format",
        "{{.State.Running}} {{.State.ExitCode}}",
        container,
    ])?;
    let parsed = state
        .split_once(' ')
        .and_then(|(running, code)| Some((running.parse().ok()?, code.trim().parse().ok()?)));
    parsed.ok_or_else(|| TestError::Parse {
        message: format!("unexpected state of {container}: {state:?}"),
    })
}

pub fn is_running(container: &str) -> TestResult<bool> {
    state(container).map(|(running, _)| running)
}

/// Waits until the container has exited, and returns its exit code.
pub fn wait_exited(container: &str, timeout: Duration) -> TestResult<i64> {
    let start = Instant::now();
    loop {
        let (running, code) = state(container)?;
        if !running {
            return Ok(code);
        }
        if start.elapsed() > timeout {
            return Err(TestError::Timeout { timeout });
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The container's output, both streams, since `since` (see [`now`]).
pub fn logs_since(container: &str, since: &str) -> TestResult<String> {
    docker(&["logs", "--since", since, container])
}

/// Waits until the container's output since `since` contains `needle`.
pub fn wait_for_log(
    container: &str,
    since: &str,
    needle: &str,
    timeout: Duration,
) -> TestResult<()> {
    let start = Instant::now();
    while !logs_since(container, since)?.contains(needle) {
        if start.elapsed() > timeout {
            return Err(TestError::Verification {
                message: format!("{container} did not log {needle:?} within {timeout:?}"),
            });
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Ok(())
}

/// A paused container, unpaused when this is dropped, whichever way the scenario ends.
pub struct Paused(&'static str);

impl Paused {
    pub fn new(container: &'static str) -> TestResult<Self> {
        docker(&["pause", container])?;
        Ok(Self(container))
    }
}

impl Drop for Paused {
    fn drop(&mut self) {
        if let Err(e) = docker(&["unpause", self.0]) {
            eprintln!("Failed to unpause {}: {e}", self.0);
        }
    }
}
