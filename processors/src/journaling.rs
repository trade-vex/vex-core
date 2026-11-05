use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use common::{OrderCommand, OrderCommandType, Snowflake, order_debug};
use vex_networking::server::Publications;

pub struct JournalingProcessor {
    snowflake: Snowflake,
    publications: Arc<Publications>,
    replay_enabled: ReplayControl,
    /// Timestamp of the last command this stage stamped or replayed.
    ///
    /// Every event a command produces carries its timestamp, and consumers rank a key's events by
    /// it, so it must never repeat or step backwards, even when the wall clock does.
    last_timestamp: u64,
}

impl JournalingProcessor {
    pub fn new(publications: Arc<Publications>, replay_control: ReplayControl) -> Self {
        Self {
            snowflake: Snowflake::new(None).unwrap(),
            publications,
            replay_enabled: replay_control,
            last_timestamp: 0,
        }
    }

    pub fn journal_command(&mut self, cmd: &mut OrderCommand) {
        // during replay, we do not re-assign order IDs, timestamps, re-journal to archive
        if self.replay_enabled.is_enabled() {
            // Seed the clock from the journal, so the first live command after a restart is
            // stamped above every command of the previous run even if the wall clock is behind.
            self.last_timestamp = self.last_timestamp.max(cmd.timestamp);
            order_debug!("replay_passthrough", cmd, stage = "journal");
            return;
        }

        if cmd.command != OrderCommandType::CancelOrder {
            // Generate order_id embedding the sender gateway id captured at ingress
            cmd.order_id = self
                .snowflake
                .generate(cmd.route_gateway_id as u64)
                .unwrap();
        }
        cmd.timestamp = self.next_timestamp();
        self.publications.publish_to_archive(cmd);
        order_debug!("command_ingested", cmd, stage = "journal");
    }

    /// Wall-clock nanoseconds, forced strictly above the previous command's timestamp.
    ///
    /// It equals the wall clock except when the clock steps backwards (NTP step, restart on a
    /// host whose clock is behind) or two commands land in the same nanosecond; then it advances
    /// by 1 ns per command until the wall clock catches up.
    fn next_timestamp(&mut self) -> u64 {
        self.last_timestamp = self
            .snowflake
            .timestamp()
            .max(self.last_timestamp.saturating_add(1));
        self.last_timestamp
    }
}

/// Control structure to enable/disable replay mode
/// when the vex-core runs in replay mode
/// the switch sets the flag to true, untill the replay is done
/// allowing to skip certain processors namely 1. Journalling 2. Events
#[derive(Clone)]
pub struct ReplayControl {
    flag: Arc<AtomicBool>,
}

impl ReplayControl {
    pub fn enabled() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn disabled() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
        }
    }
    // we want to make sure that when enabling replay mode
    // all subsequent reads of the flag see the updated value
    // similarly when disabling replay mode
    // we want to make sure that all prior writes are visible
    // before the flag is set to false
    pub fn enable(&self) {
        self.flag.store(true, Ordering::SeqCst);
    }

    pub fn disable(&self) {
        self.flag.store(false, Ordering::SeqCst);
    }

    pub fn set(&self, enabled: bool) {
        self.flag.store(enabled, Ordering::SeqCst);
    }

    pub fn is_enabled(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{Side, TimeInForce};
    use std::time::{SystemTime, UNIX_EPOCH};

    const HOUR_NANOS: u64 = 3_600 * 1_000_000_000;

    /// `Publications::new()` has no archive publication, so the archive write is skipped.
    fn journal(replay: ReplayControl) -> JournalingProcessor {
        JournalingProcessor::new(Arc::new(Publications::new()), replay)
    }

    fn order() -> OrderCommand {
        OrderCommand::place_order(TimeInForce::Gtc, 1, 100, 1, Side::Bid, 1, 0)
    }

    fn wall_clock_nanos() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    #[test]
    fn live_timestamps_strictly_increase_per_command() {
        let mut journal = journal(ReplayControl::disabled());
        let mut previous = 0;
        // Back to back, many commands land in the same wall-clock nanosecond on a fast host.
        for _ in 0..10_000 {
            let mut cmd = order();
            journal.journal_command(&mut cmd);
            assert!(cmd.timestamp > previous, "timestamps must never repeat");
            previous = cmd.timestamp;
        }
    }

    #[test]
    fn timestamp_does_not_step_back_when_the_wall_clock_does() {
        let mut journal = journal(ReplayControl::disabled());
        // The previous command was stamped while the wall clock read an hour ahead; the clock
        // has since been stepped back.
        let previous = wall_clock_nanos() + HOUR_NANOS;
        journal.last_timestamp = previous;

        let mut first = order();
        journal.journal_command(&mut first);
        let mut second = order();
        journal.journal_command(&mut second);

        assert_eq!(first.timestamp, previous + 1);
        assert_eq!(second.timestamp, previous + 2);
    }

    #[test]
    fn replayed_journal_seeds_the_clock_across_a_restart() {
        // The previous run's last command was stamped an hour ahead of this host's wall clock.
        let previous_run_last = wall_clock_nanos() + HOUR_NANOS;
        let replay = ReplayControl::enabled();
        let mut journal = journal(replay.clone());

        let mut replayed = order();
        replayed.order_id = 7;
        replayed.timestamp = previous_run_last;
        journal.journal_command(&mut replayed);
        assert_eq!(
            (replayed.order_id, replayed.timestamp),
            (7, previous_run_last),
            "replay must pass journaled commands through unchanged"
        );

        replay.disable();
        let mut live = order();
        journal.journal_command(&mut live);
        assert!(
            live.timestamp > previous_run_last,
            "the first live command must sort after every replayed one"
        );
    }
}
