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
    /// Timestamp of the last command this stage stamped.
    ///
    /// Every event a command produces carries its timestamp, and consumers rank a key's events by
    /// it, so it must never repeat or step backwards, even when the wall clock does. Order ids
    /// are derived from it, so they cannot either.
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
            order_debug!("replay_passthrough", cmd, stage = "journal");
            return;
        }

        cmd.timestamp = self.next_timestamp();
        if cmd.command != OrderCommandType::CancelOrder {
            // Generate order_id embedding the sender gateway id captured at ingress
            cmd.order_id = self
                .snowflake
                .generate(cmd.timestamp, cmd.route_gateway_id as u64)
                .unwrap();
        }
        self.publications.publish_to_archive(cmd);
        order_debug!("command_ingested", cmd, stage = "journal");
    }

    /// Wall-clock nanoseconds, forced strictly above the previous command's timestamp and above
    /// the last command in the journal ([`Publications::journal_tail`]).
    ///
    /// It equals the wall clock except when the clock is behind (an NTP step back, a restart on a
    /// host whose clock is behind the previous run's) or two commands land in the same
    /// nanosecond; then it advances by 1 ns per command until the wall clock catches up.
    fn next_timestamp(&mut self) -> u64 {
        let previous = self.last_timestamp.max(self.publications.journal_tail());
        self.last_timestamp = self.snowflake.timestamp().max(previous.saturating_add(1));
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

    /// The last command in the journal, stamped `timestamp` by the previous run: an order from
    /// the highest gateway, so its id is the highest any command of that nanosecond can have.
    /// The server hands its timestamp to journaling before it accepts a command.
    fn recorded_tail(publications: &Publications, timestamp: u64) -> OrderCommand {
        let mut recorded = order();
        recorded.route_gateway_id = 15;
        recorded.timestamp = timestamp;
        recorded.order_id = Snowflake::new(None)
            .unwrap()
            .generate(timestamp, 15)
            .unwrap();
        publications.set_journal_tail(timestamp);
        recorded
    }

    #[test]
    fn live_timestamps_and_ids_strictly_increase_per_command() {
        let mut journal = journal(ReplayControl::disabled());
        let (mut previous_timestamp, mut previous_id) = (0, 0);
        // Back to back, many commands land in the same wall-clock nanosecond on a fast host.
        for _ in 0..10_000 {
            let mut cmd = order();
            journal.journal_command(&mut cmd);
            assert!(
                cmd.timestamp > previous_timestamp,
                "timestamps must never repeat"
            );
            assert!(cmd.order_id > previous_id, "ids must never repeat");
            (previous_timestamp, previous_id) = (cmd.timestamp, cmd.order_id);
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
        assert!(first.order_id < second.order_id);
    }

    #[test]
    fn a_start_without_replay_continues_above_the_journal_tail() {
        let publications = Arc::new(Publications::new());
        // The previous run's last command was stamped an hour ahead of this host's wall clock.
        let recorded = recorded_tail(&publications, wall_clock_nanos() + HOUR_NANOS);
        let mut journal = JournalingProcessor::new(publications, ReplayControl::disabled());

        let mut live = order();
        journal.journal_command(&mut live);

        assert_eq!(live.timestamp, recorded.timestamp + 1);
        assert!(
            live.order_id > recorded.order_id,
            "the first live id must be above every recorded one"
        );
    }

    #[test]
    fn a_start_with_replay_continues_above_the_journal_tail() {
        let publications = Arc::new(Publications::new());
        let recorded = recorded_tail(&publications, wall_clock_nanos() + HOUR_NANOS);
        let replay = ReplayControl::enabled();
        let mut journal = JournalingProcessor::new(publications, replay.clone());

        let mut replayed = recorded.clone();
        journal.journal_command(&mut replayed);
        assert_eq!(
            replayed, recorded,
            "replay must pass journaled commands through unchanged"
        );

        replay.disable();
        let mut live = order();
        journal.journal_command(&mut live);

        assert_eq!(live.timestamp, recorded.timestamp + 1);
        assert!(
            live.order_id > recorded.order_id,
            "the first live id must be above every replayed one"
        );
    }

    #[test]
    fn commands_in_the_tick_of_the_journal_tail_are_sequenced_above_it() {
        let publications = Arc::new(Publications::new());
        // A tail on the first nanosecond of its tick, ahead of the wall clock: the next live
        // commands share its tick.
        let ahead = wall_clock_nanos() + HOUR_NANOS;
        let snowflake = Snowflake::new(None).unwrap();
        let (_, _, sequence) = Snowflake::parse_id(snowflake.generate(ahead, 0).unwrap());
        let recorded = recorded_tail(&publications, ahead - u64::from(sequence));
        let mut journal = JournalingProcessor::new(publications, ReplayControl::disabled());

        let mut first = order();
        journal.journal_command(&mut first);
        let mut second = order();
        journal.journal_command(&mut second);

        let (tick, _, _) = Snowflake::parse_id(recorded.order_id);
        assert_eq!(Snowflake::parse_id(recorded.order_id), (tick, 15, 0));
        assert_eq!(Snowflake::parse_id(first.order_id), (tick, 0, 1));
        assert_eq!(Snowflake::parse_id(second.order_id), (tick, 0, 2));
        assert!(recorded.order_id < first.order_id && first.order_id < second.order_id);
    }
}
