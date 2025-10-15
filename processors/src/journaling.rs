use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use common::{OrderCommand, OrderCommandType, Snowflake, order_debug, order_info};
use vex_networking::server::Publications;

pub struct JournalingProcessor {
    snowflake: Snowflake,
    publications: Arc<Publications>,
    replay_enabled: Arc<AtomicBool>,
}

impl JournalingProcessor {
    pub fn new(publications: Arc<Publications>) -> Self {
        Self {
            snowflake: Snowflake::new(None).unwrap(),
            publications,
            replay_enabled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn journal_command(&mut self, cmd: &mut OrderCommand) {
        // during replay, we do not re-assign order IDs, timestamps, re-journal to archive
        if self.replay_enabled.load(Ordering::Relaxed) {
            order_debug!("replay_passthrough", cmd, stage = "journal");
            return;
        }

        if cmd.command != OrderCommandType::CancelOrder {
            cmd.order_id = self.snowflake.generate(cmd.order_id).unwrap();
        }
        cmd.timestamp = self.snowflake.timestamp();
        self.publications.publish_to_archive(cmd);
        order_info!("command_ingested", cmd, stage = "journal");
    }

    pub fn replay_control(&self) -> ReplayControl {
        ReplayControl {
            flag: Arc::clone(&self.replay_enabled),
        }
    }

    pub fn journal_event(&self, cmd: &mut OrderCommand) {
        order_debug!("command_written", cmd, stage = "journal");

        // Also journal any trade events if they exist
        if let Some(event) = cmd.events() {
            tracing::debug!(
                target: "order_pipeline",
                event = "trade_snapshot",
                order_id = cmd.order_id,
                price = event.price,
                size = event.size
            );
        }
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
        self.flag.load(Ordering::Relaxed)
    }
}
