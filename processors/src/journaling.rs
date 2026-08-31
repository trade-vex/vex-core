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
}

impl JournalingProcessor {
    pub fn new(publications: Arc<Publications>, replay_control: ReplayControl) -> Self {
        Self {
            snowflake: Snowflake::new(None).unwrap(),
            publications,
            replay_enabled: replay_control,
        }
    }

    pub fn journal_command(&mut self, cmd: &mut OrderCommand) {
        // during replay, we do not re-assign order IDs, timestamps, re-journal to archive
        if self.replay_enabled.is_enabled() {
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
        cmd.timestamp = self.snowflake.timestamp();
        self.publications.publish_to_archive(cmd);
        order_debug!("command_ingested", cmd, stage = "journal");
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
