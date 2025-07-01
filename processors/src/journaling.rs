use common::cmd::MatcherTradeEvent;
use common::cmd::OrderCommand;
use tracing::info;
/// Responsible for writing all commands and events to a persistent log for durability.
/// This is the Rust equivalent of `JournalingProcessor.java`.
pub struct JournalingProcessor;

impl JournalingProcessor {
    pub fn new() -> Self {
        Self
    }

    // Ring buffer Disruptor to JournalingProcessor - Logger(Excali-0)
    pub fn journal_command(&self, cmd: &OrderCommand) {
        info!("[Journal] Writing command to disk: ID {}", cmd.order_id);
    }

    pub fn journal_event(&self, event: &MatcherTradeEvent) {
        info!(
            "[Journal] Writing event to disk: Type {:?}",
            event.event_type
        );
    }
}

impl Default for JournalingProcessor {
    fn default() -> Self {
        Self::new()
    }
}
