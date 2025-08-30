use common::OrderCommand;
use common::ProcessedOrderCommand;
use tracing::info;

pub struct JournalingProcessor;

impl JournalingProcessor {
    pub fn new() -> Self {
        Self
    }

    // Ring buffer Disruptor to JournalingProcessor - Logger(Excali-0)
    pub fn journal_command(&self, cmd: &OrderCommand) {
        info!("[Journal] Writing command to disk: ID {}", cmd.order_id);
    }

    pub fn journal_event(&self, processed_cmd: &ProcessedOrderCommand) {
        info!(
            "[Journal] Writing processed command to disk: Order ID {}, Status {:?}",
            processed_cmd.order_id(),
            processed_cmd.status()
        );

        // Also journal any trade events if they exist
        if let Some(event) = processed_cmd.events() {
            info!(
                "[Journal] Writing trade event to disk: Price {}, Size {}",
                event.price, event.size
            );
        }
    }
}

impl Default for JournalingProcessor {
    fn default() -> Self {
        Self::new()
    }
}
