use common::{OrderCommand, Snowflake};
use tracing::info;

pub struct JournalingProcessor{
    snowflake: Snowflake,
}

impl JournalingProcessor {
    pub fn new() -> Self {
        Self {
            snowflake: Snowflake::new(None).unwrap(),
        }
    }

    // Ring buffer Disruptor to JournalingProcessor - Logger(Excali-0)
    pub fn journal_command(&mut self, cmd: &mut OrderCommand) {
        cmd.order_id = self.snowflake.generate(cmd.order_id).unwrap();
        cmd.timestamp = self.snowflake.timestamp();
        info!("[Journal] Writing command to disk: ID {}", cmd.order_id);
    }

    pub fn journal_event(&self, cmd: &mut OrderCommand) {
        info!(
            "[Journal] Writing processed command to disk: Order ID {}, Status {:?}",
            cmd.order_id(),
            cmd.status()
        );

        // Also journal any trade events if they exist
        if let Some(event) = cmd.events() {
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
