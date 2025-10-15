use common::{OrderCommand, OrderCommandType, Snowflake, order_debug, order_info};

pub struct JournalingProcessor {
    snowflake: Snowflake,
}

impl JournalingProcessor {
    pub fn new() -> Self {
        Self {
            snowflake: Snowflake::new(None).unwrap(),
        }
    }

    // Ring buforder_idfer Disruptor to JournalingProcessor - Logger(Excali-0)
    pub fn journal_command(&mut self, cmd: &mut OrderCommand) {
        if cmd.command != OrderCommandType::CancelOrder {
            cmd.order_id = self.snowflake.generate(cmd.order_id).unwrap();
        }
        cmd.timestamp = self.snowflake.timestamp();
        order_info!("command_ingested", cmd, stage = "journal");
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

impl Default for JournalingProcessor {
    fn default() -> Self {
        Self::new()
    }
}
