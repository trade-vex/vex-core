use common::cmd::{OrderCommand, decode_order_command};
use rusteron_client::{AeronFragmentHandlerCallback, AeronHeader};
use std::sync::mpsc::Sender;
use tracing::{error, info};

pub struct OrderCommandHandler {
    gateway_id: String,
    sender: Sender<OrderCommand>,
}

impl AeronFragmentHandlerCallback for OrderCommandHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        // Deserialize OrderCommand
        match decode_order_command(buffer) {
            Ok(order_command) => {
                info!(
                    "Gateway {}: Received OrderCommand: {:?}",
                    self.gateway_id, order_command
                );

                self.sender.send(order_command).unwrap_or_else(|e| {
                    error!(
                        "Gateway {}: Failed to send OrderCommand to channel: {:?}",
                        self.gateway_id, e
                    );
                });
            }
            Err(e) => {
                error!(
                    "Gateway {}: Failed to decode OrderCommand: {:?}",
                    self.gateway_id, e
                );
            }
        }
    }
}

impl OrderCommandHandler {
    pub fn new(gateway_id: String, sender: Sender<OrderCommand>) -> Self {
        Self { gateway_id, sender }
    }
}
