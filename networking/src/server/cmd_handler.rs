use common::{OrderCommand, OrderCommandType, Status, decode_order_command};
use disruptor::{MultiProducer, Producer, SingleConsumerBarrier};
use rusteron_client::{AeronFragmentHandlerCallback, AeronHeader};
use tracing::{debug, error};

pub struct FragmentHandler {
    pub gateway_id: u8,
    pub producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
}

impl AeronFragmentHandlerCallback for FragmentHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) {
        // is executor thread
        let session_id = match header.get_values() {
            Ok(values) => values.frame.session_id,
            Err(_) => {
                error!(
                    "gateway-{}: Missing session ID in Aeron header",
                    self.gateway_id
                );
                return;
            }
        };

        // Deserialize OrderCommand
        match decode_order_command(buffer) {
            Ok(mut order_command) => {
                order_command.status = Status::Processing;
                // order_id is updated in journaling processor
                // the snowflake algorithm requires gateway_id to be part of order_id
                // instead of adding a new field, we repurpose order_id here
                if order_command.command != OrderCommandType::CancelOrder {
                    order_command.order_id = self.gateway_id as u64;
                }
                debug!(
                    "[{}] gateway-{}: Received OrderCommand: {:?}",
                    session_id, self.gateway_id, order_command
                );

                if let Err(e) = self.producer.try_publish(|cmd| {
                    *cmd = order_command.clone();
                }) {
                    error!(
                        "[{}] gateway-{}: Failed to publish OrderCommand to ring buffer: {}",
                        session_id, self.gateway_id, e
                    );
                    return;
                }
            }
            Err(e) => {
                error!(
                    "[{}] gateway-{}: Failed to decode OrderCommand: {:?}",
                    session_id, self.gateway_id, e
                );
            }
        }
    }
}
