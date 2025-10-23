use common::{OrderCommand, OrderCommandType, Status, decode_order_command};
use disruptor::{MultiProducer, Producer, SingleConsumerBarrier};
use rusteron_archive::{AeronFragmentHandlerCallback, AeronHeader};
use tracing::{debug, error};

pub struct FragmentHandler {
    pub gateway_id: u8,
    pub producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
    pub message_counter: u64,
}

impl AeronFragmentHandlerCallback for FragmentHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        match decode_order_command(buffer) {
            Ok(mut order_command) => {
                order_command.status = Status::Processing;
                // Set gateway_id for response routing
                order_command.gateway_id = self.gateway_id;
                // order_id is updated in journaling processor for PlaceOrder
                // the snowflake algorithm requires gateway_id to be part of order_id
                if order_command.command != OrderCommandType::CancelOrder {
                    order_command.order_id = self.gateway_id as u64;
                }

                // Sample logging: log every 1024th message
                self.message_counter += 1;
                if (self.message_counter & 0x3FF) == 0 {
                    debug!(
                        target: "order_command",
                        gateway_id = self.gateway_id,
                        client_order_id = ?order_command,
                        counter = self.message_counter,
                        "received order command"
                    );
                }

                if let Err(e) = self.producer.try_publish(|cmd| {
                    *cmd = order_command; // move, not clone
                }) {
                    error!(
                        target: "gateway_fragment",
                        gateway_id = self.gateway_id,
                        error = %e,
                        "failed to publish order command to ring buffer"
                    );
                    return;
                }
            }
            Err(e) => {
                error!(
                    target: "gateway_fragment",
                    gateway_id = self.gateway_id,
                    error = ?e,
                    "failed to decode order command"
                );
            }
        }
    }
}

pub struct ReplayFragmentHandler {
    pub gateway_id: u8,
    pub producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
    pub bytes_consumed: i64,
}

impl AeronFragmentHandlerCallback for ReplayFragmentHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) {
        // Track actual bytes consumed from the fragment
        let values = match header.get_values() {
            Ok(values) => values,
            Err(e) => {
                error!(
                    target: "replay_fragment",
                    gateway_id = self.gateway_id,
                    error = %e,
                    "failed to decode header values"
                );
                return;
            }
        };
        self.bytes_consumed += values.frame.frame_length as i64;

        if cfg!(debug_assertions) {
            debug!(
                target: "replay_fragment",
                gateway_id = self.gateway_id,
                session_id = values.frame.session_id,
                stream_id = values.frame.stream_id,
                term_id = values.frame.term_id,
                term_offset = values.frame.term_offset,
                frame_length = values.frame.frame_length,
                "received replay fragment"
            );
        }

        match decode_order_command(buffer) {
            Ok(mut order_command) => {
                order_command.status = Status::Processing;
                debug!(
                    target: "replay_fragment",
                    gateway_id = self.gateway_id,
                    time_stamp = ?order_command.timestamp,
                    client_order_id = ?order_command.client_order_id,
                    "processing replay order command"
                );

                if let Err(e) = self.producer.try_publish(|cmd| {
                    *cmd = order_command; // move, not clone
                }) {
                    error!(
                        target: "replay_fragment",
                        gateway_id = self.gateway_id,
                        error = %e,
                        "failed to publish replay order command to ring buffer"
                    );
                    return;
                }
            }
            Err(e) => {
                error!(
                    target: "replay_fragment",
                    gateway_id = self.gateway_id,
                    error = ?e,
                    "failed to decode replay order command"
                );
            }
        }
    }
}
