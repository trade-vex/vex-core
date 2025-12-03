use common::{OrderCommand, OrderCommandType, Status, decode_order_command};
use disruptor::{MultiProducer, Producer, SingleConsumerBarrier};
use rusteron_archive::{AeronFragmentHandlerCallback, AeronHeader};
use tracing::{debug, error, info};

pub struct FragmentHandler {
    pub gateway_id: u8,
    pub producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
}

impl AeronFragmentHandlerCallback for FragmentHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        match decode_order_command(buffer) {
            Ok(mut order_command) => {
                order_command.status = Status::Processing;
                // order_id is updated in journaling processor
                // the snowflake algorithm requires gateway_id to be part of order_id
                // instead of adding a new field, we repurpose order_id here
                if order_command.command != OrderCommandType::CancelOrder {
                    order_command.order_id = self.gateway_id as u64;
                } else {
                    order_command.user_id = self.gateway_id as u64;
                }
                info!(
                    target: "order_cammand",
                    gateway_id = self.gateway_id,
                    client_order_id = ?order_command,
                    "received order command"
                );

                if let Err(e) = self.producer.try_publish(|cmd| {
                    *cmd = order_command.clone();
                }) {
                    error!(
                        target: "gateway_fragment",
                        gateway_id = self.gateway_id,
                        error = %e,
                        "failed to publish order command to ring buffer"
                    );
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
}

impl AeronFragmentHandlerCallback for ReplayFragmentHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) {
        if cfg!(debug_assertions) {
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
            debug!(
                target: "replay_fragment",
                gateway_id = self.gateway_id,
                session_id = values.frame.session_id,
                stream_id = values.frame.stream_id,
                term_id = values.frame.term_id,
                term_offset = values.frame.term_offset,
                frame_size = values.position_bits_to_shift(),
                "received replay fragment"
            );
        }

        match decode_order_command(buffer) {
            Ok(mut order_command) => {
                order_command.status = Status::Processing;
                info!(
                    target: "replay_fragment",
                    gateway_id = self.gateway_id,
                    time_stamp = ?order_command.timestamp,
                    client_order_id = ?order_command.client_order_id,
                    "processing replay order command"
                );

                if let Err(e) = self.producer.try_publish(|cmd| {
                    *cmd = order_command.clone();
                }) {
                    error!(
                        target: "replay_fragment",
                        gateway_id = self.gateway_id,
                        error = %e,
                        "failed to publish replay order command to ring buffer"
                    );
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
