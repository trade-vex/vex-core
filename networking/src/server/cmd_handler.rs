use common::cmd::{OrderCommand, decode_order_command, encode_order_command};
use disruptor::{MultiConsumerBarrier, MultiProducer, Producer};
use rusteron_client::{
    AeronFragmentHandlerCallback, AeronHeader, AeronPublication, AeronReservedValueSupplierLogger,
};
use tracing::{debug, error};

pub struct FragmentHandler {
    pub publication: AeronPublication,
    pub gateway_id: String,
    pub producer: MultiProducer<OrderCommand, MultiConsumerBarrier>,
}

<<<<<<< HEAD
impl AeronFragmentHandlerCallback for &mut FragmentHandler {
=======
impl AeronFragmentHandlerCallback for FragmentHandler {
>>>>>>> chore/refactor-common-ob
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) {
        // is executor thread
        let session_id = header.get_values().unwrap().frame.session_id;

        // Deserialize OrderCommand
        match decode_order_command(buffer) {
            Ok(order_command) => {
                debug!(
                    "[{}] Gateway '{}': Received OrderCommand: {:?}",
                    session_id, self.gateway_id, order_command
                );

                // Process the order command (placeholder function)
                self.producer.publish(|cmd| {
                    *cmd = order_command.clone();
                });

                // Serialize and send back the processed command
<<<<<<< HEAD
                let mut response_buffer = vec![0u8; 2048];
=======
                let mut response_buffer = vec![0u8; 67];
>>>>>>> chore/refactor-common-ob
                match encode_order_command(order_command, &mut response_buffer) {
                    Ok(_) => {
                        // Send the processed command back
                        let result = self
                            .publication
                            .offer::<AeronReservedValueSupplierLogger>(&response_buffer, None);

                        if result < 0 {
                            error!(
                                "[{}] Gateway '{}': Failed to send processed OrderCommand, result: {}",
                                session_id, self.gateway_id, result
                            );
                        } else {
                            debug!(
                                "[{}] Gateway '{}': Successfully sent processed OrderCommand",
                                session_id, self.gateway_id
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            "[{}] Gateway '{}': Failed to encode processed OrderCommand: {:?}",
                            session_id, self.gateway_id, e
                        );
                    }
                }
            }
            Err(e) => {
                error!(
                    "[{}] Gateway '{}': Failed to decode OrderCommand: {:?}",
                    session_id, self.gateway_id, e
                );
            }
        }
    }
}
