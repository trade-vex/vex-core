use common::cmd::{OrderCommand, decode_order_command, encode_order_command};
use rusteron_client::{
    AeronFragmentHandlerCallback, AeronHeader, AeronPublication, AeronReservedValueSupplierLogger,
};
use std::time::SystemTime;
use tracing::{debug, error};

pub struct FragmentHandler {
    pub publication: AeronPublication,
    pub gateway_id: String,
}

impl AeronFragmentHandlerCallback for &FragmentHandler {
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
                let processed_command = process_order_command(order_command);

                // Serialize and send back the processed command
                let mut response_buffer = vec![0u8; 2048];
                match encode_order_command(processed_command, &mut response_buffer) {
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

/// Placeholder function for processing OrderCommand
/// This is where the actual business logic would go
fn process_order_command(mut order_command: OrderCommand) -> OrderCommand {
    // TODO: Implement actual order processing logic here
    // For now, just add a timestamp and return the command

    // info!("Processing OrderCommand: {:?}", order_command);

    // Example processing:
    // - Validate the order
    // - Check risk limits
    // - Route to matching engine
    // - Update order status

    // For now, just update the timestamp
    order_command.timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Return the processed command
    order_command
}
