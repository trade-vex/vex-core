use std::{time::{Duration, SystemTime}};

use rusteron_client::{AeronAvailableImageCallback, AeronCError, AeronFragmentHandlerCallback, AeronHeader, AeronImage, AeronNotificationLogger, AeronPublication, AeronReservedValueSupplierLogger, AeronSubscription, AeronUnavailableImageCallback, Handler};
use tracing::{debug, info, error};
use common::cmd::{OrderCommand, encode_order_command, decode_order_command};

pub const DUOLOGUE_STREAM_ID: i32 = 1002;

#[derive(Clone)]
pub struct FragmentHandler {
    pub publication: AeronPublication,
    pub gateway_id: String,
}

impl AeronFragmentHandlerCallback for &FragmentHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) -> () {
        // is executor thread
        let session_id = header.get_values().unwrap().frame.session_id;
        // debug!("[{}] Gateway '{}': Received OrderCommand", session_id, self.gateway_id);

        // Deserialize OrderCommand
        match decode_order_command(buffer) {
            Ok(order_command) => {
                debug!("[{}] Gateway '{}': Received OrderCommand: {:?}", 
                    session_id, self.gateway_id, order_command);
                
                // Process the order command (placeholder function)
                let processed_command = process_order_command(order_command);
                
                // Serialize and send back the processed command
                let mut response_buffer = vec![0u8; 2048];
                match encode_order_command(processed_command, &mut response_buffer) {
                    Ok(_) => {
                        // Send the processed command back
                        let result = self.publication.offer::<AeronReservedValueSupplierLogger>(
                            &response_buffer, 
                            None
                        );
                        
                        if result < 0 {
                            error!("[{}] Gateway '{}': Failed to send processed OrderCommand, result: {}", 
                                session_id, self.gateway_id, result);
                        } else {
                            debug!("[{}] Gateway '{}': Successfully sent processed OrderCommand", 
                                session_id, self.gateway_id);
                        }
                    }
                    Err(e) => {
                        error!("[{}] Gateway '{}': Failed to encode processed OrderCommand: {:?}", 
                            session_id, self.gateway_id, e);
                    }
                }
            }
            Err(e) => {
                error!("[{}] Gateway '{}': Failed to decode OrderCommand: {:?}", 
                    session_id, self.gateway_id, e);
                
                // Optionally send an error response back
                // For now, we just log the error
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

#[derive(Clone)]
pub struct Duologue {
    pub fragment_handler: FragmentHandler,
    pub session_id: i32,
    pub buffer: [u8; 2048],
    pub gateway_id: String,
    // pub publication: AeronPublication,
    pub subscription: AeronSubscription,
    pub owner: String,
    pub port_data: u16,
    pub port_control: u16,
    pub expire_time: u64,
    pub is_closed: bool,
}

impl Duologue {
    pub fn new(gateway_id: &str, owner: &str, port_data: u16, port_control: u16, session_id: i32, publication: AeronPublication, subscription: AeronSubscription) -> Result<Self, AeronCError> {
        let buffer = [0; 2048];
        let expire_time = (SystemTime::now() + Duration::from_secs(10_00_000))
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let fragment_handler = FragmentHandler {
            publication,
            gateway_id: gateway_id.to_string(),
        };


        Ok(Self {
            fragment_handler,
            gateway_id: gateway_id.to_string(),
            owner: owner.to_string(),
            port_data,
            port_control,
            is_closed: false,
            expire_time,
            session_id,
            buffer,
            // publication,
            subscription,
        })
    }

    pub fn poll(&self) -> Result<i32, AeronCError> {
        self.subscription.poll(Some(&Handler::leak(&self.fragment_handler)), 2048)
    }

    pub fn is_expired(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now > self.expire_time
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub fn close(&mut self) -> Result<(), AeronCError> {
        self.is_closed = true;
        self.subscription.close::<AeronNotificationLogger>(None)?;
        Ok(())
    }
}

pub struct DuologueImageAvailable{
    pub owner: String,
}

impl AeronAvailableImageCallback for DuologueImageAvailable {
    fn handle_aeron_on_available_image(&mut self, _subscription: AeronSubscription, image: AeronImage) {
        let binding = image.get_constants().unwrap();
        let remote_addr = binding.source_identity();
        let session_id = binding.session_id;

        if remote_addr != self.owner {
            error!("Client Connecting witht the wrong address, expected: {}, got: {}", self.owner, remote_addr);
        } else {
            info!("[{}] Client Connected, address: {}", session_id, remote_addr);
        }
    }
}

pub struct DuologueImageUnavailable{
    pub owner: String,
}

impl AeronUnavailableImageCallback for DuologueImageUnavailable {
    fn handle_aeron_on_unavailable_image(&mut self, _subscription: AeronSubscription, image: AeronImage) {
        let binding = image.get_constants().unwrap();
        let remote_addr = binding.source_identity();
        let session_id = binding.session_id;
        // check image_count and close?
        info!("[{}] Client Disconnected, address: {}", session_id, remote_addr);
    }
}