use std::{net::SocketAddr, time::Duration};

use rusteron_client::{
    Aeron, AeronFragmentHandlerCallback, AeronAvailableImageLogger, AeronCError, AeronContext, AeronImage, AeronPublication, AeronReservedValueSupplierLogger, AeronSubscription, AeronUnavailableImageLogger, Handler,
    AeronHeader,
};
use thiserror::Error;
use std::thread;
pub struct FragmentHandler;

impl AeronFragmentHandlerCallback for FragmentHandler {
  fn handle_aeron_fragment_handler(&mut self, message: &[u8], _: AeronHeader) { 
    println!("Fragment received: {:?}", message);
   }

}
pub struct VexClient {
    aeron: Aeron,
    publisher: AeronPublication,
    subscriber: AeronSubscription,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
}

#[derive(Error, Debug, PartialEq)]
pub enum ClientError {
    #[error("Aeron error: {0}")]
    Aeron(#[from] AeronCError),
    #[error("Channel closed")]
    ChannelClosed,
    #[error("Invalid input: {0}")]
    InvalidInput(#[from] std::ffi::NulError),
    #[error("Publication not found")]
    PublicationNotFound,
    #[error("Empty message")]
    EmptyMessage,
    #[error("Publisher not running")]
    NotRunning,
}

impl VexClient {
    pub fn new(context_dir: &str, channel: &str, stream_id: i32, local_addr: SocketAddr, remote_addr: SocketAddr) -> Result<Self, ClientError> {
        let ctx = AeronContext::new()?;
        let context_dir = std::ffi::CString::new(context_dir)?;
        ctx.set_dir(& context_dir)?;
        ctx.set_driver_timeout_ms(1_000)?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        let channel = std::ffi::CString::new(channel)?;
        let publication = aeron.add_publication(&channel, stream_id, std::time::Duration::from_secs(1))?;
        let available_logger = AeronAvailableImageLogger {};
        let available_handler = Handler::leak(available_logger);
        let unavailable_logger = AeronUnavailableImageLogger {};
        let unavailable_handler = Handler::leak(unavailable_logger);
        let subscription = aeron.add_subscription(&channel, stream_id, Some(&available_handler), Some(&unavailable_handler), Duration::from_secs(1))?;

        Ok(Self {
            aeron,
            publisher: publication,
            subscriber: subscription,
            local_addr, 
            remote_addr
        })
    }

    fn send_message(&mut self, message: &[u8]) -> Result<(), ClientError> {
        if message.is_empty() {
            return Err(ClientError::EmptyMessage);
        }
        if !self.publisher.is_connected() {
            return Err(ClientError::NotRunning);
        }
        let reserved = AeronReservedValueSupplierLogger {};
        let handler = Handler::leak(reserved);
        let result = self.publisher.offer(message, Some(&handler));
        if result < 0 {
            // You may want to match on specific negative values for more detailed errors
                return Err(AeronCError::from_code(result as i32).into());
        }
        Ok(())
    }

    pub fn run(&mut self) -> Result<(), ClientError> {
        
        let str = format!("Hello, {}", self.local_addr.port());
        let message = str.as_bytes();

        loop {
            // Poll for messages from the subscriber
            if self.publisher.is_connected() {
                self.send_message(message)?;
                break;
            }
            // Here you can add logic to publish messages using self.publisher
        }
        let fragment_handler = FragmentHandler {};
        let fragment_handler = Handler::leak(fragment_handler);

        loop {
            if self.publisher.is_connected() {
                self.send_message(message)?;
            }

            if self.subscriber.is_connected() {
                self.subscriber.poll(Some(&fragment_handler), 10)?;
            }
            thread::sleep(Duration::from_millis(1000)); // Adjust sleep duration as needed
        }
        // Here you can implement the main loop for processing messages
        Ok(())

    }

    // Additional methods for publishing and subscribing can be added here
}