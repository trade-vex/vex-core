use rusteron_client::{
    AeronAvailableImageCallback, AeronFragmentHandlerCallback, AeronHeader, AeronImage,
    AeronPublication, AeronSubscription, AeronUnavailableImageCallback,
};
use std::sync::Arc;
use tracing::{debug, error};

use super::gateway_manager::GatewayManager;

/// Handles initial handshake messages from gateways
pub struct HandshakeMessageHandler {
    gateways: Arc<GatewayManager>,
    publication: AeronPublication,
}

impl HandshakeMessageHandler {
    /// Creates a new handshake message handler
    ///
    /// # Arguments
    /// * `gateways` - Shared gateway manager instance
    /// * `publication` - Aeron publication for sending responses
    pub fn new(gateways: Arc<GatewayManager>, publication: AeronPublication) -> Self {
        Self {
            gateways,
            publication,
        }
    }
}

impl AeronFragmentHandlerCallback for HandshakeMessageHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) {
        let session_id = match header.get_values() {
            Ok(values) => values.frame.session_id,
            Err(e) => {
                error!("Failed to get header values: {}", e);
                return;
            }
        };

        // Process the handshake message
        if let Err(e) =
            self.gateways
                .process_handshake_message(&self.publication, session_id, buffer)
        {
            error!(
                "Error processing handshake from session 0x{:x}: {}",
                session_id, e
            );
        }
    }
}

/// Handles gateway image availability events
pub struct GatewayImageAvailableHandler;

impl AeronAvailableImageCallback for GatewayImageAvailableHandler {
    fn handle_aeron_on_available_image(
        &mut self,
        _subscription: AeronSubscription,
        image: AeronImage,
    ) {
        let session_id = match image.get_constants() {
            Ok(b) => b.session_id,
            Err(e) => {
                error!("Failed to get image constants: {}", e);
                return;
            }
        };
        let binding = image.get_constants().unwrap();
        let address = binding.source_identity();

        debug!(
            "Gateway image available for session 0x{:x} from {}",
            session_id, address
        );
    }
}

/// Handles gateway image unavailability events
pub struct GatewayImageUnavailableHandler;

impl AeronUnavailableImageCallback for GatewayImageUnavailableHandler {
    fn handle_aeron_on_unavailable_image(
        &mut self,
        _subscription: AeronSubscription,
        image: AeronImage,
    ) {
        let (session_id, binding) = match image.get_constants() {
            Ok(b) => (b.session_id, b),
            Err(e) => {
                error!("Failed to get image constants: {}", e);
                return;
            }
        };
        let address = binding.source_identity();

        debug!(
            "Gateway image unavailable for session 0x{:x} from {}",
            session_id, address
        );
    }
}
