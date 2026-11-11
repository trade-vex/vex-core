use rusteron_archive::{AeronFragmentHandlerCallback, AeronHeader, AeronImage, AeronPublication};
use std::rc::Rc;
use tracing::error;

use super::gateway_manager::GatewayManager;

/// Handles initial handshake messages from gateways
pub struct HandshakeMessageHandler {
    gateways: Rc<GatewayManager>,
    publication: AeronPublication,
}

impl HandshakeMessageHandler {
    /// Creates a new handshake message handler
    ///
    /// # Arguments
    /// * `gateways` - Shared gateway manager instance
    /// * `publication` - Aeron publication for sending responses
    pub fn new(gateways: Rc<GatewayManager>, publication: AeronPublication) -> Self {
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
                error!(
                    target: "gateway_handler",
                    action = "header_values_failed",
                    error = %e
                );
                return;
            }
        };
        let source = if header.context().is_null() {
            None
        } else {
            AeronImage::from(header.context().cast())
                .get_constants()
                .ok()
                .map(|constants| constants.source_identity().to_string())
        }
        .unwrap_or_else(|| format!("aeron-session-{session_id}"));

        if let Err(e) =
            self.gateways
                .process_handshake_message(&self.publication, session_id, &source, buffer)
        {
            error!(
                target: "gateway_handler",
                action = "handshake_processing_failed",
                session = format_args!("{:#x}", session_id),
                error = %e
            );
        }
    }
}
