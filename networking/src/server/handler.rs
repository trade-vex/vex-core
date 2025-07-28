use rusteron_client::{
    AeronFragmentHandlerCallback, AeronHeader, AeronPublication, AeronReservedValueSupplierLogger,
};
use tracing::debug;

pub struct FragmentHandler {
    pub publication: AeronPublication,
}

impl AeronFragmentHandlerCallback for &FragmentHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) -> () {
        // is executor thread
        let session_id = header.get_values().unwrap().frame.session_id;

        // handle and deserialize message
        let message = String::from_utf8(buffer.to_vec()).unwrap();

        debug!("[{}] Received Message: {}", session_id, message);

        // send message to client with buffer data and session_id
        self.publication
            .offer::<AeronReservedValueSupplierLogger>(buffer, None);
        // if it fails send a message "ERROR bad message" and close
    }
}
