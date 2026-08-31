use arc_swap::ArcSwapOption;
use common::{MAX_GATEWAYS, ORDERCOMMANDSIZE, OrderCommand, encode_order_command};
use rusteron_archive::{AeronPublication, AeronReservedValueSupplierLogger};
use std::sync::Arc;
use tracing::{debug, error};

use super::ServerError;

/// Manages Gateway Publications from gateway id 0 to MAX_GATEWAYS - 1
/// Index MAX_GATEWAYS is reserved for archival publication
pub struct Publications {
    gateways: [ArcSwapOption<AeronPublication>; MAX_GATEWAYS + 1],
}

impl Publications {
    pub fn new() -> Self {
        Self {
            gateways: core::array::from_fn::<
                ArcSwapOption<AeronPublication>,
                { MAX_GATEWAYS + 1 },
                _,
            >(|_| ArcSwapOption::const_empty()),
        }
    }

    pub fn set_archive_publication(&self, publication: AeronPublication) {
        self.gateways[MAX_GATEWAYS].store(Some(Arc::new(publication)));
    }

    pub fn set(
        &self,
        gateway_id: u8,
        publication: Arc<AeronPublication>,
    ) -> Result<(), ServerError> {
        let index = Self::gateway_index(gateway_id)?;
        self.gateways[index].store(Some(publication));
        Ok(())
    }

    pub fn get(&self, gateway_id: u8) -> Result<Option<Arc<AeronPublication>>, ServerError> {
        let index = Self::gateway_index(gateway_id)?;
        Ok(self.gateways[index].load_full())
    }

    pub fn remove(&self, gateway_id: u8) -> Result<(), ServerError> {
        let index = Self::gateway_index(gateway_id)?;
        self.gateways[index].store(None);
        Ok(())
    }

    fn gateway_index(gateway_id: u8) -> Result<usize, ServerError> {
        let index = usize::from(gateway_id);
        if index >= MAX_GATEWAYS {
            return Err(ServerError::GatewayMessageError(format!(
                "Gateway ID {gateway_id} out of range (max: {})",
                MAX_GATEWAYS - 1
            )));
        }
        Ok(index)
    }

    // Publisher (event handler thread)
    pub fn publish_response(&self, cmd: &OrderCommand) {
        let gateway_id = cmd.route_gateway_id;
        let ptr = match self.get(gateway_id) {
            Ok(ptr) => ptr,
            Err(e) => {
                error!("gateway-{gateway_id}: invalid gateway id to send response: {e}");
                return;
            }
        };
        let publication = ptr.as_ref();
        if publication.is_none() {
            error!(
                "gateway-{}: No publication found to send response",
                gateway_id
            );
            return;
        }
        let publication = publication.unwrap();
        let mut response_buffer = [0; ORDERCOMMANDSIZE];
        match encode_order_command(cmd, &mut response_buffer) {
            Ok(_) => {
                // Send the processed command back
                let result =
                    publication.offer::<AeronReservedValueSupplierLogger>(&response_buffer, None);

                if result < 0 {
                    error!(
                        "gateway-{}: Failed to send processed OrderCommand, result: {}",
                        gateway_id, result
                    );
                } else {
                    debug!(
                        "gateway-{}: Successfully sent processed OrderCommand",
                        gateway_id
                    );
                }
            }
            Err(e) => {
                error!(
                    "gateway-{}: Failed to encode processed OrderCommand: {:?}",
                    gateway_id, e
                );
            }
        }
    }

    // Publisher (event handler thread)
    pub fn publish_to_archive(&self, cmd: &OrderCommand) {
        let gateway_id = cmd.order_id;
        let ptr = self.gateways[MAX_GATEWAYS].load_full();
        let publication = ptr.as_ref();
        if publication.is_none() {
            error!(
                "gateway-{}: Archive publication not set, cannot archive command, client order_id: {}",
                gateway_id, cmd.client_order_id
            );
            return;
        }
        let publication = publication.unwrap();
        let mut response_buffer = [0; ORDERCOMMANDSIZE];
        match encode_order_command(cmd, &mut response_buffer) {
            Ok(_) => {
                let result =
                    publication.offer::<AeronReservedValueSupplierLogger>(&response_buffer, None);

                if result < 0 {
                    error!(
                        "gateway-{}: Failed to archive OrderCommand, client order_id: {}, result: {}",
                        gateway_id, cmd.client_order_id, result
                    );
                } else {
                    debug!(
                        "gateway-{}: successfully published to archive, client order_id: {}",
                        gateway_id, cmd.client_order_id
                    );
                }
            }
            Err(e) => {
                error!(
                    "gateway-{}: Failed to encode processed OrderCommand: {:?}",
                    gateway_id, e
                );
            }
        }
    }
}

impl Default for Publications {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_out_of_range_gateway_id() {
        let publications = Publications::new();

        assert!(matches!(
            publications.get(MAX_GATEWAYS as u8),
            Err(ServerError::GatewayMessageError(_))
        ));
    }
}
