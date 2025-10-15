use arc_swap::ArcSwapOption;
use common::{MAX_GATEWAYS, ORDERCOMMANDSIZE, OrderCommand, Snowflake, encode_order_command};
use rusteron_archive::{AeronCError, AeronPublication, AeronReservedValueSupplierLogger};
use std::sync::Arc;
use tracing::{debug, error};

/// Manages Gateway Publications from gateway id 0 to MAX_GATEWAYS
/// Index MAX_GATEWAYS is reserved for archival publication
pub struct Publications {
    gateways: [ArcSwapOption<AeronPublication>; MAX_GATEWAYS + 1],
}

impl Publications {
    pub fn new() -> Self {
        const INIT: ArcSwapOption<AeronPublication> = ArcSwapOption::const_empty();
        Self {
            gateways: [INIT; MAX_GATEWAYS + 1],
        }
    }

    pub fn set_archive_publication(&self, publication: AeronPublication) {
        self.gateways[MAX_GATEWAYS].store(Some(Arc::new(publication)));
    }

    pub fn set(&self, gateway_id: u8, publication: Arc<AeronPublication>) {
        self.gateways[gateway_id as usize].store(Some(publication));
    }

    pub fn get(&self, gateway_id: u8) -> Option<Arc<AeronPublication>> {
        self.gateways[gateway_id as usize].load_full()
    }

    pub fn remove(&self, gateway_id: u8) {
        self.gateways[gateway_id as usize].store(None);
    }

    // Publisher (event handler thread)
    pub fn publish_response(&self, cmd: &OrderCommand) {
        let gateway_id = Snowflake::gateway_from_id(cmd.order_id());
        let ptr = self.get(gateway_id);
        let publication = ptr.as_ref();
        let ptr = self.get(MAX_GATEWAYS as u8);
        let archive_publication = ptr.as_ref();
        if publication.is_none() {
            error!(
                "gateway-{}: No publication found to send response",
                gateway_id
            );
            return;
        }
        let skip_archival = archive_publication.is_none();
        if skip_archival {
            // undesired behavior
            error!("No archive publication found to send response, skipping archival");
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

                if !skip_archival {
                    let archive_publication = archive_publication.unwrap();
                    let archive_result = archive_publication
                        .offer::<AeronReservedValueSupplierLogger>(&response_buffer, None);
                    if archive_result < 0 {
                        error!(
                            "gateway-{}: Failed to archive processed OrderCommand, result: {}",
                            gateway_id,
                            AeronCError::from_code(archive_result as i32)
                        );
                    } else {
                        debug!(
                            "gateway-{}: Successfully archived processed OrderCommand",
                            gateway_id
                        );
                    }
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
