use arc_swap::ArcSwapOption;
use common::{MAX_GATEWAYS, ORDERCOMMANDSIZE, OrderCommand, Snowflake, encode_order_command};
use rusteron_client::{AeronPublication, AeronReservedValueSupplierLogger};
use std::sync::Arc;
use tracing::{debug, error};

pub struct GatewayPublications {
    gateways: [ArcSwapOption<AeronPublication>; MAX_GATEWAYS],
}

impl GatewayPublications {
    pub fn new() -> Self {
        const INIT: ArcSwapOption<AeronPublication> = ArcSwapOption::const_empty();
        Self {
            gateways: [INIT; MAX_GATEWAYS],
        }
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
        let publication =  ptr.as_ref();
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
}
