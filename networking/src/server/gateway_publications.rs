use std::sync::atomic::{AtomicPtr, Ordering};

use common::{MAX_GATEWAYS, ORDERCOMMANDSIZE, OrderCommand, Snowflake, encode_order_command};
use rusteron_client::{
    AeronPublication, AeronReservedValueSupplierLogger,
};
use tracing::{debug, error};

pub struct GatewayPublications {
    gateways: [AtomicPtr<AeronPublication>; MAX_GATEWAYS],
}

impl GatewayPublications {
    pub fn new() -> Self {
        const INIT: AtomicPtr<AeronPublication> = AtomicPtr::new(std::ptr::null_mut());
        Self {
            gateways: [INIT; MAX_GATEWAYS],
        }
    }

    // Writer (networking thread)
    pub fn set(&self, gateway_id: u8, publication: Box<AeronPublication>) {
        self.gateways[gateway_id as usize].store(Box::into_raw(publication), Ordering::Release);
    }

    // Reader (event handler thread)
    pub fn get(&self, gateway_id: u8) -> &AeronPublication {
        let ptr = self.gateways[gateway_id as usize].load(Ordering::Acquire);
        unsafe { &*ptr }
    }

    // Publisher (event handler thread)
    pub fn publish_response(&self, cmd: &OrderCommand) {
        let gateway_id = Snowflake::gateway_from_id(cmd.order_id());
        let ptr = self.gateways[gateway_id as usize].load(Ordering::Acquire);
        let publication = unsafe { &*ptr };
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
