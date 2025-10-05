use std::sync::atomic::{AtomicPtr, Ordering};

use common::MAX_GATEWAYS;
use rusteron_client::AeronPublication;

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
    pub fn get(&self, gateway_id: usize) -> &AeronPublication {
        let ptr = self.gateways[gateway_id].load(Ordering::Acquire);
        unsafe { &*ptr }
    }
}
