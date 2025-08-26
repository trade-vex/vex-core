use std::time::{Duration, SystemTime};

use crate::server::cmd_handler::FragmentHandler;
use crate::utils::{
    new_publication_with_mdc_and_session, new_subsciption_with_handlers_and_session,
};
use common::cmd::OrderCommand;
use disruptor::{MultiProducer, MultiConsumerBarrier};
use rusteron_client::{
    Aeron, AeronAvailableImageCallback, AeronCError, AeronImage, AeronNotificationLogger, AeronSubscription, AeronUnavailableImageCallback, Handler
};
use tracing::{error, info};

pub const DUOLOGUE_STREAM_ID: i32 = 1002;

pub struct Duologue {
    pub fragment_handler: Handler<FragmentHandler>,
    pub session_id: i32,
    pub gateway_id: String,
    pub subscription: AeronSubscription,
    pub port_data: u16,
    pub port_control: u16,
    pub expire_time: u64,
    pub is_closed: bool,
}

impl Duologue {
    pub fn new(
        aeron: &Aeron,
        local: &str,
        gateway_id: &str,
        owner: &str,
        port_data: u16,
        port_control: u16,
        session_id: i32,
        producer: MultiProducer<OrderCommand, MultiConsumerBarrier>,
    ) -> Result<Self, AeronCError> {
        let expire_time = (SystemTime::now() + Duration::from_secs(1_000_000))
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let publication = new_publication_with_mdc_and_session(
            aeron,
            local,
            port_control,
            DUOLOGUE_STREAM_ID,
            session_id,
        )?;

        let on_image_available = DuologueImageAvailable {
            owner: owner.to_string(),
        };
        let on_image_unavailable = DuologueImageUnavailable {
            owner: owner.to_string(),
        };

        let subscription = new_subsciption_with_handlers_and_session(
            aeron,
            local,
            port_data,
            DUOLOGUE_STREAM_ID,
            session_id,
            on_image_available,
            on_image_unavailable,
        )?;

        let fragment_handler = FragmentHandler {
            publication,
            gateway_id: gateway_id.to_string(),
            producer,
        };

        Ok(Self {
            fragment_handler: Handler::leak(fragment_handler),
            gateway_id: gateway_id.to_string(),
            port_data,
            port_control,
            is_closed: false,
            expire_time,
            session_id,
            subscription,
        })
    }

    pub fn poll(&mut self) -> Result<i32, AeronCError> {
        self.subscription
            .poll( Some(&mut self.fragment_handler), 2048)
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
        self.fragment_handler.release();
        Ok(())
    }
}

pub struct DuologueImageAvailable {
    pub owner: String,
}

impl AeronAvailableImageCallback for DuologueImageAvailable {
    fn handle_aeron_on_available_image(
        &mut self,
        _subscription: AeronSubscription,
        image: AeronImage,
    ) {
        let binding = image.get_constants().unwrap();
        let remote_addr = binding.source_identity();
        let session_id = binding.session_id;

        let expected_address = self.owner.split(':').next().unwrap_or("");
        let actual_address = remote_addr.split(':').next().unwrap_or("");

        if actual_address != expected_address {
            error!(
                "Client Connecting with the wrong address, expected: {}, got: {}",
                expected_address, actual_address
            );
        } else {
            info!(
                "[{}] Client Connected, address: {}",
                session_id, actual_address
            );
        }
    }
}

pub struct DuologueImageUnavailable {
    pub owner: String,
}

impl AeronUnavailableImageCallback for DuologueImageUnavailable {
    fn handle_aeron_on_unavailable_image(
        &mut self,
        _subscription: AeronSubscription,
        image: AeronImage,
    ) {
        let binding = image.get_constants().unwrap();
        let remote_addr = binding.source_identity();
        let session_id = binding.session_id;
        // check image_count and close?
        info!(
            "[{}] Client Disconnected, address: {}, gateway: {}",
            session_id, remote_addr, self.owner
        );
    }
}
