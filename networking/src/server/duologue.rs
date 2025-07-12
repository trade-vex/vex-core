use std::{time::{Duration, SystemTime}};

use rusteron_client::{Aeron, AeronAvailableImageCallback, AeronCError, AeronImage, AeronNotificationLogger, AeronSubscription, AeronUnavailableImageCallback, Handler};
use tracing::{info, error};
use crate::server::handler::FragmentHandler;
use crate::utils::{new_publication_with_mdc_and_session, new_subsciption_with_handlers_and_session};

pub const DUOLOGUE_STREAM_ID: i32 = 1002;

pub struct Duologue {
    pub fragment_handler: FragmentHandler,
    pub session_id: i32,
    pub buffer: [u8; 2048],
    // pub publication: AeronPublication,
    pub subscription: AeronSubscription,
    pub owner: String,
    pub port_data: u16,
    pub port_control: u16,
    pub expire_time: u64,
    pub is_closed: bool,
}

impl Duologue {
    pub fn new(aeron: &Aeron,local: &str, owner: &str, port_data: u16, port_control: u16, session_id: i32) -> Result<Self, AeronCError> {
        let buffer = [0; 2048];
        let expire_time = (SystemTime::now() + Duration::from_secs(10_00_000))
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let publication = new_publication_with_mdc_and_session(aeron, &local, port_control, DUOLOGUE_STREAM_ID, session_id)?;

        let on_image_available = DuologueImageAvailable { owner: owner.to_string() };
        let on_image_unavailable = DuologueImageUnavailable { owner: owner.to_string() };

        let subscription = new_subsciption_with_handlers_and_session(aeron, &local, port_data, DUOLOGUE_STREAM_ID, session_id, on_image_available, on_image_unavailable)?;

        let fragment_handler = FragmentHandler {
            publication
        };


        Ok(Self {
            fragment_handler,
            owner: owner.to_string(),
            port_data,
            port_control,
            is_closed: false,
            expire_time,
            session_id,
            buffer,
            // publication,
            subscription,
        })
    }

    pub fn poll(&self) -> Result<i32, AeronCError> {
        self.subscription.poll(Some(&Handler::leak(&self.fragment_handler)), 2048)
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
        Ok(())
    }
}

pub struct DuologueImageAvailable{
    pub owner: String,
}

impl AeronAvailableImageCallback for DuologueImageAvailable {
    fn handle_aeron_on_available_image(&mut self, _subscription: AeronSubscription, image: AeronImage) {
        let binding = image.get_constants().unwrap();
        let remote_addr = binding.source_identity();
        let session_id = binding.session_id;

        if remote_addr != self.owner {
            error!("Client Connecting witht the wrong address");
        } else {
            info!("[{}] Client Connected, address: {}", session_id, remote_addr);
        }
    }
}

pub struct DuologueImageUnavailable{
    pub owner: String,
}

impl AeronUnavailableImageCallback for DuologueImageUnavailable {
    fn handle_aeron_on_unavailable_image(&mut self, _subscription: AeronSubscription, image: AeronImage) {
        let binding = image.get_constants().unwrap();
        let remote_addr = binding.source_identity();
        let session_id = binding.session_id;
        // check image_count and close?
        info!("[{}] Client Disconnected, address: {}", session_id, remote_addr);
    }
}