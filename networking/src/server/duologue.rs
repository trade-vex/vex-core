use crate::server::cmd_handler::FragmentHandler;
use crate::utils::{
    new_publication_with_mdc_and_session, new_subsciption_with_handlers_and_session,
};
use common::OrderCommand;
use disruptor::{MultiProducer, SingleConsumerBarrier};
use rusteron_client::{
    Aeron, AeronAvailableImageCallback, AeronCError, AeronImage, AeronNotificationLogger,
    AeronPublication, AeronSubscription, AeronUnavailableImageCallback, Handler,
};
use tracing::{error, info};

pub const DUOLOGUE_STREAM_ID: i32 = 1002;

pub struct Duologue {
    fragment_handler: Handler<FragmentHandler>,
    pub session_id: i32,
    pub gateway_id: u8,
    subscription: AeronSubscription,
    pub port_data: u16,
    pub port_control: u16,
    pub is_closed: bool,
    on_image_available_handler: Handler<DuologueImageAvailable>,
    on_image_unavailable_handler: Handler<DuologueImageUnavailable>,
}

#[allow(clippy::too_many_arguments)]
impl Duologue {
    pub fn new(
        aeron: &Aeron,
        local: &str,
        gateway_id: u8,
        owner: &str,
        port_data: u16,
        port_control: u16,
        session_id: i32,
        producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
    ) -> Result<(Self, AeronPublication), AeronCError> {
        let publication = new_publication_with_mdc_and_session(
            aeron,
            local,
            port_control,
            DUOLOGUE_STREAM_ID,
            session_id,
        )?;

        let on_image_available_handler = Handler::leak(DuologueImageAvailable {
            owner: owner.to_string(),
        });
        let on_image_unavailable_handler = Handler::leak(DuologueImageUnavailable {
            owner: owner.to_string(),
        });

        let subscription = new_subsciption_with_handlers_and_session(
            aeron,
            local,
            port_data,
            DUOLOGUE_STREAM_ID,
            session_id,
            Some(&on_image_available_handler),
            Some(&on_image_unavailable_handler),
        )?;

        let fragment_handler = FragmentHandler {
            gateway_id,
            producer,
        };

        Ok((
            Self {
                fragment_handler: Handler::leak(fragment_handler),
                gateway_id,
                port_data,
                port_control,
                is_closed: false,
                session_id,
                subscription,
                on_image_available_handler,
                on_image_unavailable_handler,
            },
            publication,
        ))
    }

    pub fn poll(&mut self) -> Result<i32, AeronCError> {
        self.subscription.poll(Some(&self.fragment_handler), 2048)
    }

    pub fn close(&mut self) -> Result<(), AeronCError> {
        self.subscription.close::<AeronNotificationLogger>(None)?;
        self.fragment_handler.release();
        self.on_image_available_handler.release();
        self.on_image_unavailable_handler.release();
        self.is_closed = true;
        Ok(())
    }
}

impl Drop for Duologue {
    fn drop(&mut self) {
        if !self.is_closed
            && let Err(e) = self.close()
        {
            error!("Failed to close Duologue during drop: {:?}", e);
        }
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
        let binding = match image.get_constants() {
            Ok(b) => b,
            Err(e) => {
                error!(
                    "Failed to get image constants for gateway {}: {:?}",
                    self.owner, e
                );
                return;
            }
        };
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
        let binding = match image.get_constants() {
            Ok(b) => b,
            Err(e) => {
                error!(
                    "Failed to get image constants for gateway {}: {:?}",
                    self.owner, e
                );
                return;
            }
        };
        let remote_addr = binding.source_identity();
        let session_id = binding.session_id;
        // check image_count and close?
        info!(
            "[{}] Client Disconnected, address: {}, gateway: {}",
            session_id, remote_addr, self.owner
        );
    }
}
