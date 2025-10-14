use crate::server::cmd_handler::FragmentHandler;
use common::OrderCommand;
use disruptor::{MultiProducer, SingleConsumerBarrier};
use rusteron_client::{
    AeronAvailableImageCallback, AeronCError, AeronImage, AeronNotificationLogger,
    AeronSubscription, AeronUnavailableImageCallback, Handler,
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
        subscription: AeronSubscription,
        on_image_available_handler: Handler<DuologueImageAvailable>,
        on_image_unavailable_handler: Handler<DuologueImageUnavailable>,
        gateway_id: u8,
        port_data: u16,
        port_control: u16,
        session_id: i32,
        producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
    ) -> Self {
        let fragment_handler = FragmentHandler {
            gateway_id,
            producer,
        };

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
        }
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
    pub expected_session_id: i32,
    pub gateway_id: u8,
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
                    "Failed to get image constants for session {:x}: {:?}",
                    self.expected_session_id, e
                );
                return;
            }
        };
        let address = binding.source_identity();
        let session_id = binding.session_id;

        if self.expected_session_id != session_id {
            error!(
                "Expected session ID {:x}, but got {:x}",
                self.expected_session_id, session_id
            );
        } else {
            info!(
                "gateway-{}, [{:x}] session connected, address: {}",
                self.gateway_id, session_id, address
            );
        }
    }
}

pub struct DuologueImageUnavailable {
    pub session_id: i32,
    pub gateway_id: u8,
}

impl AeronUnavailableImageCallback for DuologueImageUnavailable {
    fn handle_aeron_on_unavailable_image(
        &mut self,
        _subscription: AeronSubscription,
        _image: AeronImage,
    ) {
        info!(
            "gateway-{}, session: [{:#?}] session disconnected",
            self.gateway_id, self.session_id
        );
    }
}
