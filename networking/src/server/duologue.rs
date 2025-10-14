use crate::server::cmd_handler::FragmentHandler;
use common::OrderCommand;
use disruptor::{MultiProducer, SingleConsumerBarrier};
use rusteron_client::{
    AeronAvailableImageCallback, AeronCError, AeronImage, AeronNotificationLogger,
    AeronSubscription, AeronUnavailableImageCallback, Handler,
};
use std::sync::Arc;
use tracing::{error, info};

pub const DUOLOGUE_STREAM_ID: i32 = 1002;

pub struct Duologue {
    fragment_handler: Handler<FragmentHandler>,
    pub gateway_id: u8,
    subscription: AeronSubscription,
    pub is_closed: bool,
    on_image_available_handler: Handler<DuologueImageAvailable>,
    on_image_unavailable_handler: Handler<DuologueImageUnavailable>,
}

impl Duologue {
    pub fn new(
        subscription: AeronSubscription,
        on_image_available_handler: Handler<DuologueImageAvailable>,
        on_image_unavailable_handler: Handler<DuologueImageUnavailable>,
        gateway_id: u8,
        producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
    ) -> Self {
        let fragment_handler = FragmentHandler {
            gateway_id,
            producer,
        };

        Self {
            fragment_handler: Handler::leak(fragment_handler),
            gateway_id,
            is_closed: false,
            subscription,
            on_image_available_handler,
            on_image_unavailable_handler,
        }
    }

    pub fn poll(&self) -> Result<i32, AeronCError> {
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
            error!(
                target: "gateway_session",
                action = "close_failed_on_drop",
                gateway_id = self.gateway_id,
                error = ?e
            );
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
                    target: "gateway_session",
                    action = "image_constants_failed",
                    gateway_id = self.gateway_id,
                    expected_session = format_args!("{:#x}", self.expected_session_id),
                    error = ?e
                );
                return;
            }
        };
        let address = binding.source_identity();
        let session_id = binding.session_id;

        if self.expected_session_id != session_id {
            error!(
                target: "gateway_session",
                action = "session_mismatch",
                gateway_id = self.gateway_id,
                expected_session = format_args!("{:#x}", self.expected_session_id),
                actual_session = format_args!("{:#x}", session_id)
            );
        } else {
            info!(
                target: "gateway_session",
                action = "image_connected",
                gateway_id = self.gateway_id,
                session = format_args!("{:#x}", session_id),
                address = %address
            );
        }
    }
}

pub struct DuologueImageUnavailable {
    pub session_id: i32,
    pub gateway_id: u8,
    pub cleanup_callback: Option<Arc<dyn Fn(u8) + Send + Sync>>,
}

impl AeronUnavailableImageCallback for DuologueImageUnavailable {
    fn handle_aeron_on_unavailable_image(
        &mut self,
        _subscription: AeronSubscription,
        _image: AeronImage,
    ) {
        info!(
            target: "gateway_session",
            action = "image_disconnected",
            gateway_id = self.gateway_id,
            session = format_args!("{:#x}", self.session_id)
        );

        if let Some(ref callback) = self.cleanup_callback {
            callback(self.gateway_id);
        }
    }
}
