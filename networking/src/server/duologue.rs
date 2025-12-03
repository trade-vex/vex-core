use crate::server::cmd_handler::FragmentHandler;
use common::OrderCommand;
use disruptor::{MultiProducer, SingleConsumerBarrier};
use rusteron_archive::{
    AeronAvailableImageCallback, AeronCError, AeronImage, AeronNotificationCallback,
    AeronSubscription, AeronUnavailableImageCallback, Handler,
};
use std::sync::mpsc::Sender;
use tracing::{error, info};

pub const DUOLOGUE_STREAM_ID: i32 = 1002;

pub struct Duologue {
    fragment_handler: Option<Handler<FragmentHandler>>,
    pub gateway_id: u8,
    subscription: AeronSubscription,
    pub is_closed: bool,
    on_image_available_handler: Option<Handler<DuologueImageAvailable>>,
    on_image_unavailable_handler: Option<Handler<DuologueImageUnavailable>>,
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
            fragment_handler: Some(Handler::leak(fragment_handler)),
            gateway_id,
            is_closed: false,
            subscription,
            on_image_available_handler: Some(on_image_available_handler),
            on_image_unavailable_handler: Some(on_image_unavailable_handler),
        }
    }

    pub fn poll(&self) -> Result<i32, AeronCError> {
        if let Some(handler) = &self.fragment_handler {
            self.subscription.poll(Some(handler), 2048)
        } else {
            // should not reach here as the handler is only taken during close()
            Ok(0)
        }
    }

    pub fn close(&mut self) -> Result<(), AeronCError> {
        // taking ownership of the handlers to move into the close notification
        // this is required because, the subsciption.close() is an async operation,
        // hence it is unsafe to release the handlers immediately after calling close()
        let fragment_handler = self.fragment_handler.take();
        let on_image_available_handler = self.on_image_available_handler.take();
        let on_image_unavailable_handler = self.on_image_unavailable_handler.take();

        if let (Some(fh), Some(iah), Some(iuh)) = (
            fragment_handler,
            on_image_available_handler,
            on_image_unavailable_handler,
        ) {
            let close_notification = DuologueCloseNotification {
                gateway_id: self.gateway_id,
                fragment_handler: fh,
                on_image_available_handler: iah,
                on_image_unavailable_handler: iuh,
            };
            self.subscription
                .close(Some(&Handler::leak(close_notification)))?;
        } else {
            self.subscription.close::<DuologueCloseNotification>(None)?;
        }

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
    pub tx: Sender<u8>,
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

        if let Err(e) = self.tx.send(self.gateway_id) {
            error!(
                target: "gateway_manager",
                action = "core_publication_cleanup_request_failed",
                gateway_id = self.gateway_id,
                error = %e
            );
        }
    }
}

pub struct DuologueCloseNotification {
    pub gateway_id: u8,
    pub fragment_handler: Handler<FragmentHandler>,
    pub on_image_available_handler: Handler<DuologueImageAvailable>,
    pub on_image_unavailable_handler: Handler<DuologueImageUnavailable>,
}

impl AeronNotificationCallback for DuologueCloseNotification {
    fn handle_aeron_notification(&mut self) {
        // Only release handlers after the subscription is fully closed
        self.fragment_handler.release();
        self.on_image_available_handler.release();
        self.on_image_unavailable_handler.release();

        info!(
            target: "gateway_session",
            action = "subscription_closed_handlers_released",
            gateway_id = self.gateway_id
        );
    }
}
