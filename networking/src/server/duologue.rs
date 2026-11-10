use crate::server::cmd_handler::FragmentHandler;
use crate::server::gateway_manager::GatewaySessionEvent;
use crate::server::gateway_publications::Publications;
use common::OrderCommand;
use disruptor::{MultiProducer, SingleConsumerBarrier};
use rusteron_archive::{
    AeronAvailableImageCallback, AeronCError, AeronImage, AeronNotificationCallback,
    AeronSubscription, AeronUnavailableImageCallback, Handler,
};
use std::sync::{Arc, mpsc::Sender};
use tracing::{error, info};

pub const DUOLOGUE_STREAM_ID: i32 = 1002;
/// Upper bound on fragments consumed per poll. It only bounds the work one poll call does.
///
/// It does not decide refusals. Each fragment is published to the ring on its own: one that finds
/// the ring full is answered `Busy` (`FragmentHandler`), and fragments a poll does not consume
/// stay in the Aeron log for the next poll. The value matches the default build's ring capacity
/// (`vex_server::engine::BUFFER_SIZE`, 1024), but nothing depends on that.
const RING_BUFFER_CAPACITY: usize = 1024;

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
        publications: Arc<Publications>,
    ) -> Self {
        let fragment_handler = FragmentHandler {
            gateway_id,
            producer,
            publications,
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
            self.subscription.poll(Some(handler), RING_BUFFER_CAPACITY)
        } else {
            // should not reach here as the handler is only taken during close()
            Ok(0)
        }
    }

    /// Closes the session's subscription, and releases its ring producer before returning.
    ///
    /// The fragment handler holds the session's clone of the ring producer, and the pipeline joins
    /// its consumers when the last clone drops. Aeron calls the handler only inside
    /// [`Self::poll`], on this thread, so nothing can reach it once the session stops polling:
    /// it is released here. The close notification runs on Aeron's conductor thread: at shutdown,
    /// after the server thread has dropped its own producers, or never once the Aeron client is
    /// torn down. Holding the clone, it made the pipeline join on that thread, or not at all, and
    /// the process exited without the events stage's shutdown (#200). The image handlers are
    /// registered with the subscription, and Aeron's conductor may call them until the close
    /// completes, so the close notification releases them.
    pub fn close(&mut self) -> Result<(), AeronCError> {
        if let Some(mut fragment_handler) = self.fragment_handler.take() {
            fragment_handler.release();
        }

        if let (Some(on_image_available_handler), Some(on_image_unavailable_handler)) = (
            self.on_image_available_handler.take(),
            self.on_image_unavailable_handler.take(),
        ) {
            let closed = Handler::leak(DuologueCloseNotification {
                gateway_id: self.gateway_id,
                on_image_available_handler,
                on_image_unavailable_handler,
            });
            self.subscription.close(Some(&closed))?;
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
    pub tx: Sender<GatewaySessionEvent>,
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
            if let Err(e) = self.tx.send(GatewaySessionEvent::ImageAvailable {
                gateway_id: self.gateway_id,
                session_id,
            }) {
                error!(
                    target: "gateway_manager",
                    action = "image_available_notification_failed",
                    gateway_id = self.gateway_id,
                    error = %e
                );
            }
        }
    }
}

pub struct DuologueImageUnavailable {
    pub session_id: i32,
    pub gateway_id: u8,
    pub tx: Sender<GatewaySessionEvent>,
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

        if let Err(e) = self.tx.send(GatewaySessionEvent::ImageUnavailable {
            gateway_id: self.gateway_id,
            session_id: self.session_id,
        }) {
            error!(
                target: "gateway_manager",
                action = "core_publication_cleanup_request_failed",
                gateway_id = self.gateway_id,
                error = %e
            );
        }
    }
}

/// Releases a gateway subscription's image handlers once Aeron has closed the subscription: until
/// then, Aeron's conductor may still call them.
pub struct DuologueCloseNotification {
    pub gateway_id: u8,
    pub on_image_available_handler: Handler<DuologueImageAvailable>,
    pub on_image_unavailable_handler: Handler<DuologueImageUnavailable>,
}

impl AeronNotificationCallback for DuologueCloseNotification {
    fn handle_aeron_notification(&mut self) {
        self.on_image_available_handler.release();
        self.on_image_unavailable_handler.release();

        info!(
            target: "gateway_session",
            action = "subscription_closed_handlers_released",
            gateway_id = self.gateway_id
        );
    }
}
