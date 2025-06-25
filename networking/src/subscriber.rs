use rusteron_client::{
    Aeron, AeronAvailableImageLogger, AeronCError, AeronContext, AeronFragmentAssembler, AeronSubscription, AeronUnavailableImageLogger, Handler
};
use std::{
    ffi::{CStr, CString},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SubscriberError {
    #[error("Aeron error: {0}")]
    Aeron(#[from] AeronCError),
    #[error("Channel closed")]
    ChannelClosed,
    #[error("Invalid input: {0}")]
    InvalidInput(#[from] std::ffi::NulError),
}

pub struct AeronSubscriber {
    aeron: Arc<Aeron>,
    subscriptions: Vec<AeronSubscription>,
    data_handler: AeronFragmentAssembler,
    running: Arc<AtomicBool>,
}

impl AeronSubscriber {
    /// Creates a new subscriber connected to an Aeron IPC channel.
    pub fn new(
        context_dir: &CStr,
        data_handler: AeronFragmentAssembler,
    ) -> Result<Self, SubscriberError> {
        let ctx = AeronContext::new()?;
        ctx.set_dir(context_dir)?;
        ctx.set_driver_timeout_ms(1_000_000)?;

        let aeron = Arc::new(Aeron::new(&ctx)?);

        Ok(Self {
            aeron,
            subscriptions: Vec::new(),
            data_handler,
            running: Arc::new(AtomicBool::new(true)),
        })
    }

    pub fn add_subscription(&mut self, channel: &str, stream_id: i32) -> Result<(), SubscriberError> {
        let channel_cstr = CString::new(channel)?;

        let available_cb = AeronAvailableImageLogger {};
        let available_cb_handler = Handler::leak(available_cb);

        let unavailable_cb = AeronUnavailableImageLogger {};
        let unavailable_cb_handler = Handler::leak(unavailable_cb);
        
        let subscription = self.aeron.add_subscription(
            &channel_cstr,
            stream_id,
            Some(&available_cb_handler),
            Some(&unavailable_cb_handler),
            Duration::from_secs(1),
        )?;

        self.subscriptions.push(subscription);
        Ok(())
    }

    pub fn start(&self) {
        let running = self.running.clone();
        let subscriptions = self.subscriptions.clone();
        let data_handler = Arc::new(self.data_handler.clone());
        // let idle_strategy = Arc::new(self.idle_strategy.clone());
        
        let handler = Handler::leak((*data_handler).clone());
        std::thread::spawn(move || {
            while running.load(Ordering::Relaxed) {
                for sub in &subscriptions {
                    sub.poll(Some(&handler), 1).unwrap_or(0);
                }
            }
        });
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}