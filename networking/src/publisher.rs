use rusteron_client::{
    Aeron, AeronCError, AeronContext, 
    AeronPublication, AeronReservedValueSupplierLogger
};
use std::{
    collections::HashMap,
    ffi::{CStr, CString},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    }, time::Duration,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PublisherError {
    #[error("Aeron error: {0}")]
    Aeron(#[from] AeronCError),
    #[error("Channel closed")]
    ChannelClosed,
    #[error("Invalid input: {0}")]
    InvalidInput(#[from] std::ffi::NulError),
}

pub struct AeronPublisher {
    aeron: Arc<Aeron>,
    publications: HashMap<String, AeronPublication>,
    running: Arc<AtomicBool>,
    // idle_strategy: AeronIdleStrategy,
}

impl AeronPublisher {
    pub fn new(context_dir: &CStr) -> Result<Self, PublisherError> {
        let ctx = AeronContext::new()?;
        ctx.set_dir(context_dir)?;
        ctx.set_driver_timeout_ms(1_000_000)?;

        let aeron = Arc::new(Aeron::new(&ctx)?);

        Ok(Self {
            aeron,
            publications: HashMap::new(),
            running: Arc::new(AtomicBool::new(true)),
            // idle_strategy,
        })
    }

    pub fn add_publication(&mut self, channel: &str, stream_id: i32) -> Result<(), PublisherError> {
        let channel_cstr = CString::new(channel)?;
        let publication = self.aeron.add_publication(&channel_cstr, stream_id, Duration::from_secs(1))?;
        self.publications.insert(Self::get_key(channel, stream_id), publication);
        Ok(())
    }

    pub fn send(&self, msg: &[u8], channel: &str, stream_id: i32) -> Result<(), PublisherError> {
        if !self.running.load(Ordering::Relaxed) || msg.is_empty() {
            return Ok(());
        }

        if let Some(pub_) = self.publications.get(&Self::get_key(channel, stream_id)) {
            let result = pub_.offer::<AeronReservedValueSupplierLogger>(msg, None);
            if result < 0 {
                return Err(AeronCError::from_code(result as i32).into());
            }
        }

        Ok(())
    }

    pub fn send_all(&self, msg: &[u8]) -> Result<(), PublisherError> {
        if msg.is_empty() {
            return Ok(());
        }

        for publication in self.publications.values() {
            let result = publication.offer::<AeronReservedValueSupplierLogger>(msg, None);
            if result < 0 {
                return Err(AeronCError::from_code(result as i32).into());
            }
        }

        Ok(())
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        self.publications.clear(); // Publications are closed when dropped
    }

    fn get_key(channel: &str, stream_id: i32) -> String {
        format!("{}_{}", channel, stream_id)
    }
}
