use arc_swap::ArcSwapOption;
use common::{MAX_GATEWAYS, ORDERCOMMANDSIZE, OrderCommand, encode_order_command};
use rusteron_archive::{
    AeronPublication, AeronReservedValueSupplierLogger,
    bindings::{
        AERON_PUBLICATION_ADMIN_ACTION, AERON_PUBLICATION_BACK_PRESSURED,
        AERON_PUBLICATION_NOT_CONNECTED,
    },
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::{debug, error, warn};

use super::ServerError;
use super::progress::{DRAIN_TIMEOUT, PipelineProgress};
use crate::utils::offer_once;

const ARCHIVE_OFFER_RETRY_LIMIT: usize = 5_000;
const OFFER_RETRY_BACKOFF: Duration = Duration::from_millis(1);
/// How long a gateway's replies may keep failing (its channel full, or gone) before the core
/// closes its session. Aeron's publication window is the per-gateway buffer, so a failed offer
/// already means the gateway has not read for far longer than a healthy one takes; and the
/// gateway gives a request 5 s, so a reply older than that answers nobody.
pub(super) const SLOW_GATEWAY_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, PartialEq, Eq)]
enum OfferResult {
    Success,
    Retryable,
    Fatal,
}

fn classify_offer_result(result: i64) -> OfferResult {
    if result >= 0 {
        OfferResult::Success
    } else if result == i64::from(AERON_PUBLICATION_BACK_PRESSURED)
        || result == i64::from(AERON_PUBLICATION_ADMIN_ACTION)
        || result == i64::from(AERON_PUBLICATION_NOT_CONNECTED)
    {
        OfferResult::Retryable
    } else {
        OfferResult::Fatal
    }
}

/// Manages Gateway Publications from gateway id 0 to MAX_GATEWAYS - 1
/// Index MAX_GATEWAYS is reserved for archival publication
pub struct Publications {
    gateways: [ArcSwapOption<AeronPublication>; MAX_GATEWAYS + 1],
    /// Per gateway: the millisecond (`elapsed_ms`) of the first failed reply offer since the last
    /// delivered one; 0 while replies are being delivered.
    failing_since_ms: [AtomicU64; MAX_GATEWAYS],
    /// Per gateway: set when its reply publication is dropped for `SLOW_GATEWAY_TIMEOUT` of
    /// failed offers; the server thread then closes that session (`take_closing`).
    closing: [AtomicBool; MAX_GATEWAYS],
    started: Instant,
    progress: PipelineProgress,
    journal_tail: AtomicU64,
    delivered: AtomicU64,
}

impl Publications {
    pub fn new() -> Self {
        Self {
            progress: PipelineProgress::new(),
            journal_tail: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            gateways: core::array::from_fn::<
                ArcSwapOption<AeronPublication>,
                { MAX_GATEWAYS + 1 },
                _,
            >(|_| ArcSwapOption::const_empty()),
            failing_since_ms: core::array::from_fn(|_| AtomicU64::new(0)),
            closing: core::array::from_fn(|_| AtomicBool::new(false)),
            started: Instant::now(),
        }
    }

    /// Milliseconds since this process's publications were created, never 0.
    fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis())
            .unwrap_or(u64::MAX)
            .saturating_add(1)
    }

    /// Call after a successful ingress publication, before polling more ingress.
    pub fn submitted(&self, sequence: i64) {
        self.progress.submitted(sequence);
    }

    /// Call only after the terminal handler has finished all work for this sequence.
    pub fn completed(&self, sequence: i64) {
        self.progress.completed(sequence);
    }

    pub fn drain_pipeline(&self) -> Result<(), ServerError> {
        self.drain_pipeline_within(DRAIN_TIMEOUT)
    }

    /// [`Self::drain_pipeline`], failing once `stall` passes without progress.
    pub fn drain_pipeline_within(&self, stall: Duration) -> Result<(), ServerError> {
        self.progress.drain(stall)
    }

    /// The last sequence submitted to the pipeline; -1 before the first.
    pub fn submitted_sequence(&self) -> i64 {
        self.progress.submitted_sequence()
    }

    pub(super) fn archive_publication(&self) -> Option<Arc<AeronPublication>> {
        self.gateways[MAX_GATEWAYS].load_full()
    }

    pub fn set_archive_publication(&self, publication: AeronPublication) {
        self.gateways[MAX_GATEWAYS].store(Some(Arc::new(publication)));
    }

    /// Timestamp of the last command journaled so far: the journal's last command when this
    /// process started, which the server sets before it accepts any command, then each command
    /// journaling stamps (above it). A snapshot records it as the last command it holds.
    ///
    /// 0 while there is none: an empty archive, or journaling disabled (no archive control
    /// channel). Timestamps, and the order ids derived from them, then follow the wall clock
    /// alone.
    pub fn journal_tail(&self) -> u64 {
        self.journal_tail.load(Ordering::Acquire)
    }

    pub fn set_journal_tail(&self, timestamp: u64) {
        self.journal_tail.store(timestamp, Ordering::Release);
    }

    /// The delivered watermark W as the events stage last handed it to Kafka (the value read at
    /// start until then): every event of every command up to this timestamp is acknowledged.
    pub fn delivered(&self) -> u64 {
        self.delivered.load(Ordering::Acquire)
    }

    pub fn set_delivered(&self, timestamp: u64) {
        self.delivered.store(timestamp, Ordering::Release);
    }

    pub fn set(
        &self,
        gateway_id: u8,
        publication: Arc<AeronPublication>,
    ) -> Result<(), ServerError> {
        let index = Self::gateway_index(gateway_id)?;
        // A new session starts healthy, whatever the one before it did.
        self.failing_since_ms[index].store(0, Ordering::Relaxed);
        self.closing[index].store(false, Ordering::Release);
        self.gateways[index].store(Some(publication));
        Ok(())
    }

    pub fn get(&self, gateway_id: u8) -> Result<Option<Arc<AeronPublication>>, ServerError> {
        let index = Self::gateway_index(gateway_id)?;
        Ok(self.gateways[index].load_full())
    }

    pub fn remove(&self, gateway_id: u8) -> Result<(), ServerError> {
        let index = Self::gateway_index(gateway_id)?;
        self.gateways[index].store(None);
        Ok(())
    }

    fn gateway_index(gateway_id: u8) -> Result<usize, ServerError> {
        let index = usize::from(gateway_id);
        if index >= MAX_GATEWAYS {
            return Err(ServerError::GatewayMessageError(format!(
                "Gateway ID {gateway_id} out of range (max: {})",
                MAX_GATEWAYS - 1
            )));
        }
        Ok(index)
    }

    /// Records a reply offer's result for `index`. True when the gateway has been undeliverable
    /// for `SLOW_GATEWAY_TIMEOUT`, so its session must go.
    fn record_reply(&self, index: usize, result: i64, now_ms: u64) -> bool {
        if result >= 0 {
            self.failing_since_ms[index].store(0, Ordering::Relaxed);
            return false;
        }
        let since = match self.failing_since_ms[index].compare_exchange(
            0,
            now_ms,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => now_ms,
            Err(since) => since,
        };
        now_ms.saturating_sub(since) >= SLOW_GATEWAY_TIMEOUT.as_millis() as u64
    }

    /// For the server thread's poll: whether this gateway's session must be closed because its
    /// replies could not be delivered (`publish_response`).
    pub fn take_closing(&self, gateway_id: u8) -> bool {
        let index = usize::from(gateway_id);
        index < MAX_GATEWAYS
            && self.closing[index].load(Ordering::Acquire)
            && self.closing[index].swap(false, Ordering::Acquire)
    }

    /// Sends `cmd` back to its gateway with one non-blocking offer (`offer_once`). The events
    /// thread answers every gateway and the ingress thread reads every gateway's commands, so
    /// this never sleeps. A reply the gateway's channel has no room for is dropped: its events
    /// are already in Kafka, and the gateway answers 408 and resolves the outcome from the read
    /// model. A gateway undeliverable for `SLOW_GATEWAY_TIMEOUT` loses its session, so it stops
    /// sending orders the core could not answer, and reconnects.
    pub fn publish_response(&self, cmd: &OrderCommand) {
        let gateway_id = cmd.route_gateway_id;
        let index = match Self::gateway_index(gateway_id) {
            Ok(index) => index,
            Err(e) => {
                error!("gateway-{gateway_id}: invalid gateway id to send response: {e}");
                return;
            }
        };
        let Some(publication) = self.gateways[index].load_full() else {
            debug!(
                gateway_id,
                client_order_id = cmd.client_order_id,
                "no session to answer"
            );
            return;
        };
        let mut buffer = [0; ORDERCOMMANDSIZE];
        if let Err(e) = encode_order_command(cmd, &mut buffer) {
            error!("gateway-{gateway_id}: Failed to encode processed OrderCommand: {e:?}");
            return;
        }
        let result =
            offer_once(|| publication.offer::<AeronReservedValueSupplierLogger>(&buffer, None));
        let was_failing = self.failing_since_ms[index].load(Ordering::Relaxed) != 0;
        let close = self.record_reply(index, result, self.elapsed_ms());
        if result >= 0 {
            return;
        }
        if !was_failing {
            warn!(
                gateway_id,
                result,
                "gateway stopped reading replies; dropping replies until it reads or its session closes"
            );
        }
        debug!(
            gateway_id,
            client_order_id = cmd.client_order_id,
            order_id = cmd.order_id,
            final_status = ?cmd.status,
            result,
            "reply dropped"
        );
        if close {
            // Drop this session's publication only: a session that replaced it meanwhile keeps
            // its own. The server thread finishes the close (`take_closing`).
            let previous = self.gateways[index].compare_and_swap(&publication, None);
            if previous
                .as_ref()
                .is_some_and(|previous| Arc::ptr_eq(previous, &publication))
            {
                self.closing[index].store(true, Ordering::Release);
                warn!(
                    gateway_id,
                    timeout_ms = SLOW_GATEWAY_TIMEOUT.as_millis() as u64,
                    "gateway undeliverable for the slow-gateway timeout; closing its session"
                );
            }
        }
    }

    // Publisher (event handler thread)
    pub fn publish_to_archive(&self, cmd: &OrderCommand) {
        // PR #182: this is an ORDER id, not a gateway id — label it accordingly.
        let order_id = cmd.order_id;
        let ptr = self.gateways[MAX_GATEWAYS].load_full();
        let publication = ptr.as_ref();
        if publication.is_none() {
            // None means archive recording is not configured; see server/mod.rs.
            debug!(
                "order-{}: Archive recording not configured, skipping command, client order_id: {}",
                order_id, cmd.client_order_id
            );
            return;
        }
        let publication = publication.unwrap();
        let mut response_buffer = [0; ORDERCOMMANDSIZE];
        match encode_order_command(cmd, &mut response_buffer) {
            Ok(_) => {
                for retry in 0..=ARCHIVE_OFFER_RETRY_LIMIT {
                    let result = publication
                        .offer::<AeronReservedValueSupplierLogger>(&response_buffer, None);

                    match classify_offer_result(result) {
                        OfferResult::Success => {
                            debug!(
                                "order-{}: successfully published to archive, client order_id: {}",
                                order_id, cmd.client_order_id
                            );
                            return;
                        }
                        OfferResult::Retryable if retry < ARCHIVE_OFFER_RETRY_LIMIT => {
                            std::thread::sleep(OFFER_RETRY_BACKOFF);
                        }
                        OfferResult::Retryable | OfferResult::Fatal => {
                            error!(
                                "order-{}: Failed to archive OrderCommand, client order_id: {}, result: {}",
                                order_id, cmd.client_order_id, result
                            );
                            std::process::abort();
                        }
                    }
                }
            }
            Err(e) => {
                error!(
                    "order-{}: Failed to encode archive OrderCommand, client order_id: {}, error: {:?}",
                    order_id, cmd.client_order_id, e
                );
                std::process::abort();
            }
        }
    }
}

impl Default for Publications {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_out_of_range_gateway_id() {
        let publications = Publications::new();

        assert!(matches!(
            publications.get(MAX_GATEWAYS as u8),
            Err(ServerError::GatewayMessageError(_))
        ));
    }

    use rusteron_archive::bindings::{
        AERON_PUBLICATION_CLOSED, AERON_PUBLICATION_ERROR, AERON_PUBLICATION_MAX_POSITION_EXCEEDED,
    };

    #[test]
    fn classifies_successful_offer() {
        assert_eq!(classify_offer_result(0), OfferResult::Success);
        assert_eq!(classify_offer_result(1), OfferResult::Success);
    }

    #[test]
    fn classifies_retryable_offer_results() {
        for result in [
            AERON_PUBLICATION_BACK_PRESSURED,
            AERON_PUBLICATION_ADMIN_ACTION,
            AERON_PUBLICATION_NOT_CONNECTED,
        ] {
            assert_eq!(
                classify_offer_result(i64::from(result)),
                OfferResult::Retryable
            );
        }
    }

    #[test]
    fn classifies_fatal_offer_results() {
        for result in [
            AERON_PUBLICATION_CLOSED,
            AERON_PUBLICATION_MAX_POSITION_EXCEEDED,
            AERON_PUBLICATION_ERROR,
        ] {
            assert_eq!(classify_offer_result(i64::from(result)), OfferResult::Fatal);
        }
        assert_eq!(classify_offer_result(-7), OfferResult::Fatal);
    }

    #[test]
    fn a_gateway_whose_replies_keep_failing_is_closed_after_the_timeout() {
        let p = Publications::new();
        let (bp, nc) = (
            i64::from(AERON_PUBLICATION_BACK_PRESSURED),
            i64::from(AERON_PUBLICATION_NOT_CONNECTED),
        );
        assert!(!p.record_reply(1, bp, 1)); // the first failure starts the clock
        assert!(!p.record_reply(1, bp, 999)); // inside the budget: dropped, session kept
        assert!(!p.record_reply(1, 0, 1_000)); // a delivered reply ends the incident
        assert!(!p.record_reply(1, nc, 1_001));
        assert!(!p.record_reply(1, nc, 2_000));
        assert!(p.record_reply(1, nc, 2_001)); // 1 s of continuous failure: close
        assert!(!p.record_reply(2, bp, 2_001)); // another gateway's clock is its own
        assert!(!p.take_closing(1)); // only publish_response sets the flag
    }
}
