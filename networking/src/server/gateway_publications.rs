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
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tracing::{debug, error};

use super::ServerError;
use super::progress::{DRAIN_TIMEOUT, PipelineProgress};

const ARCHIVE_OFFER_RETRY_LIMIT: usize = 5_000;
const OFFER_RETRY_BACKOFF: Duration = Duration::from_millis(1);
const RESPONSE_OFFER_RETRY_LIMIT: usize = 10;

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

fn classify_response_offer_result(result: i64) -> OfferResult {
    if result == i64::from(AERON_PUBLICATION_NOT_CONNECTED) {
        OfferResult::Fatal
    } else {
        classify_offer_result(result)
    }
}

/// Offers once, then re-offers a retryable result at most `retry_limit` times with
/// `OFFER_RETRY_BACKOFF` between attempts. Returns the last offer result.
fn offer_response(retry_limit: usize, mut offer: impl FnMut() -> i64) -> i64 {
    let mut result = offer();
    for _ in 0..retry_limit {
        if classify_response_offer_result(result) != OfferResult::Retryable {
            break;
        }
        std::thread::sleep(OFFER_RETRY_BACKOFF);
        result = offer();
    }
    result
}

/// Manages Gateway Publications from gateway id 0 to MAX_GATEWAYS - 1
/// Index MAX_GATEWAYS is reserved for archival publication
pub struct Publications {
    gateways: [ArcSwapOption<AeronPublication>; MAX_GATEWAYS + 1],
    progress: PipelineProgress,
    journal_tail: AtomicU64,
}

impl Publications {
    pub fn new() -> Self {
        Self {
            progress: PipelineProgress::new(),
            journal_tail: AtomicU64::new(0),
            gateways: core::array::from_fn::<
                ArcSwapOption<AeronPublication>,
                { MAX_GATEWAYS + 1 },
                _,
            >(|_| ArcSwapOption::const_empty()),
        }
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
        self.progress.drain(DRAIN_TIMEOUT)
    }

    pub(super) fn archive_publication(&self) -> Option<Arc<AeronPublication>> {
        self.gateways[MAX_GATEWAYS].load_full()
    }

    pub fn set_archive_publication(&self, publication: AeronPublication) {
        self.gateways[MAX_GATEWAYS].store(Some(Arc::new(publication)));
    }

    /// Timestamp of the last command in the journal when this process started, which journaling
    /// stamps every live command above. The server sets it before it accepts any command.
    ///
    /// 0 while there is none: an empty archive, or journaling disabled (no archive control
    /// channel, as `VEX_DISABLE_ARCHIVE` sets). Timestamps, and the order ids derived from them,
    /// then follow the wall clock alone.
    pub fn journal_tail(&self) -> u64 {
        self.journal_tail.load(Ordering::Acquire)
    }

    pub fn set_journal_tail(&self, timestamp: u64) {
        self.journal_tail.store(timestamp, Ordering::Release);
    }

    pub fn set(
        &self,
        gateway_id: u8,
        publication: Arc<AeronPublication>,
    ) -> Result<(), ServerError> {
        let index = Self::gateway_index(gateway_id)?;
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

    /// Sends `cmd` back to the gateway that submitted it, retrying a back-pressured offer up to
    /// `RESPONSE_OFFER_RETRY_LIMIT` times.
    // Publisher (event handler thread)
    pub fn publish_response(&self, cmd: &OrderCommand) {
        self.send_response(cmd, RESPONSE_OFFER_RETRY_LIMIT);
    }

    /// Sends `cmd` back to its gateway with a single offer that never sleeps. For the ingress poll
    /// thread, where a retry backoff would stall every gateway's commands; a reply lost to
    /// back-pressure surfaces at the gateway as its request timeout.
    pub fn try_publish_response(&self, cmd: &OrderCommand) {
        self.send_response(cmd, 0);
    }

    fn send_response(&self, cmd: &OrderCommand, retry_limit: usize) {
        let gateway_id = cmd.route_gateway_id;
        let ptr = match self.get(gateway_id) {
            Ok(ptr) => ptr,
            Err(e) => {
                error!("gateway-{gateway_id}: invalid gateway id to send response: {e}");
                return;
            }
        };
        let publication = ptr.as_ref();
        if publication.is_none() {
            error!(
                "gateway-{}: No publication found to send response",
                gateway_id
            );
            return;
        }
        let publication = publication.unwrap();
        let mut response_buffer = [0; ORDERCOMMANDSIZE];
        match encode_order_command(cmd, &mut response_buffer) {
            Ok(_) => {
                // Send the processed command back
                let result = offer_response(retry_limit, || {
                    publication.offer::<AeronReservedValueSupplierLogger>(&response_buffer, None)
                });
                if result >= 0 {
                    debug!(
                        "gateway-{}: Successfully sent processed OrderCommand",
                        gateway_id
                    );
                } else {
                    error!(
                        gateway_id,
                        client_order_id = cmd.client_order_id,
                        order_id = cmd.order_id,
                        final_status = ?cmd.status,
                        last_result_code = result,
                        "Failed to send processed OrderCommand"
                    );
                }
            }
            Err(e) => {
                error!(
                    "gateway-{}: Failed to encode processed OrderCommand: {:?}",
                    gateway_id, e
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
        AERON_PUBLICATION_ADMIN_ACTION, AERON_PUBLICATION_BACK_PRESSURED, AERON_PUBLICATION_CLOSED,
        AERON_PUBLICATION_ERROR, AERON_PUBLICATION_MAX_POSITION_EXCEEDED,
        AERON_PUBLICATION_NOT_CONNECTED,
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
    fn classifies_response_and_archive_offer_results() {
        assert_eq!(
            classify_response_offer_result(i64::from(AERON_PUBLICATION_NOT_CONNECTED)),
            OfferResult::Fatal
        );
        assert_eq!(
            classify_offer_result(i64::from(AERON_PUBLICATION_NOT_CONNECTED)),
            OfferResult::Retryable
        );

        for result in [
            AERON_PUBLICATION_BACK_PRESSURED,
            AERON_PUBLICATION_ADMIN_ACTION,
        ] {
            assert_eq!(
                classify_response_offer_result(i64::from(result)),
                OfferResult::Retryable
            );
            assert_eq!(
                classify_offer_result(i64::from(result)),
                OfferResult::Retryable
            );
        }

        for result in [
            AERON_PUBLICATION_CLOSED,
            AERON_PUBLICATION_MAX_POSITION_EXCEEDED,
            AERON_PUBLICATION_ERROR,
        ] {
            assert_eq!(
                classify_response_offer_result(i64::from(result)),
                OfferResult::Fatal
            );
            assert_eq!(classify_offer_result(i64::from(result)), OfferResult::Fatal);
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
    fn single_offer_response_never_retries_back_pressure() {
        let back_pressured = i64::from(AERON_PUBLICATION_BACK_PRESSURED);
        let mut offers = 0;

        let result = offer_response(0, || {
            offers += 1;
            back_pressured
        });

        // One offer means no backoff sleep was taken on the caller's thread.
        assert_eq!(offers, 1);
        assert_eq!(result, back_pressured);
    }

    #[test]
    fn response_offer_retries_only_retryable_results_and_is_bounded() {
        let mut offers = 0;
        offer_response(RESPONSE_OFFER_RETRY_LIMIT, || {
            offers += 1;
            i64::from(AERON_PUBLICATION_BACK_PRESSURED)
        });
        assert_eq!(offers, RESPONSE_OFFER_RETRY_LIMIT + 1);

        let mut results = [i64::from(AERON_PUBLICATION_ADMIN_ACTION), 0].into_iter();
        assert_eq!(
            offer_response(RESPONSE_OFFER_RETRY_LIMIT, || results.next().unwrap()),
            0
        );

        let mut offers = 0;
        offer_response(RESPONSE_OFFER_RETRY_LIMIT, || {
            offers += 1;
            i64::from(AERON_PUBLICATION_NOT_CONNECTED)
        });
        assert_eq!(offers, 1);
    }
}
