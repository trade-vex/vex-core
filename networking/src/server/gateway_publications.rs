use arc_swap::ArcSwapOption;
use common::{MAX_GATEWAYS, ORDERCOMMANDSIZE, OrderCommand, encode_order_command};
use rusteron_archive::{
    AeronPublication, AeronReservedValueSupplierLogger,
    bindings::{
        AERON_PUBLICATION_ADMIN_ACTION, AERON_PUBLICATION_BACK_PRESSURED,
        AERON_PUBLICATION_NOT_CONNECTED,
    },
};
use std::{sync::Arc, time::Duration};
use tracing::{debug, error};

use super::ServerError;

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

/// Manages Gateway Publications from gateway id 0 to MAX_GATEWAYS - 1
/// Index MAX_GATEWAYS is reserved for archival publication
pub struct Publications {
    gateways: [ArcSwapOption<AeronPublication>; MAX_GATEWAYS + 1],
}

impl Publications {
    pub fn new() -> Self {
        Self {
            gateways: core::array::from_fn::<
                ArcSwapOption<AeronPublication>,
                { MAX_GATEWAYS + 1 },
                _,
            >(|_| ArcSwapOption::const_empty()),
        }
    }

    pub fn set_archive_publication(&self, publication: AeronPublication) {
        self.gateways[MAX_GATEWAYS].store(Some(Arc::new(publication)));
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

    // Publisher (event handler thread)
    pub fn publish_response(&self, cmd: &OrderCommand) {
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
                for retry in 0..=RESPONSE_OFFER_RETRY_LIMIT {
                    let result = publication
                        .offer::<AeronReservedValueSupplierLogger>(&response_buffer, None);

                    match classify_response_offer_result(result) {
                        OfferResult::Success => {
                            debug!(
                                "gateway-{}: Successfully sent processed OrderCommand",
                                gateway_id
                            );
                            return;
                        }
                        OfferResult::Retryable if retry < RESPONSE_OFFER_RETRY_LIMIT => {
                            std::thread::sleep(OFFER_RETRY_BACKOFF);
                        }
                        OfferResult::Retryable | OfferResult::Fatal => {
                            error!(
                                gateway_id,
                                client_order_id = cmd.client_order_id,
                                order_id = cmd.order_id,
                                final_status = ?cmd.status,
                                last_result_code = result,
                                "Failed to send processed OrderCommand"
                            );
                            return;
                        }
                    }
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
        let gateway_id = cmd.order_id;
        let ptr = self.gateways[MAX_GATEWAYS].load_full();
        let publication = ptr.as_ref();
        if publication.is_none() {
            // None means archive recording is not configured; see server/mod.rs.
            debug!(
                "gateway-{}: Archive recording not configured, skipping command, client order_id: {}",
                gateway_id, cmd.client_order_id
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
                                "gateway-{}: successfully published to archive, client order_id: {}",
                                gateway_id, cmd.client_order_id
                            );
                            return;
                        }
                        OfferResult::Retryable if retry < ARCHIVE_OFFER_RETRY_LIMIT => {
                            std::thread::sleep(OFFER_RETRY_BACKOFF);
                        }
                        OfferResult::Retryable | OfferResult::Fatal => {
                            error!(
                                "gateway-{}: Failed to archive OrderCommand, client order_id: {}, result: {}",
                                gateway_id, cmd.client_order_id, result
                            );
                            std::process::abort();
                        }
                    }
                }
            }
            Err(e) => {
                error!(
                    "gateway-{}: Failed to encode archive OrderCommand, client order_id: {}, error: {:?}",
                    gateway_id, cmd.client_order_id, e
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
}
