use crate::server::ServerError;
use rand::Rng;
use rand::thread_rng;
use rusteron_archive::bindings::AERON_PUBLICATION_ADMIN_ACTION;
use rusteron_archive::{
    Aeron, AeronAvailableImageCallback, AeronAvailableImageLogger, AeronCError, AeronPublication,
    AeronReservedValueSupplierLogger, AeronSubscription, AeronUnavailableImageCallback,
    AeronUnavailableImageLogger, Handler,
};
use std::thread;
use std::{ffi::CString, time::Duration};
use tracing::error;

const MESSAGE_RETRY_COUNT: usize = 5;

pub fn new_publication(
    aeron: &Aeron,
    address: &str,
    port: u16,
    stream_id: i32,
) -> Result<AeronPublication, AeronCError> {
    let endpoint = format!("{address}:{port}");
    let uri =
        CString::new(format!("aeron:udp?endpoint={endpoint}")).expect("Creation of CString failed");
    aeron.add_publication(&uri, stream_id, Duration::from_secs(1))
}

pub fn new_publication_with_mdc(
    aeron: &Aeron,
    address: &str,
    port: u16,
    stream_id: i32,
) -> Result<AeronPublication, AeronCError> {
    let control_endpoint = format!("{address}:{port}");
    let uri = CString::new(format!(
        "aeron:udp?control={control_endpoint}|control-mode=dynamic"
    ))
    .expect("Creation of CString failed");
    aeron.add_publication(&uri, stream_id, Duration::from_secs(1))
}

pub fn new_publication_with_session(
    aeron: &Aeron,
    address: &str,
    port: u16,
    stream_id: i32,
    session_id: i32,
) -> Result<AeronPublication, AeronCError> {
    let endpoint = format!("{address}:{port}");
    let uri = CString::new(format!(
        "aeron:udp?endpoint={endpoint}|session-id={session_id}"
    ))
    .expect("Creation of CString failed");
    aeron.add_publication(&uri, stream_id, Duration::from_secs(1))
}

pub fn new_subscription_with_mdc(
    aeron: &Aeron,
    address: &str,
    port: u16,
    stream_id: i32,
) -> Result<AeronSubscription, AeronCError> {
    let control_endpoint = format!("{address}:{port}");
    let uri = CString::new(format!(
        "aeron:udp?control={control_endpoint}|control-mode=dynamic"
    ))
    .expect("Creation of CString failed");
    let available_logger = AeronAvailableImageLogger {};
    let available_handler = Handler::leak(available_logger);
    let unavailable_logger = AeronUnavailableImageLogger {};
    let unavailable_handler = Handler::leak(unavailable_logger);
    aeron.add_subscription(
        &uri,
        stream_id,
        Some(&available_handler),
        Some(&unavailable_handler),
        Duration::from_secs(1),
    )
}

pub fn new_subscription_with_handlers_and_session<
    X: AeronAvailableImageCallback,
    Y: AeronUnavailableImageCallback,
>(
    aeron: &Aeron,
    address: &str,
    port: u16,
    stream_id: i32,
    session_id: i32,
    on_image_available: Option<&Handler<X>>,
    on_image_unavailable: Option<&Handler<Y>>,
) -> Result<AeronSubscription, AeronCError> {
    let endpoint = format!("{address}:{port}");
    let uri = CString::new(format!(
        "aeron:udp?endpoint={endpoint}|session-id={session_id}"
    ))
    .expect("Creation of CString failed");
    aeron.add_subscription(
        &uri,
        stream_id,
        on_image_available,
        on_image_unavailable,
        Duration::from_secs(1),
    )
}

pub fn new_subscription_with_handlers<
    X: AeronAvailableImageCallback,
    Y: AeronUnavailableImageCallback,
>(
    aeron: &Aeron,
    address: &str,
    port: u16,
    stream_id: i32,
    on_image_available: Option<&Handler<X>>,
    on_image_unavailable: Option<&Handler<Y>>,
) -> Result<AeronSubscription, AeronCError> {
    let endpoint = format!("{address}:{port}");
    let uri =
        CString::new(format!("aeron:udp?endpoint={endpoint}")).expect("Creation of CString failed");
    aeron.add_subscription(
        &uri,
        stream_id,
        on_image_available,
        on_image_unavailable,
        Duration::from_secs(1),
    )
}

pub fn send_message(publication: &AeronPublication, buffer: &[u8]) -> Result<(), AeronCError> {
    let result = publication.offer::<AeronReservedValueSupplierLogger>(buffer, None);
    if result < 0 {
        return Err(AeronCError::from_code(result as i32));
    }
    Ok(())
}

/// Offers once, and once more at an `ADMIN_ACTION` (a log-buffer term rotation, which Aeron
/// documents as likely to succeed on the next attempt). Never sleeps.
pub fn offer_once(mut offer: impl FnMut() -> i64) -> i64 {
    let result = offer();
    if result == i64::from(AERON_PUBLICATION_ADMIN_ACTION) {
        offer()
    } else {
        result
    }
}

pub fn send_message_with_retries(
    publication: &AeronPublication,
    buffer: &[u8],
) -> Result<(), AeronCError> {
    for i in 0..MESSAGE_RETRY_COUNT {
        let result = publication.offer::<AeronReservedValueSupplierLogger>(buffer, None);
        if result >= 0 {
            return Ok(());
        }
        error!(
            "Failed to send message (attempt {} of {}): {}",
            i + 1,
            MESSAGE_RETRY_COUNT,
            AeronCError::from_code(result as i32)
        );
        if i == MESSAGE_RETRY_COUNT - 1 {
            return Err(AeronCError::from_code(result as i32));
        }
        thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

/// Port allocator that hands out the lowest free ports of a range.
/// Does not track allocated ports - the Session struct is the source of truth.
/// Tracks recently freed ports to avoid OS-level port binding race conditions.
#[derive(Debug)]
pub struct PortAllocator {
    low: u16,
    high: u16,
    /// Tracks ports that were recently freed with their timestamp
    /// Ports are kept in this set for PORT_COOLDOWN_SECONDS to allow OS to release them
    recently_freed_ports: std::sync::RwLock<std::collections::HashMap<u16, std::time::Instant>>,
}

/// Time to wait before reusing a freed port (in seconds)
/// This allows the OS to fully release the UDP port binding
pub(crate) const PORT_COOLDOWN_SECONDS: u64 = 10;

impl PortAllocator {
    /// Create a new port allocator.
    ///
    /// # Arguments
    /// * `port_base` - The base port (must be in range [1, 65535])
    /// * `max_ports` - The maximum number of ports that will be allocated
    ///
    /// # Returns
    /// A new port allocator
    ///
    /// # Errors
    /// Returns `ResourceAllocationError` if the port range is invalid
    pub fn new(port_base: u16, max_ports: usize) -> Result<Self, ServerError> {
        if port_base == 0 {
            return Err(ServerError::ResourceAllocationError(
                "Base port must be greater than 0".to_string(),
            ));
        }

        if max_ports == 0 {
            return Err(ServerError::ResourceAllocationError(
                "Max ports must be greater than 0".to_string(),
            ));
        }
        let hi_u32 = port_base as u32 + (max_ports as u32) - 1;
        if hi_u32 > u16::MAX as u32 {
            return Err(ServerError::ResourceAllocationError(
                "Port range exceeds u16::MAX".to_string(),
            ));
        }
        let port_hi = hi_u32 as u16;

        Ok(Self {
            low: port_base,
            high: port_hi,
            recently_freed_ports: std::sync::RwLock::new(std::collections::HashMap::new()),
        })
    }

    /// Allocate the `count` lowest ports that are neither in use nor recently freed.
    ///
    /// # Arguments
    /// * `count` - The number of ports that will be allocated
    /// * `ports_in_use` - Slice of ports currently in use (from Session)
    ///
    /// # Returns
    /// A vector of allocated ports
    ///
    /// # Errors
    /// Returns `ResourceAllocationError` if fewer than `count` ports are free
    pub fn allocate(&self, count: usize, ports_in_use: &[u16]) -> Result<Vec<u16>, ServerError> {
        self.cleanup_expired_ports();
        let recently_freed = self.recently_freed_ports.read().unwrap();
        let ports: Vec<u16> = (self.low..=self.high)
            .filter(|port| !ports_in_use.contains(port) && !recently_freed.contains_key(port))
            .take(count)
            .collect();
        if ports.len() < count {
            return Err(ServerError::ResourceAllocationError(format!(
                "Requested {count} ports, {} free ({} in use, {} in cooldown)",
                ports.len(),
                ports_in_use.len(),
                recently_freed.len()
            )));
        }
        Ok(ports)
    }

    /// Mark ports as recently freed. They will be unavailable for allocation for PORT_COOLDOWN_SECONDS.
    ///
    /// # Arguments
    /// * `ports` - Slice of ports that were just freed
    pub fn mark_freed(&self, ports: &[u16]) {
        let now = std::time::Instant::now();
        let mut freed = self.recently_freed_ports.write().unwrap();
        for &port in ports {
            freed.insert(port, now);
        }
    }

    /// Remove expired entries from recently_freed_ports
    fn cleanup_expired_ports(&self) {
        let now = std::time::Instant::now();
        let cooldown = std::time::Duration::from_secs(PORT_COOLDOWN_SECONDS);
        let mut freed = self.recently_freed_ports.write().unwrap();
        freed.retain(|_, &mut timestamp| now.duration_since(timestamp) < cooldown);
    }
}

/// An allocator for session IDs. The allocator randomly selects values from
/// the given range `[min, max)`.
/// Does not track allocated sessions - the Session struct is the source of truth.
#[derive(Debug, Clone)]
pub struct SessionAllocator {
    min: i32,
    max_count: i32,
}

impl SessionAllocator {
    /// Create a new session allocator.
    ///
    /// # Arguments
    /// * `min` - The minimum session ID (inclusive)
    /// * `max` - The maximum session ID (exclusive)
    ///
    /// # Returns
    /// A new allocator
    ///
    /// # Errors
    /// Returns `ResourceAllocationError` if max <= min or the range is too large
    pub fn new(min: i32, max: i32) -> Result<Self, ServerError> {
        if max <= min {
            return Err(ServerError::ResourceAllocationError(format!(
                "Maximum value {max} must be greater than minimum value {min}"
            )));
        }

        let max_count = max.checked_sub(min).ok_or_else(|| {
            ServerError::ResourceAllocationError(format!("Session range {min}..{max} is too large"))
        })?;

        Ok(Self { min, max_count })
    }

    /// Allocate a new session, avoiding sessions already in use.
    ///
    /// # Arguments
    /// * `sessions_in_use` - Slice of session IDs currently in use (from Session)
    ///
    /// # Returns
    /// A new session ID
    ///
    /// # Errors
    /// Returns `ResourceAllocationError` if there are no non-allocated sessions left
    pub fn allocate(&self, sessions_in_use: &[i32]) -> Result<i32, ServerError> {
        if sessions_in_use.len() as i32 >= self.max_count {
            return Err(ServerError::ResourceAllocationError(
                "No session IDs left to allocate".to_string(),
            ));
        }

        // Try up to max_count times to find an unused session ID
        let mut rng = thread_rng();
        for _ in 0..self.max_count {
            let session = rng.gen_range(self.min..self.min + self.max_count);
            if !sessions_in_use.contains(&session) {
                return Ok(session);
            }
        }

        Err(ServerError::ResourceAllocationError(format!(
            "Unable to allocate a session ID after {} attempts ({} values in use)",
            self.max_count,
            sessions_in_use.len()
        )))
    }

    /// Get the maximum number of sessions that can be allocated
    pub fn max_sessions(&self) -> i32 {
        self.max_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offer_once_reoffers_only_an_admin_action() {
        use rusteron_archive::bindings::AERON_PUBLICATION_BACK_PRESSURED;
        let scripted = |results: &[i32]| {
            let mut offers = 0;
            let result = offer_once(|| {
                offers += 1;
                i64::from(results[offers - 1])
            });
            (result, offers)
        };
        let admin = AERON_PUBLICATION_ADMIN_ACTION;
        let back_pressured = AERON_PUBLICATION_BACK_PRESSURED;

        assert_eq!(scripted(&[admin, 0]), (0, 2));
        assert_eq!(scripted(&[back_pressured]), (i64::from(back_pressured), 1));
        assert_eq!(scripted(&[admin, admin]), (i64::from(admin), 2));
    }

    #[test]
    fn allocator_hands_out_the_last_free_ports() {
        let allocator = PortAllocator::new(10_000, 64).unwrap();
        let in_use: Vec<u16> = (10_000..10_064).filter(|port| port % 31 != 0).collect();
        assert_eq!(in_use.len(), 62);

        for _ in 0..1_000 {
            assert_eq!(
                allocator.allocate(2, &in_use).unwrap(),
                vec![10_013, 10_044]
            );
        }
        allocator.mark_freed(&[10_013]);
        assert!(allocator.allocate(2, &in_use).is_err());
    }

    #[test]
    fn session_allocator_rejects_inverted_range() {
        assert!(SessionAllocator::new(10, 9).is_err());
    }

    #[test]
    fn session_allocator_rejects_range_that_exceeds_i32_capacity() {
        assert!(SessionAllocator::new(i32::MIN, i32::MAX).is_err());
    }
}
