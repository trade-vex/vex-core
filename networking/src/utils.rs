use crate::server::ServerError;
use rand::Rng;
use rand::thread_rng;
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

pub fn new_publication_with_mdc_and_session(
    aeron: &Aeron,
    address: &str,
    port: u16,
    stream_id: i32,
    session_id: i32,
) -> Result<AeronPublication, AeronCError> {
    let control_endpoint = format!("{address}:{port}");
    let uri = CString::new(format!(
        "aeron:udp?control={control_endpoint}|control-mode=dynamic|session-id={session_id}"
    ))
    .expect("Creation of CString failed");
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

pub fn new_subscription_with_mdc_and_session(
    aeron: &Aeron,
    address: &str,
    port: u16,
    stream_id: i32,
    session_id: i32,
) -> Result<AeronSubscription, AeronCError> {
    let control_endpoint = format!("{address}:{port}");
    let uri = CString::new(format!(
        "aeron:udp?control={control_endpoint}|control-mode=dynamic|session-id={session_id}"
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

/// Port allocator that randomly selects ports from a given range.
/// Does not track allocated ports - the Session struct is the source of truth.
/// Tracks recently freed ports to avoid OS-level port binding race conditions.
#[derive(Debug)]
pub struct PortAllocator {
    port_range: std::ops::RangeInclusive<u16>,
    low: u16,
    high: u16,
    /// Tracks ports that were recently freed with their timestamp
    /// Ports are kept in this set for PORT_COOLDOWN_SECONDS to allow OS to release them
    recently_freed_ports: std::sync::RwLock<std::collections::HashMap<u16, std::time::Instant>>,
}

/// Time to wait before reusing a freed port (in seconds)
/// This allows the OS to fully release the UDP port binding
const PORT_COOLDOWN_SECONDS: u64 = 10;

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
        let port_range = port_base..=port_hi;

        Ok(Self {
            port_range,
            low: port_base,
            high: port_hi,
            recently_freed_ports: std::sync::RwLock::new(std::collections::HashMap::new()),
        })
    }

    /// Get the total number of ports in the range
    pub fn total_ports(&self) -> usize {
        self.port_range.clone().count()
    }

    /// Allocate `count` ports randomly, avoiding the ports already in use and recently freed ports.
    ///
    /// # Arguments
    /// * `count` - The number of ports that will be allocated
    /// * `ports_in_use` - Slice of ports currently in use (from Session)
    ///
    /// # Returns
    /// A vector of allocated ports
    ///
    /// # Errors
    /// Returns `ResourceAllocationError` if there are fewer than `count` ports available to allocate
    /// or if unable to find free ports after reasonable attempts
    pub fn allocate(&self, count: usize, ports_in_use: &[u16]) -> Result<Vec<u16>, ServerError> {
        if count == 0 {
            return Ok(Vec::new());
        }

        // Clean up expired entries from recently_freed_ports
        self.cleanup_expired_ports();

        // Get list of ports that are still in cooldown
        let recently_freed: Vec<u16> = {
            let freed = self.recently_freed_ports.read().unwrap();
            freed.keys().copied().collect()
        };

        let total = self.total_ports();
        let used = ports_in_use.len();
        let in_cooldown = recently_freed.len();

        // Account for ports in cooldown when checking availability
        if count > total.saturating_sub(used).saturating_sub(in_cooldown) {
            return Err(ServerError::ResourceAllocationError(format!(
                "Requested {count} ports, but only {} available ({} in use, {} in cooldown)",
                total.saturating_sub(used).saturating_sub(in_cooldown),
                used,
                in_cooldown
            )));
        }

        let mut result = Vec::with_capacity(count);
        let mut rng = rand::thread_rng();
        let max_attempts = (total * 2).max(100); // Try at most 2x the total ports or 100 attempts
        let mut attempts = 0;

        while result.len() < count {
            if attempts >= max_attempts {
                return Err(ServerError::ResourceAllocationError(format!(
                    "Failed to allocate {} ports after {} attempts ({} already allocated, {} in cooldown)",
                    count,
                    max_attempts,
                    result.len(),
                    in_cooldown
                )));
            }

            let port = rng.gen_range(self.low..=self.high);

            // Check if port is not in use, not in cooldown, and not already in result
            if !ports_in_use.contains(&port)
                && !recently_freed.contains(&port)
                && !result.contains(&port)
            {
                result.push(port);
            }

            attempts += 1;
        }

        Ok(result)
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
    /// Returns `ResourceAllocationError` if max < min
    pub fn new(min: i32, max: i32) -> Result<Self, ServerError> {
        if max <= min {
            return Err(ServerError::ResourceAllocationError(format!(
                "Maximum value {max} must be >= minimum value {min}"
            )));
        }

        Ok(Self {
            min,
            max_count: std::cmp::max(max - min, 1),
        })
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
