use std::{ffi::CString, time::Duration};
use dashmap::DashSet;
use rand::{seq::SliceRandom, thread_rng};
use rusteron_client::{Aeron, AeronPublication, AeronSubscription, AeronCError, AeronAvailableImageLogger, AeronUnavailableImageLogger, Handler, AeronAvailableImageCallback, AeronUnavailableImageCallback, AeronReservedValueSupplierLogger};
use rand::Rng;
use tracing::info;

use crate::server::ServerError;

pub fn new_publication(aeron: &Aeron, address: &str, port: u16, stream_id: i32) -> Result<AeronPublication, AeronCError> {
    let endpoint = format!("{address}:{port}");
    let uri = CString::new(format!("aeron:udp?endpoint={endpoint}")).unwrap();
    aeron.add_publication(&uri, stream_id, Duration::from_secs(1))
}

pub fn new_publication_with_mdc_and_session(aeron: &Aeron, address: &str, port: u16, stream_id: i32, session_id: i32) -> Result<AeronPublication, AeronCError> {
    let control_endpoint = format!("{address}:{port}");
    let uri = CString::new(format!("aeron:udp?control={control_endpoint}|control-mode=dynamic|session-id={session_id}")).unwrap();
    aeron.add_publication(&uri, stream_id, Duration::from_secs(1))
}

pub fn new_publication_with_mdc(aeron: &Aeron, address: &str, port: u16, stream_id: i32) -> Result<AeronPublication, AeronCError> {
    info!("server: new_publication_with_mdc: address: {}, port: {}, stream_id: {}", address, port, stream_id);
    let control_endpoint = format!("{address}:{port}");
    let uri = CString::new(format!("aeron:udp?control={control_endpoint}|control-mode=dynamic")).unwrap();
    info!("server: new_publication_with_mdc: uri: {}", uri.to_string_lossy());
    aeron.add_publication(&uri, stream_id, Duration::from_secs(1))
}

pub fn new_publication_with_session(aeron: &Aeron, address: &str, port: u16, stream_id: i32, session_id: i32) -> Result<AeronPublication, AeronCError> {
    let endpoint = format!("{address}:{port}");
    let uri = CString::new(format!("aeron:udp?endpoint={endpoint}|session-id={session_id}")).unwrap();
    aeron.add_publication(&uri, stream_id, Duration::from_secs(1))
}

pub fn new_subscription_with_mdc(aeron: &Aeron, address: &str, port: u16, stream_id: i32) -> Result<AeronSubscription, AeronCError> {
    let control_endpoint = format!("{address}:{port}");
    info!("client: new_subsciption_with_mdc: control_endpoint: {}", control_endpoint);
    let uri = CString::new(format!("aeron:udp?control={control_endpoint}|control-mode=dynamic")).unwrap();
    info!("client: new_subsciption_with_mdc: uri: {}", uri.to_string_lossy());
    let available_logger = AeronAvailableImageLogger {};
    let available_handler = Handler::leak(available_logger);
    let unavailable_logger = AeronUnavailableImageLogger {};
    let unavailable_handler = Handler::leak(unavailable_logger);
    aeron.add_subscription(&uri, stream_id, Some(&available_handler), Some(&unavailable_handler), Duration::from_secs(1))
}

pub fn new_subscription_with_mdc_and_session(aeron: &Aeron, address: &str, port: u16, stream_id: i32, session_id: i32) -> Result<AeronSubscription, AeronCError> {
    let control_endpoint = format!("{address}:{port}");
    let uri = CString::new(format!("aeron:udp?control={control_endpoint}|control-mode=dynamic|session-id={session_id}")).unwrap();
    let available_logger = AeronAvailableImageLogger {};
    let available_handler = Handler::leak(available_logger);
    let unavailable_logger = AeronUnavailableImageLogger {};
    let unavailable_handler = Handler::leak(unavailable_logger);
    aeron.add_subscription(&uri, stream_id, Some(&available_handler), Some(&unavailable_handler), Duration::from_secs(1))
}

pub fn new_subsciption_with_handlers_and_session<X: AeronAvailableImageCallback, Y: AeronUnavailableImageCallback>(aeron: &Aeron, address: &str, port: u16, stream_id: i32, session_id: i32, on_image_available: X, on_image_unavailable: Y) -> Result<AeronSubscription, AeronCError> {
    let endpoint = format!("{address}:{port}");
    let uri = CString::new(format!("aeron:udp?endpoint={endpoint}|session-id={session_id}")).unwrap();
    aeron.add_subscription(&uri, stream_id, Some(& Handler::leak(on_image_available)), Some(& Handler::leak(on_image_unavailable)), Duration::from_secs(1))
}

pub fn new_subscription_with_handlers<X: AeronAvailableImageCallback, Y: AeronUnavailableImageCallback>(aeron: &Aeron, address: &str, port: u16, stream_id: i32, on_image_available: X, on_image_unavailable: Y) -> Result<AeronSubscription, AeronCError> {
    let endpoint = format!("{address}:{port}");
    let uri = CString::new(format!("aeron:udp?endpoint={endpoint}")).unwrap();
    aeron.add_subscription(&uri, stream_id, Some(& Handler::leak(on_image_available)), Some(& Handler::leak(on_image_unavailable)), Duration::from_secs(1))
}

pub fn send_message(publication: &AeronPublication, buffer: &mut [u8], message: &str) -> Result<(), AeronCError> {
    let message_bytes = message.as_bytes();
    buffer[0..message_bytes.len()].copy_from_slice(message_bytes);
    let result = publication.offer::<AeronReservedValueSupplierLogger>(buffer, None);
    if result < 0 {
        return Err(AeronCError::from_code(result as i32));
    }

    Ok(())
}

#[derive(Debug)]
pub struct PortAllocator {
    port_range: std::ops::RangeInclusive<u16>,
    ports_used: dashmap::DashSet<u16>,
    low: u16,
    high: u16
}

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
            return Err(ServerError::ResourceAllocationError("Base port must be greater than 0".to_string()));
        }

        let port_hi = port_base.checked_add(max_ports as u16 - 1)
            .ok_or_else(|| ServerError::ResourceAllocationError("Port range exceeds u16::MAX".to_string()))?;
        
        let port_range = port_base..=port_hi;
        let mut ports_free: Vec<u16> = port_range.clone().collect();
        
        // Shuffle the ports for random allocation
        let mut rng = rand::thread_rng();
        ports_free.shuffle(&mut rng);

        Ok(Self {
            port_range,
            ports_used: dashmap::DashSet::new(),
            low: port_base,
            high: port_hi,
        })
    }

    /// Get the total number of ports in the range
    pub fn total_ports(&self) -> usize {
        self.port_range.clone().count()
    }

    // /// Get the number of available ports
    // pub fn available_ports(&self) -> usize {
    //     self.ports_free.len()
    // }

    // /// Get the number of used ports
    // pub fn used_ports(&self) -> usize {
    //     self.ports_used.len()
    // }

    /// Free a given port. Has no effect if the given port is outside of the range
    /// considered by the allocator.
    ///
    /// # Arguments
    /// * `port` - The port to free
    pub fn free(&self, port: u16) {
        if self.port_range.contains(&port) {
            self.ports_used.remove(&port);
        }
    }

    /// Allocate `count` ports.
    ///
    /// # Arguments
    /// * `count` - The number of ports that will be allocated
    ///
    /// # Returns
    /// A vector of allocated ports
    ///
    /// # Errors
    /// Returns `ResourceAllocationError` if there are fewer than `count` ports available to allocate
    pub fn allocate(&self, mut count: usize) -> Result<Vec<u16>, ServerError> {
        let mut result = Vec::with_capacity(count);
        let mut rng = rand::thread_rng();
        while count != 0 {
            let port = rng.gen_range(self.low..=self.high);
            if !self.ports_used.contains(&port) {
                result.push(port);
                self.ports_used.insert(port);
                count -= 1;
            }
        }

        Ok(result)
    }
}

/// An allocator for session IDs. The allocator randomly selects values from
/// the given range `[min, max)` and will not return a previously-returned value `x`
/// until `x` has been freed with `free()`.
///
/// This implementation uses storage proportional to the number of currently-allocated
/// values. Allocation time is bounded by `max - min`, will be `O(1)` with no allocated
/// values, and will increase to `O(n)` as the number of allocated values approaches `max - min`.
#[derive(Debug)]
pub struct SessionAllocator {
    used: DashSet<i32>,
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
        if max < min {
            return Err(ServerError::ResourceAllocationError(format!("Maximum value {max} must be >= minimum value {min}")));
        }

        Ok(Self {
            used: DashSet::new(),
            min,
            max_count: std::cmp::max(max - min, 1),
        })
    }

    /// Allocate a new session.
    ///
    /// # Returns
    /// A new session ID
    ///
    /// # Errors
    /// Returns `ResourceAllocationError` if there are no non-allocated sessions left
    pub fn allocate(&self) -> Result<i32, ServerError> {
        if self.used.len() as i32 == self.max_count {
            return Err(ServerError::ResourceAllocationError("No session IDs left to allocate".to_string()));
        }

        // Try up to max_count times to find an unused session ID
        let mut rng = thread_rng();
        for _ in 0..self.max_count {
            let session = rng.gen_range(self.min..self.min + self.max_count);
            if !self.used.contains(&session) {
                self.used.insert(session);
                return Ok(session);
            }
        }

        Err(ServerError::ResourceAllocationError(
            format!(
                "Unable to allocate a session ID after {} attempts ({} values in use)",
                self.max_count,
                self.used.len()
            )
        ))
    }

    /// Free a session. After this method returns, `session` becomes eligible
    /// for allocation by future calls to `allocate()`.
    ///
    /// # Arguments
    /// * `session` - The session to free
    pub fn free(&self, session: i32) {
        self.used.remove(&session);
    }

    /// Get the number of currently allocated sessions
    pub fn allocated_count(&self) -> usize {
        self.used.len()
    }

    /// Get the maximum number of sessions that can be allocated
    pub fn max_sessions(&self) -> i32 {
        self.max_count
    }

    /// Check if a session is currently allocated
    pub fn is_allocated(&self, session: i32) -> bool {
        self.used.contains(&session)
    }
}
