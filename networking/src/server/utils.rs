use std::{ffi::CString, ptr, time::Duration, collections::HashSet};
use rand::seq::SliceRandom;
use rand::Rng;

use rusteron_client::{bindings::{aeron_uri_string_builder_close, aeron_uri_string_builder_init_on_string, aeron_uri_string_builder_put, aeron_uri_string_builder_t}, Aeron, AeronAvailableImageCallback, AeronCError, AeronPublication, AeronReservedValueSupplierLogger, AeronSubscription, AeronUnavailableImageCallback, Handler};

use crate::server::server::EchoServerError;

pub fn build_channel_uri(
    addr_string: &str,
    port: u16,
    session: Option<i32>,
) -> Result<CString, AeronCError> {
    let builder: *mut aeron_uri_string_builder_t = ptr::null_mut();
    
    // Initialize the builder with an empty URI string
    let uri = std::ffi::CString::new("").unwrap();
    let result = unsafe { aeron_uri_string_builder_init_on_string(builder, uri.as_ptr()) };
    if result < 0 {
        return Err(AeronCError::from_code(result));
    }
    
    // Set reliable to true
    let reliable = "true";
    let result = unsafe { 
        aeron_uri_string_builder_put(
            builder, 
            "reliable\0".as_ptr() as *const i8, 
            reliable.as_ptr() as *const i8
        ) 
    };
    if result < 0 {
        unsafe { aeron_uri_string_builder_close(builder); }
        return Err(AeronCError::from_code(result));
    }
    
    // Set media to udp
    let media = "udp";
    let result = unsafe { 
        aeron_uri_string_builder_put(
            builder, 
            "media\0".as_ptr() as *const i8, 
            media.as_ptr() as *const i8
        ) 
    };
    if result < 0 {
        unsafe { aeron_uri_string_builder_close(builder); }
        return Err(AeronCError::from_code(result));
    }
    
    // Set control endpoint
    let control_endpoint = format!("{}:{}", addr_string, port);
    let result = unsafe { 
        aeron_uri_string_builder_put(
            builder, 
            "control\0".as_ptr() as *const i8, 
            control_endpoint.as_ptr() as *const i8
        ) 
    };
    if result < 0 {
        unsafe { aeron_uri_string_builder_close(builder); }
        return Err(AeronCError::from_code(result));
    }
    
    // Set control mode to dynamic
    let control_mode = "dynamic";
    let result = unsafe { 
        aeron_uri_string_builder_put(
            builder, 
            "control-mode\0".as_ptr() as *const i8, 
            control_mode.as_ptr() as *const i8
        ) 
    };
    if result < 0 {
        unsafe { aeron_uri_string_builder_close(builder); }
        return Err(AeronCError::from_code(result));
    }
    
    // Set session ID if provided
    if let Some(session_id) = session {
        let session_str = session_id.to_string();
        let result = unsafe { 
            aeron_uri_string_builder_put(
                builder, 
                "session-id\0".as_ptr() as *const i8, 
                session_str.as_ptr() as *const i8
            ) 
        };
        if result < 0 {
            unsafe { aeron_uri_string_builder_close(builder); }
            return Err(AeronCError::from_code(result));
        }
    }
    
    unsafe { aeron_uri_string_builder_close(builder); }
    
    if result < 0 {
        return Err(AeronCError::from_code(result));
    }
    
    Ok(uri)
}

pub fn new_publication_with_mdc_and_session(aeron: &Aeron, address: &str, port: u16, stream_id: i32, session_id: i32) -> Result<AeronPublication, AeronCError> {
    let uri = build_channel_uri(address, port, Some(session_id))?;
    aeron.add_publication(&uri, stream_id, Duration::from_secs(1))
}

pub fn new_publication_with_mdc(aeron: &Aeron, address: &str, port: u16, stream_id: i32) -> Result<AeronPublication, AeronCError> {
    let uri = build_channel_uri(address, port, None)?;
    aeron.add_publication(&uri, stream_id, Duration::from_secs(1))
}

pub fn new_subsciption_with_handlers_and_session<X: AeronAvailableImageCallback, Y: AeronUnavailableImageCallback>(aeron: &Aeron, address: &str, port: u16, stream_id: i32, session_id: i32, on_image_available: X, on_image_unavailable: Y) -> Result<AeronSubscription, AeronCError> {
    let uri = build_channel_uri(address, port, Some(session_id))?;
    aeron.add_subscription(&uri, stream_id, Some(& Handler::leak(on_image_available)), Some(& Handler::leak(on_image_unavailable)), Duration::from_secs(1))
}

pub fn new_subsciption_with_handlers<X: AeronAvailableImageCallback, Y: AeronUnavailableImageCallback>(aeron: &Aeron, address: &str, port: u16, stream_id: i32, on_image_available: X, on_image_unavailable: Y) -> Result<AeronSubscription, AeronCError> {
    let uri = build_channel_uri(address, port, None)?;
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
    ports_used: HashSet<u16>,
    ports_free: Vec<u16>,
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
    /// Returns `PortAllocationError` if the port range is invalid
    pub fn new(port_base: u16, max_ports: usize) -> Result<Self, EchoServerError> {
        if port_base == 0 {
            return Err(EchoServerError::PortAllocationError("Base port must be greater than 0".to_string()));
        }

        let port_hi = port_base.checked_add(max_ports as u16 - 1)
            .ok_or_else(|| EchoServerError::PortAllocationError("Port range exceeds u16::MAX".to_string()))?;
        
        let port_range = port_base..=port_hi;
        let mut ports_free: Vec<u16> = port_range.clone().collect();
        
        // Shuffle the ports for random allocation
        let mut rng = rand::thread_rng();
        ports_free.shuffle(&mut rng);

        Ok(Self {
            port_range,
            ports_used: HashSet::new(),
            ports_free,
        })
    }

    /// Get the total number of ports in the range
    pub fn total_ports(&self) -> usize {
        self.port_range.clone().count()
    }

    /// Get the number of available ports
    pub fn available_ports(&self) -> usize {
        self.ports_free.len()
    }

    /// Get the number of used ports
    pub fn used_ports(&self) -> usize {
        self.ports_used.len()
    }

    /// Free a given port. Has no effect if the given port is outside of the range
    /// considered by the allocator.
    ///
    /// # Arguments
    /// * `port` - The port to free
    pub fn free(&mut self, port: u16) {
        if self.port_range.contains(&port) {
            self.ports_used.remove(&port);
            self.ports_free.push(port);
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
    /// Returns `PortAllocationError` if there are fewer than `count` ports available to allocate
    pub fn allocate(&mut self, count: usize) -> Result<Vec<u16>, EchoServerError> {
        if self.ports_free.len() < count {
            return Err(EchoServerError::PortAllocationError(format!("Too few ports available to allocate {} ports", count)));
        }

        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(port) = self.ports_free.pop() {
                self.ports_used.insert(port);
                result.push(port);
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
    used: HashSet<i32>,
    random: rand::rngs::ThreadRng,
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
    /// Returns `PortAllocationError` if max < min
    pub fn new(min: i32, max: i32) -> Result<Self, EchoServerError> {
        if max < min {
            return Err(EchoServerError::PortAllocationError(format!("Maximum value {} must be >= minimum value {}", max, min)));
        }

        Ok(Self {
            used: HashSet::new(),
            random: rand::thread_rng(),
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
    /// Returns `PortAllocationError` if there are no non-allocated sessions left
    pub fn allocate(&mut self) -> Result<i32, EchoServerError> {
        if self.used.len() as i32 == self.max_count {
            return Err(EchoServerError::SessionAllocationError("No session IDs left to allocate".to_string()));
        }

        // Try up to max_count times to find an unused session ID
        for _ in 0..self.max_count {
            let session = self.random.gen_range(self.min..self.min + self.max_count);
            if !self.used.contains(&session) {
                self.used.insert(session);
                return Ok(session);
            }
        }

        Err(EchoServerError::SessionAllocationError(
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
    pub fn free(&mut self, session: i32) {
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


