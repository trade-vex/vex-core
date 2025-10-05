use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::error::Error;
use std::fmt;

const NODE_BITS: u8 = 4;
const STEP_BITS: u8 = 12;
const TIMESTAMP_BITS: u8 = 48;

const NODE_MAX: u8 = (1 << NODE_BITS) - 1;
const STEP_MAX: u16 = (1 << STEP_BITS) - 1;

const TIMESTAMP_SHIFT: u8 = NODE_BITS + STEP_BITS;
const NODE_SHIFT: u8 = STEP_BITS;

/// Default epoch (2025-01-01T00:00:00Z in milliseconds since Unix epoch)
const DEFAULT_EPOCH: u64 = 1735689600000;

/// Errors that can occur during Snowflake ID generation
#[derive(Debug)]
pub enum SnowflakeError {
    MachineIdOutOfRange,
    SequenceOverflow,
    EpochInTheFuture,
}

impl fmt::Display for SnowflakeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SnowflakeError::MachineIdOutOfRange => write!(f, "Machine ID is out of range (must be 0-{})", NODE_MAX),
            SnowflakeError::SequenceOverflow => write!(f, "Sequence overflow: cannot generate a unique ID"),
            SnowflakeError::EpochInTheFuture => write!(f, "Epoch is set in the future"),
        }
    }
}

impl Error for SnowflakeError {}

/// Snowflake ID generator
///
/// This struct implements the Snowflake algorithm for generating unique monotonically increasing IDs.
/// Each ID is composed of:
/// - Timestamp (48 bits)
/// - Node ID (4 bits)
/// - Sequence number (12 bits)
pub struct Snowflake {
    start: Instant,
    last_timestamp: u64,
    sequence: u16,
    epoch_offset: u64,
}

impl Snowflake {
    /// Creates a new Snowflake ID generator with the specified epoch.
    /// epoch: The custom epoch in milliseconds since Unix epoch.
    /// If no epoch is provided, it defaults to 2025-01-01T00:00:00Z.
    pub fn new(epoch: Option<u64>) -> Result<Self, SnowflakeError> {
        let current_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("SystemTime before UNIX EPOCH!")
            .as_millis() as u64;
        let epoch_ms = epoch.unwrap_or(DEFAULT_EPOCH);

        if epoch_ms > current_unix_ms {
            return Err(SnowflakeError::EpochInTheFuture);
        }

        let epoch_offset = current_unix_ms - epoch_ms;
        Ok(Snowflake {
            epoch_offset,
            start: Instant::now(),
            last_timestamp: 0,
            sequence: 0,
        })
    }

    pub fn generate(&mut self, gateway: u8) -> Result<u64, SnowflakeError> {
        if gateway > NODE_MAX {
            return Err(SnowflakeError::MachineIdOutOfRange);
        }

        let current_timestamp = self.current_time_millis();

        if current_timestamp == self.last_timestamp {
            self.sequence += 1;

            if self.sequence > STEP_MAX {
                let next_timestamp = self.wait_next_millis(current_timestamp)?;
                self.last_timestamp = next_timestamp;
                self.sequence = 0;
            }
        } else {
            self.last_timestamp = current_timestamp;
            self.sequence = 0;
        }

        // Create the ID from the final state.
        Ok(self.create_id(self.last_timestamp, self.sequence, gateway))
    }
    
    /// Parses a Snowflake ID into its components.
    pub fn parse_id(id: u64) -> (u64, u8, u16) {
        let timestamp = (id >> TIMESTAMP_SHIFT) & ((1 << TIMESTAMP_BITS) - 1);
        let node = ((id >> NODE_SHIFT) & (NODE_MAX as u64)) as u8;
        let sequence = (id & (STEP_MAX as u64)) as u16;
        (timestamp, node, sequence)
    }

    /// Spins until the clock ticks to the next millisecond.
    fn wait_next_millis(&self, last_timestamp: u64) -> Result<u64, SnowflakeError> {
        let start = Instant::now();
        loop {
            let current_timestamp = self.current_time_millis();
            if current_timestamp > last_timestamp {
                return Ok(current_timestamp);
            }
            if start.elapsed().as_millis() > 5000 {
                return Err(SnowflakeError::SequenceOverflow);
            }
            std::thread::yield_now();
        }
    }

    /// Creates the final ID by combining timestamp, gateway ID, and sequence.
    fn create_id(&self, timestamp: u64, sequence: u16, gateway: u8) -> u64 {
        (timestamp << TIMESTAMP_SHIFT)
            | ((gateway as u64) << NODE_SHIFT)
            | (sequence as u64)
    }

    /// Returns the milliseconds elapsed since the Snowflake generator was created.
    fn current_time_millis(&self) -> u64 {
        self.epoch_offset + self.start.elapsed().as_millis() as u64
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_with_default_epoch() {
        let snowflake = Snowflake::new(None);
        assert!(snowflake.is_ok());
    }

    #[test]
    fn test_new_with_custom_epoch() {
        // Use an epoch from 10 years ago
        let ten_years_ago = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64 - (10 * 365 * 24 * 60 * 60 * 1000);
        
        let snowflake = Snowflake::new(Some(ten_years_ago));
        assert!(snowflake.is_ok());
    }

    #[test]
    fn test_epoch_in_future() {
        let future_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64 + 1000000;
        
        let result = Snowflake::new(Some(future_epoch));
        assert!(matches!(result, Err(SnowflakeError::EpochInTheFuture)));
    }

    #[test]
    fn test_machine_id_out_of_range() {
        let mut snowflake = Snowflake::new(None).unwrap();
        let result = snowflake.generate(NODE_MAX + 1);
        assert!(matches!(result, Err(SnowflakeError::MachineIdOutOfRange)));
    }

    #[test]
    fn test_basic_id_generation() {
        let mut snowflake = Snowflake::new(None).unwrap();
        let id = snowflake.generate(5).unwrap();
        assert!(id > 0);
    }

    #[test]
    fn test_monotonic_increase() {
        let mut snowflake = Snowflake::new(None).unwrap();
        let mut prev_id = 0u64;
        
        for _ in 0..100 {
            let id = snowflake.generate(3).unwrap();
            assert!(id > prev_id, "ID should be monotonically increasing");
            prev_id = id;
        }
    }

    #[test]
    fn test_uniqueness() {
        let mut snowflake = Snowflake::new(None).unwrap();
        let mut ids = std::collections::HashSet::new();
        
        for _ in 0..1000 {
            let id = snowflake.generate(7).unwrap();
            assert!(ids.insert(id), "Generated duplicate ID: {}", id);
        }
    }

    #[test]
    fn test_parse_id() {
        let mut snowflake = Snowflake::new(None).unwrap();
        let gateway = 5u8;
        let id = snowflake.generate(gateway).unwrap();
        
        let (timestamp, node, sequence) = Snowflake::parse_id(id);
        
        assert_eq!(node, gateway);
        assert!(timestamp > 0);
        assert_eq!(sequence, 0); // First ID should have sequence 0
    }

    #[test]
    fn test_sequence_increment() {
        let mut snowflake = Snowflake::new(None).unwrap();
        let gateway = 3u8;
        
        // Generate multiple IDs rapidly to ensure same timestamp
        let ids: Vec<u64> = (0..10)
            .map(|_| snowflake.generate(gateway).unwrap())
            .collect();
        
        // Parse and check sequences
        for (i, &id) in ids.iter().enumerate() {
            let (_, node, sequence) = Snowflake::parse_id(id);
            assert_eq!(node, gateway);
            // Sequence should increment (though may reset if millisecond changes)
            if i > 0 {
                let (prev_ts, _, prev_seq) = Snowflake::parse_id(ids[i - 1]);
                let (curr_ts, _, _) = Snowflake::parse_id(id);
                
                if prev_ts == curr_ts {
                    assert_eq!(sequence, prev_seq + 1, "Sequence should increment within same millisecond");
                }
            }
        }
    }

    #[test]
    fn test_different_gateways() {
        let mut snowflake = Snowflake::new(None).unwrap();
        
        for gateway in 0..=NODE_MAX {
            let id = snowflake.generate(gateway).unwrap();
            let (_, node, _) = Snowflake::parse_id(id);
            assert_eq!(node, gateway);
        }
    }

    #[test]
    fn test_timestamp_component() {
        let mut snowflake = Snowflake::new(None).unwrap();
        
        let id1 = snowflake.generate(0).unwrap();
        let (ts1, _, _) = Snowflake::parse_id(id1);
        
        // Sleep for a bit to ensure timestamp advances
        std::thread::sleep(std::time::Duration::from_millis(10));
        
        let id2 = snowflake.generate(0).unwrap();
        let (ts2, _, _) = Snowflake::parse_id(id2);
        
        assert!(ts2 >= ts1, "Timestamp should not go backwards");
    }

    #[test]
    fn test_sequence_overflow_handling() {
        let mut snowflake = Snowflake::new(None).unwrap();
        
        // Try to generate more IDs than STEP_MAX in rapid succession
        // This should trigger overflow handling
        let mut ids = Vec::new();
        for _ in 0..5000 {
            match snowflake.generate(1) {
                Ok(id) => ids.push(id),
                Err(_) => break, // Might timeout in rare cases
            }
        }
        
        // All generated IDs should be unique
        let unique_ids: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique_ids.len(), ids.len(), "All IDs should be unique");
    }

    #[test]
    fn test_bit_layout() {
        let mut snowflake = Snowflake::new(None).unwrap();
        let gateway = 0b1111u8; // All bits set (15)
        
        // Generate ID and verify bit layout
        let id = snowflake.generate(gateway).unwrap();
        let (timestamp, node, sequence) = Snowflake::parse_id(id);
        
        // Verify node bits are correct
        assert_eq!(node, gateway);
        
        // Verify timestamp doesn't overflow into node bits
        assert!(timestamp < (1u64 << TIMESTAMP_BITS));
        
        // Verify sequence doesn't overflow into node bits
        assert!(sequence <= STEP_MAX);
    }
}
