use std::error::Error;
use std::fmt;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const NODE_BITS: u8 = 4;
const STEP_BITS: u8 = 12;
const TIMESTAMP_BITS: u8 = 48;

// Right-shift time by 10 bits.
// 1 Tick = 1024 nanoseconds (~1 microsecond).
const TIME_GRANULARITY_SHIFT: u8 = 10;

const NODE_MAX: u64 = (1 << NODE_BITS) - 1;
const STEP_MAX: u16 = (1 << STEP_BITS) - 1;
const TIMESTAMP_MAX: u64 = (1 << TIMESTAMP_BITS) - 1;

const TIMESTAMP_SHIFT: u8 = STEP_BITS + NODE_BITS; // 12 + 4 = 16
const SEQUENCE_SHIFT: u8 = NODE_BITS; // 4

/// Default epoch (2025-01-01T00:00:00Z in nanoseconds since Unix epoch)
const DEFAULT_EPOCH: u64 = 1735689600000000000;

/// Errors that can occur during Snowflake ID generation
#[derive(Debug)]
pub enum SnowflakeError {
    MachineIdOutOfRange,
    SequenceOverflow,
    EpochInTheFuture,
    EpochTooOld, // Triggered if Epoch is > ~9 years old
    TimeBackwards,
    TimestampOverflow, // Triggered if running > ~9 years
}

impl fmt::Display for SnowflakeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SnowflakeError::MachineIdOutOfRange => {
                write!(f, "Machine ID is out of range (must be 0-{})", NODE_MAX)
            }
            SnowflakeError::SequenceOverflow => {
                write!(f, "Sequence overflow: cannot generate a unique ID")
            }
            SnowflakeError::EpochInTheFuture => write!(f, "Epoch is set in the future"),
            SnowflakeError::EpochTooOld => {
                write!(f, "Epoch is too old for Shift-10 (Max ~9 years)")
            }
            SnowflakeError::TimeBackwards => write!(f, "System time moved backwards"),
            SnowflakeError::TimestampOverflow => {
                write!(f, "Critical: Timestamp limit reached (Redeploy needed)")
            }
        }
    }
}

impl Error for SnowflakeError {}

/// Snowflake ID generator
///
/// This struct implements the Snowflake algorithm for generating unique monotonically increasing IDs.
/// Each ID is composed of:
/// - Timestamp (48 bits, representing ~1.024µs ticks)
/// - Sequence number (12 bits)
/// - Node ID (4 bits)
///
/// Uses Shift-10 encoding: nanoseconds are right-shifted by 10 bits (divided by 1024) to fit
/// ~9.13 years of time range into 48 bits. This provides ~1.024 microsecond resolution.
pub struct Snowflake {
    start_time: Instant,  // Monotonic clock for duration measurement
    start_system_ns: u64, // Wall-clock start time
    epoch_ns: u64,        // Configured epoch
    last_tick: u64,       // Last used time bucket (Shifted value)
    sequence: u16,        // Sequence within the bucket
}

impl Snowflake {
    /// Creates a new Snowflake ID generator with the specified epoch.
    /// epoch: The custom epoch in nanoseconds since Unix epoch.
    /// If no epoch is provided, it defaults to 2025-01-01T00:00:00Z.
    pub fn new(epoch: Option<u64>) -> Result<Self, SnowflakeError> {
        let current_unix_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("SystemTime before UNIX EPOCH!")
            .as_nanos() as u64;
        let epoch_ns = epoch.unwrap_or(DEFAULT_EPOCH);

        if epoch_ns > current_unix_ns {
            return Err(SnowflakeError::EpochInTheFuture);
        }

        let raw_delta = current_unix_ns - epoch_ns;

        // Check if the starting time already fits in 48 bits after shifting
        if (raw_delta >> TIME_GRANULARITY_SHIFT) > TIMESTAMP_MAX {
            return Err(SnowflakeError::EpochTooOld);
        }

        Ok(Snowflake {
            epoch_ns,
            start_time: Instant::now(),
            start_system_ns: current_unix_ns,
            last_tick: 0,
            sequence: 0,
        })
    }

    pub fn generate(&mut self, gateway: u64) -> Result<u64, SnowflakeError> {
        if gateway > NODE_MAX {
            return Err(SnowflakeError::MachineIdOutOfRange);
        }

        let mut current_tick = self.current_tick()?;

        if current_tick == self.last_tick {
            // We are in the same 1.024µs bucket. Increment sequence.
            self.sequence += 1;

            if self.sequence > STEP_MAX {
                // We burned through 4096 IDs in less than 1µs.
                // Wait for the next bucket.
                current_tick = self.wait_next_tick(current_tick)?;
                self.sequence = 0;
            }
        } else {
            // New bucket (new microsecond). Reset sequence.
            self.sequence = 0;
        }

        self.last_tick = current_tick;
        Ok(self.create_id(current_tick, self.sequence, gateway))
    }

    /// Parses a Snowflake ID into its components based on the new layout.
    /// Returns: (timestamp_ticks, node_id, sequence)
    /// timestamp_ticks represents ~1.024µs buckets, not raw nanoseconds
    pub fn parse_id(id: u64) -> (u64, u8, u16) {
        let timestamp = (id >> TIMESTAMP_SHIFT) & TIMESTAMP_MAX;
        let sequence = ((id >> SEQUENCE_SHIFT) & (STEP_MAX as u64)) as u16;
        let node = (id & NODE_MAX) as u8;
        (timestamp, node, sequence)
    }

    /// Returns the current time in "Compressed Ticks" (approx 1.024µs chunks)
    fn current_tick(&self) -> Result<u64, SnowflakeError> {
        // We use Instant to ensure monotonicity within the process lifespan
        let elapsed_ns = self.start_time.elapsed().as_nanos() as u64;
        let current_ns = self.start_system_ns + elapsed_ns;

        if current_ns < self.epoch_ns {
            return Err(SnowflakeError::TimeBackwards);
        }

        let delta_ns = current_ns - self.epoch_ns;

        // CRITICAL: Shift bits to fit 9 years into 48 bits
        let shifted_time = delta_ns >> TIME_GRANULARITY_SHIFT;
        if shifted_time > TIMESTAMP_MAX {
            return Err(SnowflakeError::TimestampOverflow);
        }

        Ok(shifted_time)
    }

    /// Busy-wait loop until the next time bucket arrives
    fn wait_next_tick(&self, last_tick: u64) -> Result<u64, SnowflakeError> {
        let deadline = Instant::now() + Duration::from_micros(100);

        loop {
            // Spin with CPU hint first
            for _ in 0..64 {
                let current = self.current_tick()?;
                if current > last_tick {
                    return Ok(current);
                }
                std::hint::spin_loop();
            }

            // Fall back to yield if spinning too long
            if Instant::now() > deadline {
                return Err(SnowflakeError::SequenceOverflow);
            }
            thread::yield_now();
        }
    }

    /// Creates the final ID using the Timestamp-Sequence-Node layout.
    fn create_id(&self, timestamp: u64, sequence: u16, gateway: u64) -> u64 {
        (timestamp << TIMESTAMP_SHIFT) | ((sequence as u64) << SEQUENCE_SHIFT) | gateway
    }

    /// Extracts the Gateway ID from an ID based on the new layout.
    pub fn gateway_from_id(id: u64) -> u8 {
        (id & NODE_MAX) as u8
    }

    /// Returns the current system time in nanoseconds since Unix epoch
    /// This is used for external timestamping
    pub fn timestamp(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_nanos() as u64
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
        // Use an epoch from 5 years ago (should work with Shift-10)
        let five_years_ago = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
            - (5 * 365 * 24 * 60 * 60 * 1_000_000_000);

        let snowflake = Snowflake::new(Some(five_years_ago));
        assert!(snowflake.is_ok());
    }

    #[test]
    fn test_epoch_in_future() {
        let future_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
            + 1_000_000_000;

        let result = Snowflake::new(Some(future_epoch));
        assert!(matches!(result, Err(SnowflakeError::EpochInTheFuture)));
    }

    #[test]
    fn test_epoch_too_old() {
        // Trying an epoch from 20 years ago , it should fail
        let twenty_years_ns = 20u64 * 365 * 24 * 3600 * 1_000_000_000;
        let old_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
            - twenty_years_ns;

        let result = Snowflake::new(Some(old_epoch));
        assert!(matches!(result, Err(SnowflakeError::EpochTooOld)));
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
        let gateway = 5;
        let id = snowflake.generate(gateway).unwrap();

        let (timestamp, node, sequence) = Snowflake::parse_id(id);

        assert_eq!(node, gateway as u8);
        assert!(timestamp > 0);
        assert_eq!(sequence, 0); // First ID should have sequence 0
    }

    #[test]
    fn test_sequence_increment() {
        let mut snowflake = Snowflake::new(None).unwrap();
        let gateway = 3;

        // Generate multiple IDs rapidly to ensure same tick
        let ids: Vec<u64> = (0..10)
            .map(|_| snowflake.generate(gateway).unwrap())
            .collect();

        // Parse and check sequences
        for (i, &id) in ids.iter().enumerate() {
            let (_, node, sequence) = Snowflake::parse_id(id);
            assert_eq!(node, gateway as u8);
            // Sequence should increment (though may reset if tick changes)
            if i > 0 {
                let (prev_ts, _, prev_seq) = Snowflake::parse_id(ids[i - 1]);
                let (curr_ts, _, _) = Snowflake::parse_id(id);

                if prev_ts == curr_ts {
                    assert_eq!(
                        sequence,
                        prev_seq + 1,
                        "Sequence should increment within same tick"
                    );
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
            assert_eq!(node, gateway as u8);
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
    fn test_burst_throughput() {
        // Can we generate 4096 IDs instantly without error?
        // More robust: just verify monotonicity
        let mut snowflake = Snowflake::new(None).unwrap();
        let mut prev_id = 0u64;

        for _ in 0..5000 {
            let id = snowflake.generate(1).unwrap();
            assert!(id > prev_id, "IDs must be strictly monotonic");
            prev_id = id;
        }
    }

    #[test]
    fn test_bit_layout() {
        let mut snowflake = Snowflake::new(None).unwrap();
        let gateway = 0b1111; // All bits set (15)

        // Generate ID and verify bit layout
        let id = snowflake.generate(gateway).unwrap();
        let (timestamp, node, sequence) = Snowflake::parse_id(id);

        // Verify node bits are correct
        assert_eq!(node, gateway as u8);

        // Verify timestamp doesn't overflow into node bits
        assert!(timestamp <= TIMESTAMP_MAX);

        // Verify sequence doesn't overflow into node bits
        assert!(sequence <= STEP_MAX);
    }

    #[test]
    fn test_gateway_from_id() {
        let mut snowflake = Snowflake::new(None).unwrap();
        for gateway in 0..=NODE_MAX {
            let id = snowflake.generate(gateway).unwrap();
            let extracted_gateway = Snowflake::gateway_from_id(id);
            assert_eq!(
                extracted_gateway, gateway as u8,
                "Extracted gateway should match original"
            );
        }
    }
}
