use std::error::Error;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

const NODE_BITS: u8 = 4;
const STEP_BITS: u8 = 12;
const TIMESTAMP_BITS: u8 = 48;

// Right-shift time by 10 bits.
// 1 Tick = 1024 nanoseconds (~1 microsecond).
const TIME_GRANULARITY_SHIFT: u8 = 10;

const NODE_MAX: u64 = (1 << NODE_BITS) - 1;
const STEP_MAX: u16 = (1 << STEP_BITS) - 1;
const TIMESTAMP_MAX: u64 = (1 << TIMESTAMP_BITS) - 1;
/// The nanoseconds within a tick (0-1023), which become the id's sequence.
const TICK_NANOS_MASK: u64 = (1 << TIME_GRANULARITY_SHIFT) - 1;

const TIMESTAMP_SHIFT: u8 = STEP_BITS + NODE_BITS; // 12 + 4 = 16
const SEQUENCE_SHIFT: u8 = NODE_BITS; // 4

/// Default epoch (2025-01-01T00:00:00Z in nanoseconds since Unix epoch)
const DEFAULT_EPOCH: u64 = 1735689600000000000;

/// Errors that can occur during Snowflake ID generation
#[derive(Debug)]
pub enum SnowflakeError {
    MachineIdOutOfRange,
    EpochInTheFuture,
    EpochTooOld, // Triggered if Epoch is > ~9 years old
    BeforeEpoch,
    TimestampOverflow, // Triggered if running > ~9 years
}

impl fmt::Display for SnowflakeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SnowflakeError::MachineIdOutOfRange => {
                write!(f, "Machine ID is out of range (must be 0-{})", NODE_MAX)
            }
            SnowflakeError::EpochInTheFuture => write!(f, "Epoch is set in the future"),
            SnowflakeError::EpochTooOld => {
                write!(f, "Epoch is too old for Shift-10 (Max ~9 years)")
            }
            SnowflakeError::BeforeEpoch => write!(f, "Timestamp is before the epoch"),
            SnowflakeError::TimestampOverflow => {
                write!(f, "Critical: Timestamp limit reached (Redeploy needed)")
            }
        }
    }
}

impl Error for SnowflakeError {}

/// Snowflake ID generator
///
/// An id is the timestamp journaling stamped on the command, plus the gateway that sent it:
/// - Timestamp (48 bits): ~1.024µs ticks since the epoch (nanoseconds right-shifted by 10)
/// - Sequence (12 bits): the timestamp's nanoseconds within its tick (0-1023)
/// - Node (4 bits): the gateway id
///
/// Ids therefore order exactly as their timestamps do, whatever their gateways: commands in one
/// tick are sequenced by their nanoseconds. Journaling stamps every command strictly above the
/// previous one and above the last command in the journal, so an id is never issued twice, within
/// a process or across a restart.
///
/// Shift-10 fits ~9.13 years of time range into 48 bits.
pub struct Snowflake {
    epoch_ns: u64, // Configured epoch
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

        Ok(Snowflake { epoch_ns })
    }

    /// The id of a command stamped `timestamp_ns` (nanoseconds since the Unix epoch) that
    /// `gateway` sent.
    pub fn generate(&self, timestamp_ns: u64, gateway: u64) -> Result<u64, SnowflakeError> {
        if gateway > NODE_MAX {
            return Err(SnowflakeError::MachineIdOutOfRange);
        }

        let since_epoch = timestamp_ns
            .checked_sub(self.epoch_ns)
            .ok_or(SnowflakeError::BeforeEpoch)?;

        // CRITICAL: Shift bits to fit 9 years into 48 bits
        let tick = since_epoch >> TIME_GRANULARITY_SHIFT;
        if tick > TIMESTAMP_MAX {
            return Err(SnowflakeError::TimestampOverflow);
        }

        Ok(Self::create_id(
            tick,
            since_epoch & TICK_NANOS_MASK,
            gateway,
        ))
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

    /// Creates the final ID using the Timestamp-Sequence-Node layout.
    fn create_id(timestamp: u64, sequence: u64, gateway: u64) -> u64 {
        (timestamp << TIMESTAMP_SHIFT) | (sequence << SEQUENCE_SHIFT) | gateway
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

    /// The first nanosecond of tick `tick` under the default epoch.
    fn tick_start(tick: u64) -> u64 {
        DEFAULT_EPOCH + (tick << TIME_GRANULARITY_SHIFT)
    }

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
        let snowflake = Snowflake::new(None).unwrap();
        let result = snowflake.generate(tick_start(1), NODE_MAX + 1);
        assert!(matches!(result, Err(SnowflakeError::MachineIdOutOfRange)));
    }

    #[test]
    fn timestamps_outside_the_epoch_range_are_rejected() {
        let snowflake = Snowflake::new(None).unwrap();

        assert!(matches!(
            snowflake.generate(DEFAULT_EPOCH - 1, 0),
            Err(SnowflakeError::BeforeEpoch)
        ));
        assert!(snowflake.generate(tick_start(TIMESTAMP_MAX), 0).is_ok());
        assert!(matches!(
            snowflake.generate(tick_start(TIMESTAMP_MAX + 1), 0),
            Err(SnowflakeError::TimestampOverflow)
        ));
    }

    #[test]
    fn same_tick_ids_are_sequenced_by_their_nanoseconds() {
        let snowflake = Snowflake::new(None).unwrap();
        let start = tick_start(5);

        let ids: Vec<u64> = [start, start + 1, start + 2, start + 1023, start + 1024]
            .into_iter()
            .map(|timestamp| snowflake.generate(timestamp, 3).unwrap())
            .collect();

        let parsed: Vec<_> = ids.iter().map(|&id| Snowflake::parse_id(id)).collect();
        assert_eq!(
            parsed,
            [(5, 3, 0), (5, 3, 1), (5, 3, 2), (5, 3, 1023), (6, 3, 0)]
        );
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn ids_order_as_their_timestamps_whatever_the_gateway() {
        let snowflake = Snowflake::new(None).unwrap();
        let earlier = tick_start(7) + 500;

        for later in [earlier + 1, tick_start(8)] {
            assert!(
                snowflake.generate(earlier, NODE_MAX).unwrap()
                    < snowflake.generate(later, 0).unwrap()
            );
        }
        // One timestamp is one command, so this tie never happens in practice; the gateway
        // still keeps the ids apart.
        assert!(snowflake.generate(earlier, 0).unwrap() < snowflake.generate(earlier, 1).unwrap());
    }

    #[test]
    fn test_parse_id() {
        let snowflake = Snowflake::new(None).unwrap();
        let gateway = 5;
        let id = snowflake.generate(tick_start(42) + 17, gateway).unwrap();

        let (timestamp, node, sequence) = Snowflake::parse_id(id);

        assert_eq!(node, gateway as u8);
        assert_eq!(timestamp, 42);
        assert_eq!(sequence, 17);
    }

    #[test]
    fn test_different_gateways() {
        let snowflake = Snowflake::new(None).unwrap();

        for gateway in 0..=NODE_MAX {
            let id = snowflake.generate(tick_start(1), gateway).unwrap();
            let (_, node, _) = Snowflake::parse_id(id);
            assert_eq!(node, gateway as u8);
        }
    }

    #[test]
    fn test_bit_layout() {
        let snowflake = Snowflake::new(None).unwrap();
        let gateway = 0b1111; // All bits set (15)

        // The last nanosecond of the last tick: every field at its maximum.
        let id = snowflake
            .generate(tick_start(TIMESTAMP_MAX) + TICK_NANOS_MASK, gateway)
            .unwrap();
        let (timestamp, node, sequence) = Snowflake::parse_id(id);

        assert_eq!(node, gateway as u8);
        assert_eq!(timestamp, TIMESTAMP_MAX);
        assert_eq!(u64::from(sequence), TICK_NANOS_MASK);
    }

    #[test]
    fn test_gateway_from_id() {
        let snowflake = Snowflake::new(None).unwrap();
        for gateway in 0..=NODE_MAX {
            let id = snowflake.generate(tick_start(1), gateway).unwrap();
            let extracted_gateway = Snowflake::gateway_from_id(id);
            assert_eq!(
                extracted_gateway, gateway as u8,
                "Extracted gateway should match original"
            );
        }
    }
}
