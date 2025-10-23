use crate::L2SIZE;

/// Represents Level 2 market data with a fixed number of price levels.
///
/// Uses fixed-size arrays to avoid heap allocations on the hot path.
/// Depth fields track the actual number of valid entries in each array.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2MarketData {
    pub ask_prices: [u64; L2SIZE],
    pub ask_volumes: [u64; L2SIZE],
    pub ask_orders: [u64; L2SIZE],
    pub ask_depth: usize,
    pub bid_prices: [u64; L2SIZE],
    pub bid_volumes: [u64; L2SIZE],
    pub bid_orders: [u64; L2SIZE],
    pub bid_depth: usize,
    pub timestamp: u64,
    pub reference_seq: u64,
}

impl Default for L2MarketData {
    fn default() -> Self {
        Self::new()
    }
}

impl L2MarketData {
    /// Creates a new, empty `L2MarketData` instance with all values initialized to zero.
    pub fn new() -> Self {
        Self {
            ask_prices: [0; L2SIZE],
            ask_volumes: [0; L2SIZE],
            ask_orders: [0; L2SIZE],
            ask_depth: 0,
            bid_prices: [0; L2SIZE],
            bid_volumes: [0; L2SIZE],
            bid_orders: [0; L2SIZE],
            bid_depth: 0,
            timestamp: 0,
            reference_seq: 0,
        }
    }

    /// Returns the depth of the bid side.
    pub fn bid_depth(&self) -> usize {
        self.bid_depth
    }

    /// Returns the depth of the ask side.
    pub fn ask_depth(&self) -> usize {
        self.ask_depth
    }

    /// Calculates the total volume on the ask side of the order book.
    pub fn total_ask_volume(&self) -> u64 {
        self.ask_volumes[..self.ask_depth].iter().sum()
    }

    /// Calculates the total volume on the bid side of the order book.
    pub fn total_bid_volume(&self) -> u64 {
        self.bid_volumes[..self.bid_depth].iter().sum()
    }
}
