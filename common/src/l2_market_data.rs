use crate::L2SIZE;

/// Represents Level 2 market data with a fixed number of price levels.
///
/// `LEVEL` is a const generic parameter that defines the depth of the order book
/// for both asks and bids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2MarketData {
    pub ask_prices: Vec<u64>,
    pub ask_volumes: Vec<u64>,
    pub ask_orders: Vec<u64>,
    pub bid_prices: Vec<u64>,
    pub bid_volumes: Vec<u64>,
    pub bid_orders: Vec<u64>,
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
            ask_prices: Vec::with_capacity(L2SIZE),
            ask_volumes: Vec::with_capacity(L2SIZE),
            ask_orders: Vec::with_capacity(L2SIZE),
            bid_prices: Vec::with_capacity(L2SIZE),
            bid_volumes: Vec::with_capacity(L2SIZE),
            bid_orders: Vec::with_capacity(L2SIZE),
            timestamp: 0,
            reference_seq: 0,
        }
    }

    /// Returns the depth of the order book.
    pub fn bid_depth(&self) -> usize {
        self.bid_prices.len()
    }

    /// Returns the depth of the order book.
    pub fn ask_depth(&self) -> usize {
        self.ask_prices.len()
    }

    /// Calculates the total volume on the ask side of the order book.
    pub fn total_ask_volume(&self) -> u64 {
        self.ask_volumes.iter().sum()
    }

    /// Calculates the total volume on the bid side of the order book.
    pub fn total_bid_volume(&self) -> u64 {
        self.bid_volumes.iter().sum()
    }
}
