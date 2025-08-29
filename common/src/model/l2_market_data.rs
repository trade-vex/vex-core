/// Represents Level 2 market data with a fixed number of price levels.
///
/// `LEVEL` is a const generic parameter that defines the depth of the order book
/// for both asks and bids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2MarketData<const LEVEL: usize> {
    pub ask_prices: [u64; LEVEL],
    pub ask_volumes: [u64; LEVEL],
    pub ask_orders: [u64; LEVEL],
    pub bid_prices: [u64; LEVEL],
    pub bid_volumes: [u64; LEVEL],
    pub bid_orders: [u64; LEVEL],
    pub timestamp: u64,
    pub reference_seq: u64,
}

impl<const LEVEL: usize> L2MarketData<LEVEL> {
    /// Creates a new, empty `L2MarketData` instance with all values initialized to zero.
    pub fn new() -> Self {
        Self {
            ask_prices: [0; LEVEL],
            ask_volumes: [0; LEVEL],
            ask_orders: [0; LEVEL],
            bid_prices: [0; LEVEL],
            bid_volumes: [0; LEVEL],
            bid_orders: [0; LEVEL],
            timestamp: 0,
            reference_seq: 0,
        }
    }

    /// Returns the depth of the order book.
    pub fn depth(&self) -> usize {
        LEVEL
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
