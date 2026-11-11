use crate::L2SIZE;

/// The top `L2SIZE` price levels of each side, best first.
///
/// Stored inline, so taking a snapshot on every place and cancel allocates nothing. Only the
/// first `bid_depth` bid and `ask_depth` ask entries are set.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct L2MarketData {
    pub ask_prices: [u64; L2SIZE],
    pub ask_volumes: [u64; L2SIZE],
    pub bid_prices: [u64; L2SIZE],
    pub bid_volumes: [u64; L2SIZE],
    pub ask_depth: usize,
    pub bid_depth: usize,
}

impl L2MarketData {
    /// The set bid levels as (prices, volumes).
    pub fn bids(&self) -> (&[u64], &[u64]) {
        (
            &self.bid_prices[..self.bid_depth],
            &self.bid_volumes[..self.bid_depth],
        )
    }

    /// The set ask levels as (prices, volumes).
    pub fn asks(&self) -> (&[u64], &[u64]) {
        (
            &self.ask_prices[..self.ask_depth],
            &self.ask_volumes[..self.ask_depth],
        )
    }
}
