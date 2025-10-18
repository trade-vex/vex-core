//! Reusable test fixtures and configurations
//!
//! This module provides common test data and helper functions.

/// Common user IDs for testing
pub mod users {
    pub const ALICE: u64 = 1;
    pub const BOB: u64 = 2;
    pub const CHARLIE: u64 = 3;
    pub const DAVE: u64 = 4;
    pub const EVE: u64 = 5;
}

/// Common asset IDs
pub mod assets {
    pub const USD: u16 = 1;  // Quote asset
    pub const BTC: u16 = 2;  // Base asset
    pub const ETH: u16 = 3;
}

/// Common market configurations
pub mod markets {
    pub const BTC_USD: u32 = 0x00010002;  // ((1 << 16) | 2)
    pub const ETH_USD: u32 = 0x00010003;  // ((1 << 16) | 3)
}

/// Common amounts for testing
pub mod amounts {
    pub const SMALL: u64 = 100;
    pub const MEDIUM: u64 = 1_000;
    pub const LARGE: u64 = 10_000;
    pub const XLARGE: u64 = 100_000;

    // Funding amounts
    pub const FUND_1M_USD: u64 = 1_000_000;
    pub const FUND_10K_BTC: u64 = 10_000;
}

/// Common prices for testing (in smallest units)
pub mod prices {
    pub const LOW: u64 = 40_000;
    pub const MID: u64 = 50_000;
    pub const HIGH: u64 = 60_000;
}
