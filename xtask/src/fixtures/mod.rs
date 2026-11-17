//! Reusable test fixtures and configurations
//!
//! This module provides common test data and helper functions.

/// Common user IDs for testing
pub mod users {
    pub const ALICE: u64 = 1;
    pub const BOB: u64 = 2;
    pub const CHARLIE: u64 = 3;
}

/// Common asset IDs
pub mod assets {
    pub const USD: u16 = 1; // Quote asset
    pub const BTC: u16 = 2; // Base asset
}

/// Common prices for testing (in smallest units)
pub mod prices {
    pub const LOW: u64 = 40_000;
    pub const MID: u64 = 50_000;
    pub const HIGH: u64 = 60_000;
}
