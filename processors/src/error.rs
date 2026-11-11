use common::BalanceError;
use thiserror::Error;

/// Result type alias for processor operations
pub type Result<T> = std::result::Result<T, RiskEngineError>;

/// Risk engine specific errors
#[derive(Error, Debug, PartialEq, Eq)]
pub enum RiskEngineError {
    /// Market/symbol specification not found
    #[error("market specification not found: market_id {market_id}")]
    MarketSpecNotFound { market_id: u32 },

    /// Invalid order arguments (price, size, etc.)
    #[error("invalid order arguments: price {price}, size {size}")]
    InvalidArguments { price: u64, size: u64 },

    /// Balance related errors
    #[error("balance error: {0}")]
    BalanceError(#[from] BalanceError),
}
