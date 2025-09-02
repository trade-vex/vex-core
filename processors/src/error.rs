use common::BalanceError;
use thiserror::Error;

/// Result type alias for processor operations
pub type Result<T> = std::result::Result<T, RiskEngineError>;

/// Risk engine specific errors
#[derive(Error, Debug, PartialEq, Eq)]
pub enum RiskEngineError {
    /// User not found in the risk engine's user balance store
    #[error("user not found: user_id {user_id}")]
    UserNotFound { user_id: u64 },

    /// Market/symbol specification not found
    #[error("market specification not found: market_id {market_id}")]
    MarketSpecNotFound { market_id: u32 },

    /// Invalid order arguments (price, size, etc.)
    #[error("invalid order arguments: price {price}, size {size}")]
    InvalidArguments { price: u64, size: u64 },

    /// Insufficient funds to place the order
    #[error("insufficient funds: user_id {user_id}, required {required}, available {available}")]
    InsufficientFunds {
        user_id: u64,
        required: u64,
        available: u64,
    },

    /// Order command not supported by this risk engine
    #[error("unsupported command: {command:?}")]
    UnsupportedCommand { command: String },

    /// Balance related errors
    #[error("balance error: {0}")]
    BalanceError(#[from] BalanceError),
}
