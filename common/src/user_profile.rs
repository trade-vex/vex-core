use ahash::AHashMap;
use parking_lot::Mutex;
use thiserror::Error;

pub type MarketId = u32;

/// A utility struct to handle market ID logic.
#[derive(Debug, Clone, Copy)]
pub struct Market(MarketId);

impl Market {
    pub fn new(id: MarketId) -> Self {
        Self(id)
    }

    /// The asset being bought or sold (e.g., BTC in BTC/USDT).
    /// Stored in the lower 16 bits of the MarketId.
    pub fn base_asset(&self) -> u16 {
        (self.0 & 0xFFFF) as u16
    }

    /// The asset used to price the base asset (e.g., USDT in BTC/USDT).
    /// Stored in the upper 16 bits of the MarketId.
    pub fn quote_asset(&self) -> u16 {
        (self.0 >> 16) as u16
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UserBalance {
    pub available: u64,
    pub locked: u64,
}

impl UserBalance {
    pub fn new() -> Self {
        Self {
            available: 0,
            locked: 0,
        }
    }
    pub fn total(&self) -> u64 {
        self.available.saturating_add(self.locked)
    }
    pub fn available(&self) -> u64 {
        self.available
    }
    pub fn locked(&self) -> u64 {
        self.locked
    }
}

/// The key for the balances map. A user's balance is per-asset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BalanceKey {
    user_id: u64,
    asset_id: u16,
}

impl BalanceKey {
    pub fn new(user_id: u64, asset_id: u16) -> Self {
        Self { user_id, asset_id }
    }
}

/// The internal, non-thread-safe balance store.
/// This should not be used directly by concurrent processors.
pub struct BalanceStore {
    balances: AHashMap<BalanceKey, UserBalance>,
}

impl Default for BalanceStore {
    fn default() -> Self {
        Self::new()
    }
}

// NOTE: All methods now correctly use `asset_id` instead of `market_id`.
impl BalanceStore {
    pub fn new() -> Self {
        Self {
            balances: AHashMap::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            balances: AHashMap::with_capacity(capacity),
        }
    }

    pub fn get_balance(&self, user_id: u64, asset_id: u16) -> UserBalance {
        let key = BalanceKey { user_id, asset_id };
        *self.balances.get(&key).unwrap_or(&UserBalance::default())
    }

    pub fn get_balance_mut(&mut self, user_id: u64, asset_id: u16) -> &mut UserBalance {
        let key = BalanceKey { user_id, asset_id };
        self.balances
            .entry(key)
            .or_insert_with(UserBalance::default)
    }

    // Lock funds (move from available to locked)
    pub fn lock_funds(
        &mut self,
        user_id: u64,
        asset_id: u16,
        amount: u64,
    ) -> Result<(), BalanceError> {
        let balance = self.get_balance_mut(user_id, asset_id);
        if balance.available >= amount {
            balance.available -= amount;
            balance.locked += amount;
            Ok(())
        } else {
            Err(BalanceError::InsufficientAvailableFunds {
                available: balance.available,
                needed: amount,
            })
        }
    }

    // Unlock funds (move from locked to available)
    pub fn unlock_funds(
        &mut self,
        user_id: u64,
        asset_id: u16,
        amount: u64,
    ) -> Result<(), BalanceError> {
        let balance = self.get_balance_mut(user_id, asset_id);
        if balance.locked >= amount {
            balance.locked -= amount;
            balance.available += amount;
            Ok(())
        } else {
            Err(BalanceError::InsufficientLockedFunds {
                locked: balance.locked,
                needed: amount,
            })
        }
    }

    // Additional helper methods can be added here...
}

/// Represents an error related to balance operations.
#[derive(Error, Debug, PartialEq, Eq)]
pub enum BalanceError {
    #[error("insufficient available funds: needed {needed}, but have {available}")]
    InsufficientAvailableFunds { available: u64, needed: u64 },
    #[error("insufficient locked funds: needed {needed}, but have {locked}")]
    InsufficientLockedFunds { locked: u64, needed: u64 },
    #[error("operation would cause numeric overflow")]
    Overflow,
}
