use ahash::AHashMap;
use thiserror::Error;

type UserId = u64;
type MarketId = u32;

#[derive(Debug, Clone, Copy)]
pub struct UserBalance {
    available: u64,
    locked: u64,
}

impl UserBalance {
    pub fn new() -> Self {
        Self {
            available: 0,
            locked: 0,
        }
    }

    pub fn total(&self) -> u64 {
        self.available + self.locked
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BalanceKey {
    user_id: UserId,
    market_id: MarketId,
}

pub struct BalanceStore {
    balances: AHashMap<BalanceKey, UserBalance>,
}

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

    pub fn get_balance(
        &self,
        user_id: UserId,
        market_id: MarketId,
    ) -> Result<UserBalance, BalanceError> {
        let key = BalanceKey { user_id, market_id };
        match self.balances.get(&key) {
            Some(balance) => Ok(*balance),
            None => Err(BalanceError::UserNotFound { user_id, market_id }),
        }
    }

    pub fn set_balance(&mut self, user_id: UserId, market_id: MarketId, balance: UserBalance) {
        let key = BalanceKey { user_id, market_id };
        self.balances.insert(key, balance);
    }

    pub fn update_available(
        &mut self,
        user_id: UserId,
        market_id: MarketId,
        amount: u64,
    ) -> Result<(), BalanceError> {
        let key = BalanceKey { user_id, market_id };
        match self.balances.get_mut(&key) {
            Some(balance) => balance.available = amount,
            None => return Err(BalanceError::UserNotFound { user_id, market_id }),
        };
        Ok(())
    }

    pub fn update_locked(
        &mut self,
        user_id: UserId,
        market_id: MarketId,
        amount: u64,
    ) -> Result<(), BalanceError> {
        let key = BalanceKey { user_id, market_id };
        match self.balances.get_mut(&key) {
            Some(balance) => balance.locked = amount,
            None => return Err(BalanceError::UserNotFound { user_id, market_id }),
        };
        Ok(())
    }

    // Lock funds (move from available to locked)
    pub fn lock_funds(
        &mut self,
        user_id: UserId,
        market_id: MarketId,
        amount: u64,
    ) -> Result<(), BalanceError> {
        let key = BalanceKey { user_id, market_id };
        let balance = match self.balances.get_mut(&key) {
            Some(balance) => balance,
            None => return Err(BalanceError::UserNotFound { user_id, market_id }),
        };

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
        user_id: UserId,
        market_id: MarketId,
        amount: u64,
    ) -> Result<(), BalanceError> {
        let key = BalanceKey { user_id, market_id };
        let balance = match self.balances.get_mut(&key) {
            Some(balance) => balance,
            None => return Err(BalanceError::UserNotFound { user_id, market_id }),
        };

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

    // Consume locked funds (remove from locked, e.g., after trade execution)
    pub fn consume_locked(
        &mut self,
        user_id: UserId,
        market_id: MarketId,
        amount: u64,
    ) -> Result<(), BalanceError> {
        let key = BalanceKey { user_id, market_id };
        let balance = match self.balances.get_mut(&key) {
            Some(balance) => balance,
            None => return Err(BalanceError::UserNotFound { user_id, market_id }),
        };

        if balance.locked >= amount {
            balance.locked -= amount;
            Ok(())
        } else {
            Err(BalanceError::InsufficientLockedFunds {
                locked: balance.locked,
                needed: amount,
            })
        }
    }
}

/// Represents an error related to balance operations.
#[derive(Error, Debug, PartialEq, Eq)]
pub enum BalanceError {
    /// Occurs when there are not enough available funds to perform an operation.
    #[error("insufficient available funds: needed {needed}, but have {available}")]
    InsufficientAvailableFunds { available: u64, needed: u64 },

    /// Occurs when there are not enough locked funds to perform an operation.
    #[error("insufficient locked funds: needed {needed}, but have {locked}")]
    InsufficientLockedFunds { locked: u64, needed: u64 },

    /// Occurs when an operation would cause an overflow
    #[error("operation would cause overflow")]
    Overflow,

    /// Occurs when an operation would cause an underflow
    #[error("operation would cause underflow")]
    Underflow,

    /// Occurs when user is not found
    #[error("user not found")]
    UserNotFound {
        user_id: UserId,
        market_id: MarketId,
    },
}
