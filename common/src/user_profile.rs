use ahash::AHashMap;
use thiserror::Error;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UserBalance {
    pub available: u64,
    pub locked: u64,
}

impl UserBalance {
    pub fn new(available: u64, locked: u64) -> Self {
        Self { available, locked }
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

    pub fn try_get_balance(
        &self,
        user_id: u64,
        asset_id: u16,
    ) -> Result<UserBalance, BalanceError> {
        let key = BalanceKey { user_id, asset_id };
        self.balances
            .get(&key)
            .copied()
            .ok_or(BalanceError::UserAssetNotFound(user_id, asset_id))
    }

    pub fn get_balance_mut(
        &mut self,
        user_id: u64,
        asset_id: u16,
    ) -> Result<&mut UserBalance, BalanceError> {
        let key = BalanceKey { user_id, asset_id };
        self.balances
            .get_mut(&key)
            .ok_or(BalanceError::UserAssetNotFound(user_id, asset_id))
    }

    pub fn set_balance(&mut self, user_id: u64, asset_id: u16, balance: UserBalance) {
        let key = BalanceKey { user_id, asset_id };
        self.balances.insert(key, balance);
    }

    // Lock funds (move from available to locked)
    pub fn lock_funds(
        &mut self,
        user_id: u64,
        asset_id: u16,
        amount: u64,
    ) -> Result<(), BalanceError> {
        let balance = self.get_balance_mut(user_id, asset_id)?;
        if balance.available >= amount {
            let available = balance.available - amount;
            let locked = balance
                .locked
                .checked_add(amount)
                .ok_or(BalanceError::Overflow)?;
            balance.available = available;
            balance.locked = locked;
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
        let balance = self.get_balance_mut(user_id, asset_id)?;
        if balance.locked >= amount {
            let locked = balance.locked - amount;
            let available = balance
                .available
                .checked_add(amount)
                .ok_or(BalanceError::Overflow)?;
            balance.locked = locked;
            balance.available = available;
            Ok(())
        } else {
            Err(BalanceError::InsufficientLockedFunds {
                locked: balance.locked,
                needed: amount,
            })
        }
    }

    // Add Funds
    pub fn add_funds(
        &mut self,
        user_id: u64,
        asset_id: u16,
        amount: u64,
    ) -> Result<UserBalance, BalanceError> {
        let key = BalanceKey { user_id, asset_id };
        let balance = self.balances.entry(key).or_default();
        balance.available = balance
            .available
            .checked_add(amount)
            .ok_or(BalanceError::Overflow)?;
        Ok(*balance)
    }

    // Subtract from available funds
    pub fn subtract_funds(
        &mut self,
        user_id: u64,
        asset_id: u16,
        amount: u64,
    ) -> Result<UserBalance, BalanceError> {
        let balance = self.get_balance_mut(user_id, asset_id)?;
        if balance.available >= amount {
            balance.available -= amount;
            Ok(*balance)
        } else {
            Err(BalanceError::InsufficientAvailableFunds {
                available: balance.available,
                needed: amount,
            })
        }
    }

    // Subtract from locked funds
    pub fn subtract_locked_funds(
        &mut self,
        user_id: u64,
        asset_id: u16,
        amount: u64,
    ) -> Result<UserBalance, BalanceError> {
        let balance = self.get_balance_mut(user_id, asset_id)?;
        if balance.locked >= amount {
            balance.locked -= amount;
            Ok(*balance)
        } else {
            Err(BalanceError::InsufficientLockedFunds {
                locked: balance.locked,
                needed: amount,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejected_lock_does_not_insert_balance() {
        let mut store = BalanceStore::new();

        assert_eq!(
            store.lock_funds(7, 3, 1),
            Err(BalanceError::UserAssetNotFound(7, 3))
        );
        assert!(store.balances.is_empty());
    }

    #[test]
    fn rejected_unlock_does_not_insert_balance() {
        let mut store = BalanceStore::new();

        assert_eq!(
            store.unlock_funds(7, 3, 1),
            Err(BalanceError::UserAssetNotFound(7, 3))
        );
        assert!(store.balances.is_empty());
    }

    #[test]
    fn lock_funds_checks_locked_overflow_without_mutation() {
        let mut store = BalanceStore::new();
        store.set_balance(7, 3, UserBalance::new(1, u64::MAX));

        assert_eq!(store.lock_funds(7, 3, 1), Err(BalanceError::Overflow));
        assert_eq!(store.get_balance(7, 3), UserBalance::new(1, u64::MAX));
    }

    #[test]
    fn unlock_funds_checks_available_overflow_without_mutation() {
        let mut store = BalanceStore::new();
        store.set_balance(7, 3, UserBalance::new(u64::MAX, 1));

        assert_eq!(store.unlock_funds(7, 3, 1), Err(BalanceError::Overflow));
        assert_eq!(store.get_balance(7, 3), UserBalance::new(u64::MAX, 1));
    }
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
    #[error("User asset not found: user_id={0}, asset_id={1}")]
    UserAssetNotFound(u64, u16),
}
