//! Balance Preload Configuration
//!
//! Simple configuration for pre-funding user accounts on startup.
//! Only for test/local environments.

use crate::{ConfigError, Result, SymbolSpecificationConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Single balance entry for a user
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserBalance {
    pub asset_id: u16,
    pub amount: u64,
}

/// Balance preload configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BalancePreloadConfig {
    /// Whether preloading is enabled
    #[serde(default)]
    pub enabled: bool,

    /// Map of user_id -> list of balances to fund
    #[serde(default)]
    pub users: HashMap<u64, Vec<UserBalance>>,
}

impl BalancePreloadConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// A preloaded balance obeys the same supply bound as a deposited one.
    pub fn validate(&self, symbols: &SymbolSpecificationConfig) -> Result<()> {
        for (user_id, balances) in &self.users {
            if balances.is_empty() {
                return Err(ConfigError::ValidationError(format!(
                    "User {} has no balances configured",
                    user_id
                )));
            }
            for balance in balances {
                let asset_id = balance.asset_id;
                let Some(asset) = symbols.assets.get(&asset_id) else {
                    return Err(ConfigError::ValidationError(format!(
                        "User {user_id}: preloaded asset {asset_id} is not in symbols.assets"
                    )));
                };
                if u128::from(balance.amount) > asset.atomic_supply() {
                    return Err(ConfigError::ValidationError(format!(
                        "User {user_id}: preloaded {} of asset {asset_id} exceeds its max_supply * native_scale",
                        balance.amount
                    )));
                }
            }
        }
        Ok(())
    }
}
