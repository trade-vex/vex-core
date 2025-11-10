//! Balance Preload Configuration
//!
//! Simple configuration for pre-funding user accounts on startup.
//! Only for test/local environments.

use crate::{ConfigError, Result};
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

    pub fn validate(&self) -> Result<()> {
        for (user_id, balances) in &self.users {
            if balances.is_empty() {
                return Err(ConfigError::ValidationError(format!(
                    "User {} has no balances configured",
                    user_id
                )));
            }
        }
        Ok(())
    }
}

