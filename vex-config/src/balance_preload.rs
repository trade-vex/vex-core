//! Balance Preload Configuration
//!
//! Simple configuration for pre-funding user accounts on startup.
//! Only for test/local environments.

use crate::{ConfigError, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
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
    /// Uses custom serialization to handle large u64 keys in YAML
    #[serde(
        serialize_with = "serialize_users",
        deserialize_with = "deserialize_users",
        default
    )]
    pub users: HashMap<u64, Vec<UserBalance>>,
}

fn serialize_users<S>(
    users: &HashMap<u64, Vec<UserBalance>>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut new_users = HashMap::new();
    for (key, value) in users {
        new_users.insert(key.to_string(), value.clone());
    }
    new_users.serialize(serializer)
}

fn deserialize_users<'de, D>(
    deserializer: D,
) -> std::result::Result<HashMap<u64, Vec<UserBalance>>, D::Error>
where
    D: Deserializer<'de>,
{
    let new_users = HashMap::<String, Vec<UserBalance>>::deserialize(deserializer)?;
    let mut users = HashMap::new();
    for (key, value) in new_users {
        // Try parsing as i64 first to handle negative numbers (which represent
        // large u64 values when interpreted as two's complement)
        // If that fails, try parsing as u64 directly
        let key = if let Ok(i64_val) = key.parse::<i64>() {
            // Convert i64 to u64 using bit casting (two's complement representation)
            i64_val as u64
        } else {
            // If not a valid i64, try parsing as u64 directly
            key.parse::<u64>().map_err(|e| {
                serde::de::Error::custom(format!(
                    "Failed to parse user_id '{}' as i64 or u64: {}",
                    key, e
                ))
            })?
        };
        users.insert(key, value);
    }
    Ok(users)
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

