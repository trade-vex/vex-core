//! Symbol specifications configuration
//!
//! This module provides configuration management for trading symbols and their specifications.
//! It supports loading symbols from configuration files and provides validation and management.

use crate::{ConfigError, Environment, Result};
use common::CoreMarketSpecification;
use common::MarketType;
use hashbrown::HashMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Configuration for trading symbols
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolSpecificationConfig {
    /// Map of market_id to symbol specification
    #[serde(
        serialize_with = "serialize_symbols",
        deserialize_with = "deserialize_symbols"
    )]
    pub symbols: HashMap<u32, CoreMarketSpecification>,
}

fn serialize_symbols<S>(
    symbols: &HashMap<u32, CoreMarketSpecification>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut new_symbols = HashMap::new();
    for (key, value) in symbols {
        new_symbols.insert(key.to_string(), value.clone());
    }
    new_symbols.serialize(serializer)
}

fn deserialize_symbols<'de, D>(
    deserializer: D,
) -> std::result::Result<HashMap<u32, CoreMarketSpecification>, D::Error>
where
    D: Deserializer<'de>,
{
    let new_symbols = HashMap::<String, CoreMarketSpecification>::deserialize(deserializer)?;
    let mut symbols = HashMap::new();
    for (key, value) in new_symbols {
        let key = key.parse::<u32>().map_err(serde::de::Error::custom)?;
        symbols.insert(key, value);
    }
    Ok(symbols)
}

impl SymbolSpecificationConfig {
    /// Create default configuration for the specified environment
    pub fn for_environment(environment: &Environment) -> Self {
        match environment {
            Environment::Development => Self::development_defaults(),
            Environment::Test => Self::test_defaults(),
            Environment::Production => Self::production_defaults(),
        }
    }

    /// Default symbols for development environment
    fn development_defaults() -> Self {
        let mut symbols = HashMap::new();

        let usdt_asset_id = 1;
        let btc_asset_id = 2;
        let eth_asset_id = 3;

        // BTC/USDT currency exchange pair
        let btc_usdt_market_id = ((usdt_asset_id as u32) << 16) | (btc_asset_id as u32);
        let eth_usdt_market_id = ((usdt_asset_id as u32) << 16) | (eth_asset_id as u32);

        // ETH/USDT currency exchange pair
        symbols.insert(
            eth_usdt_market_id,
            CoreMarketSpecification {
                market_id: eth_usdt_market_id,
                market_type: MarketType::Spot,
                base_asset: eth_asset_id,
                quote_asset: usdt_asset_id,
                base_scale_k: 100_000,
                quote_scale_k: 10,
                base_native_scale: 1_000_000_000_000_000_000, // ETH has 18 decimals
                quote_native_scale: 1_000_000, // USDT has 6 decimals
                taker_fee: 0,
                maker_fee: 0,
                slippage: 150, // 1.5%
            },
        );

        // BTC/USDT futures contract
        symbols.insert(
            btc_usdt_market_id,
            CoreMarketSpecification {
                market_id: btc_usdt_market_id,
                market_type: MarketType::FuturesContract,
                base_asset: btc_asset_id,
                quote_asset: usdt_asset_id,
                base_scale_k: 1,
                quote_scale_k: 1,
                base_native_scale: 100_000_000, // BTC has 8 decimals
                quote_native_scale: 1_000_000, // USDT has 6 decimals
                taker_fee: 0,
                maker_fee: 0,
                slippage: 150, // 1.5%
            },
        );

        Self { symbols }
    }

    /// Default symbols for test environment
    fn test_defaults() -> Self {
        let mut symbols = HashMap::new();

        let usdt_asset_id = 1;
        let btc_asset_id = 2;
        let eth_asset_id = 3;

        // BTC/USDT currency exchange pair
        let btc_usdt_market_id = ((usdt_asset_id as u32) << 16) | (btc_asset_id as u32);
        let eth_usdt_market_id = ((usdt_asset_id as u32) << 16) | (eth_asset_id as u32);

        // ETH/USDT currency exchange pair
        symbols.insert(
            eth_usdt_market_id,
            CoreMarketSpecification {
                market_id: eth_usdt_market_id,
                market_type: MarketType::Spot,
                base_asset: eth_asset_id,
                quote_asset: usdt_asset_id,
                base_scale_k: 100_000,
                quote_scale_k: 10,
                base_native_scale: 1_000_000_000_000_000_000, // ETH has 18 decimals
                quote_native_scale: 1_000_000, // USDT has 6 decimals
                taker_fee: 0,
                maker_fee: 0,
                slippage: 150, // 1.5%
            },
        );

        // BTC/USDT futures contract
        symbols.insert(
            btc_usdt_market_id,
            CoreMarketSpecification {
                market_id: btc_usdt_market_id,
                market_type: MarketType::FuturesContract,
                base_asset: btc_asset_id,
                quote_asset: usdt_asset_id,
                base_scale_k: 1,
                quote_scale_k: 1,
                base_native_scale: 100_000_000, // BTC has 8 decimals
                quote_native_scale: 1_000_000, // USDT has 6 decimals
                taker_fee: 0,
                maker_fee: 0,
                slippage: 150, // 1.5%
            },
        );

        Self { symbols }
    }

    /// Default symbols for production environment (empty by default)
    fn production_defaults() -> Self {
        Self {
            symbols: HashMap::new(),
        }
    }

    /// Validate the symbol configuration
    pub fn validate(&self) -> Result<()> {
        for (market_id, spec) in &self.symbols {
            // market id must be quote_asset << 16 | base_asset
            // this is absolutely necessary for the matching engine to work correctly
            if (spec.quote_asset as u32) << 16 | (spec.base_asset as u32) != *market_id {
                return Err(ConfigError::ValidationError(format!(
                    "Symbol ID {market_id} does not match quote_asset << 16 | base_asset = {}",
                    ((spec.quote_asset as u32) << 16) | spec.base_asset as u32
                )));
            }

            // Validate market_id matches the key
            if *market_id != spec.market_id {
                return Err(ConfigError::ValidationError(format!(
                    "Symbol ID mismatch: key {} != spec.market_id {}",
                    market_id, spec.market_id
                )));
            }

            // Validate scale factors are non-zero
            if spec.base_scale_k == 0 {
                return Err(ConfigError::ValidationError(format!(
                    "Symbol {market_id}: base_scale_k cannot be zero"
                )));
            }

            if spec.quote_scale_k == 0 {
                return Err(ConfigError::ValidationError(format!(
                    "Symbol {market_id}: quote_scale_k cannot be zero"
                )));
            }

            // Validate fees
            if spec.taker_fee < spec.maker_fee {
                return Err(ConfigError::ValidationError(format!(
                    "Symbol {}: taker_fee ({}) should be >= maker_fee ({})",
                    market_id, spec.taker_fee, spec.maker_fee
                )));
            }
        }

        Ok(())
    }

    /// Merge configuration with another config, with the other config taking precedence
    pub fn merge_with(&mut self, other: &Self) -> Result<()> {
        // Extend symbols, with other taking precedence
        self.symbols.extend(other.symbols.clone());
        Ok(())
    }

    /// Get symbol specification by ID
    pub fn get_symbol(&self, market_id: u32) -> Option<&CoreMarketSpecification> {
        self.symbols.get(&market_id)
    }

    /// Get all symbol IDs
    pub fn get_market_ids(&self) -> Vec<u32> {
        self.symbols.keys().copied().collect()
    }

    /// Add a new symbol specification
    pub fn add_symbol(&mut self, spec: CoreMarketSpecification) -> Result<()> {
        // Validate the specification first
        let temp_config = SymbolSpecificationConfig {
            symbols: [(spec.market_id, spec.clone())].into_iter().collect(),
        };
        temp_config.validate()?;

        self.symbols.insert(spec.market_id, spec);
        Ok(())
    }

    /// Remove a symbol specification
    pub fn remove_symbol(&mut self, market_id: u32) -> Option<CoreMarketSpecification> {
        self.symbols.remove(&market_id)
    }

    /// Check if configuration is empty
    pub fn is_empty(&self) -> bool {
        self.symbols.is_empty()
    }

    /// Get the number of configured symbols
    pub fn len(&self) -> usize {
        self.symbols.len()
    }
}

impl Default for SymbolSpecificationConfig {
    fn default() -> Self {
        Self::for_environment(&Environment::Development)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_development_defaults() {
        let config = SymbolSpecificationConfig::development_defaults();
        assert!(!config.symbols.is_empty());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_test_defaults() {
        let config = SymbolSpecificationConfig::test_defaults();
        assert!(!config.symbols.is_empty());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_production_defaults() {
        let config = SymbolSpecificationConfig::production_defaults();
        assert!(config.symbols.is_empty());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validation_market_id_mismatch() {
        let mut symbols = HashMap::new();
        let spec = CoreMarketSpecification {
            market_id: 0,
            ..Default::default()
        };
        symbols.insert(456, spec); // Mismatch: key 456 != spec.market_id 123

        let config = SymbolSpecificationConfig { symbols };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_zero_scale() {
        let mut symbols = HashMap::new();
        let spec = CoreMarketSpecification {
            market_id: 123,
            base_scale_k: 0,
            ..Default::default()
        }; // Invalid: base_scale_k = 0
        symbols.insert(123, spec);

        let config = SymbolSpecificationConfig { symbols };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_taker_fee_less_than_maker() {
        let mut symbols = HashMap::new();
        let spec = CoreMarketSpecification {
            market_id: 123,
            base_scale_k: 1,
            quote_scale_k: 1,
            taker_fee: 100,
            maker_fee: 200,
            ..Default::default()
        }; // Invalid: taker < maker
        symbols.insert(123, spec);

        let config = SymbolSpecificationConfig { symbols };
        assert!(config.validate().is_err());
    }
}
