//! Symbol specifications configuration
//! 
//! This module provides configuration management for trading symbols and their specifications.
//! It supports loading symbols from configuration files and provides validation and management.

use serde::{Deserialize, Serialize};
use hashbrown::HashMap;
use common::model::symbol_specification::CoreSymbolSpecification;
use common::model::enums::SymbolType;
use crate::{Environment, ConfigError, Result};

/// Configuration for trading symbols
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolSpecificationConfig {
    /// Map of symbol_id to symbol specification
    pub symbols: HashMap<u32, CoreSymbolSpecification>,
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

        // ETH/XBT currency exchange pair
        symbols.insert(
            9269,
            CoreSymbolSpecification {
                symbol_id: 9269,
                symbol_type: SymbolType::CurrencyExchangePair,
                base_currency: 3928, // ETH (szabo)
                quote_currency: 3762, // XBT (satoshi)
                base_scale_k: 100_000, // 1 lot = 100K szabo (0.1 ETH)
                quote_scale_k: 10, // 1 step = 10 satoshi
                taker_fee: 0,
                maker_fee: 0,
                margin_buy: 0,
                margin_sell: 0,
            }
        );

        // EUR/USD futures contract
        symbols.insert(
            5991,
            CoreSymbolSpecification {
                symbol_id: 5991,
                symbol_type: SymbolType::FuturesContract,
                base_currency: 978, // EUR
                quote_currency: 840, // USD
                base_scale_k: 1,
                quote_scale_k: 1,
                taker_fee: 0,
                maker_fee: 0,
                margin_buy: 2200,
                margin_sell: 3210,
            }
        );

        Self { symbols }
    }

    /// Default symbols for test environment
    fn test_defaults() -> Self {
        let mut symbols = HashMap::new();

        // XBT/LTC currency exchange pair with fees
        symbols.insert(
            9340,
            CoreSymbolSpecification {
                symbol_id: 9340,
                symbol_type: SymbolType::CurrencyExchangePair,
                base_currency: 3762, // XBT (satoshi)
                quote_currency: 1005, // LTC (litoshi)
                base_scale_k: 1_000_000, // 1 lot = 1M satoshi (0.01 BTC)
                quote_scale_k: 10_000, // 1 step = 10K litoshi
                taker_fee: 1900, // taker fee 1900 litoshi per 1 lot
                maker_fee: 700, // maker fee 700 litoshi per 1 lot
                margin_buy: 0,
                margin_sell: 0,
            }
        );

        // Include development symbols as well for testing
        let dev_config = Self::development_defaults();
        symbols.extend(dev_config.symbols);

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
        for (symbol_id, spec) in &self.symbols {
            // Validate symbol_id matches the key
            if *symbol_id != spec.symbol_id {
                return Err(ConfigError::ValidationError(
                    format!("Symbol ID mismatch: key {} != spec.symbol_id {}", symbol_id, spec.symbol_id)
                ));
            }

            // Validate scale factors are non-zero
            if spec.base_scale_k == 0 {
                return Err(ConfigError::ValidationError(
                    format!("Symbol {}: base_scale_k cannot be zero", symbol_id)
                ));
            }

            if spec.quote_scale_k == 0 {
                return Err(ConfigError::ValidationError(
                    format!("Symbol {}: quote_scale_k cannot be zero", symbol_id)
                ));
            }

            // Validate fees
            if spec.taker_fee < spec.maker_fee {
                return Err(ConfigError::ValidationError(
                    format!("Symbol {}: taker_fee ({}) should be >= maker_fee ({})", 
                        symbol_id, spec.taker_fee, spec.maker_fee)
                ));
            }

            // Validate margin requirements for futures contracts
            if spec.symbol_type == SymbolType::FuturesContract {
                if spec.margin_buy == 0 || spec.margin_sell == 0 {
                    return Err(ConfigError::ValidationError(
                        format!("Symbol {}: futures contract must have non-zero margin requirements", symbol_id)
                    ));
                }
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
    pub fn get_symbol(&self, symbol_id: u32) -> Option<&CoreSymbolSpecification> {
        self.symbols.get(&symbol_id)
    }

    /// Get all symbol IDs
    pub fn get_symbol_ids(&self) -> Vec<u32> {
        self.symbols.keys().copied().collect()
    }

    /// Add a new symbol specification
    pub fn add_symbol(&mut self, spec: CoreSymbolSpecification) -> Result<()> {
        // Validate the specification first
        let temp_config = SymbolSpecificationConfig {
            symbols: [(spec.symbol_id, spec.clone())].into_iter().collect(),
        };
        temp_config.validate()?;

        self.symbols.insert(spec.symbol_id, spec);
        Ok(())
    }

    /// Remove a symbol specification
    pub fn remove_symbol(&mut self, symbol_id: u32) -> Option<CoreSymbolSpecification> {
        self.symbols.remove(&symbol_id)
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
    fn test_validation_symbol_id_mismatch() {
        let mut symbols = HashMap::new();
        let mut spec = CoreSymbolSpecification::default();
        spec.symbol_id = 123;
        symbols.insert(456, spec); // Mismatch: key 456 != spec.symbol_id 123

        let config = SymbolSpecificationConfig { symbols };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_zero_scale() {
        let mut symbols = HashMap::new();
        let mut spec = CoreSymbolSpecification::default();
        spec.symbol_id = 123;
        spec.base_scale_k = 0; // Invalid: zero scale
        symbols.insert(123, spec);

        let config = SymbolSpecificationConfig { symbols };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_taker_fee_less_than_maker() {
        let mut symbols = HashMap::new();
        let mut spec = CoreSymbolSpecification::default();
        spec.symbol_id = 123;
        spec.base_scale_k = 1;
        spec.quote_scale_k = 1;
        spec.taker_fee = 100;
        spec.maker_fee = 200; // Invalid: taker < maker
        symbols.insert(123, spec);

        let config = SymbolSpecificationConfig { symbols };
        assert!(config.validate().is_err());
    }
}
