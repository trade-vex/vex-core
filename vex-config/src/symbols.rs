//! Symbol specifications configuration
//!
//! This module provides configuration management for trading symbols and their specifications.
//! It supports loading symbols from configuration files and provides validation and management.

use crate::{ConfigError, Environment, Result};
use common::CoreMarketSpecification;
use common::MarketType;
use hashbrown::HashMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::Display;
use std::hash::Hash;
use std::str::FromStr;

/// Half of u64: the largest atomic supply whose every pair of holdings still sums inside u64.
pub const MAX_ATOMIC_SUPPLY: u128 = (u64::MAX / 2) as u128;

/// One listed asset. `native_scale` atomic units make one whole unit (10^decimals); every balance,
/// size and transfer of the asset is a u64 count of atomic units.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetSpec {
    pub native_scale: u64,
    /// Whole units the asset's supply can never reach. Validation only: with
    /// max_supply * native_scale <= u64::MAX / 2 no sum of two ledger values can overflow.
    pub max_supply: u64,
}

impl AssetSpec {
    /// `max_supply` in atomic units.
    pub fn atomic_supply(&self) -> u128 {
        u128::from(self.max_supply) * u128::from(self.native_scale)
    }
}

/// Configuration for listed assets and trading symbols
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolSpecificationConfig {
    /// Map of asset_id to asset specification
    #[serde(
        serialize_with = "serialize_int_keyed",
        deserialize_with = "deserialize_int_keyed"
    )]
    pub assets: HashMap<u16, AssetSpec>,
    /// Map of market_id to symbol specification
    #[serde(
        serialize_with = "serialize_int_keyed",
        deserialize_with = "deserialize_int_keyed"
    )]
    pub symbols: HashMap<u32, CoreMarketSpecification>,
}

// config-rs turns map keys into strings, so integer-keyed maps go through string keys.
fn serialize_int_keyed<S, K, V>(
    map: &HashMap<K, V>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
    K: Display,
    V: Serialize,
{
    serializer.collect_map(map.iter().map(|(key, value)| (key.to_string(), value)))
}

fn deserialize_int_keyed<'de, D, K, V>(
    deserializer: D,
) -> std::result::Result<HashMap<K, V>, D::Error>
where
    D: Deserializer<'de>,
    K: FromStr + Eq + Hash,
    K::Err: Display,
    V: Deserialize<'de>,
{
    HashMap::<String, V>::deserialize(deserializer)?
        .into_iter()
        .map(|(key, value)| {
            key.parse()
                .map(|key| (key, value))
                .map_err(serde::de::Error::custom)
        })
        .collect()
}

fn is_power_of_ten(mut n: u64) -> bool {
    while n > 1 && n.is_multiple_of(10) {
        n /= 10;
    }
    n == 1
}

impl SymbolSpecificationConfig {
    /// Create default configuration for the specified environment
    pub fn for_environment(environment: &Environment) -> Self {
        match environment {
            Environment::Development | Environment::Test => Self::development_defaults(),
            Environment::Production => Self::production_defaults(),
        }
    }

    /// Default symbols for the development and test environments
    fn development_defaults() -> Self {
        let mut symbols = HashMap::new();

        let usdt_asset_id = 1;
        let btc_asset_id = 2;
        let eth_asset_id = 3;
        let assets = HashMap::from([
            (
                usdt_asset_id,
                AssetSpec {
                    native_scale: 1_000_000, // USDT has 6 decimals
                    max_supply: 1_000_000_000_000,
                },
            ),
            (
                btc_asset_id,
                AssetSpec {
                    native_scale: 100_000_000, // BTC has 8 decimals
                    max_supply: 21_000_000,
                },
            ),
            (
                eth_asset_id,
                AssetSpec {
                    native_scale: 1_000_000_000, // ETH in gwei: 18 decimals cannot fit u64
                    max_supply: 1_000_000_000,
                },
            ),
        ]);

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
                quote_scale_k: 10,
                base_native_scale: 1_000_000_000, // ETH in gwei
                quote_native_scale: 1_000_000,    // USDT has 6 decimals
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
                quote_scale_k: 1,
                base_native_scale: 100_000_000, // BTC has 8 decimals
                quote_native_scale: 1_000_000,  // USDT has 6 decimals
                taker_fee: 0,
                maker_fee: 0,
                slippage: 150, // 1.5%
            },
        );

        Self { assets, symbols }
    }

    /// Default symbols for production environment (empty by default)
    fn production_defaults() -> Self {
        Self {
            assets: HashMap::new(),
            symbols: HashMap::new(),
        }
    }

    /// Validate the symbol configuration
    pub fn validate(&self) -> Result<()> {
        for (asset_id, asset) in &self.assets {
            if !is_power_of_ten(asset.native_scale) {
                return Err(ConfigError::ValidationError(format!(
                    "asset {asset_id}: native_scale must be a power of ten (10^decimals), got {}",
                    asset.native_scale
                )));
            }
            if asset.max_supply == 0 {
                return Err(ConfigError::ValidationError(format!(
                    "asset {asset_id}: max_supply cannot be zero"
                )));
            }
            let atomic_supply = asset.atomic_supply();
            if atomic_supply > MAX_ATOMIC_SUPPLY {
                return Err(ConfigError::ValidationError(format!(
                    "asset {asset_id}: max_supply {} * native_scale {} = {atomic_supply} exceeds u64::MAX / 2 ({MAX_ATOMIC_SUPPLY}); drop decimals",
                    asset.max_supply, asset.native_scale
                )));
            }
        }

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
            if spec.quote_scale_k == 0 {
                return Err(ConfigError::ValidationError(format!(
                    "Symbol {market_id}: quote_scale_k cannot be zero"
                )));
            }

            // Settlement debits one asset and credits the other from two separate copies.
            if spec.base_asset == spec.quote_asset {
                return Err(ConfigError::ValidationError(format!(
                    "Symbol {market_id}: base and quote asset are both {}",
                    spec.base_asset
                )));
            }

            for (role, asset_id, scale) in [
                ("base", spec.base_asset, spec.base_native_scale),
                ("quote", spec.quote_asset, spec.quote_native_scale),
            ] {
                let Some(asset) = self.assets.get(&asset_id) else {
                    return Err(ConfigError::ValidationError(format!(
                        "Symbol {market_id}: {role} asset {asset_id} is not in assets"
                    )));
                };
                if scale != asset.native_scale {
                    return Err(ConfigError::ValidationError(format!(
                        "Symbol {market_id}: {role}_native_scale {scale} differs from assets.{asset_id}.native_scale {}",
                        asset.native_scale
                    )));
                }
            }

            // Validate fees
            if spec.taker_fee >= 10000 {
                return Err(ConfigError::ValidationError(format!(
                    "Symbol {}: taker_fee ({}) must be less than 10000 basis points",
                    market_id, spec.taker_fee
                )));
            }

            if spec.maker_fee >= 10000 {
                return Err(ConfigError::ValidationError(format!(
                    "Symbol {}: maker_fee ({}) must be less than 10000 basis points",
                    market_id, spec.maker_fee
                )));
            }

            if spec.taker_fee < spec.maker_fee {
                return Err(ConfigError::ValidationError(format!(
                    "Symbol {}: taker_fee ({}) should be >= maker_fee ({})",
                    market_id, spec.taker_fee, spec.maker_fee
                )));
            }
        }

        Ok(())
    }

    /// Check if configuration is empty
    pub fn is_empty(&self) -> bool {
        self.symbols.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ETH_USDT: u32 = (1 << 16) | 3;

    fn validation_error(config: &SymbolSpecificationConfig) -> String {
        match config.validate() {
            Err(ConfigError::ValidationError(message)) => message,
            other => panic!("expected a validation error, got {other:?}"),
        }
    }

    #[test]
    fn test_development_defaults() {
        let config = SymbolSpecificationConfig::development_defaults();
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
        let mut config = SymbolSpecificationConfig::development_defaults();
        config.symbols.get_mut(&ETH_USDT).unwrap().market_id = 0;

        assert_eq!(
            validation_error(&config),
            format!("Symbol ID mismatch: key {ETH_USDT} != spec.market_id 0")
        );
    }

    #[test]
    fn test_validation_zero_quote_scale_k() {
        let mut config = SymbolSpecificationConfig::development_defaults();
        let spec = config.symbols.values_mut().next().unwrap();
        let market_id = spec.market_id;
        spec.quote_scale_k = 0;

        assert!(matches!(
            config.validate(),
            Err(ConfigError::ValidationError(message))
                if message == format!("Symbol {market_id}: quote_scale_k cannot be zero")
        ));
    }

    #[test]
    fn test_validation_rejects_supply_that_overflows_u64() {
        let mut config = SymbolSpecificationConfig::development_defaults();
        config.assets.insert(
            9,
            AssetSpec {
                native_scale: 1,
                max_supply: u64::MAX / 2,
            },
        );
        assert!(config.validate().is_ok(), "the bound itself is allowed");

        // 18-decimal ETH: 1e9 ETH is 1e27 wei, which no u64 balance can hold.
        config.assets.get_mut(&3).unwrap().native_scale = 1_000_000_000_000_000_000;
        config.symbols.get_mut(&ETH_USDT).unwrap().base_native_scale = 1_000_000_000_000_000_000;

        assert!(validation_error(&config).contains("exceeds u64::MAX / 2"));
    }

    #[test]
    fn test_validation_rejects_non_power_of_ten_scale() {
        for scale in [0, 500] {
            let mut config = SymbolSpecificationConfig::development_defaults();
            config.assets.get_mut(&3).unwrap().native_scale = scale;

            assert_eq!(
                validation_error(&config),
                format!("asset 3: native_scale must be a power of ten (10^decimals), got {scale}")
            );
        }
    }

    #[test]
    fn test_validation_market_scale_must_match_asset() {
        let mut config = SymbolSpecificationConfig::development_defaults();
        config.symbols.get_mut(&ETH_USDT).unwrap().base_native_scale = 100_000_000;

        assert_eq!(
            validation_error(&config),
            format!(
                "Symbol {ETH_USDT}: base_native_scale 100000000 differs from assets.3.native_scale 1000000000"
            )
        );
    }

    #[test]
    fn test_validation_rejects_market_with_unknown_asset() {
        let mut config = SymbolSpecificationConfig::development_defaults();
        config.assets.remove(&3);

        assert_eq!(
            validation_error(&config),
            format!("Symbol {ETH_USDT}: base asset 3 is not in assets")
        );
    }

    #[test]
    fn test_validation_rejects_market_with_one_asset_on_both_sides() {
        let mut config = SymbolSpecificationConfig::development_defaults();
        let market_id = (1 << 16) | 1;
        let mut spec = config.symbols[&ETH_USDT].clone();
        spec.market_id = market_id;
        spec.base_asset = 1;
        config.symbols.insert(market_id, spec);

        assert_eq!(
            validation_error(&config),
            format!("Symbol {market_id}: base and quote asset are both 1")
        );
    }

    #[test]
    fn test_validation_taker_fee_less_than_maker() {
        let mut config = SymbolSpecificationConfig::development_defaults();
        let spec = config.symbols.get_mut(&ETH_USDT).unwrap();
        let taker_fee = spec.taker_fee;
        spec.maker_fee = taker_fee + 1;

        assert_eq!(
            validation_error(&config),
            format!(
                "Symbol {ETH_USDT}: taker_fee ({taker_fee}) should be >= maker_fee ({})",
                taker_fee + 1
            )
        );
    }

    #[test]
    fn test_validation_rejects_fee_at_100_percent() {
        let mut config = SymbolSpecificationConfig::development_defaults();
        let spec = config.symbols.values_mut().next().unwrap();
        let market_id = spec.market_id;
        spec.taker_fee = 10000;

        assert!(matches!(
            config.validate(),
            Err(ConfigError::ValidationError(message))
                if message == format!(
                    "Symbol {market_id}: taker_fee (10000) must be less than 10000 basis points"
                )
        ));
    }
}
