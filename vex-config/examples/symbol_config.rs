//! Example demonstrating symbol specification configuration
//!
//! This example shows how to:
//! 1. Load symbol configurations from TOML files
//! 2. Create symbol specifications programmatically
//! 3. Validate and merge symbol configurations

use common::CoreMarketSpecification;
use common::MarketType;
use vex_config::{Environment, SymbolSpecificationConfig, VexConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();

    println!("=== Symbol Configuration Example ===\n");

    // Example 1: Create default configuration with built-in symbols
    println!("1. Creating default development configuration:");
    let dev_config = VexConfig::new(Environment::Development);
    println!(
        "   Loaded {} symbols for development environment",
        dev_config.symbols.len()
    );
    for market_id in dev_config.symbols.get_market_ids() {
        if let Some(spec) = dev_config.symbols.get_symbol(market_id) {
            println!(
                "   Symbol {}: {:?} ({}/{})",
                market_id, spec.market_type, spec.base_currency, spec.quote_currency
            );
        }
    }

    // Example 2: Load configuration from TOML file
    println!("\n2. Loading configuration from TOML file:");
    let config_path = "examples/symbols.toml";
    match VexConfig::load_from_file(config_path) {
        Ok(config) => {
            println!("   Successfully loaded configuration from {}", config_path);
            println!("   Environment: {}", config.environment);
            println!("   Symbols configured: {}", config.symbols.len());
        }
        Err(e) => {
            println!("   Failed to load from {}: {}", config_path, e);
            println!("   (This is expected if the file doesn't exist)");
        }
    }

    // Example 3: Create custom symbol configuration
    println!("\n3. Creating custom symbol configuration:");
    let mut custom_symbols = SymbolSpecificationConfig::default();

    // Add a custom BTC/USD pair
    let btc_usd_spec = CoreMarketSpecification {
        market_id: 1001,
        market_type: MarketType::CurrencyExchangePair,
        base_currency: 3762,   // BTC (satoshi)
        quote_currency: 840,   // USD
        base_scale_k: 100_000, // 1 lot = 0.001 BTC
        quote_scale_k: 100,    // 1 step = $0.01
        taker_fee: 25,         // 0.25 USD per lot
        maker_fee: 10,         // 0.10 USD per lot
    };

    custom_symbols.add_symbol(btc_usd_spec)?;
    println!("   Added custom BTC/USD symbol (ID: 1001)");

    // Example 4: Validate configuration
    println!("\n4. Validating symbol configuration:");
    match custom_symbols.validate() {
        Ok(()) => println!("   ✓ Configuration is valid"),
        Err(e) => println!("   ✗ Configuration validation failed: {}", e),
    }

    // Example 5: Create full VEX configuration with custom symbols
    println!("\n5. Creating complete VEX configuration:");
    let mut full_config = VexConfig::new(Environment::Development);
    full_config.symbols.merge_with(&custom_symbols)?;

    println!(
        "   Total symbols after merge: {}",
        full_config.symbols.len()
    );
    println!(
        "   Configuration validation: {}",
        if full_config.validate().is_ok() {
            "✓ Valid"
        } else {
            "✗ Invalid"
        }
    );

    // Example 6: Save configuration to file
    println!("\n6. Saving configuration to file:");
    let output_path = "/tmp/vex_config_example.toml";
    match full_config.save_to_file(output_path) {
        Ok(()) => println!("   ✓ Configuration saved to {}", output_path),
        Err(e) => println!("   ✗ Failed to save configuration: {}", e),
    }

    println!("\n=== Example completed successfully ===");
    Ok(())
}
