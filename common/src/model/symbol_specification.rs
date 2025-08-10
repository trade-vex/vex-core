use super::enums::SymbolType;
use borsh::{to_vec, BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

/// Core symbol specification that defines trading parameters for a symbol.
/// This mirrors the Java CoreSymbolSpecification class exactly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct CoreSymbolSpecification {
    pub symbol_id: i32,
    pub symbol_type: SymbolType,

    // Currency pair specification
    pub base_currency: i32,  // base currency
    pub quote_currency: i32, // quote/counter currency (OR futures contract currency)
    pub base_scale_k: i64,   // base currency amount multiplier (lot size in base currency units)
    pub quote_scale_k: i64,  // quote currency amount multiplier (step size in quote currency units)

    // Fees per lot in quote currency units
    pub taker_fee: i64, // taker fee (should be >= maker fee)
    pub maker_fee: i64, // maker fee

    // Margin settings (for type=FUTURES_CONTRACT only)
    pub margin_buy: i64,  // buy margin (quote currency)
    pub margin_sell: i64, // sell margin (quote currency)
}

impl CoreSymbolSpecification {
    pub fn builder() -> CoreSymbolSpecificationBuilder {
        CoreSymbolSpecificationBuilder::default()
    }
}

/// Calculate the state hash for this symbol specification
impl Hash for CoreSymbolSpecification {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let encoded = to_vec(self).unwrap();
        state.write(&encoded);
    }
}

/// Builder for CoreSymbolSpecification to match Java's builder pattern
#[derive(Debug, Default)]
pub struct CoreSymbolSpecificationBuilder {
    symbol_id: Option<i32>,
    symbol_type: Option<SymbolType>,
    base_currency: Option<i32>,
    quote_currency: Option<i32>,
    base_scale_k: Option<i64>,
    quote_scale_k: Option<i64>,
    taker_fee: Option<i64>,
    maker_fee: Option<i64>,
    margin_buy: Option<i64>,
    margin_sell: Option<i64>,
}

impl CoreSymbolSpecificationBuilder {
    pub fn symbol_id(mut self, symbol_id: i32) -> Self {
        self.symbol_id = Some(symbol_id);
        self
    }

    pub fn symbol_type(mut self, symbol_type: SymbolType) -> Self {
        self.symbol_type = Some(symbol_type);
        self
    }

    pub fn base_currency(mut self, base_currency: i32) -> Self {
        self.base_currency = Some(base_currency);
        self
    }

    pub fn quote_currency(mut self, quote_currency: i32) -> Self {
        self.quote_currency = Some(quote_currency);
        self
    }

    pub fn base_scale_k(mut self, base_scale_k: i64) -> Self {
        self.base_scale_k = Some(base_scale_k);
        self
    }

    pub fn quote_scale_k(mut self, quote_scale_k: i64) -> Self {
        self.quote_scale_k = Some(quote_scale_k);
        self
    }

    pub fn taker_fee(mut self, taker_fee: i64) -> Self {
        self.taker_fee = Some(taker_fee);
        self
    }

    pub fn maker_fee(mut self, maker_fee: i64) -> Self {
        self.maker_fee = Some(maker_fee);
        self
    }

    pub fn margin_buy(mut self, margin_buy: i64) -> Self {
        self.margin_buy = Some(margin_buy);
        self
    }

    pub fn margin_sell(mut self, margin_sell: i64) -> Self {
        self.margin_sell = Some(margin_sell);
        self
    }

    pub fn build(self) -> Result<CoreSymbolSpecification, &'static str> {
        Ok(CoreSymbolSpecification {
            symbol_id: self.symbol_id.ok_or("symbol_id is required")?,
            symbol_type: self.symbol_type.ok_or("symbol_type is required")?,
            base_currency: self.base_currency.ok_or("base_currency is required")?,
            quote_currency: self.quote_currency.ok_or("quote_currency is required")?,
            base_scale_k: self.base_scale_k.ok_or("base_scale_k is required")?,
            quote_scale_k: self.quote_scale_k.ok_or("quote_scale_k is required")?,
            taker_fee: self.taker_fee.unwrap_or(0),
            maker_fee: self.maker_fee.unwrap_or(0),
            margin_buy: self.margin_buy.unwrap_or(0),
            margin_sell: self.margin_sell.unwrap_or(0),
        })
    }
}

// Test constants to match Java TestConstants
pub struct TestConstants;

impl TestConstants {
    pub const SYMBOL_MARGIN: i32 = 5991;
    pub const SYMBOL_EXCHANGE: i32 = 9269;
    pub const SYMBOL_EXCHANGE_FEE: i32 = 9340;

    pub const CURRENCY_EUR: i32 = 978;
    pub const CURRENCY_USD: i32 = 840;
    pub const CURRENCY_XBT: i32 = 3762; // satoshi, 1E-8
    pub const CURRENCY_ETH: i32 = 3928; // szabo, 1E-6
    pub const CURRENCY_LTC: i32 = 1005; // litoshi, 1E-8

    /// EUR/USD futures contract for margin trading
    pub fn symbol_spec_eur_usd() -> CoreSymbolSpecification {
        CoreSymbolSpecification::builder()
            .symbol_id(Self::SYMBOL_MARGIN)
            .symbol_type(SymbolType::FuturesContract)
            .base_currency(Self::CURRENCY_EUR)
            .quote_currency(Self::CURRENCY_USD)
            .base_scale_k(1)
            .quote_scale_k(1)
            .margin_buy(2200)
            .margin_sell(3210)
            .taker_fee(0)
            .maker_fee(0)
            .build()
            .unwrap()
    }

    /// ETH/XBT currency exchange pair (no fees)
    pub fn symbol_spec_eth_xbt() -> CoreSymbolSpecification {
        CoreSymbolSpecification::builder()
            .symbol_id(Self::SYMBOL_EXCHANGE)
            .symbol_type(SymbolType::CurrencyExchangePair)
            .base_currency(Self::CURRENCY_ETH) // base = szabo
            .quote_currency(Self::CURRENCY_XBT) // quote = satoshi
            .base_scale_k(100_000) // 1 lot = 100K szabo (0.1 ETH)
            .quote_scale_k(10) // 1 step = 10 satoshi
            .taker_fee(0)
            .maker_fee(0)
            .build()
            .unwrap()
    }

    /// XBT/LTC currency exchange pair (with fees)
    pub fn symbol_spec_fee_xbt_ltc() -> CoreSymbolSpecification {
        CoreSymbolSpecification::builder()
            .symbol_id(Self::SYMBOL_EXCHANGE_FEE)
            .symbol_type(SymbolType::CurrencyExchangePair)
            .base_currency(Self::CURRENCY_XBT) // base = satoshi
            .quote_currency(Self::CURRENCY_LTC) // quote = litoshi
            .base_scale_k(1_000_000) // 1 lot = 1M satoshi (0.01 BTC)
            .quote_scale_k(10_000) // 1 step = 10K litoshi
            .taker_fee(1900) // taker fee 1900 litoshi per 1 lot
            .maker_fee(700) // maker fee 700 litoshi per 1 lot
            .build()
            .unwrap()
    }
}
