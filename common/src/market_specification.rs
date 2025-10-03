use crate::MarketType;
use serde::{Deserialize, Serialize};

/// Core symbol specification that defines trading parameters for a symbol.
/// This mirrors the Java CoreMarketSpecification class exactly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CoreMarketSpecification {
    pub market_id: u32,
    pub market_type: MarketType,

    // Currency pair specification
    pub base_currency: u16,  // base currency
    pub quote_currency: u16, // quote/counter currency (OR futures contract currency)
    pub base_scale_k: u64,   // base currency amount multiplier (lot size in base currency units)
    pub quote_scale_k: u64,  // quote currency amount multiplier (step size in quote currency units)

    // Fees per lot in quote currency units
    pub taker_fee: u64, // taker fee (should be >= maker fee)
    pub maker_fee: u64, // maker fee

    // slippage, in terms of 100x of a percent (e.g. 150 = 1.5%)
    pub slippage: u32,
}

impl CoreMarketSpecification {
    pub fn builder() -> CoreMarketSpecificationBuilder {
        CoreMarketSpecificationBuilder::default()
    }
}

/// Builder for CoreMarketSpecification to match Java's builder pattern
#[derive(Debug, Default)]
pub struct CoreMarketSpecificationBuilder {
    market_id: Option<u32>,
    market_type: Option<MarketType>,
    base_currency: Option<u16>,
    quote_currency: Option<u16>,
    base_scale_k: Option<u64>,
    quote_scale_k: Option<u64>,
    taker_fee: Option<u64>,
    maker_fee: Option<u64>,
    slippage: Option<u32>,
}

impl CoreMarketSpecificationBuilder {
    pub fn market_id(mut self, market_id: u32) -> Self {
        self.market_id = Some(market_id);
        self.base_currency = Some(base_asset(market_id));
        self.quote_currency = Some(quote_asset(market_id));
        self
    }

    pub fn market_type(mut self, market_type: MarketType) -> Self {
        self.market_type = Some(market_type);
        self
    }

    pub fn base_scale_k(mut self, base_scale_k: u64) -> Self {
        self.base_scale_k = Some(base_scale_k);
        self
    }

    pub fn quote_scale_k(mut self, quote_scale_k: u64) -> Self {
        self.quote_scale_k = Some(quote_scale_k);
        self
    }

    pub fn taker_fee(mut self, taker_fee: u64) -> Self {
        self.taker_fee = Some(taker_fee);
        self
    }

    pub fn maker_fee(mut self, maker_fee: u64) -> Self {
        self.maker_fee = Some(maker_fee);
        self
    }

    pub fn slippage(mut self, slippage: u32) -> Self {
        self.slippage = Some(slippage);
        self
    }

    pub fn build(self) -> Result<CoreMarketSpecification, &'static str> {
        Ok(CoreMarketSpecification {
            market_id: self.market_id.ok_or("market_id is required")?,
            market_type: self.market_type.ok_or("market_type is required")?,
            base_currency: self.base_currency.ok_or("base_currency is required")?,
            quote_currency: self.quote_currency.ok_or("quote_currency is required")?,
            base_scale_k: self.base_scale_k.unwrap_or(1),
            quote_scale_k: self.quote_scale_k.unwrap_or(1),
            taker_fee: self.taker_fee.unwrap_or(0),
            maker_fee: self.maker_fee.unwrap_or(0),
            slippage: self.slippage.unwrap_or(150), // 1.5% by default
        })
    }
}

/// Market ID Specification helper functions
///
/// Base And Quote asset extraction from market_id
/// The Base asset is stored in the lower 16 bits of the market_id
/// The Quote asset is stored in the upper 16 bits of the market_id
#[inline]
pub fn base_asset(market_id: u32) -> u16 {
    (market_id & 0xFFFF) as u16
}

#[inline]
pub fn quote_asset(market_id: u32) -> u16 {
    (market_id >> 16) as u16
}
