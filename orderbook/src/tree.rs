// --- Abstraction for a side of the order book ---

use crate::PriceLevel;
use std::cmp::Reverse;
use std::collections::BTreeMap;

/// A trait for one side of the order book (bids or asks).
/// This allows us to swap out the underlying data structure (e.g., BTreeMap, custom tree).
pub trait BookSide {
    /// Gets a mutable reference to a price level.
    fn get_level_mut(&mut self, price: u64) -> Option<&mut PriceLevel>;

    /// Gets or creates a new price level.
    fn get_or_create_level(&mut self, price: u64) -> &mut PriceLevel;

    /// Removes a price level if it has no more orders.
    fn remove_level_if_empty(&mut self, price: u64);

    /// Provides a mutable iterator over price levels in matching priority.
    /// For asks, this is ascending price. For bids, it's descending price.
    fn iter_mut_for_matching<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = (u64, &'a mut PriceLevel)> + 'a>;

    /// Provides an iterator over price levels for generating L2 market data.
    fn iter_for_l2<'a>(&'a self) -> Box<dyn Iterator<Item = (u64, &'a PriceLevel)> + 'a>;

    /// Provides an iterator over price levels for getting prices and volumes
    fn iter(&self) -> Box<dyn Iterator<Item = (u64, &PriceLevel)> + '_>;

    /// Returns Best Price Available
    fn best_price(&self) -> Option<u64> {
        self.iter().next().map(|(price, _)| price)
    }
}

/// A `BTreeMap`-backed implementation for the Ask side of the book (ascending prices).
pub struct BTreeAskSide {
    tree: BTreeMap<u64, PriceLevel>,
}

impl Default for BTreeAskSide {
    fn default() -> Self {
        Self::new()
    }
}

impl BTreeAskSide {
    pub fn new() -> Self {
        Self {
            tree: BTreeMap::new(),
        }
    }
}

impl BookSide for BTreeAskSide {
    fn get_level_mut(&mut self, price: u64) -> Option<&mut PriceLevel> {
        self.tree.get_mut(&price)
    }

    fn get_or_create_level(&mut self, price: u64) -> &mut PriceLevel {
        self.tree.entry(price).or_insert(PriceLevel::new())
    }

    fn remove_level_if_empty(&mut self, price: u64) {
        if let Some(level) = self.tree.get(&price) {
            if level.orders.is_empty() {
                self.tree.remove(&price);
            }
        }
    }

    fn iter_mut_for_matching<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = (u64, &'a mut PriceLevel)> + 'a> {
        Box::new(self.tree.iter_mut().map(|(price, level)| (*price, level)))
    }

    fn iter_for_l2<'a>(&'a self) -> Box<dyn Iterator<Item = (u64, &'a PriceLevel)> + 'a> {
        Box::new(self.tree.iter().map(|(price, level)| (*price, level)))
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (u64, &'a PriceLevel)> + 'a> {
        Box::new(self.tree.iter().map(|(price, level)| (*price, level)))
    }
}

/// A `BTreeMap`-backed implementation for the Bid side of the book (descending prices).
pub struct BTreeBidSide {
    tree: BTreeMap<Reverse<u64>, PriceLevel>,
}

impl Default for BTreeBidSide {
    fn default() -> Self {
        Self::new()
    }
}

impl BTreeBidSide {
    pub fn new() -> Self {
        Self {
            tree: BTreeMap::new(),
        }
    }
}

impl BookSide for BTreeBidSide {
    fn get_level_mut(&mut self, price: u64) -> Option<&mut PriceLevel> {
        self.tree.get_mut(&Reverse(price))
    }

    fn get_or_create_level(&mut self, price: u64) -> &mut PriceLevel {
        self.tree.entry(Reverse(price)).or_insert(PriceLevel::new())
    }

    fn remove_level_if_empty(&mut self, price: u64) {
        if let Some(level) = self.tree.get(&Reverse(price)) {
            if level.orders.is_empty() {
                self.tree.remove(&Reverse(price));
            }
        }
    }

    fn iter_mut_for_matching<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = (u64, &'a mut PriceLevel)> + 'a> {
        Box::new(self.tree.iter_mut().map(|(price, level)| (price.0, level)))
    }

    fn iter_for_l2<'a>(&'a self) -> Box<dyn Iterator<Item = (u64, &'a PriceLevel)> + 'a> {
        Box::new(self.tree.iter().map(|(price, level)| (price.0, level)))
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (u64, &'a PriceLevel)> + 'a> {
        Box::new(self.tree.iter().map(|(price, level)| (price.0, level)))
    }
}
