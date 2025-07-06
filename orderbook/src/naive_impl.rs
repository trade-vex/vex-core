use crate::events::EventHelper;
use crate::{MatcherTradeEvent, OrderBook, OrderBookError, OrderBookImplType, OrderCommand};
use common::model::enums::{OrderAction, OrderType, SymbolType};
use common::model::order::{Order, OrderTrait};
use common::model::symbol_specification::CoreSymbolSpecification;

use borsh::{BorshDeserialize, BorshSerialize};
use hashbrown::HashMap;
use hashlink::LinkedHashMap;
use std::collections::BTreeMap;
use tracing::warn;

#[derive(Clone)]
pub struct OrdersBucketNaive {
    price: i64,
    entries: LinkedHashMap<i64, Order>,
    total_volume: i64,
}

impl BorshSerialize for OrdersBucketNaive {
    fn serialize<W: borsh::io::Write>(&self, writer: &mut W) -> Result<(), borsh::io::Error> {
        self.price.serialize(writer)?;
        self.total_volume.serialize(writer)?;
        (self.entries.len() as u32).serialize(writer)?;
        for (k, v) in self.entries.iter() {
            k.serialize(writer)?;
            v.serialize(writer)?;
        }
        Ok(())
    }
}

impl BorshDeserialize for OrdersBucketNaive {
    fn deserialize_reader<R: borsh::io::Read>(reader: &mut R) -> Result<Self, borsh::io::Error> {
        let price = i64::deserialize_reader(reader)?;
        let total_volume = i64::deserialize_reader(reader)?;
        let len = u32::deserialize_reader(reader)?;
        let mut entries = LinkedHashMap::with_capacity(len as usize);
        for _ in 0..len {
            let k = i64::deserialize_reader(reader)?;
            let v = Order::deserialize_reader(reader)?;
            entries.insert(k, v);
        }
        Ok(OrdersBucketNaive {
            price,
            entries,
            total_volume,
        })
    }
}

impl OrdersBucketNaive {
    pub fn new(price: i64) -> Self {
        Self {
            price,
            entries: LinkedHashMap::new(),
            total_volume: 0,
        }
    }

    pub fn put(&mut self, order: &Order) {
        self.entries.insert(order.order_id, *order);
        self.total_volume += order.size - order.filled;
    }

    pub fn remove(&mut self, order_id: i64, uid: i64) -> Option<Order> {
        if let Some(order) = self.entries.get(&order_id) {
            if order.uid == uid {
                let removed_order = self.entries.remove(&order_id).unwrap();
                self.total_volume -= removed_order.size - removed_order.filled;
                return Some(removed_order);
            }
        }
        None
    }

    pub fn match_order(
        &mut self,
        cmd: &OrderCommand,
        mut volume_to_collect: i64,
        symbol_spec: &CoreSymbolSpecification,
    ) -> (i64, Vec<Box<MatcherTradeEvent>>, Vec<i64>, Vec<Order>) {
        let mut total_matching_volume = 0;
        let mut orders_to_remove = Vec::new();
        let mut partially_matched_orders = Vec::new();
        let mut events = Vec::new();

        let orders_to_process: Vec<_> = self.entries.iter_mut().collect();

        for (order_id, maker_order) in orders_to_process {
            if volume_to_collect <= 0 {
                break;
            }

            let trade_size =
                std::cmp::min(volume_to_collect, maker_order.size - maker_order.filled);
            if trade_size > 0 {
                total_matching_volume += trade_size;
                maker_order.filled += trade_size;
                volume_to_collect -= trade_size;
                self.total_volume -= trade_size;

                let maker_filled = maker_order.filled == maker_order.size;

                let trade_event = EventHelper::create_trade_event(
                    cmd,
                    *order_id,
                    maker_order.uid,
                    maker_filled,
                    maker_order.price,
                    trade_size,
                    symbol_spec,
                );
                events.push(trade_event);

                if maker_filled {
                    orders_to_remove.push(*order_id);
                } else {
                    partially_matched_orders.push(*maker_order);
                }
            }
        }

        for order_id in &orders_to_remove {
            self.entries.remove(order_id);
        }

        (
            total_matching_volume,
            events,
            orders_to_remove,
            partially_matched_orders,
        )
    }

    pub fn get_num_orders(&self) -> i32 {
        self.entries.len() as i32
    }

    pub fn get_total_volume(&self) -> i64 {
        self.total_volume
    }

    pub fn reduce_size(&mut self, reduce_by: i64) {
        self.total_volume -= reduce_by;
    }
}

#[derive(Clone, BorshDeserialize, BorshSerialize)]
pub struct OrderBookNaiveImpl {
    ask_buckets: BTreeMap<i64, OrdersBucketNaive>,
    bid_buckets: BTreeMap<i64, OrdersBucketNaive>,
    order_id_map: HashMap<i64, Order>,
    symbol_spec: CoreSymbolSpecification,
}

impl OrderBookNaiveImpl {
    pub fn new(symbol_spec: CoreSymbolSpecification) -> Self {
        Self {
            ask_buckets: BTreeMap::new(),
            bid_buckets: BTreeMap::new(),
            order_id_map: HashMap::new(),
            symbol_spec,
        }
    }

    pub fn from_bytes(bytes: &mut &[u8]) -> Result<Self, borsh::io::Error> {
        Self::deserialize(bytes)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, borsh::io::Error> {
        let mut writer = Vec::new();
        self.serialize(&mut writer)?;
        Ok(writer)
    }

    fn get_buckets_mut(&mut self, action: OrderAction) -> &mut BTreeMap<i64, OrdersBucketNaive> {
        match action {
            OrderAction::Ask => &mut self.ask_buckets,
            OrderAction::Bid => &mut self.bid_buckets,
        }
    }

    fn new_order_place_gtc(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        let filled = self.try_match(cmd, 0);

        let remaining_size = cmd.size - filled;
        if remaining_size <= 0 {
            return Ok(());
        }

        if self.order_id_map.contains_key(&cmd.order_id) {
            warn!("duplicate order id: {}", cmd.order_id);
            EventHelper::attach_reject_event(cmd, remaining_size);
            return Err(OrderBookError::DuplicateOrderId);
        }

        let order = Order {
            order_id: cmd.order_id,
            price: cmd.price,
            size: cmd.size,
            filled,
            reserve_bid_price: cmd.reserve_bid_price,
            action: cmd.action,
            uid: cmd.uid,
            timestamp: cmd.timestamp,
        };

        let buckets = if order.action == OrderAction::Ask {
            &mut self.ask_buckets
        } else {
            &mut self.bid_buckets
        };

        let bucket = buckets
            .entry(order.price)
            .or_insert_with(|| OrdersBucketNaive::new(order.price));
        bucket.put(&order);
        self.order_id_map.insert(order.order_id, order);

        Ok(())
    }

    fn try_match(&mut self, cmd: &mut OrderCommand, mut filled: i64) -> i64 {
        let mut remaining_size = cmd.size - filled;
        if remaining_size <= 0 {
            return filled;
        }

        let price = cmd.price;
        let action = cmd.action;

        let (matching_buckets, keys_to_iterate): (&mut BTreeMap<i64, OrdersBucketNaive>, Vec<i64>) =
            if action == OrderAction::Bid {
                let keys: Vec<_> = self
                    .ask_buckets
                    .keys()
                    .filter(|&&p| p <= price)
                    .cloned()
                    .collect();
                (&mut self.ask_buckets, keys)
            } else {
                let keys: Vec<_> = self
                    .bid_buckets
                    .keys()
                    .rev()
                    .filter(|&&p| p >= price)
                    .cloned()
                    .collect();
                (&mut self.bid_buckets, keys)
            };

        let mut buckets_to_remove = Vec::new();

        for bucket_price in keys_to_iterate {
            if remaining_size <= 0 {
                break;
            }

            if let Some(bucket) = matching_buckets.get_mut(&bucket_price) {
                let (matched_volume, events, removed_orders, partially_matched) =
                    bucket.match_order(cmd, remaining_size, &self.symbol_spec);

                if matched_volume > 0 {
                    filled += matched_volume;
                    remaining_size -= matched_volume;

                    for event in events {
                        cmd.attach_matcher_event(event);
                    }

                    for order_id in removed_orders {
                        self.order_id_map.remove(&order_id);
                    }

                    for order in partially_matched {
                        self.order_id_map.insert(order.order_id, order);
                    }
                }

                if bucket.get_total_volume() == 0 {
                    buckets_to_remove.push(bucket_price);
                }
            }
        }

        for price in buckets_to_remove {
            matching_buckets.remove(&price);
        }

        filled
    }
}

impl<'a> OrderBook<'a> for OrderBookNaiveImpl {
    fn new_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        match cmd.order_type {
            OrderType::Gtc => self.new_order_place_gtc(cmd),
            OrderType::Ioc | OrderType::FokBudget => {
                let filled = self.try_match(cmd, 0);
                if filled < cmd.size {
                    EventHelper::attach_reject_event(cmd, cmd.size - filled);
                }
                Ok(())
            }
            _ => {
                warn!("Unsupported order type: {:?}", cmd.order_type);
                EventHelper::attach_reject_event(cmd, cmd.size());
                Err(OrderBookError::UnsupportedCommand)
            }
        }
    }

    fn cancel_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        if let Some(order) = self.order_id_map.get(&cmd.order_id).cloned() {
            if order.uid != cmd.uid {
                return Err(OrderBookError::UnknownOrderId);
            }
            let buckets = if order.action == OrderAction::Ask {
                &mut self.ask_buckets
            } else {
                &mut self.bid_buckets
            };

            let mut remove_bucket = false;
            if let Some(bucket) = buckets.get_mut(&order.price) {
                bucket.remove(order.order_id, order.uid);
                if bucket.get_num_orders() == 0 {
                    remove_bucket = true;
                }
            }

            if remove_bucket {
                buckets.remove(&order.price);
            }

            self.order_id_map.remove(&order.order_id);
            cmd.attach_matcher_event(EventHelper::send_reduce_event(
                &order,
                order.size - order.filled,
                true,
            ));
            Ok(())
        } else {
            Err(OrderBookError::UnknownOrderId)
        }
    }

    fn reduce_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        let order_id = cmd.order_id;
        let requested_reduce_size = cmd.size;

        if cmd.size <= 0 {
            return Err(OrderBookError::ReduceFailedWrongSize);
        }

        // Get order info first
        let order = *self
            .order_id_map
            .get(&order_id)
            .ok_or(OrderBookError::UnknownOrderId)?;

        if order.uid != cmd.uid {
            return Err(OrderBookError::UnknownOrderId);
        }

        let remaining_size = order.size - order.filled;
        let reduce_by = std::cmp::min(remaining_size, requested_reduce_size);

        // Update bucket volume and entry size
        if reduce_by == remaining_size {
            // Remove the order completely
            {
                let buckets = self.get_buckets_mut(order.action);

                let bucket = buckets.get_mut(&order.price).unwrap_or_else(|| {
                    panic!(
                        "Can not find bucket for order price={} for order {:?}",
                        order.price, order
                    );
                });

                bucket.remove(order_id, cmd.uid);
                if bucket.get_total_volume() == 0 {
                    buckets.remove(&order.price);
                }
            }
            self.order_id_map.remove(&order_id);
        } else {
            // Reduce the order size
            {
                let buckets = self.get_buckets_mut(order.action);

                let bucket = buckets.get_mut(&order.price).unwrap_or_else(|| {
                    panic!(
                        "Can not find bucket for order price={} for order {:?}",
                        order.price, order
                    );
                });

                bucket.reduce_size(reduce_by);

                // Update the order size in the bucket's entries
                if let Some(bucket_order) = bucket.entries.get_mut(&order_id) {
                    bucket_order.size -= reduce_by;
                }
            }

            // Update the order size in the order_id_map
            if let Some(order_map_entry) = self.order_id_map.get_mut(&order_id) {
                order_map_entry.size -= reduce_by;
            }
        }

        cmd.matcher_event = Some(EventHelper::send_reduce_event(&order, reduce_by, false));
        cmd.action = order.action;

        Ok(())
    }

    fn move_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        let order = self.order_id_map.get(&cmd.order_id).cloned();

        if let Some(mut order) = order {
            if order.uid != cmd.uid {
                return Err(OrderBookError::UnknownOrderId);
            }

            // Reserved price risk check for exchange bids
            if self.symbol_spec.symbol_type == SymbolType::CurrencyExchangePair
                && order.action == OrderAction::Bid
                && cmd.price > order.reserve_bid_price
            {
                return Err(OrderBookError::MoveFailedPriceOverRiskLimit);
            }

            // Remove from old bucket
            let old_buckets = self.get_buckets_mut(order.action);
            if let Some(bucket) = old_buckets.get_mut(&order.price) {
                bucket.remove(order.order_id, order.uid);
                if bucket.get_num_orders() == 0 {
                    old_buckets.remove(&order.price);
                }
            }

            // Update order price
            order.price = cmd.price;

            // Fill action field for events handling
            cmd.action = order.action;

            cmd.size = order.size;

            // Try matching after price change
            let total_filled = self.try_match(cmd, order.filled);
            order.filled = total_filled;

            // If order was fully matched, remove it from order book
            if order.filled == order.size {
                self.order_id_map.remove(&cmd.order_id);
                return Ok(());
            }

            // If not filled completely - put it into corresponding bucket
            let new_buckets = self.get_buckets_mut(order.action);
            let bucket = new_buckets
                .entry(order.price)
                .or_insert_with(|| OrdersBucketNaive::new(order.price));
            bucket.put(&order);

            // Update the order in the map
            self.order_id_map.insert(order.order_id, order);

            Ok(())
        } else {
            Err(OrderBookError::UnknownOrderId)
        }
    }

    fn get_orders_num(&self, action: OrderAction) -> i32 {
        (if action == OrderAction::Ask {
            &self.ask_buckets
        } else {
            &self.bid_buckets
        })
        .values()
        .map(|b| b.get_num_orders())
        .sum()
    }

    fn get_total_orders_volume(&self, action: OrderAction) -> i64 {
        (if action == OrderAction::Ask {
            &self.ask_buckets
        } else {
            &self.bid_buckets
        })
        .values()
        .map(|b| b.get_total_volume())
        .sum()
    }

    fn get_order_by_id(&self, order_id: i64) -> Option<&dyn OrderTrait> {
        self.order_id_map
            .get(&order_id)
            .map(|o| o as &dyn OrderTrait)
    }

    fn find_user_orders(&self, uid: i64) -> Vec<Order> {
        self.order_id_map
            .values()
            .filter(|o| o.uid == uid)
            .cloned()
            .collect()
    }

    fn ask_orders_stream(
        &'a self,
        sorted: bool,
    ) -> Box<dyn Iterator<Item = &'a dyn OrderTrait> + 'a> {
        let iter = self
            .ask_buckets
            .values()
            .flat_map(|bucket| bucket.entries.values())
            .map(|o| o as &dyn OrderTrait);

        if sorted {
            Box::new(iter)
        } else {
            let collected: Vec<_> = iter.collect();
            Box::new(collected.into_iter().rev())
        }
    }

    fn bid_orders_stream(
        &'a self,
        sorted: bool,
    ) -> Box<dyn Iterator<Item = &'a dyn OrderTrait> + 'a> {
        let iter = self
            .bid_buckets
            .values()
            .rev() // Bids are high to low
            .flat_map(|bucket| bucket.entries.values())
            .map(|o| o as &dyn OrderTrait);

        if sorted {
            Box::new(iter)
        } else {
            let collected: Vec<_> = iter.collect();
            Box::new(collected.into_iter().rev())
        }
    }

    fn get_l2_market_data_snapshot(
        &self,
        size: usize,
    ) -> common::model::l2_market_data::L2MarketData {
        let asks_size = self.get_total_ask_buckets(size);
        let bids_size = self.get_total_bid_buckets(size);
        let mut data = common::model::l2_market_data::L2MarketData::with_size(asks_size, bids_size);
        self.fill_asks(asks_size, &mut data);
        self.fill_bids(bids_size, &mut data);
        data
    }

    fn publish_l2_market_data_snapshot(
        &self,
        data: &mut common::model::l2_market_data::L2MarketData,
    ) {
        self.fill_asks(data.ask_prices.capacity(), data);
        self.fill_bids(data.bid_prices.capacity(), data);
    }

    fn fill_asks(&self, size: usize, data: &mut common::model::l2_market_data::L2MarketData) {
        data.ask_prices.clear();
        data.ask_volumes.clear();
        data.ask_orders.clear();

        for (&price, bucket) in self.ask_buckets.iter().take(size) {
            data.ask_prices.push(price);
            data.ask_volumes.push(bucket.get_total_volume());
            data.ask_orders.push(bucket.get_num_orders() as i64);
        }
    }

    fn fill_bids(&self, size: usize, data: &mut common::model::l2_market_data::L2MarketData) {
        data.bid_prices.clear();
        data.bid_volumes.clear();
        data.bid_orders.clear();

        for (&price, bucket) in self.bid_buckets.iter().rev().take(size) {
            data.bid_prices.push(price);
            data.bid_volumes.push(bucket.get_total_volume());
            data.bid_orders.push(bucket.get_num_orders() as i64);
        }
    }

    fn get_total_ask_buckets(&self, limit: usize) -> usize {
        self.ask_buckets.len().min(limit)
    }

    fn get_total_bid_buckets(&self, limit: usize) -> usize {
        self.bid_buckets.len().min(limit)
    }

    fn get_implementation_type(&self) -> OrderBookImplType {
        OrderBookImplType::Naive
    }

    fn get_symbol_spec(&self) -> &CoreSymbolSpecification {
        &self.symbol_spec
    }

    fn validate_internal_state(&self) {
        // No-op for naive implementation
    }
}
