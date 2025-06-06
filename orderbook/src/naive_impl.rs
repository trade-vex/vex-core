use std::collections::{BTreeMap, HashMap};
use common::model::order::{IOrder, Order};
use common::model::enums::{OrderAction, OrderType, MatcherEventType, SymbolType};
use common::model::symbol_specification::CoreSymbolSpecification;
use crate::{OrderBook, OrderBookImplType, OrderCommand, OrderBookError, MatcherTradeEvent, MatcherResult};
use crate::events::EventHelper;
use tracing::warn;
use borsh::{BorshDeserialize, BorshSerialize};

#[derive(BorshSerialize, BorshDeserialize)]
pub struct OrderBookNaiveImpl {
    ask_buckets: BTreeMap<i64, OrdersBucketNaive>,
    bid_buckets: BTreeMap<i64, OrdersBucketNaive>,
    id_map: HashMap<i64, Order>,
    symbol_spec: CoreSymbolSpecification,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct OrdersBucketNaive {
    price: i64,
    entries: Vec<Order>,
    total_volume: i64,
}

impl OrdersBucketNaive {
    pub fn new(price: i64) -> Self {
        Self {
            price,
            entries: Vec::new(),
            total_volume: 0,
        }
    }

    pub fn get_num_orders(&self) -> usize {
        self.entries.len()
    }

    pub fn get_total_volume(&self) -> i64 {
        self.total_volume
    }

    pub fn put(&mut self, order: Order) {
        self.total_volume += order.size() - order.filled();
        self.entries.push(order);
    }

    pub fn remove(&mut self, order_id: i64, uid: i64) -> Option<Order> {
        if let Some(pos) = self.entries.iter().position(|o| o.order_id() == order_id && o.uid() == uid) {
            let order = self.entries.remove(pos);
            self.total_volume -= order.size() - order.filled();
            Some(order)
        } else {
            None
        }
    }

    pub fn match_order(
        &mut self,
        volume_to_collect: i64,
        active_order_cmd: &mut OrderCommand,
        spec: &CoreSymbolSpecification,
    ) -> MatcherResult {
        let mut total_matching_volume = 0;
        let mut orders_to_remove = Vec::new();

        let mut volume_to_collect = volume_to_collect;

        for order in self.entries.iter_mut() {
            if volume_to_collect <= 0 {
                break;
            }

            let v = std::cmp::min(volume_to_collect, order.size() - order.filled());
            total_matching_volume += v;

            order.filled += v;
            volume_to_collect -= v;
            self.total_volume -= v;

            let full_match = order.size() == order.filled();

            let trade_event = MatcherTradeEvent {
                event_type: MatcherEventType::Trade,
                section: 0, // TODO
                active_order_completed: volume_to_collect == 0,
                matched_order_id: order.order_id(),
                matched_order_uid: order.uid(),
                matched_order_completed: full_match,
                price: order.price(),
                size: v,
                bidder_hold_price: if active_order_cmd.action == OrderAction::Ask { active_order_cmd.reserve_bid_price } else { order.reserve_bid_price },
                taker_fee: v * spec.taker_fee,
                maker_fee: v * spec.maker_fee,
                next_event: None,
            };

            active_order_cmd.attach_matcher_event(Box::new(trade_event));

            if full_match {
                orders_to_remove.push(order.order_id());
            }
        }

        self.entries.retain(|o| o.size() != o.filled());

        MatcherResult {
            volume: total_matching_volume,
            orders_to_remove,
        }
    }
}

impl<'a> OrderBook<'a> for OrderBookNaiveImpl {
    fn new_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        match cmd.order_type {
            OrderType::Gtc => self.new_order_place_gtc(cmd),
            OrderType::Ioc => self.new_order_match_ioc(cmd),
            OrderType::FokBudget => self.new_order_match_fok_budget(cmd),
            _ => {
                warn!("Unsupported order type: {:?}", cmd.order_type);
                Err(OrderBookError::UnsupportedCommand)
            }
        }
    }

    fn cancel_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        let order_id = cmd.order_id;
        let uid = cmd.uid;

        let order_to_cancel = self
            .id_map
            .get(&order_id)
            .ok_or(OrderBookError::UnknownOrderId)?;

        if order_to_cancel.uid() != uid {
            return Err(OrderBookError::UnknownOrderId);
        }

        let price = order_to_cancel.price();
        let action = order_to_cancel.action();
        let order_clone = order_to_cancel.clone(); // Clone for event generation

        let buckets = if action == OrderAction::Ask {
            &mut self.ask_buckets
        } else {
            &mut self.bid_buckets
        };

        if let Some(bucket) = buckets.get_mut(&price) {
            if bucket.remove(order_id, uid).is_none() {
                // This should not happen if state is consistent
                return Err(OrderBookError::UnknownOrderId);
            }
            if bucket.get_num_orders() == 0 {
                buckets.remove(&price);
            }
        } else {
            // This should not happen if state is consistent
            return Err(OrderBookError::UnknownOrderId);
        }

        self.id_map.remove(&order_id);

        // Generate reduce event for the full remaining size
        let remaining_size = order_clone.size() - order_clone.filled();
        cmd.matcher_event = Some(Box::new(EventHelper::send_reduce_event(
            &order_clone,
            remaining_size,
            true,
        )));
        cmd.action = action; // fill action for event handling

        Ok(())
    }

    fn reduce_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        let order_id = cmd.order_id;
        let requested_reduce_size = cmd.size;
        let uid = cmd.uid;

        if requested_reduce_size <= 0 {
            return Err(OrderBookError::ReduceFailedWrongSize);
        }

        let order = self
            .id_map
            .get_mut(&order_id)
            .ok_or(OrderBookError::UnknownOrderId)?;

        if order.uid() != uid {
            return Err(OrderBookError::UnknownOrderId);
        }
        cmd.action = order.action;

        let remaining_size = order.size() - order.filled();
        let reduce_by = std::cmp::min(remaining_size, requested_reduce_size);
        let order_clone = order.clone();

        if reduce_by >= remaining_size {
            // Treat as full cancel
            let price = order.price();
            let action = order.action();
            let buckets = if action == OrderAction::Ask {
                &mut self.ask_buckets
            } else {
                &mut self.bid_buckets
            };
            if let Some(bucket) = buckets.get_mut(&price) {
                if bucket.remove(order_id, uid).is_some() {
                    self.id_map.remove(&order_id);
                    if bucket.get_num_orders() == 0 {
                        buckets.remove(&price);
                    }
                    cmd.matcher_event = Some(Box::new(EventHelper::send_reduce_event(
                        &order_clone,
                        remaining_size,
                        true,
                    )));
                } else {
                    return Err(OrderBookError::UnknownOrderId);
                }
            } else {
                return Err(OrderBookError::UnknownOrderId);
            }
        } else {
            // Partial reduce
            let price = order.price();
            let action = order.action();
            let buckets = if action == OrderAction::Ask {
                &mut self.ask_buckets
            } else {
                &mut self.bid_buckets
            };
            if let Some(bucket) = buckets.get_mut(&price) {
                if let Some(bucket_order) = bucket.entries.iter_mut().find(|o| o.order_id() == order_id)
                {
                    bucket_order.size -= reduce_by;
                    bucket.total_volume -= reduce_by;
                    order.size = bucket_order.size;
                    cmd.matcher_event = Some(Box::new(EventHelper::send_reduce_event(
                        &order_clone,
                        reduce_by,
                        false,
                    )));
                } else {
                    return Err(OrderBookError::UnknownOrderId);
                }
            }
        }

        Ok(())
    }

    fn move_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        let order_id = cmd.order_id;
        let new_price = cmd.price;

        let mut order_to_move = self
            .id_map
            .remove(&order_id)
            .ok_or(OrderBookError::UnknownOrderId)?;

        if order_to_move.uid() != cmd.uid {
            self.id_map.insert(order_id, order_to_move);
            return Err(OrderBookError::UnknownOrderId);
        }

        cmd.action = order_to_move.action;

        // Risk check for currency exchange pairs
        if self.symbol_spec.symbol_type == SymbolType::CurrencyExchangePair
            && order_to_move.action() == OrderAction::Bid
            && cmd.price > order_to_move.reserve_bid_price()
        {
            self.id_map.insert(order_id, order_to_move);
            return Err(OrderBookError::MoveFailedPriceOverRiskLimit);
        }

        let buckets = if order_to_move.action() == OrderAction::Ask {
            &mut self.ask_buckets
        } else {
            &mut self.bid_buckets
        };

        if let Some(bucket) = buckets.get_mut(&order_to_move.price()) {
            if bucket.remove(order_id, order_to_move.uid()).is_none() {
                self.id_map.insert(order_id, order_to_move);
                return Err(OrderBookError::UnknownOrderId);
            }
            if bucket.get_num_orders() == 0 {
                buckets.remove(&order_to_move.price());
            }
        } else {
            self.id_map.insert(order_id, order_to_move);
            return Err(OrderBookError::UnknownOrderId);
        }

        // Update price and create a new command to essentially place a new order
        cmd.price = new_price;
        order_to_move.price = new_price;
        cmd.size = order_to_move.size - order_to_move.filled; // only place the remaining size
        cmd.uid = order_to_move.uid;

        // Try to match instantly at the new price
        let filled_size = self.try_match_instantly(cmd, 0);

        let remaining_size_after_match = cmd.size - filled_size;

        if remaining_size_after_match > 0 {
            // Place the rest of the order as a new limit order
            order_to_move.filled += filled_size;

            let new_buckets = if order_to_move.action() == OrderAction::Ask {
                &mut self.ask_buckets
            } else {
                &mut self.bid_buckets
            };

            let new_bucket = new_buckets
                .entry(new_price)
                .or_insert_with(|| OrdersBucketNaive::new(new_price));
            new_bucket.put(order_to_move.clone());
            self.id_map.insert(order_id, order_to_move);
        }
        Ok(())
    }

    fn get_orders_num(&self, action: OrderAction) -> i32 {
        let buckets = if action == OrderAction::Ask {
            &self.ask_buckets
        } else {
            &self.bid_buckets
        };
        buckets.values().map(|bucket| bucket.entries.len()).sum::<usize>() as i32
    }

    fn get_total_orders_volume(&self, action: OrderAction) -> i64 {
        let buckets = if action == OrderAction::Ask {
            &self.ask_buckets
        } else {
            &self.bid_buckets
        };
        buckets.values().map(|bucket| bucket.total_volume).sum()
    }

    fn get_order_by_id(&self, order_id: i64) -> Option<&Order> {
        self.id_map.get(&order_id)
    }

    fn find_user_orders(&self, uid: i64) -> Vec<Order> {
        self.id_map.values()
            .filter(|order| order.uid == uid)
            .cloned()
            .collect()
    }

    fn ask_orders_stream(&'a self, _sorted: bool) -> Box<dyn Iterator<Item = &'a dyn IOrder> + 'a> {
        Box::new(self.ask_buckets.values().flat_map(|bucket| bucket.entries.iter()).map(|o| o as &'a dyn IOrder))
    }

    fn bid_orders_stream(&'a self, _sorted: bool) -> Box<dyn Iterator<Item = &'a dyn IOrder> + 'a> {
        Box::new(self.bid_buckets.values().flat_map(|bucket| bucket.entries.iter()).map(|o| o as &'a dyn IOrder))
    }

    fn get_l2_market_data_snapshot(&self, size: usize) -> common::model::l2_market_data::L2MarketData {
        let mut data = common::model::l2_market_data::L2MarketData::with_size(0, 0);
        self.fill_asks(size, &mut data);
        self.fill_bids(size, &mut data);
        data
    }

    fn publish_l2_market_data_snapshot(&self, data: &mut common::model::l2_market_data::L2MarketData) {
        self.fill_asks(data.ask_prices.capacity(), data);
        self.fill_bids(data.bid_prices.capacity(), data);
    }

    fn fill_asks(&self, size: usize, data: &mut common::model::l2_market_data::L2MarketData) {
        data.ask_prices.clear();
        data.ask_volumes.clear();
        data.ask_orders.clear();

        for (&price, bucket) in self.ask_buckets.iter().take(size) {
            data.ask_prices.push(price);
            data.ask_volumes.push(bucket.total_volume);
            data.ask_orders.push(bucket.entries.len() as i64);
        }
    }

    fn fill_bids(&self, size: usize, data: &mut common::model::l2_market_data::L2MarketData) {
        data.bid_prices.clear();
        data.bid_volumes.clear();
        data.bid_orders.clear();

        for (&price, bucket) in self.bid_buckets.iter().rev().take(size) {
            data.bid_prices.push(price);
            data.bid_volumes.push(bucket.total_volume);
            data.bid_orders.push(bucket.entries.len() as i64);
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
        // Validate that id_map is consistent with buckets
        let mut bucket_orders = 0;
        
        // Count orders in buckets
        for bucket in self.ask_buckets.values() {
            bucket_orders += bucket.get_num_orders();
        }
        for bucket in self.bid_buckets.values() {
            bucket_orders += bucket.get_num_orders();
        }
        
        let id_map_orders = self.id_map.len();
        
        assert_eq!(id_map_orders, bucket_orders, 
            "Inconsistent state: id_map has {} orders but buckets have {} orders", 
            id_map_orders, bucket_orders);
            
        // Validate bucket total volumes
        for (price, bucket) in &self.ask_buckets {
            let calculated_volume: i64 = bucket.entries.iter()
                .map(|order| order.size() - order.filled())
                .sum();
            assert_eq!(bucket.total_volume, calculated_volume,
                "Ask bucket at price {} has inconsistent volume: bucket.total_volume={}, calculated={}",
                price, bucket.total_volume, calculated_volume);
        }
        
        for (price, bucket) in &self.bid_buckets {
            let calculated_volume: i64 = bucket.entries.iter()
                .map(|order| order.size() - order.filled())
                .sum();
            assert_eq!(bucket.total_volume, calculated_volume,
                "Bid bucket at price {} has inconsistent volume: bucket.total_volume={}, calculated={}",
                price, bucket.total_volume, calculated_volume);
        }
    }

    fn write_marshallable(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        self.serialize(writer)
    }
}

impl OrderBookNaiveImpl {
    pub fn new(symbol_spec: CoreSymbolSpecification) -> OrderBookNaiveImpl {
        OrderBookNaiveImpl {
            ask_buckets: BTreeMap::new(),
            bid_buckets: BTreeMap::new(),
            id_map: HashMap::new(),
            symbol_spec,
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, std::io::Error> {
        OrderBookNaiveImpl::try_from_slice(bytes)
    }

    /// Create a simple orderbook for testing
    pub fn new_simple() -> OrderBookNaiveImpl {
        use common::model::symbol_specification::TestConstants;
        OrderBookNaiveImpl {
            ask_buckets: BTreeMap::new(),
            bid_buckets: BTreeMap::new(),
            id_map: HashMap::new(),
            symbol_spec: TestConstants::symbol_spec_eth_xbt(),
        }
    }

    fn new_order_place_gtc(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        let filled_size = self.try_match_instantly(cmd, 0);
        if filled_size == cmd.size {
            // Matched completely, no need to place
            return Ok(());
        }

        if self.id_map.contains_key(&cmd.order_id) {
            // Duplicate order id - can match, but can not place
            let unplaced_size = cmd.size - filled_size;
            EventHelper::attach_reject_event(cmd, unplaced_size);
            warn!("duplicate order id: {}", cmd.order_id);
            return Err(OrderBookError::DuplicateOrderId);
        }

        let order = Order {
            order_id: cmd.order_id,
            price: cmd.price,
            size: cmd.size,
            filled: filled_size,
            reserve_bid_price: cmd.reserve_bid_price,
            action: cmd.action,
            uid: cmd.uid,
            timestamp: cmd.timestamp,
        };

        let buckets = if cmd.action == OrderAction::Ask {
            &mut self.ask_buckets
        } else {
            &mut self.bid_buckets
        };

        buckets
            .entry(order.price)
            .or_insert_with(|| OrdersBucketNaive::new(order.price))
            .put(order.clone());
        self.id_map.insert(order.order_id, order);
        Ok(())
    }

    fn new_order_match_ioc(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        let filled_size = self.try_match_instantly(cmd, 0);
        let rejected_size = cmd.size - filled_size;

        if rejected_size != 0 {
            EventHelper::attach_reject_event(cmd, rejected_size);
        }
        Ok(())
    }

    fn new_order_match_fok_budget(
        &mut self,
        cmd: &mut OrderCommand,
    ) -> Result<(), OrderBookError> {
        if let Some(budget) = self.check_budget_to_fill(cmd.size, cmd.action) {
            if self.is_budget_limit_satisfied(cmd.action, budget, cmd.price) {
                self.try_match_instantly(cmd, 0);
            } else {
                EventHelper::attach_reject_event(cmd, cmd.size);
            }
        } else {
            EventHelper::attach_reject_event(cmd, cmd.size);
        }
        Ok(())
    }

    fn check_budget_to_fill(&self, size: i64, action: OrderAction) -> Option<i64> {
        let mut size_to_fill = size;
        let mut budget = 0;

        let buckets: Box<dyn Iterator<Item = (&i64, &OrdersBucketNaive)>> = if action == OrderAction::Ask {
            Box::new(self.bid_buckets.iter().rev())
        } else {
            Box::new(self.ask_buckets.iter())
        };

        for (_, bucket) in buckets {
            let available_size = bucket.total_volume;
            let price = bucket.price;

            if size_to_fill > available_size {
                size_to_fill -= available_size;
                budget += available_size * price;
            } else {
                return Some(budget + size_to_fill * price);
            }
        }
        None
    }

    fn is_budget_limit_satisfied(&self, order_action: OrderAction, calculated: i64, limit: i64) -> bool {
        if calculated == limit {
            return true;
        }
        (order_action == OrderAction::Bid) ^ (calculated > limit)
    }

    fn try_match_instantly(&mut self, active_order_cmd: &mut OrderCommand, filled: i64) -> i64 {
        let mut filled = filled;
        let spec = self.symbol_spec.clone(); // Clone to pass to match_order

        if active_order_cmd.action == OrderAction::Ask {
            // Incoming ASK is matched against existing BID orders
            let mut buckets_to_remove = Vec::new();
            for (&price, bucket) in self.bid_buckets.range_mut((active_order_cmd.price)..).rev() {
                let size_left = active_order_cmd.size - filled;
                if size_left <= 0 {
                    break;
                }
                let result = bucket.match_order(size_left, active_order_cmd, &spec);
                filled += result.volume;
                for order_id in result.orders_to_remove {
                    self.id_map.remove(&order_id);
                }
                if bucket.get_num_orders() == 0 {
                    buckets_to_remove.push(price);
                }
            }
            for price in buckets_to_remove {
                self.bid_buckets.remove(&price);
            }
        } else {
            // Incoming BID is matched against existing ASK orders
            let mut buckets_to_remove = Vec::new();
            for (&price, bucket) in self.ask_buckets.range_mut(..(active_order_cmd.price + 1)) {
                let size_left = active_order_cmd.size - filled;
                if size_left <= 0 {
                    break;
                }
                let result = bucket.match_order(size_left, active_order_cmd, &spec);
                filled += result.volume;
                for order_id in result.orders_to_remove {
                    self.id_map.remove(&order_id);
                }
                if bucket.get_num_orders() == 0 {
                    buckets_to_remove.push(price);
                }
            }
            for price in buckets_to_remove {
                self.ask_buckets.remove(&price);
            }
        }

        filled
    }
} 