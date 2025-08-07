use crate::events::EventHelper;
use crate::{MatcherTradeEvent, OrderBook, OrderBookError, OrderBookImplType, OrderCommand};
use blart::TreeMap;
use borsh::{BorshDeserialize, BorshSerialize};
use common::model::enums::{MatcherEventType, OrderAction, OrderType, SymbolType};
use common::model::order::{Order, OrderTrait};
use common::model::symbol_specification::CoreSymbolSpecification;
use hashbrown::HashMap;
use slab::Slab;
use tracing::warn;

pub struct OrderBookDirectImpl {
    ask_price_buckets: TreeMap<i64, Bucket>,
    bid_price_buckets: TreeMap<i64, Bucket>,
    order_id_index: HashMap<i64, usize>,
    orders: Slab<DirectOrder>,
    symbol_spec: CoreSymbolSpecification,
    best_ask_order: Option<usize>,
    best_bid_order: Option<usize>,
}

impl OrderBookDirectImpl {
    pub fn new(symbol_spec: CoreSymbolSpecification) -> Self {
        Self {
            ask_price_buckets: TreeMap::new(),
            bid_price_buckets: TreeMap::new(),
            order_id_index: HashMap::new(),
            orders: Slab::new(),
            symbol_spec,
            best_ask_order: None,
            best_bid_order: None,
        }
    }

    pub fn from_bytes(bytes: &mut &[u8]) -> Result<Self, borsh::io::Error> {
        let symbol_spec = CoreSymbolSpecification::deserialize(bytes)?;
        let num_orders = u32::deserialize(bytes)?;

        let mut book = OrderBookDirectImpl::new(symbol_spec);

        for _ in 0..num_orders {
            let order = DirectOrder::deserialize(bytes)?;
            book.insert_order(order);
        }

        Ok(book)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, borsh::io::Error> {
        let mut writer = Vec::new();
        self.get_implementation_type().serialize(&mut writer)?;
        self.symbol_spec.serialize(&mut writer)?;

        let num_orders = self.orders.len() as u32;
        num_orders.serialize(&mut writer)?;

        for (_, order) in self.orders.iter() {
            order.serialize(&mut writer)?;
        }

        Ok(writer)
    }

    fn new_order_place_gtc(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        let filled_size = self.try_match_instantly(cmd, 0);
        if filled_size == cmd.size() {
            return Ok(());
        }

        if self.order_id_index.contains_key(&cmd.order_id()) {
            EventHelper::attach_reject_event(cmd, cmd.size() - filled_size);
            warn!("duplicate order id: {}", cmd.order_id());
            return Err(OrderBookError::DuplicateOrderId);
        }

        let order = DirectOrder {
            order_id: cmd.order_id(),
            price: cmd.price(),
            size: cmd.size(),
            filled: filled_size,
            reserve_bid_price: cmd.reserve_bid_price(),
            action: cmd.action(),
            uid: cmd.uid(),
            timestamp: cmd.timestamp(),
            next: None,
            prev: None,
        };

        self.insert_order(order);

        Ok(())
    }

    fn new_order_match_ioc(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        let filled_size = self.try_match_instantly(cmd, 0);
        let rejected_size = cmd.size() - filled_size;

        if rejected_size > 0 {
            EventHelper::attach_reject_event(cmd, rejected_size);
        }
        Ok(())
    }

    fn new_order_match_fok_budget(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        let budget = self.check_budget_to_fill(cmd.action(), cmd.size());

        if budget != i64::MAX && (cmd.action() == OrderAction::Ask || budget <= cmd.price()) {
            self.try_match_instantly(cmd, 0);
        } else {
            EventHelper::attach_reject_event(cmd, cmd.size());
        }
        Ok(())
    }

    fn check_budget_to_fill(&self, action: OrderAction, mut size: i64) -> i64 {
        let mut budget = 0i64;

        let mut next_key = if action == OrderAction::Bid {
            self.best_ask_order
        } else {
            self.best_bid_order
        };

        while let Some(key) = next_key {
            if size == 0 {
                break;
            }
            let order = &self.orders[key];
            let available_size = order.size - order.filled;
            let trade_size = std::cmp::min(size, available_size);

            budget += trade_size * order.price;
            size -= trade_size;

            next_key = order.prev;
        }

        if size == 0 {
            budget
        } else {
            i64::MAX
        }
    }

    fn try_match_instantly(&mut self, cmd: &mut OrderCommand, filled: i64) -> i64 {
        let mut filled = filled;
        let mut remaining_size = cmd.size() - filled;
        if remaining_size <= 0 {
            return filled;
        }

        let is_bid_action = cmd.action() == OrderAction::Bid;
        let limit_price = cmd.price();

        let best_maker_key = if is_bid_action {
            self.best_ask_order
        } else {
            self.best_bid_order
        };

        let mut maker_key_opt = best_maker_key;

        while let Some(maker_key) = maker_key_opt {
            if remaining_size <= 0 {
                break;
            }

            let maker_order_price;
            let maker_order_size;
            let maker_order_filled;
            let maker_order_prev;
            let _maker_action;

            {
                let maker_order = &self.orders[maker_key];
                maker_order_price = maker_order.price;
                maker_order_size = maker_order.size;
                maker_order_filled = maker_order.filled;
                maker_order_prev = maker_order.prev;
                _maker_action = maker_order.action;
            }

            let can_match = if is_bid_action {
                maker_order_price <= limit_price
            } else {
                maker_order_price >= limit_price
            };

            if !can_match {
                break;
            }

            let trade_size = std::cmp::min(remaining_size, maker_order_size - maker_order_filled);

            if trade_size > 0 {
                remaining_size -= trade_size;
                filled += trade_size;

                let maker_order_mut = &mut self.orders[maker_key];
                maker_order_mut.filled += trade_size;

                let maker_filled = maker_order_mut.filled == maker_order_mut.size;

                if !maker_filled {
                    // Only subtract from volume if not going to remove order
                    let buckets = if cmd.action() == OrderAction::Ask {
                        &mut self.bid_price_buckets
                    } else {
                        &mut self.ask_price_buckets
                    };
                    if let Some(bucket) = buckets.get_mut(&maker_order_mut.price) {
                        bucket.volume -= trade_size;
                    }
                }

                let trade_event = MatcherTradeEvent {
                    event_type: MatcherEventType::Trade,
                    section: 0, // TODO
                    active_order_completed: remaining_size == 0,
                    matched_order_id: maker_order_mut.order_id,
                    matched_order_uid: maker_order_mut.uid,
                    matched_order_completed: maker_filled,
                    price: maker_order_mut.price,
                    size: trade_size,
                    bidder_hold_price: if cmd.action() == OrderAction::Ask {
                        cmd.reserve_bid_price()
                    } else {
                        maker_order_mut.reserve_bid_price
                    },
                    ..MatcherTradeEvent::default()
                };

                cmd.attach_matcher_event(Box::new(trade_event));

                if maker_filled {
                    self.remove_order(maker_key);
                }
            }

            maker_key_opt = maker_order_prev;
        }

        filled
    }

    fn remove_order(&mut self, order_key: usize) {
        if let Some(order_to_remove) = self.orders.try_remove(order_key) {
            self.order_id_index.remove(&order_to_remove.order_id);

            let prev_key = order_to_remove.prev;
            let next_key = order_to_remove.next;

            // 1. Patch the global linked list
            if let Some(p_key) = prev_key {
                if let Some(prev_order) = self.orders.get_mut(p_key) {
                    prev_order.next = next_key;
                }
            }
            if let Some(n_key) = next_key {
                if let Some(next_order) = self.orders.get_mut(n_key) {
                    next_order.prev = prev_key;
                }
            }

            // 2. Update best order pointers if needed
            let action = order_to_remove.action;
            if action == OrderAction::Ask {
                if self.best_ask_order == Some(order_key) {
                    self.best_ask_order = next_key;
                }
            } else {
                // Bid
                if self.best_bid_order == Some(order_key) {
                    self.best_bid_order = next_key;
                }
            }

            // 3. Update bucket
            let buckets = if action == OrderAction::Ask {
                &mut self.ask_price_buckets
            } else {
                &mut self.bid_price_buckets
            };

            let price = order_to_remove.price;
            if let Some(bucket) = buckets.get_mut(&price) {
                bucket.volume -= order_to_remove.size - order_to_remove.filled;
                bucket.num_orders -= 1;

                if bucket.tail == Some(order_key) {
                    // The order we removed was the tail of the bucket. The new tail is its successor in the global list.
                    // We only care if the successor is in the same bucket.
                    if let Some(n_key) = next_key {
                        if self.orders.get(n_key).is_some_and(|p| p.price == price) {
                            bucket.tail = next_key;
                        } else {
                            bucket.tail = None; // Successor is in another bucket, this bucket is now empty of its tail.
                        }
                    } else {
                        bucket.tail = None;
                    }
                }

                if bucket.num_orders == 0 {
                    buckets.remove(&price);
                }
            }
        }
    }

    fn insert_order(&mut self, mut order: DirectOrder) {
        let is_ask = order.action == OrderAction::Ask;
        let buckets = if is_ask {
            &mut self.ask_price_buckets
        } else {
            &mut self.bid_price_buckets
        };
        let price = order.price;

        let (predecessor, successor) = if let Some(bucket) = buckets.get_mut(&price) {
            // Price level exists, new order becomes the new tail.
            let old_tail_key = bucket.tail.unwrap();
            let old_tail_prev = self.orders[old_tail_key].prev;
            (old_tail_prev, Some(old_tail_key))
        } else {
            // New price level, find successor bucket.
            let successor_bucket_tail = if is_ask {
                buckets
                    .range((std::ops::Bound::Excluded(price), std::ops::Bound::Unbounded))
                    .next()
                    .map(|(_, b)| b.tail.unwrap())
            } else {
                buckets
                    .range((std::ops::Bound::Unbounded, std::ops::Bound::Excluded(price)))
                    .next_back()
                    .map(|(_, b)| b.tail.unwrap())
            };

            if let Some(succ_key) = successor_bucket_tail {
                (self.orders[succ_key].prev, Some(succ_key))
            } else {
                // New best price level.
                (
                    if is_ask {
                        self.best_ask_order
                    } else {
                        self.best_bid_order
                    },
                    None,
                )
            }
        };

        order.prev = predecessor;
        order.next = successor;

        let new_order_key = self.orders.insert(order);
        let new_order_id = self.orders[new_order_key].order_id;
        self.order_id_index.insert(new_order_id, new_order_key);

        if let Some(p_key) = predecessor {
            self.orders[p_key].next = Some(new_order_key);
        }
        if let Some(s_key) = successor {
            self.orders[s_key].prev = Some(new_order_key);
        }

        // Update best order if necessary
        if successor.is_none() {
            if is_ask {
                self.best_ask_order = Some(new_order_key);
            } else {
                self.best_bid_order = Some(new_order_key);
            }
        }

        // Update or create bucket
        let bucket = buckets.entry(price).or_insert_with(|| Bucket {
            volume: 0,
            num_orders: 0,
            tail: None,
        });

        let new_order = &self.orders[new_order_key];
        bucket.volume += new_order.size - new_order.filled;
        bucket.num_orders += 1;
        bucket.tail = Some(new_order_key);
    }

    fn validate_chain(&self, action: OrderAction, orders_in_chain: &mut hashbrown::HashSet<i64>) {
        let is_ask = action == OrderAction::Ask;
        let buckets = if is_ask {
            &self.ask_price_buckets
        } else {
            &self.bid_price_buckets
        };
        let mut current_order_key = if is_ask {
            self.best_ask_order
        } else {
            self.best_bid_order
        };

        let mut last_price = -1i64;
        let mut orders_in_bucket = 0;
        let mut volume_in_bucket = 0i64;
        let mut prev_order_key: Option<usize> = None;

        while let Some(order_key) = current_order_key {
            let order = &self.orders[order_key];
            assert_eq!(order.action, action, "Order has wrong action");
            assert!(
                orders_in_chain.insert(order.order_id),
                "Duplicate order in chain"
            );

            if last_price != -1 && order.price != last_price {
                // Moved to a new price bucket
                let bucket = buckets.get(&last_price).unwrap();
                assert_eq!(
                    bucket.num_orders, orders_in_bucket,
                    "Bucket order count mismatch"
                );
                assert_eq!(bucket.volume, volume_in_bucket, "Bucket volume mismatch");
                orders_in_bucket = 0;
                volume_in_bucket = 0;
            }

            orders_in_bucket += 1;
            volume_in_bucket += order.size - order.filled;

            assert_eq!(order.next, prev_order_key, "Next pointer is incorrect");

            last_price = order.price;
            prev_order_key = Some(order_key);
            current_order_key = order.prev;
        }

        if last_price != -1 {
            let bucket = buckets.get(&last_price).unwrap();
            assert_eq!(
                bucket.num_orders, orders_in_bucket,
                "Last bucket order count mismatch"
            );
            assert_eq!(
                bucket.volume, volume_in_bucket,
                "Last bucket volume mismatch"
            );
        }
    }
}

#[derive(Clone, BorshSerialize, BorshDeserialize)]
pub struct DirectOrder {
    pub order_id: i64,
    pub price: i64,
    pub size: i64,
    pub filled: i64,
    pub reserve_bid_price: i64,
    pub action: OrderAction,
    pub uid: i64,
    pub timestamp: i64,

    // Doubly-linked list pointers
    #[borsh(skip)]
    pub next: Option<usize>,
    #[borsh(skip)]
    pub prev: Option<usize>,
}

impl OrderTrait for DirectOrder {
    fn price(&self) -> i64 {
        self.price
    }
    fn size(&self) -> i64 {
        self.size
    }
    fn filled(&self) -> i64 {
        self.filled
    }
    fn uid(&self) -> i64 {
        self.uid
    }
    fn action(&self) -> OrderAction {
        self.action
    }
    fn order_id(&self) -> i64 {
        self.order_id
    }
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
    fn reserve_bid_price(&self) -> i64 {
        self.reserve_bid_price
    }
}

impl PartialEq for DirectOrder {
    fn eq(&self, other: &Self) -> bool {
        self.order_id == other.order_id
    }
}

impl Eq for DirectOrder {}

impl DirectOrder {
    pub fn to_order(&self) -> Order {
        Order {
            order_id: self.order_id,
            price: self.price,
            size: self.size,
            filled: self.filled,
            reserve_bid_price: self.reserve_bid_price,
            action: self.action,
            uid: self.uid,
            timestamp: self.timestamp,
        }
    }
}

#[derive(Clone, BorshSerialize, BorshDeserialize)]
pub struct Bucket {
    volume: i64,
    num_orders: i32,
    tail: Option<usize>,
}

pub struct OrderBookDirectIterator<'a> {
    orders: &'a Slab<DirectOrder>,
    bucket_iter: Box<dyn Iterator<Item = &'a Bucket> + 'a>,
    current_order_key: Option<usize>,
}

impl<'a> Iterator for OrderBookDirectIterator<'a> {
    type Item = &'a dyn OrderTrait;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(key) = self.current_order_key {
                let order = &self.orders[key];
                self.current_order_key = order.prev;
                return Some(order as &dyn OrderTrait);
            } else if let Some(bucket) = self.bucket_iter.next() {
                self.current_order_key = bucket.tail;
                continue;
            } else {
                return None;
            }
        }
    }
}

impl<'a> OrderBook<'a> for OrderBookDirectImpl {
    fn new_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        match cmd.order_type {
            OrderType::Gtc => self.new_order_place_gtc(cmd),
            OrderType::Ioc => self.new_order_match_ioc(cmd),
            OrderType::FokBudget => self.new_order_match_fok_budget(cmd),
            _ => {
                warn!("Unsupported order type: {:?}", cmd.order_type);
                EventHelper::attach_reject_event(cmd, cmd.size());
                Err(OrderBookError::UnsupportedCommand)
            }
        }
    }

    fn cancel_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        let order_key = self
            .order_id_index
            .get(&cmd.order_id())
            .ok_or(OrderBookError::UnknownOrderId)?;

        let order_to_cancel = self.orders[*order_key].clone();

        if order_to_cancel.uid() != cmd.uid() {
            return Err(OrderBookError::UnknownOrderId);
        }

        let remaining_size = order_to_cancel.size() - order_to_cancel.filled();
        cmd.matcher_event = Some(EventHelper::send_reduce_event(
            &order_to_cancel,
            remaining_size,
            true,
        ));

        self.remove_order(*order_key);

        Ok(())
    }

    fn reduce_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        let requested_reduce_size = cmd.size();
        if requested_reduce_size <= 0 {
            return Err(OrderBookError::ReduceFailedWrongSize);
        }

        let order_key = self
            .order_id_index
            .get(&cmd.order_id())
            .copied()
            .ok_or(OrderBookError::UnknownOrderId)?;

        // Use a scope to determine the action while avoiding complex borrows.
        let (reduce_by, can_remove, order_action);
        {
            let order = &self.orders[order_key];
            if order.uid() != cmd.uid() {
                return Err(OrderBookError::UnknownOrderId);
            }

            let remaining_size = order.size() - order.filled();
            reduce_by = std::cmp::min(remaining_size, requested_reduce_size);
            can_remove = reduce_by == remaining_size;
            order_action = order.action();
        }

        cmd.action = order_action;

        if can_remove {
            let order_clone = self.orders[order_key].clone();
            self.remove_order(order_key);
            cmd.attach_matcher_event(EventHelper::send_reduce_event(
                &order_clone,
                reduce_by,
                true,
            ));
        } else {
            let order_to_reduce = &mut self.orders[order_key];
            order_to_reduce.size -= reduce_by;

            let price = order_to_reduce.price();
            let buckets = if order_action == OrderAction::Ask {
                &mut self.ask_price_buckets
            } else {
                &mut self.bid_price_buckets
            };
            if let Some(bucket) = buckets.get_mut(&price) {
                bucket.volume -= reduce_by;
            }

            cmd.matcher_event = Some(EventHelper::send_reduce_event(
                order_to_reduce,
                reduce_by,
                false,
            ));
        }

        Ok(())
    }

    fn move_order(&mut self, cmd: &mut OrderCommand) -> Result<(), OrderBookError> {
        let order_key = self
            .order_id_index
            .get(&cmd.order_id())
            .ok_or(OrderBookError::UnknownOrderId)?;

        if self.orders[*order_key].uid() != cmd.uid() {
            return Err(OrderBookError::UnknownOrderId);
        }

        let mut order_to_move = self.orders[*order_key].clone();

        if self.symbol_spec.symbol_type == SymbolType::CurrencyExchangePair
            && order_to_move.action == OrderAction::Bid
            && cmd.price > order_to_move.reserve_bid_price
        {
            return Err(OrderBookError::MoveFailedPriceOverRiskLimit);
        }

        self.remove_order(*order_key);

        order_to_move.price = cmd.price();

        cmd.action = order_to_move.action;
        cmd.size = order_to_move.size;

        let total_filled = self.try_match_instantly(cmd, order_to_move.filled);
        order_to_move.filled = total_filled;

        // If not fully filled, insert the remainder back onto the book.
        if order_to_move.size > order_to_move.filled {
            self.insert_order(order_to_move);
        }

        Ok(())
    }

    fn get_orders_num(&self, action: OrderAction) -> i32 {
        let buckets = if action == OrderAction::Ask {
            &self.ask_price_buckets
        } else {
            &self.bid_price_buckets
        };
        buckets.values().map(|b| b.num_orders).sum()
    }

    fn get_total_orders_volume(&self, action: OrderAction) -> i64 {
        let buckets = if action == OrderAction::Ask {
            &self.ask_price_buckets
        } else {
            &self.bid_price_buckets
        };
        buckets.values().map(|b| b.volume).sum()
    }

    fn get_order_by_id(&self, order_id: i64) -> Option<&dyn OrderTrait> {
        self.order_id_index
            .get(&order_id)
            .and_then(|&key| self.orders.get(key))
            .map(|order| order as &dyn OrderTrait)
    }

    fn find_user_orders(&self, uid: i64) -> Vec<Order> {
        self.orders
            .iter()
            .map(|(_, order)| order)
            .filter(|order| order.uid == uid)
            .map(|direct_order| direct_order.to_order())
            .collect()
    }

    fn ask_orders_stream(
        &'a self,
        sorted: bool,
    ) -> Box<dyn Iterator<Item = &'a dyn OrderTrait> + 'a> {
        let bucket_iter: Box<dyn Iterator<Item = &'a Bucket> + 'a> = if sorted {
            Box::new(self.ask_price_buckets.values())
        } else {
            Box::new(self.ask_price_buckets.values().rev())
        };

        Box::new(OrderBookDirectIterator {
            orders: &self.orders,
            bucket_iter,
            current_order_key: None,
        })
    }

    fn bid_orders_stream(
        &'a self,
        sorted: bool,
    ) -> Box<dyn Iterator<Item = &'a dyn OrderTrait> + 'a> {
        let bucket_iter: Box<dyn Iterator<Item = &'a Bucket> + 'a> = if sorted {
            Box::new(self.bid_price_buckets.values().rev())
        } else {
            Box::new(self.bid_price_buckets.values())
        };

        Box::new(OrderBookDirectIterator {
            orders: &self.orders,
            bucket_iter,
            current_order_key: None,
        })
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

        for (&price, bucket) in self.ask_price_buckets.iter().take(size) {
            data.ask_prices.push(price);
            data.ask_volumes.push(bucket.volume);
            data.ask_orders.push(bucket.num_orders as i64);
        }
    }

    fn fill_bids(&self, size: usize, data: &mut common::model::l2_market_data::L2MarketData) {
        data.bid_prices.clear();
        data.bid_volumes.clear();
        data.bid_orders.clear();

        for (&price, bucket) in self.bid_price_buckets.iter().rev().take(size) {
            data.bid_prices.push(price);
            data.bid_volumes.push(bucket.volume);
            data.bid_orders.push(bucket.num_orders as i64);
        }
    }

    fn get_total_ask_buckets(&self, limit: usize) -> usize {
        self.ask_price_buckets.len().min(limit)
    }

    fn get_total_bid_buckets(&self, limit: usize) -> usize {
        self.bid_price_buckets.len().min(limit)
    }

    fn get_implementation_type(&self) -> OrderBookImplType {
        OrderBookImplType::Direct
    }

    fn get_symbol_spec(&self) -> &CoreSymbolSpecification {
        &self.symbol_spec
    }

    fn validate_internal_state(&self) {
        let mut orders_in_chain = hashbrown::HashSet::new();
        self.validate_chain(OrderAction::Ask, &mut orders_in_chain);
        self.validate_chain(OrderAction::Bid, &mut orders_in_chain);

        assert_eq!(
            self.order_id_index.len(),
            orders_in_chain.len(),
            "order_id_index size does not match chain size"
        );

        for key in self.order_id_index.keys() {
            assert!(
                orders_in_chain.contains(key),
                "orderIdIndex contains an order not in a chain: {key}"
            );
        }
    }
}
