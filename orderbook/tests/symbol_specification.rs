use common::model::enums::{OrderAction, OrderType, SymbolType};
use common::model::symbol_specification::TestConstants;
use orderbook::naive_impl::OrderBookNaiveImpl;
use orderbook::{OrderBook, OrderBookError, OrderCommand};

#[test]
fn should_get_symbol_specification() {
    let symbol_spec = TestConstants::symbol_spec_eth_xbt();
    let order_book = OrderBookNaiveImpl::new(symbol_spec.clone());

    let retrieved_spec = order_book.get_symbol_spec();
    assert_eq!(retrieved_spec.symbol_id, TestConstants::SYMBOL_EXCHANGE);
    assert_eq!(retrieved_spec.symbol_type, SymbolType::CurrencyExchangePair);
    assert_eq!(retrieved_spec.base_currency, TestConstants::CURRENCY_ETH);
    assert_eq!(retrieved_spec.quote_currency, TestConstants::CURRENCY_XBT);
}

#[test]
fn should_validate_internal_state() {
    let symbol_spec = TestConstants::symbol_spec_eth_xbt();
    let mut order_book = OrderBookNaiveImpl::new(symbol_spec);

    // Add some orders
    let mut cmd = OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, OrderAction::Ask);
    order_book.new_order(&mut cmd).unwrap();

    // Validation should pass
    order_book.validate_internal_state();
}

#[test]
fn should_enforce_risk_limits_for_currency_exchange() {
    let spec = TestConstants::symbol_spec_eth_xbt();
    let mut order_book = OrderBookNaiveImpl::new(spec);

    // Place a bid order with reserve price limit
    let mut cmd =
        OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 52000, 10, OrderAction::Bid);
    order_book.new_order(&mut cmd).unwrap();

    // Try to move order above reserve price limit - should fail
    let mut move_cmd = OrderCommand::move_order(1, 100, 53000);
    let result = order_book.move_order(&mut move_cmd);
    assert_eq!(result, Err(OrderBookError::MoveFailedPriceOverRiskLimit));

    // Move to price within limit - should succeed
    let mut move_cmd2 = OrderCommand::move_order(1, 100, 51000);
    order_book.move_order(&mut move_cmd2).unwrap();
}

#[test]
fn should_not_enforce_risk_limits_for_futures() {
    let spec = TestConstants::symbol_spec_eur_usd();
    let mut order_book = OrderBookNaiveImpl::new(spec);

    // Place a BID order
    let mut place_cmd =
        OrderCommand::new_order(OrderType::Gtc, 1, 100, 10000, 12000, 10, OrderAction::Bid);
    order_book.new_order(&mut place_cmd).unwrap();

    // Try to move it
    let mut move_cmd = OrderCommand::move_order(1, 100, 12000);
    order_book.move_order(&mut move_cmd).unwrap();

    // Ensure the order was moved
    assert!(order_book.get_order_by_id(1).is_some());
    assert_eq!(order_book.get_order_by_id(1).unwrap().price(), 12000);
}

#[test]
fn should_work_with_different_symbol_types() {
    // Test with a currency exchange spec
    let currency_spec = TestConstants::symbol_spec_eth_xbt();
    let mut currency_book = OrderBookNaiveImpl::new(currency_spec.clone());
    let mut cmd1 = OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, OrderAction::Ask);
    currency_book.new_order(&mut cmd1).unwrap();
    assert_eq!(currency_book.get_symbol_spec().symbol_id, 9269);

    // Test with a futures contract spec
    let futures_spec = TestConstants::symbol_spec_eur_usd();
    let mut futures_book = OrderBookNaiveImpl::new(futures_spec.clone());
    let mut cmd2 = OrderCommand::new_order(OrderType::Gtc, 2, 200, 12000, 0, 5, OrderAction::Bid);
    futures_book.new_order(&mut cmd2).unwrap();
    assert_eq!(futures_book.get_symbol_spec().symbol_id, 5991);
}

use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
#[test]
fn should_calculate_state_hash() {
    let symbol_spec = TestConstants::symbol_spec_eth_xbt();
    let mut hasher1 = DefaultHasher::new();
    symbol_spec.hash(&mut hasher1);
    let hash1 = hasher1.finish();
    
    // Same spec should produce same hash
    let symbol_spec2 = TestConstants::symbol_spec_eth_xbt();
    let mut hasher2 = DefaultHasher::new();
    symbol_spec2.hash(&mut hasher2);
    let hash2 = hasher2.finish();
    assert_eq!(hash1, hash2);

    // Different spec should produce different hash
    let symbol_spec3 = TestConstants::symbol_spec_eur_usd();
    let mut hasher3 = DefaultHasher::new();
    symbol_spec3.hash(&mut hasher3);
    let hash3 = hasher3.finish();
    assert_ne!(hash1, hash3);
}

#[test]
fn should_get_orders_num() {
    let symbol_spec = TestConstants::symbol_spec_eth_xbt();
    let mut order_book = OrderBookNaiveImpl::new(symbol_spec);

    // Add some orders
    let mut cmd = OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, OrderAction::Ask);
    order_book.new_order(&mut cmd).unwrap();

    assert_eq!(order_book.get_orders_num(OrderAction::Ask), 1);
    assert_eq!(order_book.get_orders_num(OrderAction::Bid), 0);
}

#[test]
fn should_get_l2_market_data_snapshot() {
    let symbol_spec = TestConstants::symbol_spec_eth_xbt();
    let mut order_book = OrderBookNaiveImpl::new(symbol_spec);

    // Add some orders
    let mut cmd = OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, OrderAction::Ask);
    order_book.new_order(&mut cmd).unwrap();

    let l2_data = order_book.get_l2_market_data_snapshot(10);
    assert_eq!(l2_data.ask_prices.len(), 1);
}
