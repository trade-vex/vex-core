use common::model::enums::{OrderType, Side, SymbolType};
use common::model::symbol_specification::TestConstants;
use orderbook::naive_impl::OrderBookNaiveImpl;
use orderbook::{OrderBook, OrderCommand};

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
    let mut cmd = OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, Side::Ask);
    order_book.new_order(&mut cmd).unwrap();

    // Validation should pass
    order_book.validate_internal_state();
}

#[test]
fn should_work_with_different_symbol_types() {
    // Test with a currency exchange spec
    let currency_spec = TestConstants::symbol_spec_eth_xbt();
    let mut currency_book = OrderBookNaiveImpl::new(currency_spec.clone());
    let mut cmd1 = OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, Side::Ask);
    currency_book.new_order(&mut cmd1).unwrap();
    assert_eq!(currency_book.get_symbol_spec().symbol_id, 9269);

    // Test with a futures contract spec
    let futures_spec = TestConstants::symbol_spec_eur_usd();
    let mut futures_book = OrderBookNaiveImpl::new(futures_spec.clone());
    let mut cmd2 = OrderCommand::new_order(OrderType::Gtc, 2, 200, 12000, 0, 5, Side::Bid);
    futures_book.new_order(&mut cmd2).unwrap();
    assert_eq!(futures_book.get_symbol_spec().symbol_id, 5991);
}

#[test]
fn should_calculate_state_hash() {
    let symbol_spec = TestConstants::symbol_spec_eth_xbt();
    let hash1 = symbol_spec.state_hash();

    // Same spec should produce same hash
    let symbol_spec2 = TestConstants::symbol_spec_eth_xbt();
    let hash2 = symbol_spec2.state_hash();
    assert_eq!(hash1, hash2);

    // Different spec should produce different hash
    let symbol_spec3 = TestConstants::symbol_spec_eur_usd();
    let hash3 = symbol_spec3.state_hash();
    assert_ne!(hash1, hash3);
}

#[test]
fn should_get_orders_num() {
    let symbol_spec = TestConstants::symbol_spec_eth_xbt();
    let mut order_book = OrderBookNaiveImpl::new(symbol_spec);

    // Add some orders
    let mut cmd = OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, Side::Ask);
    order_book.new_order(&mut cmd).unwrap();

    assert_eq!(order_book.get_orders_num(Side::Ask), 1);
    assert_eq!(order_book.get_orders_num(Side::Bid), 0);
}

#[test]
fn should_get_l2_market_data_snapshot() {
    let symbol_spec = TestConstants::symbol_spec_eth_xbt();
    let mut order_book = OrderBookNaiveImpl::new(symbol_spec);

    // Add some orders
    let mut cmd = OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, Side::Ask);
    order_book.new_order(&mut cmd).unwrap();

    let l2_data = order_book.get_l2_market_data_snapshot(10);
    assert_eq!(l2_data.ask_prices.len(), 1);
}
