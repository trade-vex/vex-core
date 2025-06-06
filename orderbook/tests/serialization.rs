//! Tests for serialization and deserialization of the order book.
use orderbook::naive_impl::OrderBookNaiveImpl;
use orderbook::{OrderBook, OrderCommand};
use common::model::enums::{OrderAction, OrderType};
use common::model::symbol_specification::TestConstants;

#[test]
fn should_serialize_and_deserialize_order_book() {
    // 1. Setup a complex order book
    let mut order_book = OrderBookNaiveImpl::new(TestConstants::symbol_spec_eth_xbt());

    // Add some ASK orders
    order_book
        .new_order(&mut OrderCommand::new_order(
            OrderType::Gtc,
            1,
            100,
            50000,
            0,
            10,
            OrderAction::Ask,
        ))
        .unwrap();
    order_book
        .new_order(&mut OrderCommand::new_order(
            OrderType::Gtc,
            2,
            101,
            50001,
            0,
            5,
            OrderAction::Ask,
        ))
        .unwrap();
    order_book
        .new_order(&mut OrderCommand::new_order(
            OrderType::Gtc,
            3,
            100,
            50001,
            0,
            8,
            OrderAction::Ask,
        ))
        .unwrap();

    // Add some BID orders
    order_book
        .new_order(&mut OrderCommand::new_order(
            OrderType::Gtc,
            4,
            200,
            49990,
            0,
            12,
            OrderAction::Bid,
        ))
        .unwrap();
    order_book
        .new_order(&mut OrderCommand::new_order(
            OrderType::Gtc,
            5,
            201,
            49989,
            0,
            20,
            OrderAction::Bid,
        ))
        .unwrap();

    // Take a snapshot of the state before serialization
    let original_ask_vol = order_book.get_total_orders_volume(OrderAction::Ask);
    let original_bid_vol = order_book.get_total_orders_volume(OrderAction::Bid);
    let original_ask_orders = order_book.get_orders_num(OrderAction::Ask);
    let original_bid_orders = order_book.get_orders_num(OrderAction::Bid);
    let original_l2_data = order_book.get_l2_market_data_snapshot(10);
    
    // 2. Serialize the order book
    let mut buffer = Vec::new();
    order_book.write_marshallable(&mut buffer).unwrap();

    // Ensure some data was written
    assert!(!buffer.is_empty());

    // 3. Deserialize the order book
    let deserialized_book = OrderBookNaiveImpl::from_bytes(&buffer).unwrap();

    // 4. Assert that the state is identical
    assert_eq!(original_ask_vol, deserialized_book.get_total_orders_volume(OrderAction::Ask));
    assert_eq!(original_bid_vol, deserialized_book.get_total_orders_volume(OrderAction::Bid));
    assert_eq!(original_ask_orders, deserialized_book.get_orders_num(OrderAction::Ask));
    assert_eq!(original_bid_orders, deserialized_book.get_orders_num(OrderAction::Bid));
    
    let new_l2_data = deserialized_book.get_l2_market_data_snapshot(10);
    assert_eq!(original_l2_data, new_l2_data);
    
    // Also validate the internal state for good measure
    deserialized_book.validate_internal_state();
} 