//! Tests for serialization and deserialization of the order book.
use orderbook::naive_impl::OrderBookNaiveImpl;
use orderbook::{OrderBook, OrderCommand};
use common::model::enums::{OrderAction, OrderType};
use common::model::symbol_specification::{TestConstants};

#[test]
fn test_naive_serialization_deserialization() {
    let mut order_book = OrderBookNaiveImpl::new(TestConstants::symbol_spec_eth_xbt());

    let cmd = OrderCommand::new_order(OrderType::Gtc, 1, 100, 1000, 0, 10, OrderAction::Ask);
    order_book.new_order(&mut cmd.clone()).unwrap();

    let cmd2 = OrderCommand::new_order(OrderType::Gtc, 2, 101, 1001, 0, 5, OrderAction::Ask);
    order_book.new_order(&mut cmd2.clone()).unwrap();

    let cmd3 = OrderCommand::new_order(OrderType::Gtc, 3, 102, 900, 0, 20, OrderAction::Bid);
    order_book.new_order(&mut cmd3.clone()).unwrap();

    let bytes = order_book.to_bytes().unwrap();
    let mut reader = bytes.as_slice();
    let deserialized_book = OrderBookNaiveImpl::from_bytes(&mut reader).unwrap();

    assert_eq!(order_book.get_orders_num(OrderAction::Ask), deserialized_book.get_orders_num(OrderAction::Ask));
    assert_eq!(order_book.get_orders_num(OrderAction::Bid), deserialized_book.get_orders_num(OrderAction::Bid));
    assert_eq!(order_book.get_total_orders_volume(OrderAction::Ask), deserialized_book.get_total_orders_volume(OrderAction::Ask));
    assert_eq!(order_book.get_total_orders_volume(OrderAction::Bid), deserialized_book.get_total_orders_volume(OrderAction::Bid));
} 