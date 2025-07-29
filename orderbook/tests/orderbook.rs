use common::model::enums::{Side, OrderType};
use common::model::symbol_specification::TestConstants;
use orderbook::naive_impl::OrderBookNaiveImpl;
use orderbook::{OrderBook, OrderCommand};

fn create_order_book() -> OrderBookNaiveImpl {
    OrderBookNaiveImpl::new(TestConstants::symbol_spec_eth_xbt())
}

#[test]
fn test_new_order() {
    let mut order_book = create_order_book();
    let mut cmd = OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, Side::Ask);
    order_book.new_order(&mut cmd).unwrap();
    assert_eq!(order_book.get_orders_num(Side::Ask), 1);
}

#[test]
fn test_cancel_order() {
    let mut order_book = create_order_book();
    let mut cmd = OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, Side::Ask);
    order_book.new_order(&mut cmd).unwrap();
    assert_eq!(order_book.get_orders_num(Side::Ask), 1);
    let mut cancel_cmd = OrderCommand::cancel(1, 100);
    order_book.cancel_order(&mut cancel_cmd).unwrap();
    assert_eq!(order_book.get_orders_num(Side::Ask), 0);
}

#[test]
fn test_simple_matching() {
    let mut order_book = create_order_book();
    let mut ask_cmd =
        OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, Side::Ask);
    order_book.new_order(&mut ask_cmd).unwrap();
    let mut bid_cmd =
        OrderCommand::new_order(OrderType::Gtc, 2, 101, 50000, 0, 5, Side::Bid);
    order_book.new_order(&mut bid_cmd).unwrap();
    assert_eq!(order_book.get_orders_num(Side::Ask), 1);
    assert_eq!(order_book.get_orders_num(Side::Bid), 0);
    assert_eq!(order_book.get_order_by_id(1).unwrap().filled(), 5);
}

// TODO: translate all other tests from OrderBookBaseTest.java
