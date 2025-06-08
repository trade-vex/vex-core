use common::model::enums::{OrderAction, OrderType};
use common::model::order::OrderTrait;
use common::model::symbol_specification::{CoreSymbolSpecification, TestConstants};
use orderbook::naive_impl::OrderBookNaiveImpl;
use orderbook::{OrderBook, OrderBookError, OrderCommand, OrderCommandType, SymbolType};
use rand::Rng;

fn create_order_book() -> OrderBookNaiveImpl {
    OrderBookNaiveImpl::new(TestConstants::symbol_spec_eth_xbt())
}

#[test]
fn test_new_order() {
    let mut order_book = create_order_book();
    let mut cmd = OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, OrderAction::Ask);
    order_book.new_order(&mut cmd).unwrap();
    assert_eq!(order_book.get_orders_num(OrderAction::Ask), 1);
}

#[test]
fn test_cancel_order() {
    let mut order_book = create_order_book();
    let mut cmd = OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, OrderAction::Ask);
    order_book.new_order(&mut cmd).unwrap();
    assert_eq!(order_book.get_orders_num(OrderAction::Ask), 1);
    let mut cancel_cmd = OrderCommand::cancel(1, 100);
    order_book.cancel_order(&mut cancel_cmd).unwrap();
    assert_eq!(order_book.get_orders_num(OrderAction::Ask), 0);
}

#[test]
fn test_reduce_order() {
    let mut order_book = create_order_book();
    let mut cmd = OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, OrderAction::Ask);
    order_book.new_order(&mut cmd).unwrap();
    assert_eq!(order_book.get_total_orders_volume(OrderAction::Ask), 10);
    let mut reduce_cmd = OrderCommand::reduce(1, 100, 5);
    order_book.reduce_order(&mut reduce_cmd).unwrap();
    assert_eq!(order_book.get_total_orders_volume(OrderAction::Ask), 5);
}

#[test]
fn test_move_order() {
    let mut order_book = create_order_book();
    let mut cmd = OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, OrderAction::Ask);
    order_book.new_order(&mut cmd).unwrap();
    assert_eq!(order_book.get_order_by_id(1).unwrap().price(), 50000);
    let mut move_cmd = OrderCommand::move_order(1, 100, 51000);
    order_book.move_order(&mut move_cmd).unwrap();
    assert_eq!(order_book.get_order_by_id(1).unwrap().price(), 51000);
}

#[test]
fn test_simple_matching() {
    let mut order_book = create_order_book();
    let mut ask_cmd = OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, OrderAction::Ask);
    order_book.new_order(&mut ask_cmd).unwrap();
    let mut bid_cmd = OrderCommand::new_order(OrderType::Gtc, 2, 101, 50000, 0, 5, OrderAction::Bid);
    order_book.new_order(&mut bid_cmd).unwrap();
    assert_eq!(order_book.get_orders_num(OrderAction::Ask), 1);
    assert_eq!(order_book.get_orders_num(OrderAction::Bid), 0);
    assert_eq!(order_book.get_order_by_id(1).unwrap().filled(), 5);
}

// TODO: translate all other tests from OrderBookBaseTest.java 