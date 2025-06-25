//! Tests for fee calculations.
use common::model::enums::{OrderAction, OrderType};
use common::model::symbol_specification::{CoreSymbolSpecification, TestConstants};
use orderbook::naive_impl::OrderBookNaiveImpl;
use orderbook::{OrderBook, OrderCommand};

fn get_fee_testing_spec() -> CoreSymbolSpecification {
    // Use a spec with non-zero fees for testing
    TestConstants::symbol_spec_fee_xbt_ltc()
}

#[test]
fn should_calculate_fees_on_trade() {
    let spec = get_fee_testing_spec();
    let mut order_book = OrderBookNaiveImpl::new(spec.clone());

    // Setup maker order
    let mut maker_cmd =
        OrderCommand::new_order(OrderType::Gtc, 1, 100, 20000, 0, 10, OrderAction::Ask);
    order_book.new_order(&mut maker_cmd).unwrap();

    // Execute taker order that matches the maker
    let mut taker_cmd =
        OrderCommand::new_order(OrderType::Ioc, 2, 200, 20000, 0, 5, OrderAction::Bid);
    order_book.new_order(&mut taker_cmd).unwrap();

    // Verify the trade event and fees
    assert!(taker_cmd.matcher_event.is_some());
    let event = taker_cmd.matcher_event.unwrap();

    assert_eq!(
        event.event_type,
        common::model::enums::MatcherEventType::Trade
    );
    assert_eq!(event.size, 5);

    // Taker fee = size * taker_fee_per_lot
    let expected_taker_fee = 5 * spec.taker_fee;
    assert_eq!(event.taker_fee, expected_taker_fee);

    // Maker fee = size * maker_fee_per_lot
    let expected_maker_fee = 5 * spec.maker_fee;
    assert_eq!(event.maker_fee, expected_maker_fee);
}

#[test]
fn should_handle_multiple_fee_events() {
    let spec = get_fee_testing_spec();
    let mut order_book = OrderBookNaiveImpl::new(spec.clone());

    // Setup maker orders
    order_book
        .new_order(&mut OrderCommand::new_order(
            OrderType::Gtc,
            1,
            100,
            20000,
            0,
            3,
            OrderAction::Ask,
        ))
        .unwrap();
    order_book
        .new_order(&mut OrderCommand::new_order(
            OrderType::Gtc,
            2,
            101,
            20000,
            0,
            8,
            OrderAction::Ask,
        ))
        .unwrap();

    // Execute taker order that matches both makers
    let mut taker_cmd =
        OrderCommand::new_order(OrderType::Ioc, 3, 200, 20000, 0, 10, OrderAction::Bid);
    order_book.new_order(&mut taker_cmd).unwrap();

    // Verify the trade events and fees
    assert!(taker_cmd.matcher_event.is_some());
    let event = taker_cmd.matcher_event.unwrap();

    // First trade
    assert_eq!(event.size, 3);
    assert_eq!(event.taker_fee, 3 * spec.taker_fee);
    assert_eq!(event.maker_fee, 3 * spec.maker_fee);

    // Second trade
    assert!(event.next_event.is_some());
    let event2 = event.next_event.unwrap();
    assert_eq!(event2.size, 7);
    assert_eq!(event2.taker_fee, 7 * spec.taker_fee);
    assert_eq!(event2.maker_fee, 7 * spec.maker_fee);
}

#[test]
fn should_have_zero_fees_for_symbols_without_them() {
    // Use a spec with zero fees
    let spec = TestConstants::symbol_spec_eth_xbt();
    let mut order_book = OrderBookNaiveImpl::new(spec.clone());

    // Setup maker order
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

    // Execute taker order
    let mut taker_cmd =
        OrderCommand::new_order(OrderType::Ioc, 2, 200, 50000, 0, 5, OrderAction::Bid);
    order_book.new_order(&mut taker_cmd).unwrap();

    // Verify fees are zero
    assert!(taker_cmd.matcher_event.is_some());
    let event = taker_cmd.matcher_event.unwrap();
    assert_eq!(event.taker_fee, 0);
    assert_eq!(event.maker_fee, 0);
}
