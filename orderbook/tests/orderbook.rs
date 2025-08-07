use common::model::enums::{MatcherEventType, OrderAction, OrderType};
use common::model::symbol_specification::TestConstants;
use orderbook::direct_impl::OrderBookDirectImpl;
use orderbook::naive_impl::OrderBookNaiveImpl;
use orderbook::{OrderBook, OrderCommand};
fn create_order_book() -> OrderBookNaiveImpl {
    OrderBookNaiveImpl::new(TestConstants::symbol_spec_eth_xbt())
}

fn create_order_book_direct() -> OrderBookDirectImpl {
    OrderBookDirectImpl::new(TestConstants::symbol_spec_eth_xbt())
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
fn test_reduce_order_to_zero_passes() {
    let mut order_book = create_order_book();
    let mut cmd = OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, OrderAction::Ask);
    order_book.new_order(&mut cmd).unwrap();

    let mut reduce_cmd = OrderCommand::reduce(1, 100, 10); // Reduce size BY 10

    order_book.reduce_order(&mut reduce_cmd).unwrap();

    assert_eq!(order_book.get_total_orders_volume(OrderAction::Ask), 0);
    let order = order_book.get_order_by_id(1);
    assert!(
        order.is_none(),
        "Order should be fully reduced and not exist"
    );

    let order_event = reduce_cmd.matcher_event.unwrap();
    assert_eq!(order_event.matched_order_id, 1);
    assert_eq!(order_event.matched_order_uid, 100);
    assert_eq!(order_event.price, 50000);
    assert_eq!(order_event.size, 10);
    assert_eq!(order_event.bidder_hold_price, 0);
    assert_eq!(order_event.event_type, MatcherEventType::Reduce);
    assert!(!order_event.active_order_completed);
    assert!(!order_event.matched_order_completed);
    assert!(order_event.next_event.is_none());
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
fn test_move_order_into_match_naive() {
    let mut order_book = create_order_book();

    // 1. Place a BID order for 10 shares at price 49900.
    let mut bid_cmd =
        OrderCommand::new_order(OrderType::Gtc, 1, 200, 49900, 0, 10, OrderAction::Bid);
    order_book.new_order(&mut bid_cmd).unwrap();

    // 2. Place an ASK order for 10 shares at price 50000.
    let mut ask_cmd =
        OrderCommand::new_order(OrderType::Gtc, 2, 100, 50000, 0, 10, OrderAction::Ask);
    order_book.new_order(&mut ask_cmd).unwrap();

    // Verify initial state.
    assert_eq!(order_book.get_total_orders_volume(OrderAction::Bid), 10);
    assert_eq!(order_book.get_total_orders_volume(OrderAction::Ask), 10);

    // 3. Move the ASK order to a marketable price of 49800 (below the BID).
    let mut move_cmd = OrderCommand::move_order(2, 100, 49800);
    order_book.move_order(&mut move_cmd).unwrap();

    // The book should be empty because the orders matched.
    assert_eq!(order_book.get_total_orders_volume(OrderAction::Bid), 0);
    assert_eq!(order_book.get_total_orders_volume(OrderAction::Ask), 0);
    assert!(order_book.get_order_by_id(1).is_none());
    assert!(order_book.get_order_by_id(2).is_none());

    // Verify trade event generation.
    let mut events = Vec::new();
    let mut current_event = move_cmd.matcher_event;
    while let Some(event) = current_event {
        events.push(event.as_ref().event_type);
        current_event = event.next_event;
    }
    assert!(events.contains(&MatcherEventType::Trade));
}
#[test]
fn test_simple_matching() {
    let mut order_book = create_order_book();
    let mut ask_cmd =
        OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 10, OrderAction::Ask);
    order_book.new_order(&mut ask_cmd).unwrap();
    let mut bid_cmd =
        OrderCommand::new_order(OrderType::Gtc, 2, 101, 50000, 0, 5, OrderAction::Bid);
    order_book.new_order(&mut bid_cmd).unwrap();
    assert_eq!(order_book.get_orders_num(OrderAction::Ask), 1);
    assert_eq!(order_book.get_orders_num(OrderAction::Bid), 0);
    assert_eq!(order_book.get_order_by_id(1).unwrap().filled(), 5);
}

#[test]
fn test_partial_match_updates_bucket_volume() {
    let mut order_book = create_order_book_direct();

    // Place a large BID order (the maker). Size 100 at price 50000.
    let mut maker_cmd =
        OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 100, OrderAction::Bid);
    order_book.new_order(&mut maker_cmd).unwrap();

    assert_eq!(order_book.get_total_orders_volume(OrderAction::Bid), 100);

    let maker_order = order_book.get_order_by_id(1).unwrap();
    assert_eq!(maker_order.size(), 100);
    assert_eq!(maker_order.filled(), 0);

    // Place a smaller ASK order (the taker) at a marketable price. Size 30.
    let mut taker_cmd =
        OrderCommand::new_order(OrderType::Gtc, 2, 200, 49900, 0, 30, OrderAction::Ask);
    order_book.new_order(&mut taker_cmd).unwrap();

    // The taker (ASK) order should be completely filled and gone.
    assert_eq!(order_book.get_total_orders_volume(OrderAction::Ask), 0);
    assert!(order_book.get_order_by_id(2).is_none());

    // The maker (BID) order should still be on the book, but partially filled.
    let maker_order_after_trade = order_book.get_order_by_id(1).unwrap();
    assert_eq!(maker_order_after_trade.size(), 100);
    assert_eq!(
        maker_order_after_trade.filled(),
        30,
        "Maker order should be filled by 30"
    );

    assert_eq!(
        order_book.get_total_orders_volume(OrderAction::Bid),
        70,
        "Bucket volume must be updated after partial match"
    );
}

#[test]
fn test_partial_reduce_updates_all_state_consistently() {
    let mut order_book = create_order_book();
    // Place a BID order of size 100.
    let mut place_cmd =
        OrderCommand::new_order(OrderType::Gtc, 1, 100, 50000, 0, 100, OrderAction::Bid);
    order_book.new_order(&mut place_cmd).unwrap();

    assert_eq!(order_book.get_total_orders_volume(OrderAction::Bid), 100);

    // 2. Reduce the order by 40. The new size should be 60.
    let mut reduce_cmd = OrderCommand::reduce(1, 100, 40);
    order_book.reduce_order(&mut reduce_cmd).unwrap();

    assert_eq!(
        order_book.get_total_orders_volume(OrderAction::Bid),
        60,
        "Bucket total volume should be reduced"
    );

    let order_from_map = order_book.get_order_by_id(1).unwrap();
    assert_eq!(
        order_from_map.size(),
        60,
        "Order size in global map should be updated"
    );

    let order_from_stream = order_book.bid_orders_stream(true).next().unwrap();
    assert_eq!(
        order_from_stream.size(),
        60,
        "Order size INSIDE the bucket must also be updated"
    );
}
