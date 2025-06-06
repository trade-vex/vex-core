use common::model::enums::{OrderAction, OrderType};
use common::model::l2_market_data::L2MarketData;
use orderbook::naive_impl::OrderBookNaiveImpl;
use orderbook::{OrderBook, OrderBookError, OrderCommand, OrderCommandType};

#[derive(Debug, Clone, PartialEq, Eq)]
struct L2MarketDataHelper {
    snapshot: L2MarketData,
}

impl L2MarketDataHelper {
    fn new() -> Self {
        Self {
            snapshot: L2MarketData::with_size(0, 0),
        }
    }
    fn set_asks(&mut self, asks: Vec<(i64, i64, i64)>) {
        self.snapshot.ask_prices = asks.iter().map(|(p, _, _)| *p).collect();
        self.snapshot.ask_volumes = asks.iter().map(|(_, v, _)| *v).collect();
        self.snapshot.ask_orders = asks.iter().map(|(_, _, o)| *o).collect();
    }

    fn set_bids(&mut self, bids: Vec<(i64, i64, i64)>) {
        self.snapshot.bid_prices = bids.iter().map(|(p, _, _)| *p).collect();
        self.snapshot.bid_volumes = bids.iter().map(|(_, v, _)| *v).collect();
        self.snapshot.bid_orders = bids.iter().map(|(_, _, o)| *o).collect();
    }

    fn get_snapshot(&self) -> &L2MarketData {
        &self.snapshot
    }
}

fn setup_orderbook() -> (OrderBookNaiveImpl, L2MarketDataHelper) {
    (
        OrderBookNaiveImpl::new_simple(),
        L2MarketDataHelper::new(),
    )
}

fn process_and_validate(
    order_book: &mut OrderBookNaiveImpl,
    cmd: &mut OrderCommand,
    expected_state: &mut L2MarketDataHelper,
    expected_result: Result<(), OrderBookError>,
) {
    let result = match cmd.command {
        OrderCommandType::PlaceOrder => order_book.new_order(cmd),
        OrderCommandType::CancelOrder => order_book.cancel_order(cmd),
        OrderCommandType::ReduceOrder => order_book.reduce_order(cmd),
        OrderCommandType::MoveOrder => order_book.move_order(cmd),
    };

    assert_eq!(result, expected_result, "Command result mismatch");

    let actual_state = order_book.get_l2_market_data_snapshot(10);
    assert_eq!(
        expected_state.get_snapshot(),
        &actual_state,
        "\nExpected L2 State: {:?}\n  Actual L2 State: {:?}",
        expected_state.get_snapshot(),
        actual_state
    );
}

#[test]
fn should_initialize_without_errors() {
    let (mut order_book, mut expected_state) = setup_orderbook();
    let mut cmd = OrderCommand::new_order(OrderType::Gtc, 1, 100, 1000, 0, 1, OrderAction::Ask);
    cmd.command = OrderCommandType::PlaceOrder;
    expected_state.set_asks(vec![(1000, 1, 1)]);
    process_and_validate(&mut order_book, &mut cmd, &mut expected_state, Ok(()));
}

#[test]
fn should_add_gtc_orders() {
    let (mut order_book, mut expected_state) = setup_orderbook();

    let mut cmd_ask =
        OrderCommand::new_order(OrderType::Gtc, 1, 100, 1000, 0, 10, OrderAction::Ask);
    expected_state.set_asks(vec![(1000, 10, 1)]);
    process_and_validate(
        &mut order_book,
        &mut cmd_ask,
        &mut expected_state,
        Ok(()),
    );

    let mut cmd_bid =
        OrderCommand::new_order(OrderType::Gtc, 2, 200, 900, 1000, 5, OrderAction::Bid);
    expected_state.set_bids(vec![(900, 5, 1)]);
    process_and_validate(
        &mut order_book,
        &mut cmd_bid,
        &mut expected_state,
        Ok(()),
    );
}

#[test]
fn should_remove_ask_order() {
    let (mut order_book, mut expected_state) = setup_orderbook();
    order_book
        .new_order(&mut OrderCommand::new_order(
            OrderType::Gtc,
            1,
            100,
            1000,
            0,
            10,
            OrderAction::Ask,
        ))
        .unwrap();
    expected_state.set_asks(vec![(1000, 10, 1)]);

    let mut cmd_cancel = OrderCommand::cancel(1, 100);
    expected_state.set_asks(vec![]);
    process_and_validate(
        &mut order_book,
        &mut cmd_cancel,
        &mut expected_state,
        Ok(()),
    );
}

#[test]
fn should_remove_bid_order() {
    let (mut order_book, mut expected_state) = setup_orderbook();
    order_book
        .new_order(&mut OrderCommand::new_order(
            OrderType::Gtc,
            2,
            200,
            900,
            1000,
            5,
            OrderAction::Bid,
        ))
        .unwrap();
    expected_state.set_bids(vec![(900, 5, 1)]);

    let mut cmd_cancel = OrderCommand::cancel(2, 200);
    expected_state.set_bids(vec![]);
    process_and_validate(
        &mut order_book,
        &mut cmd_cancel,
        &mut expected_state,
        Ok(()),
    );
}

#[test]
fn should_return_error_when_deleting_unknown_order() {
    let (mut order_book, _) = setup_orderbook();
    let mut cmd = OrderCommand::cancel(1, 100);
    let res = order_book.cancel_order(&mut cmd);
    assert_eq!(res, Err(OrderBookError::UnknownOrderId));
}

#[test]
fn should_return_error_when_deleting_other_user_order() {
    let (mut order_book, _) = setup_orderbook();
    order_book
        .new_order(&mut OrderCommand::new_order(
            OrderType::Gtc,
            1,
            100,
            1000,
            0,
            10,
            OrderAction::Ask,
        ))
        .unwrap();
    let mut cmd = OrderCommand::cancel(1, 200); // Wrong user
    let res = order_book.cancel_order(&mut cmd);
    assert_eq!(res, Err(OrderBookError::UnknownOrderId));
}

#[test]
fn should_return_error_when_reducing_unknown_order() {
    let (mut order_book, _) = setup_orderbook();
    let mut cmd = OrderCommand::reduce(1, 100, 1);
    let res = order_book.reduce_order(&mut cmd);
    assert_eq!(res, Err(OrderBookError::UnknownOrderId));
}

#[test]
fn should_reduce_ask_order() {
    let (mut order_book, mut expected_state) = setup_orderbook();
    order_book
        .new_order(&mut OrderCommand::new_order(
            OrderType::Gtc,
            1,
            100,
            1000,
            0,
            10,
            OrderAction::Ask,
        ))
        .unwrap();
    expected_state.set_asks(vec![(1000, 10, 1)]);

    let mut cmd_reduce = OrderCommand::reduce(1, 100, 3);
    expected_state.set_asks(vec![(1000, 7, 1)]);
    process_and_validate(
        &mut order_book,
        &mut cmd_reduce,
        &mut expected_state,
        Ok(()),
    );
}

#[test]
fn should_reduce_bid_order() {
    let (mut order_book, mut expected_state) = setup_orderbook();
    order_book
        .new_order(&mut OrderCommand::new_order(
            OrderType::Gtc,
            2,
            200,
            900,
            1000,
            5,
            OrderAction::Bid,
        ))
        .unwrap();
    expected_state.set_bids(vec![(900, 5, 1)]);

    let mut cmd_reduce = OrderCommand::reduce(2, 200, 2);
    expected_state.set_bids(vec![(900, 3, 1)]);
    process_and_validate(
        &mut order_book,
        &mut cmd_reduce,
        &mut expected_state,
        Ok(()),
    );
}

#[test]
fn should_match_ioc_order_full_bbo() {
    let (mut order_book, mut expected_state) = setup_orderbook();
    order_book
        .new_order(&mut OrderCommand::new_order(
            OrderType::Gtc,
            1,
            100,
            1000,
            0,
            10,
            OrderAction::Ask,
        ))
        .unwrap();
    expected_state.set_asks(vec![(1000, 10, 1)]);

    let mut cmd_ioc =
        OrderCommand::new_order(OrderType::Ioc, 2, 200, 1000, 0, 10, OrderAction::Bid);
    expected_state.set_asks(vec![]);
    process_and_validate(&mut order_book, &mut cmd_ioc, &mut expected_state, Ok(()));
}

#[test]
fn should_match_ioc_order_partial_bbo() {
    let (mut order_book, mut expected_state) = setup_orderbook();
    order_book
        .new_order(&mut OrderCommand::new_order(
            OrderType::Gtc,
            1,
            100,
            1000,
            0,
            10,
            OrderAction::Ask,
        ))
        .unwrap();
    expected_state.set_asks(vec![(1000, 10, 1)]);

    let mut cmd_ioc =
        OrderCommand::new_order(OrderType::Ioc, 2, 200, 1000, 0, 5, OrderAction::Bid);
    expected_state.set_asks(vec![(1000, 5, 1)]);
    process_and_validate(&mut order_book, &mut cmd_ioc, &mut expected_state, Ok(()));
}

// TODO: translate all other tests from OrderBookBaseTest.java 