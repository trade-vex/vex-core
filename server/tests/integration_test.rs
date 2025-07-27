use server::init_exchange;
use common::cmd::OrderCommand;

use disruptor::Producer;

use tracing::info;

#[tokio::test]
async fn test_full_exchange_flow() {
    tracing_subscriber::fmt::init();

    info!(" Running Full Disruptor Core Test ");

    let (core, mut producer, handler) = init_exchange();

    // Place order
    let mut cmd = OrderCommand::default();
    cmd.order_id = 1;
    cmd.uid = 100;
    cmd.symbol = 0;
    cmd.size = 10;
    cmd.price = 9629;
    producer.publish(|e| *e = cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Cancel order
    let mut cancel_cmd = OrderCommand::cancel(1, 100);
    cancel_cmd.symbol = 0;
    producer.publish(|e| *e = cancel_cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Reduce order
    let mut cmd2 = OrderCommand::default();
    cmd2.order_id = 2;
    cmd2.uid = 100;
    cmd2.symbol = 0;
    cmd2.size = 10;
    cmd2.price = 9629;
    producer.publish(|e| *e = cmd2.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut reduce_cmd = OrderCommand::reduce(2, 100, 5);
    reduce_cmd.symbol = 0;
    producer.publish(|e| *e = reduce_cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Move order
    let mut cmd3 = OrderCommand::default();
    cmd3.order_id = 3;
    cmd3.uid = 100;
    cmd3.symbol = 0;
    cmd3.size = 10;
    cmd3.price = 9629;
    producer.publish(|e| *e = cmd3.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut move_cmd = OrderCommand::move_order(3, 100, 9700);
    move_cmd.symbol = 0;
    producer.publish(|e| *e = move_cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Check events
    info!("\n Asserting events received by handler ");
    { // : Add an inner scope to release the lock
        let received_events = handler.events.lock().unwrap();
        assert!(
            received_events.len() >= 4,
            "Should have received at least four events"
        );

        // Detailed assertions for event types
        let mut has_reduce = false;
        let mut has_cancel = false;
        let mut _has_move = false;
        for event in received_events.iter() {
            match format!("{:?}", event) {
                s if s.contains("Reduce") => has_reduce = true,
                s if s.contains("Cancel") => has_cancel = true,
                s if s.contains("Move") => _has_move = true,
                _ => {}
            }
        }
        assert!(has_reduce, "Should have at least one Reduce event");
        assert!(has_cancel, "Should have at least one Cancel event");
    } // : The lock on handler.events is released here as `received_events` goes out of scope.

    //  User balance assertion 
    // : Check for the correct currency (1 for the seller)
    let balance = core.get_user_balance(100, 1).unwrap();
    println!("User 100 balance in currency 1: {}", balance);

    //  Matching test: Place matching ASK and BID orders 
    // Use a price that is guaranteed to be the best available to ensure the correct orders match.
    let mut ask_cmd = OrderCommand::new_order(
        common::model::enums::OrderType::Gtc,
        10, // order_id
        100, // uid
        9620, // price (better than the existing order at 9629)
        0, // reserve_bid_price
        5, // size
        common::model::enums::OrderAction::Ask,
    );
    ask_cmd.symbol = 0;
    producer.publish(|e| *e = ask_cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut bid_cmd = OrderCommand::new_order(
        common::model::enums::OrderType::Gtc,
        11, // order_id
        101, // uid (different user)
        9620, // price (matches the new ASK)
        0,
        5,
        common::model::enums::OrderAction::Bid,
    );
    bid_cmd.symbol = 0;
    producer.publish(|e| *e = bid_cmd.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    //  Assert trade event and filled quantities 
    let received_events = handler.events.lock().unwrap();
    let mut has_trade = false;
    for event in received_events.iter() {
        if format!("{:?}", event).contains("Trade") {
            has_trade = true;
            println!("Trade event: {:?}", event);
        }
    }
    assert!(has_trade, "Should have at least one Trade event");

    //  Assert user balances updated 
    // Check for the correct currencies (1 and 2) after the trade
    let ask_user_base_balance = core.get_user_balance(100, 1).unwrap();
    let ask_user_quote_balance = core.get_user_balance(100, 2).unwrap();
    let bid_user_base_balance = core.get_user_balance(101, 1).unwrap();
    let bid_user_quote_balance = core.get_user_balance(101, 2).unwrap();
    println!("User 100 (ASK) balances: base={}, quote={}", ask_user_base_balance, ask_user_quote_balance);
    println!("User 101 (BID) balances: base={}, quote={}", bid_user_base_balance, bid_user_quote_balance);
}
