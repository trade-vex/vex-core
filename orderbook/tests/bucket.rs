use orderbook::naive_impl::OrdersBucketNaive;
use common::model::order::Order;

const PRICE: i64 = 1000;
const UID_1: i64 = 412;
const UID_2: i64 = 413;

fn create_filled_bucket() -> OrdersBucketNaive {
    let mut bucket = OrdersBucketNaive::new(PRICE);

    bucket.put(Order { order_id: 1, uid: UID_1, size: 100, price: PRICE, filled: 0, reserve_bid_price: 0, action: common::model::enums::OrderAction::Ask, timestamp: 0 });
    assert_eq!(bucket.get_num_orders(), 1);
    assert_eq!(bucket.get_total_volume(), 100);

    bucket.put(Order { order_id: 2, uid: UID_2, size: 40, price: PRICE, filled: 0, reserve_bid_price: 0, action: common::model::enums::OrderAction::Ask, timestamp: 0 });
    assert_eq!(bucket.get_num_orders(), 2);
    assert_eq!(bucket.get_total_volume(), 140);

    bucket.put(Order { order_id: 3, uid: UID_1, size: 1, price: PRICE, filled: 0, reserve_bid_price: 0, action: common::model::enums::OrderAction::Ask, timestamp: 0 });
    assert_eq!(bucket.get_num_orders(), 3);
    assert_eq!(bucket.get_total_volume(), 141);

    bucket.remove(2, UID_2);
    assert_eq!(bucket.get_num_orders(), 2);
    assert_eq!(bucket.get_total_volume(), 101);

    bucket.put(Order { order_id: 4, uid: UID_1, size: 200, price: PRICE, filled: 0, reserve_bid_price: 0, action: common::model::enums::OrderAction::Ask, timestamp: 0 });
    assert_eq!(bucket.get_num_orders(), 3);
    assert_eq!(bucket.get_total_volume(), 301);

    bucket
}

#[test]
fn should_add_order() {
    let mut bucket = create_filled_bucket();
    bucket.put(Order { order_id: 5, uid: UID_2, size: 240, price: PRICE, filled: 0, reserve_bid_price: 0, action: common::model::enums::OrderAction::Ask, timestamp: 0 });
    assert_eq!(bucket.get_num_orders(), 4);
    assert_eq!(bucket.get_total_volume(), 541);
}

#[test]
fn should_remove_orders() {
    let mut bucket = create_filled_bucket();
    let removed = bucket.remove(1, UID_1);
    assert!(removed.is_some());
    assert_eq!(bucket.get_num_orders(), 2);
    assert_eq!(bucket.get_total_volume(), 201);

    let removed = bucket.remove(4, UID_1);
    assert!(removed.is_some());
    assert_eq!(bucket.get_num_orders(), 1);
    assert_eq!(bucket.get_total_volume(), 1);

    // can not remove non-existing order
    let removed = bucket.remove(4, UID_1);
    assert!(removed.is_none());
    assert_eq!(bucket.get_num_orders(), 1);
    assert_eq!(bucket.get_total_volume(), 1);

    let removed = bucket.remove(3, UID_1);
    assert!(removed.is_some());
    assert_eq!(bucket.get_num_orders(), 0);
    assert_eq!(bucket.get_total_volume(), 0);
}

// TODO: Add more tests from OrdersBucketNaiveTest.java, especially for matching logic.
// For now, these basic tests cover put and remove.
// I also need to add helper methods to OrdersBucketNaive to get num_orders and total_volume. 