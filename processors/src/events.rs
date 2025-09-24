use common::L2MarketData;
use common::MatcherTradeEvent;
use common::Order;
use common::OrderCommand;
use common::Status;
use common::UserBalance;
use common::{base_asset, quote_asset};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{error, info};

// Real Kafka dependencies
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

pub trait EventsHandler: Send + Sync {
    fn handle_processed_command(&self, cmd: &mut OrderCommand);
}

// Real Kafka Events Handler
pub struct KafkaEventsHandler {
    producer: FutureProducer,
}

impl KafkaEventsHandler {
    pub fn new(brokers: &str) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("retry.backoff.ms", "100")
            .set("request.timeout.ms", "3000")
            .create()
            .expect("Producer creation failed");

        info!(
            "[KafkaEventsHandler] Connected to Kafka brokers at {}",
            brokers
        );

        Self { producer }
    }

    fn publish_event<T: Serialize>(&self, topic_name: &str, message_key: &str, payload: &T) {
        match serde_json::to_string(payload) {
            Ok(json_payload) => {
                let producer = self.producer.clone();
                let message_key = message_key.to_string();
                let topic_name = topic_name.to_string();

                // Spawn async task to send to Kafka
                tokio::spawn(async move {
                    let record = FutureRecord::to(&topic_name)
                        .payload(&json_payload)
                        .key(&message_key);

                    match producer.send(record, Duration::from_secs(5)).await {
                        Ok(_) => {
                            info!(
                                "[KafkaEventsHandler] Successfully sent event to topic '{}'",
                                topic_name
                            );
                        }
                        Err((e, _)) => {
                            error!(
                                "[KafkaEventsHandler] Failed to send event to topic '{}': {}",
                                topic_name, e
                            );
                        }
                    }
                });
            }
            Err(e) => error!(
                "Failed to serialize payload for topic '{}': {}",
                topic_name, e
            ),
        }
    }

    fn publish_balance_event(&self, user_id: u64, cmd: &OrderCommand, balance: &[UserBalance; 2]) {
        let base_asset_id = base_asset(cmd.market_id);
        let quote_asset_id = quote_asset(cmd.market_id);

        for (balance, asset_id) in balance.iter().zip([base_asset_id, quote_asset_id]) {
            let balance_event = BalanceEvent {
                user_id,
                asset_id: asset_id,
                available: balance.available(),
                locked: balance.locked(),
                total: balance.total(),
                timestamp: cmd.timestamp(),
            };

            let topic_name = format!("market-{}-balances", cmd.market_id);
            self.publish_event(&topic_name, &user_id.to_string(), &balance_event);
            info!(
                "[KafkaEventsHandler] Published balance event for user {} in market {}",
                user_id, cmd.market_id
            );
        }
    }

    fn publish_order_event(&self, cmd: &OrderCommand) {
        let order = Order {
            order_id: cmd.order_id(),
            user_id: cmd.taker_id(),
            price: cmd.price(),
            size: cmd.size(),
            side: cmd.side(),
            timestamp: cmd.timestamp(),
        };

        let order_event = OrderEvent {
            order,
            market_id: cmd.market_id(),
        };

        let topic_name = format!("market-{}-orders", cmd.market_id());
        self.publish_event(&topic_name, &cmd.order_id().to_string(), &order_event);
        info!(
            "[KafkaEventsHandler] Published order event for order {} in market {}",
            cmd.order_id(),
            cmd.market_id()
        );
    }

    fn publish_trade_event(
        &self,
        event: &MatcherTradeEvent,
        cmd: &OrderCommand,
        market_id: u32,
        taker_id: u64,
        taker_order_id: u64,
    ) {
        let trade_event = TradeEvent {
            maker_user_id: event.maker_user_id,
            taker_user_id: taker_id,
            market_id,
            price: event.price,
            size: event.size,
            maker_order_id: event.matched_order_id,
            taker_order_id,
            timestamp: cmd.timestamp(),
        };

        let topic_name = format!("market-{}-trades", market_id);
        let trade_key = format!("{}:{}", taker_order_id, event.matched_order_id);
        self.publish_event(&topic_name, &trade_key, &trade_event);

        info!(
            "[KafkaEventsHandler] Published trade event for maker order {} and taker order {} in market {}",
            event.matched_order_id, taker_order_id, market_id
        );
    }

    fn publish_cancel_order_event(&self, cmd: &OrderCommand) {
        let cancel_event = CancelOrderEvent {
            order_id: cmd.order_id(),
            market_id: cmd.market_id(),
            user_id: cmd.taker_id(),
            timestamp: cmd.timestamp(),
        };

        let topic_name = format!("market-{}-cancels", cmd.market_id());
        self.publish_event(&topic_name, &cmd.order_id().to_string(), &cancel_event);
        info!(
            "[KafkaEventsHandler] Published cancel order event for order {} in market {}",
            cmd.order_id(),
            cmd.market_id()
        );
    }

    fn publish_orderbook_event(&self, market_id: u32, orderbook_snapshot: &Option<L2MarketData>) {
        if let Some(snapshot) = orderbook_snapshot {
            let mut bids = Vec::new();
            let mut asks = Vec::new();

            for i in 0..snapshot.depth() {
                if snapshot.bid_prices[i] > 0 {
                    bids.push(OrderbookLevel {
                        price: snapshot.bid_prices[i],
                        size: snapshot.bid_volumes[i],
                    });
                }
            }

            for i in 0..snapshot.depth() {
                if snapshot.ask_prices[i] > 0 {
                    asks.push(OrderbookLevel {
                        price: snapshot.ask_prices[i],
                        size: snapshot.ask_volumes[i],
                    });
                }
            }

            let orderbook_event = OrderbookEvent {
                market_id,
                bids,
                asks,
                timestamp: snapshot.timestamp,
            };

            let topic_name = format!("market-{}-orderbook", market_id);
            self.publish_event(&topic_name, &market_id.to_string(), &orderbook_event);

            info!(
                "[KafkaEventsHandler] Published orderbook event for market {}",
                market_id
            );
        }
    }
}

impl EventsHandler for KafkaEventsHandler {
    fn handle_processed_command(&self, cmd: &mut OrderCommand) {
        info!(
            "[KafkaEventsHandler] Processing command: Order {}, Status {:?}",
            cmd.order_id(),
            cmd.status()
        );

        let market_id = cmd.market_id();
        let taker_id = cmd.user_id();
        let taker_order_id = cmd.order_id();

        match cmd.status() {
            Status::Rejected => {
                info!(
                    "[KafkaEventsHandler] Order {} rejected - no events published",
                    cmd.order_id()
                );
            }
            Status::Placed => {
                self.publish_order_event(cmd);
                self.publish_orderbook_event(market_id, &cmd.l2_data);
            }
            Status::Cancelled => {
                self.publish_balance_event(taker_id, &cmd, &cmd.balance);
                self.publish_cancel_order_event(cmd);
                self.publish_orderbook_event(market_id, &cmd.l2_data);
            }
            Status::PartiallyFilled | Status::Filled => {
                let mut curr_event = cmd.events();
                while let Some(event) = curr_event {
                    // Trade Event
                    self.publish_trade_event(event, cmd, market_id, taker_id, taker_order_id);

                    // Balance Event for the maker
                    self.publish_balance_event(event.maker_user_id, &cmd, &event.maker_balance);

                    curr_event = event.next_event.as_deref();
                }
                // Publish balance event for the taker
                self.publish_balance_event(taker_id, &cmd, &cmd.balance);
            }
            Status::Processing => {
                // this should ideally be unreachable
                error!("[KafkaEventsHandler] Order was not processed correctly");
                self.publish_orderbook_event(market_id, &cmd.l2_data);
            }
        }
    }
}

// Event structures for Kafka messages
#[derive(Serialize, Deserialize, Debug)]
struct BalanceEvent {
    user_id: u64,
    market_id: u32,
    available: u64,
    locked: u64,
    total: u64,
    timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct OrderEvent {
    order: Order,
    market_id: u32,
}

#[derive(Serialize, Deserialize, Debug)]
struct TradeEvent {
    maker_user_id: u64,
    taker_user_id: u64,
    market_id: u32,
    price: u64,
    size: u64,
    maker_order_id: u64,
    taker_order_id: u64,
    timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct CancelOrderEvent {
    order_id: u64,
    market_id: u32,
    user_id: u64,
    timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct OrderbookEvent {
    market_id: u32,
    bids: Vec<OrderbookLevel>,
    asks: Vec<OrderbookLevel>,
    timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct OrderbookLevel {
    price: u64,
    size: u64,
}

#[cfg(test)]
mod tests {
    // use std::sync::Arc;

    // use super::*;
    // use common::{CoreMarketSpecification, MarketType, PriceCache, Side, UserBalance};
    // use vex_orderbook::OrderBook;
    // use vex_orderbook::tree::{BTreeAskSide, BTreeBidSide};

    // const MARKET_ID: u32 = 10_000_0010; // Example market_id encoding

    // #[tokio::test]
    // async fn test_kafka_events_handler_placed_order() {
    //     let handler = KafkaEventsHandler::new("localhost:9093");

    //     let mut cmd = OrderCommand::new(
    //         common::TimeInForce::Gtc,
    //         12345, // order_id
    //         1001,  // user_id
    //         1000,  // price
    //         100,   // size
    //         Side::Bid,
    //         MARKET_ID,
    //     );
    //     cmd.set_status(Status::Placed);
    //     cmd.timestamp = 1000;

    //     handler.handle_processed_command(&mut cmd, None, None);

    //     // Wait for async Kafka send to complete
    //     tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    // }

    // #[tokio::test]
    // async fn test_kafka_events_handler_cancelled_order() {
    //     let handler = KafkaEventsHandler::new("localhost:9093");

    //     let mut cmd = OrderCommand::new(
    //         common::TimeInForce::Gtc,
    //         12346, // order_id
    //         1002,  // user_id
    //         950,   // price
    //         50,    // size
    //         Side::Ask,
    //         MARKET_ID,
    //     );
    //     cmd.set_status(Status::Cancelled);
    //     cmd.timestamp = 1001;

    //     handler.handle_processed_command(&mut cmd, None, None);

    //     tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    // }

    // #[tokio::test]
    // async fn test_kafka_events_handler_with_risk_engine() {
    //     use common::UserBalance;
    //     use hashbrown::HashMap;

    //     let risk_engine = RiskEngine::new(HashMap::new(), 0, 1);

    //     let market_id: u32 = 0b1000000100000010;
    //     let base_asset_id = base_asset(market_id);
    //     let quote_asset_id = quote_asset(market_id);
    //     let balance = UserBalance::new(10000000000, 100);
    //     risk_engine.set_balance(1001, base_asset_id, balance);
    //     risk_engine.set_balance(1001, quote_asset_id, balance);

    //     let handler = KafkaEventsHandler::new("localhost:9093");

    //     let mut cmd = OrderCommand::new(
    //         common::TimeInForce::Gtc,
    //         12349, // order_id
    //         1001,  // user_id
    //         900,   // price
    //         25,    // size
    //         Side::Ask,
    //         market_id,
    //     );
    //     cmd.set_status(Status::Cancelled);
    //     cmd.timestamp = 1004;

    //     handler.handle_processed_command(&mut cmd, Some(&risk_engine), None);

    //     tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    // }

    // #[tokio::test]
    // async fn test_orderbook_event_with_real_orderbook() {
    //     let handler = KafkaEventsHandler::new("localhost:9093");

    //     let mut orderbook = OrderBook::new(BTreeBidSide::new(), BTreeAskSide::new(), 10);
    //     let mut symbol_spec = hashbrown::HashMap::new();
    //     symbol_spec.insert(
    //         10u32,
    //         CoreMarketSpecification::builder()
    //             .market_id((1u32 << 16) | (2u32)) // Example market_id encoding
    //             .market_type(MarketType::Spot)
    //             .base_scale_k(1)
    //             .quote_scale_k(1)
    //             .build()
    //             .unwrap(),
    //     );
    //     let price_cache = Arc::new(PriceCache::new(symbol_spec.keys()));
    //     let mut bid_cmd = common::OrderCommand {
    //         command: common::OrderCommandType::PlaceOrder,
    //         order_id: 1,
    //         timestamp: 100,
    //         user_id: 1001,
    //         market_id: 1,
    //         price: 1000,
    //         size: 100,
    //         side: Side::Bid,
    //         time_in_force: common::TimeInForce::Gtc,
    //         status: common::Status::Processing,
    //         balance: [UserBalance::default(); 2],
    //         events: None,
    //     };
    //     orderbook.place_order(&mut bid_cmd, price_cache.clone());

    //     let mut ask_cmd = common::OrderCommand {
    //         command: common::OrderCommandType::PlaceOrder,
    //         order_id: 2,
    //         timestamp: 101,
    //         user_id: 1002,
    //         market_id: 1,
    //         price: 1100,
    //         size: 50,
    //         side: Side::Ask,
    //         time_in_force: common::TimeInForce::Gtc,
    //         balance: [UserBalance::default(); 2],
    //         status: common::Status::Processing,
    //         events: None,
    //     };
    //     orderbook.place_order(&mut ask_cmd, price_cache);

    //     let mut cmd = OrderCommand::new(
    //         common::TimeInForce::Gtc,
    //         12350, // order_id
    //         1003,  // user_id
    //         1050,  // price
    //         75,    // size
    //         Side::Bid,
    //         MARKET_ID,
    //     );
    //     cmd.set_status(Status::Placed);
    //     cmd.timestamp = 1005;

    //     let snapshot = orderbook.create_snapshot_with_depth(50);
    //     handler.handle_processed_command(&mut cmd, None, Some(snapshot));

    //     tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    // }

    // #[tokio::test]
    // async fn test_kafka_events_handler_filled_order_with_trades() {
    //     let handler = KafkaEventsHandler::new("localhost:9093");

    //     // Create a processed command with Filled status and trade events
    //     let mut filled_cmd = OrderCommand::new(
    //         common::TimeInForce::Gtc,
    //         12348, // order_id
    //         1004,  // user_id
    //         1050,  // price
    //         200,   // size
    //         Side::Bid,
    //         MARKET_ID,
    //     );
    //     filled_cmd.set_status(Status::Filled);
    //     filled_cmd.timestamp = 1003;

    //     // Create trade events with all required fields
    //     let trade2 = MatcherTradeEvent {
    //         active_order_completed: false,
    //         matched_order_id: 201,
    //         maker_user_id: 1003,
    //         matched_order_completed: true,
    //         price: 1050,
    //         size: 50,
    //         next_event: None,
    //         maker_balance: [UserBalance::default(); 2],
    //     };
    //     let trade1 = MatcherTradeEvent {
    //         active_order_completed: false,
    //         matched_order_id: 202,
    //         maker_user_id: 1004,
    //         matched_order_completed: false,
    //         price: 1040,
    //         size: 150,
    //         next_event: Some(Box::new(trade2)),
    //         maker_balance: [UserBalance::default(); 2],
    //     };

    //     // Use the correct method name (note the typo in the original)
    //     filled_cmd.attatch_event(Box::new(trade1));

    //     handler.handle_processed_command(&mut filled_cmd, None, None);

    //     tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    // }
}
