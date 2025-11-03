use crate::risk_engine::RiskEngine;
use common::L2MarketData;
use common::MatcherTradeEvent;
use common::Order;
use common::ProcessedOrderCommand;
use common::Status;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{error, info};

// Real Kafka dependencies
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

pub trait EventsHandler: Send + Sync {
    fn handle_processed_command(
        &self,
        processed_cmd: &ProcessedOrderCommand,
        risk_engine: Option<&RiskEngine>,
        orderbook_snapshot: Option<L2MarketData<50>>,
    );
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

    fn publish_balance_event(
        &self,
        user_id: u64,
        market_id: u32,
        risk_engine: &RiskEngine,
        cmd: &ProcessedOrderCommand,
    ) -> Result<(), String> {
        if let Some(user_profile) = risk_engine.user_balances.get(&user_id)
            && let Ok(balance) = user_profile.get_balance(user_id, market_id)
        {
            let balance_event = BalanceEvent {
                user_id,
                market_id,
                available: balance.available(),
                locked: balance.locked(),
                total: balance.total(),
                timestamp: cmd.timestamp(),
            };

            let topic_name = format!("market-{}-balances", market_id);
            self.publish_event(&topic_name, &user_id.to_string(), &balance_event);
            info!(
                "[KafkaEventsHandler] Published balance event for user {} in market {}",
                user_id, market_id
            );
        }
        Ok(())
    }

    fn publish_order_event(&self, cmd: &ProcessedOrderCommand) -> Result<(), String> {
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

        Ok(())
    }

    fn publish_trade_event(
        &self,
        event: &MatcherTradeEvent,
        cmd: &ProcessedOrderCommand,
        market_id: u32,
        taker_id: u64,
        taker_order_id: u64,
    ) -> Result<(), String> {
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

        Ok(())
    }

    fn publish_cancel_order_event(&self, cmd: &ProcessedOrderCommand) -> Result<(), String> {
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

        Ok(())
    }

    fn publish_orderbook_event(
        &self,
        market_id: u32,
        orderbook_snapshot: Option<L2MarketData<50>>,
    ) -> Result<(), String> {
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

            Ok(())
        } else {
            Ok(())
        }
    }
}

impl EventsHandler for KafkaEventsHandler {
    fn handle_processed_command(
        &self,
        processed_cmd: &ProcessedOrderCommand,
        risk_engine: Option<&RiskEngine>,
        orderbook_snapshot: Option<L2MarketData<50>>,
    ) {
        info!(
            "[KafkaEventsHandler] Processing command: Order {}, Status {:?}",
            processed_cmd.order_id(),
            processed_cmd.status()
        );

        let market_id = processed_cmd.market_id();
        let taker_id = processed_cmd.taker_id();
        let taker_order_id = processed_cmd.order_id();

        match processed_cmd.status() {
            Status::Rejected => {
                info!(
                    "[KafkaEventsHandler] Order {} rejected - no events published",
                    processed_cmd.order_id()
                );
            }
            Status::Placed => {
                if let Err(e) = self.publish_order_event(processed_cmd) {
                    error!("[KafkaEventsHandler] Failed to publish order event: {}", e);
                }
                if let Err(e) = self.publish_orderbook_event(market_id, orderbook_snapshot) {
                    error!(
                        "[KafkaEventsHandler] Failed to publish orderbook event: {}",
                        e
                    );
                }
            }
            Status::Cancelled => {
                if let Some(risk_engine) = risk_engine
                    && let Err(e) =
                        self.publish_balance_event(taker_id, market_id, risk_engine, processed_cmd)
                {
                    error!(
                        "[KafkaEventsHandler] Failed to publish balance event: {}",
                        e
                    );
                }
                if let Err(e) = self.publish_cancel_order_event(processed_cmd) {
                    error!(
                        "[KafkaEventsHandler] Failed to publish cancel order event: {}",
                        e
                    );
                }
                if let Err(e) = self.publish_orderbook_event(market_id, orderbook_snapshot) {
                    error!(
                        "[KafkaEventsHandler] Failed to publish orderbook event: {}",
                        e
                    );
                }
            }
            Status::PartiallyFilled | Status::Filled => {
                if let Some(event) = processed_cmd.events() {
                    if let Err(e) = self.publish_trade_event(
                        event,
                        processed_cmd,
                        market_id,
                        taker_id,
                        taker_order_id,
                    ) {
                        error!("[KafkaEventsHandler] Failed to publish trade event: {}", e);
                    }

                    let mut current_event = event.next_event.as_ref();
                    while let Some(next_event) = current_event {
                        if let Err(e) = self.publish_trade_event(
                            next_event,
                            processed_cmd,
                            market_id,
                            taker_id,
                            taker_order_id,
                        ) {
                            error!(
                                "[KafkaEventsHandler] Failed to publish chained trade event: {}",
                                e
                            );
                        }
                        current_event = next_event.next_event.as_ref();
                    }

                    if let Some(risk_engine) = risk_engine {
                        if let Err(e) = self.publish_balance_event(
                            taker_id,
                            market_id,
                            risk_engine,
                            processed_cmd,
                        ) {
                            error!(
                                "[KafkaEventsHandler] Failed to publish taker balance event: {}",
                                e
                            );
                        }

                        if let Err(e) = self.publish_balance_event(
                            event.maker_user_id,
                            market_id,
                            risk_engine,
                            processed_cmd,
                        ) {
                            error!(
                                "[KafkaEventsHandler] Failed to publish main maker balance event: {}",
                                e
                            );
                        }

                        let mut current_event = event.next_event.as_ref();
                        while let Some(next_event) = current_event {
                            if let Err(e) = self.publish_balance_event(
                                next_event.maker_user_id,
                                market_id,
                                risk_engine,
                                processed_cmd,
                            ) {
                                error!(
                                    "[KafkaEventsHandler] Failed to publish chained maker balance event: {}",
                                    e
                                );
                            }
                            current_event = next_event.next_event.as_ref();
                        }
                    }
                }

                if let Err(e) = self.publish_orderbook_event(market_id, orderbook_snapshot) {
                    error!(
                        "[KafkaEventsHandler] Failed to publish orderbook event: {}",
                        e
                    );
                }
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
    use super::*;
    use common::Side;
    use vex_orderbook::OrderBook;
    use vex_orderbook::tree::{BTreeAskSide, BTreeBidSide};

    #[tokio::test]
    async fn test_kafka_events_handler_placed_order() {
        let handler = KafkaEventsHandler::new("localhost:9093");

        let processed_cmd = ProcessedOrderCommand::new(
            Status::Placed,
            12345, // order_id
            1001,  // taker_id
            1,     // market_id
            1000,  // price
            100,   // size
            1000,  // timestamp
            Side::Bid,
        );

        handler.handle_processed_command(&processed_cmd, None, None);

        // Wait for async Kafka send to complete
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }

    #[tokio::test]
    async fn test_kafka_events_handler_cancelled_order() {
        let handler = KafkaEventsHandler::new("localhost:9093");

        let processed_cmd = ProcessedOrderCommand::new(
            Status::Cancelled,
            12346, // order_id
            1002,  // taker_id
            1,     // market_id
            950,   // price
            50,    // size
            1001,  // timestamp
            Side::Ask,
        );

        handler.handle_processed_command(&processed_cmd, None, None);

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }

    #[tokio::test]
    async fn test_kafka_events_handler_with_risk_engine() {
        use common::{BalanceStore, UserBalance};
        use hashbrown::HashMap;

        let mut risk_engine = RiskEngine::new(HashMap::new(), 0, 1);

        let mut user_profile = BalanceStore::new();
        let balance = UserBalance::new();
        user_profile.set_balance(1001, 1, balance);
        risk_engine.user_balances.insert(1001, user_profile);

        let handler = KafkaEventsHandler::new("localhost:9093");

        let processed_cmd = ProcessedOrderCommand::new(
            Status::Cancelled,
            12349, // order_id
            1001,  // taker_id
            1,     // market_id
            900,   // price
            25,    // size
            1004,  // timestamp
            Side::Ask,
        );

        handler.handle_processed_command(&processed_cmd, Some(&risk_engine), None);

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }

    #[tokio::test]
    async fn test_orderbook_event_with_real_orderbook() {
        let handler = KafkaEventsHandler::new("localhost:9093");

        let mut orderbook = OrderBook::new(BTreeBidSide::new(), BTreeAskSide::new());

        let bid_cmd = common::OrderCommand {
            command: common::OrderCommandType::PlaceOrder,
            order_id: 1,
            timestamp: 100,
            user_id: 1001,
            market_id: 1,
            price: 1000,
            size: 100,
            side: Side::Bid,
            time_in_force: common::TimeInForce::Gtc,
        };
        orderbook.place_order(&bid_cmd);

        let ask_cmd = common::OrderCommand {
            command: common::OrderCommandType::PlaceOrder,
            order_id: 2,
            timestamp: 101,
            user_id: 1002,
            market_id: 1,
            price: 1100,
            size: 50,
            side: Side::Ask,
            time_in_force: common::TimeInForce::Gtc,
        };
        orderbook.place_order(&ask_cmd);

        let processed_cmd = ProcessedOrderCommand::new(
            Status::Placed,
            12350, // order_id
            1003,  // taker_id
            1,     // market_id
            1050,  // price
            75,    // size
            1005,  // timestamp
            Side::Bid,
        );

        let snapshot = orderbook.create_snapshot_with_depth(50);
        handler.handle_processed_command(&processed_cmd, None, Some(snapshot));

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }

    #[tokio::test]
    async fn test_kafka_events_handler_filled_order_with_trades() {
        let handler = KafkaEventsHandler::new("localhost:9093");

        // Create a processed command with Filled status and trade events
        let mut filled_cmd = ProcessedOrderCommand::new(
            Status::Filled,
            12348, // order_id
            1004,  // taker_id
            1,     // market_id
            1050,  // price
            200,   // size
            1003,  // timestamp
            Side::Bid,
        );

        // Create trade events with all required fields
        let trade2 = MatcherTradeEvent {
            active_order_completed: false,
            matched_order_id: 201,
            maker_user_id: 1003,
            matched_order_completed: true,
            price: 1050,
            size: 50,
            next_event: None,
        };
        let trade1 = MatcherTradeEvent {
            active_order_completed: false,
            matched_order_id: 202,
            maker_user_id: 1004,
            matched_order_completed: false,
            price: 1040,
            size: 150,
            next_event: Some(Box::new(trade2)),
        };

        // Use the correct method name (note the typo in the original)
        filled_cmd.attatch_event(Box::new(trade1));

        handler.handle_processed_command(&filled_cmd, None, None);

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }
}
