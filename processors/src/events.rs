use std::sync::Arc;
use std::thread;

use crate::journaling::ReplayControl;
use common::L2MarketData;
use common::MatcherTradeEvent;
use common::Order;
use common::OrderCommand;
use common::Status;
use common::UserBalance;
use common::{
    BalanceEvent, CancelOrderEvent, OrderEvent, OrderbookEvent, OrderbookLevel, TradeEvent,
};
use common::{base_asset, order_debug, order_info, quote_asset};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde::Serialize;
use tracing::{debug, error, info};
use vex_networking::server::Publications;

pub trait EventsHandler: Send + Sync + 'static {
    fn handle_processed_command(&self, cmd: &mut OrderCommand);
}

// Real Kafka Events Handler
pub struct KafkaEventsHandler {
    producer: FutureProducer,
    publications: Arc<Publications>,
    replay_control: ReplayControl,
}

impl KafkaEventsHandler {
    pub fn new(
        brokers: &str,
        publications: Arc<Publications>,
        replay_control: ReplayControl,
    ) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("retry.backoff.ms", "100")
            .set("request.timeout.ms", "3000")
            .create()
            .expect("Producer creation failed");

        info!(
            target: "events",
            component = "kafka_handler",
            action = "connected",
            brokers = %brokers
        );

        Self {
            producer,
            publications,
            replay_control,
        }
    }

    fn publish_event<T: Serialize>(&self, topic_name: &str, message_key: &str, payload: &T) {
        match serde_json::to_string(payload) {
            Ok(json_payload) => {
                let producer = self.producer.clone();
                let message_key = message_key.to_string();
                let topic_name = topic_name.to_string();

                // Spawn async task to send to Kafka
                thread::spawn(move || {
                    let record = FutureRecord::to(&topic_name)
                        .payload(&json_payload)
                        .key(&message_key);

                    match producer.send_result(record) {
                        Ok(_) => {
                            debug!(
                                target: "events",
                                component = "kafka_handler",
                                action = "event_sent",
                                topic = %topic_name,
                                key = %message_key
                            );
                        }
                        Err((e, _)) => {
                            error!(
                                target: "events",
                                component = "kafka_handler",
                                action = "event_failed",
                                topic = %topic_name,
                                error = ?e
                            );
                        }
                    }
                });
            }
            Err(e) => error!(
                target: "events",
                component = "kafka_handler",
                action = "serialization_failed",
                topic = %topic_name,
                error = ?e
            ),
        }
    }

    fn publish_balance_event(&self, user_id: u64, cmd: &OrderCommand, balance: &[UserBalance; 2]) {
        let base_asset_id = base_asset(cmd.market_id);
        let quote_asset_id = quote_asset(cmd.market_id);

        for (balance, asset_id) in balance.iter().zip([base_asset_id, quote_asset_id]) {
            let balance_event = BalanceEvent {
                user_id,
                asset_id,
                available: balance.available(),
                locked: balance.locked(),
                total: balance.total(),
                timestamp: cmd.timestamp(),
            };

            let topic_name = format!("asset-{}-balances", asset_id);
            self.publish_event(&topic_name, &user_id.to_string(), &balance_event);
            debug!(
                target: "events",
                component = "kafka_handler",
                action = "balance_event_published",
                user_id,
                market_id = cmd.market_id(),
                asset_id,
                topic = %topic_name
            );
        }
    }

    fn publish_deposit_withdrwal_event(&self, cmd: &OrderCommand) {
        let asset_id = cmd.market_id as u16;

        let balance_event = BalanceEvent {
            user_id: cmd.user_id(),
            asset_id,
            available: cmd.balance[0].available(),
            locked: cmd.balance[0].locked(),
            total: cmd.balance[0].total(),
            timestamp: cmd.timestamp(),
        };

        let topic_name = format!("asset-{}-balances", cmd.market_id);
        self.publish_event(&topic_name, &cmd.user_id.to_string(), &balance_event);
        debug!(
            target: "events",
            component = "kafka_handler",
            action = "balance_event_published",
            user_id = cmd.user_id(),
            market_id = cmd.market_id(),
            asset_id,
            topic = %topic_name
        );
    }

    fn publish_order_event(&self, cmd: &OrderCommand) {
        let order = Order {
            order_id: cmd.order_id(),
            user_id: cmd.user_id(),
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
        debug!(
            target: "events",
            component = "kafka_handler",
            action = "order_event_published",
            order_id = cmd.order_id(),
            market_id = cmd.market_id(),
            topic = %topic_name
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

        let topic_name = format!("market-{market_id}-trades");
        let trade_key = format!("{}:{}", taker_order_id, event.matched_order_id);
        self.publish_event(&topic_name, &trade_key, &trade_event);
        debug!(
            target: "events",
            component = "kafka_handler",
            action = "trade_event_published",
            maker_order_id = event.matched_order_id,
            taker_order_id,
            market_id,
            topic = %topic_name,
            key = %trade_key
        );
    }

    fn publish_cancel_order_event(&self, cmd: &OrderCommand) {
        let cancel_event = CancelOrderEvent {
            order_id: cmd.order_id(),
            market_id: cmd.market_id(),
            user_id: cmd.user_id(),
            timestamp: cmd.timestamp(),
        };

        let topic_name = format!("market-{}-cancels", cmd.market_id());
        self.publish_event(&topic_name, &cmd.order_id().to_string(), &cancel_event);
        debug!(
            target: "events",
            component = "kafka_handler",
            action = "cancel_event_published",
            order_id = cmd.order_id(),
            market_id = cmd.market_id(),
            topic = %topic_name
        );
    }

    fn publish_orderbook_event(&self, market_id: u32, orderbook_snapshot: &Option<L2MarketData>) {
        if let Some(snapshot) = orderbook_snapshot {
            let mut bids = Vec::new();
            let mut asks = Vec::new();

            for i in 0..snapshot.bid_depth() {
                if snapshot.bid_prices[i] > 0 {
                    bids.push(OrderbookLevel {
                        price: snapshot.bid_prices[i],
                        size: snapshot.bid_volumes[i],
                    });
                }
            }

            for i in 0..snapshot.ask_depth() {
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

            let topic_name = format!("market-{market_id}-orderbook");
            self.publish_event(&topic_name, &market_id.to_string(), &orderbook_event);

            debug!(
                target: "events",
                component = "kafka_handler",
                action = "orderbook_event_published",
                market_id,
                topic = %topic_name
            );
        }
    }

    fn publish_response(&self, cmd: &OrderCommand) {
        self.publications.publish_response(cmd);
    }
}

impl EventsHandler for KafkaEventsHandler {
    fn handle_processed_command(&self, cmd: &mut OrderCommand) {
        if self.replay_control.is_enabled() {
            order_debug!(
                "events_skip_replay",
                cmd,
                stage = "events",
                handler = "kafka"
            );
            return;
        }

        order_info!(
            "command_processed",
            cmd,
            stage = "events",
            handler = "kafka"
        );

        let market_id = cmd.market_id();
        let taker_id = cmd.user_id();
        let taker_order_id = cmd.order_id();

        match cmd.status() {
            Status::Rejected => {
                order_debug!(
                    "events_noop_rejected",
                    cmd,
                    stage = "events",
                    handler = "kafka"
                );
            }
            Status::Placed => {
                order_debug!(
                    "events_publish_placed",
                    cmd,
                    stage = "events",
                    handler = "kafka"
                );
                self.publish_balance_event(taker_id, cmd, &cmd.balance);
                self.publish_order_event(cmd);
                self.publish_orderbook_event(market_id, &cmd.l2_data);
            }
            Status::Cancelled => {
                order_debug!(
                    "events_publish_cancelled",
                    cmd,
                    stage = "events",
                    handler = "kafka"
                );
                self.publish_balance_event(taker_id, cmd, &cmd.balance);
                self.publish_cancel_order_event(cmd);
                self.publish_orderbook_event(market_id, &cmd.l2_data);
            }
            Status::PartiallyFilled | Status::Filled => {
                order_debug!(
                    "events_publish_trade",
                    cmd,
                    stage = "events",
                    handler = "kafka"
                );
                let mut curr_event = cmd.events();
                while let Some(event) = curr_event {
                    // Trade Event
                    self.publish_trade_event(event, cmd, market_id, taker_id, taker_order_id);

                    // Balance Event for the maker
                    self.publish_balance_event(event.maker_user_id, cmd, &event.maker_balance);

                    curr_event = event.next_event.as_deref();
                }
                // Publish balance event for the taker
                self.publish_balance_event(taker_id, cmd, &cmd.balance);
            }
            Status::Processing => {
                // this should ideally be unreachable
                error!(
                    target: "events",
                    component = "kafka_handler",
                    action = "unexpected_processing_status",
                    order_id = cmd.order_id()
                );
                self.publish_orderbook_event(market_id, &cmd.l2_data);
            }
            Status::Processed => {
                order_debug!(
                    "events_publish_balance_update",
                    cmd,
                    stage = "events",
                    handler = "kafka"
                );
                self.publish_deposit_withdrwal_event(cmd);
            }
        }
        // Always publish the response back to the gateway
        self.publish_response(cmd);
    }
}

impl Drop for KafkaEventsHandler {
    fn drop(&mut self) {
        // Flush any remaining messages before dropping
        if let Err(e) = self.producer.flush(std::time::Duration::from_secs(20)) {
            error!(
                target: "events",
                component = "kafka_handler",
                action = "flush_failed",
                error = ?e
            );
        } else {
            debug!(
                target: "events",
                component = "kafka_handler",
                action = "flush_complete"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{Side, UserBalance};

    const MARKET_ID: u32 = 100_000_010; // Example market_id encoding

    #[tokio::test]
    async fn test_kafka_events_handler_placed_order() {
        let handler =
            KafkaEventsHandler::new("localhost:9092", Arc::new(Publications::new()), ReplayControl::disabled());

        let mut cmd = OrderCommand::new(
            common::TimeInForce::Gtc,
            12345, // order_id
            1001,  // user_id
            1000,  // price
            100,   // size
            Side::Bid,
            MARKET_ID,
        );
        cmd.set_status(Status::Placed);
        cmd.timestamp = 1000;

        handler.handle_processed_command(&mut cmd);

        // Wait for async Kafka send to complete
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }

    #[tokio::test]
    async fn test_kafka_events_handler_cancelled_order() {
        let handler =
            KafkaEventsHandler::new("localhost:9092", Arc::new(Publications::new()), ReplayControl::disabled());

        let mut cmd = OrderCommand::new(
            common::TimeInForce::Gtc,
            12346, // order_id
            1002,  // user_id
            950,   // price
            50,    // size
            Side::Ask,
            MARKET_ID,
        );
        cmd.set_status(Status::Cancelled);
        cmd.timestamp = 1001;

        handler.handle_processed_command(&mut cmd);

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }

    #[tokio::test]
    async fn test_kafka_events_handler_filled_order_with_trades() {
        let handler =
            KafkaEventsHandler::new("localhost:9092", Arc::new(Publications::new()), ReplayControl::disabled());

        // Create a processed command with Filled status and trade events
        let mut filled_cmd = OrderCommand::new(
            common::TimeInForce::Gtc,
            12348, // order_id
            1004,  // user_id
            1050,  // price
            200,   // size
            Side::Bid,
            MARKET_ID,
        );
        filled_cmd.set_status(Status::Filled);
        filled_cmd.timestamp = 1003;

        // Create trade events with all required fields
        let trade2 = MatcherTradeEvent {
            active_order_completed: false,
            matched_order_id: 201,
            maker_user_id: 1003,
            matched_order_completed: true,
            price: 1050,
            size: 50,
            next_event: None,
            maker_balance: [UserBalance::default(); 2],
        };
        let trade1 = MatcherTradeEvent {
            active_order_completed: false,
            matched_order_id: 202,
            maker_user_id: 1004,
            matched_order_completed: false,
            price: 1040,
            size: 150,
            next_event: Some(Box::new(trade2)),
            maker_balance: [UserBalance::default(); 2],
        };

        // Use the correct method name (note the typo in the original)
        filled_cmd.attatch_event(Box::new(trade1));

        handler.handle_processed_command(&mut filled_cmd);

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }
}
