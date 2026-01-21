use std::sync::Arc;

use crate::journaling::ReplayControl;
use common::L2MarketData;
use common::MatcherTradeEvent;
use common::OrderCommand;
use common::OrderCommandType;
use common::Status;
use common::UserBalance;
use common::{base_asset, order_debug, order_info, quote_asset};
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use schema_registry_converter::async_impl::easy_proto_raw::EasyProtoRawEncoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::{
    SchemaType, SubjectNameStrategy, SuppliedSchema,
};
use serde::Serialize;
use tokio::runtime::Runtime;
use tracing::{debug, error, info};
use vex_networking::server::Publications;

// Include generated protobuf code
pub mod trading_proto {
    include!(concat!(env!("OUT_DIR"), "/trading.rs"));
}

// Embed the .proto file content for Schema Registry registration
const TRADING_SCHEMA: &str = include_str!("protos/trading.proto");

// Helper struct for JSON serialization of Orderbook Levels
#[derive(Serialize)]
struct JsonOrderbookLevel {
    price: u64,
    size: u64,
}

pub trait EventsHandler: Send + Sync + 'static {
    fn handle_processed_command(&self, cmd: &mut OrderCommand);
}

// Real Kafka Events Handler
pub struct KafkaEventsHandler {
    producer: FutureProducer,
    encoder: Arc<EasyProtoRawEncoder>,
    publications: Arc<Publications>,
    replay_control: ReplayControl,
    rt: Arc<Runtime>,
}

impl KafkaEventsHandler {
    pub fn new(
        brokers: &str,
        schema_registry_url: &str,
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

        // Initialize Schema Registry Encoder
        let sr_settings = SrSettings::new(schema_registry_url.to_string());
        let encoder = Arc::new(EasyProtoRawEncoder::new(sr_settings));

        // Create a dedicated runtime for async tasks
        let rt = Arc::new(Runtime::new().expect("Failed to create tokio runtime"));

        info!(
            target: "events",
            component = "kafka_handler",
            action = "connected",
            brokers = %brokers,
            schema_registry = %schema_registry_url
        );

        Self {
            producer,
            encoder,
            publications,
            replay_control,
            rt,
        }
    }

    fn publish_proto<T: Message + Send + Sync + 'static>(
        &self,
        topic_name: &str,
        message_key: &str,
        full_name: &str,
        data: T,
    ) {
        let encoder = self.encoder.clone();
        let producer = self.producer.clone();
        let topic = topic_name.to_string();
        let key_str = message_key.to_string();
        let full_name = full_name.to_string();

        // Spawn async task on the dedicated runtime
        self.rt.spawn(async move {
            // Serialize data to bytes using Prost
            let payload_bytes = data.encode_to_vec();

            // Create SuppliedSchema with the .proto content for Schema Registry
            let supplied_schema = SuppliedSchema {
                name: Some(full_name.clone()),
                schema_type: SchemaType::Protobuf,
                schema: TRADING_SCHEMA.to_string(),
                references: vec![],
                properties: None,
                tags: None,
            };

            // Encode with schema registration (magic byte + schema ID)
            // Use TopicNameStrategyWithSchema so schema is registered as "<topic>-value"
            // This ensures Kafka Connect (JDBC Sink) can find the schema
            let strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
                topic.clone(),
                false,
                supplied_schema,
            );

            let encoded_payload = match encoder.encode(&payload_bytes, &full_name, strategy).await {
                Ok(bytes) => bytes,
                Err(e) => {
                    error!(
                        target: "events",
                        component = "kafka_handler",
                        action = "protobuf_encoding_failed",
                        topic = %topic,
                        error = ?e
                    );
                    return;
                }
            };

            // Send to Kafka
            let record = FutureRecord::to(&topic)
                .payload(&encoded_payload)
                .key(&key_str);

            match producer
                .send(record, tokio::time::Duration::from_secs(5))
                .await
            {
                Ok(_) => {
                    debug!(
                        target: "events",
                        component = "kafka_handler",
                        action = "event_sent",
                        topic = %topic,
                        key = %key_str
                    );
                }
                Err((e, _)) => {
                    error!(
                        target: "events",
                        component = "kafka_handler",
                        action = "event_failed",
                        topic = %topic,
                        error = ?e
                    );
                }
            }
        });
    }

    fn publish_balance_event(&self, user_id: u64, cmd: &OrderCommand, balance: &[UserBalance; 2]) {
        let base_asset_id = base_asset(cmd.market_id);
        let quote_asset_id = quote_asset(cmd.market_id);

        for (balance, asset_id) in balance.iter().zip([base_asset_id, quote_asset_id]) {
            let balance_event = trading_proto::BalanceEvent {
                user_id,
                asset_id: asset_id as u32,
                available: balance.available(),
                locked: balance.locked(),
                total: balance.total(),
                timestamp: cmd.timestamp(),
            };

            let topic_name = "balances";
            let composite_key = format!("{}:{}", user_id, asset_id);
            self.publish_proto(
                topic_name,
                &composite_key,
                "trading.BalanceEvent",
                balance_event,
            );
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

    fn publish_deposit_withdrawal_event(&self, cmd: &OrderCommand) {
        let asset_id = cmd.market_id as u16;

        let balance_event = trading_proto::BalanceEvent {
            user_id: cmd.user_id(),
            asset_id: asset_id as u32,
            available: cmd.balance[0].available(),
            locked: cmd.balance[0].locked(),
            total: cmd.balance[0].total(),
            timestamp: cmd.timestamp(),
        };

        let topic_name = "balances";
        let composite_key = format!("{}:{}", cmd.user_id(), asset_id);
        self.publish_proto(
            topic_name,
            &composite_key,
            "trading.BalanceEvent",
            balance_event,
        );
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

    fn publish_order_event(&self, cmd: &OrderCommand, original_size: Option<u64>) {
        let side = match cmd.side() {
            common::Side::Bid => trading_proto::Side::Bid,
            common::Side::Ask => trading_proto::Side::Ask,
        };

        let time_in_force = match cmd.time_in_force {
            common::TimeInForce::Gtc => trading_proto::TimeInForce::TifGtc,
            common::TimeInForce::Ioc => trading_proto::TimeInForce::TifIoc,
            common::TimeInForce::Fok => trading_proto::TimeInForce::TifFok,
        };

        let status = match cmd.status() {
            Status::Rejected => trading_proto::Status::Rejected,
            Status::Placed => trading_proto::Status::Placed,
            Status::Cancelled => trading_proto::Status::Cancelled,
            Status::PartiallyFilled => trading_proto::Status::PartiallyFilled,
            Status::Filled => trading_proto::Status::Filled,
            Status::Processing => trading_proto::Status::Processing,
            Status::Processed => trading_proto::Status::Processed,
        };

        let order = trading_proto::Order {
            order_id: cmd.order_id(),
            user_id: cmd.user_id(),
            price: cmd.price(),
            size: original_size.unwrap_or_else(|| cmd.size()),
            side: side as i32,
            time_in_force: time_in_force as i32,
            status: status as i32,
            timestamp: cmd.timestamp(),
        };

        let order_event = trading_proto::OrderEvent {
            order: Some(order),
            market_id: cmd.market_id(),
        };

        let topic_name = "orders";
        self.publish_proto(
            topic_name,
            &cmd.order_id().to_string(),
            "trading.OrderEvent",
            order_event,
        );
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
        let taker_side = match cmd.side() {
            common::Side::Bid => trading_proto::Side::Bid,
            common::Side::Ask => trading_proto::Side::Ask,
        };

        let trade_event = trading_proto::TradeEvent {
            maker_user_id: event.maker_user_id,
            taker_user_id: taker_id,
            market_id,
            price: event.price,
            size: event.size,
            maker_order_id: event.matched_order_id,
            taker_order_id,
            taker_side: taker_side as i32,
            timestamp: cmd.timestamp(),
        };

        let topic_name = "trades";
        let trade_key = format!("{}:{}", taker_order_id, event.matched_order_id);
        self.publish_proto(topic_name, &trade_key, "trading.TradeEvent", trade_event);
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
        let cancel_event = trading_proto::CancelOrderEvent {
            order_id: cmd.order_id(),
            market_id: cmd.market_id(),
            user_id: cmd.user_id(),
            timestamp: cmd.timestamp(),
        };

        let topic_name = "cancels";
        self.publish_proto(
            topic_name,
            &cmd.order_id().to_string(),
            "trading.CancelOrderEvent",
            cancel_event,
        );
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
            let mut bids_vec = Vec::new();
            for i in 0..snapshot.bid_depth() {
                if snapshot.bid_prices[i] > 0 {
                    bids_vec.push(JsonOrderbookLevel {
                        price: snapshot.bid_prices[i],
                        size: snapshot.bid_volumes[i],
                    });
                }
            }
            let bids_json = serde_json::to_string(&bids_vec).unwrap_or_else(|_| "[]".to_string());

            let mut asks_vec = Vec::new();
            for i in 0..snapshot.ask_depth() {
                if snapshot.ask_prices[i] > 0 {
                    asks_vec.push(JsonOrderbookLevel {
                        price: snapshot.ask_prices[i],
                        size: snapshot.ask_volumes[i],
                    });
                }
            }
            let asks_json = serde_json::to_string(&asks_vec).unwrap_or_else(|_| "[]".to_string());

            let orderbook_event = trading_proto::OrderbookEvent {
                market_id,
                bids: bids_json,
                asks: asks_json,
                timestamp: snapshot.timestamp,
            };

            let topic_name = "orderbook";
            self.publish_proto(
                topic_name,
                &market_id.to_string(),
                "trading.OrderbookEvent",
                orderbook_event,
            );

            debug!(
                target: "events",
                component = "kafka_handler",
                action = "orderbook_event_published",
                market_id,
                topic = %topic_name
            );
        }
    }

    fn publish_deposit_event(&self, cmd: &OrderCommand) {
        let deposit_event = trading_proto::DepositEvent {
            user_id: cmd.user_id(),
            asset_id: cmd.market_id(),
            amount: cmd.size(),
            timestamp: cmd.timestamp(),
        };

        let topic_name = "deposits";
        self.publish_proto(
            topic_name,
            &cmd.user_id().to_string(),
            "trading.DepositEvent",
            deposit_event,
        );
        debug!(
            target: "events",
            component = "kafka_handler",
            action = "deposit_event_published",
            user_id = cmd.user_id(),
            asset_id = cmd.market_id(),
            amount = cmd.size(),
            topic = %topic_name
        );
    }

    fn publish_withdraw_event(&self, cmd: &OrderCommand) {
        let withdraw_event = trading_proto::WithdrawEvent {
            user_id: cmd.user_id(),
            asset_id: cmd.market_id(),
            amount: cmd.size(),
            timestamp: cmd.timestamp(),
        };

        let topic_name = "withdrawals";
        self.publish_proto(
            topic_name,
            &cmd.user_id().to_string(),
            "trading.WithdrawEvent",
            withdraw_event,
        );
        debug!(
            target: "events",
            component = "kafka_handler",
            action = "withdraw_event_published",
            user_id = cmd.user_id(),
            asset_id = cmd.market_id(),
            amount = cmd.size(),
            topic = %topic_name
        );
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

        // Handle deposit and withdraw commands separately
        match cmd.command {
            OrderCommandType::DepositFunds => {
                if cmd.status() == Status::Processed {
                    order_debug!(
                        "events_publish_deposit",
                        cmd,
                        stage = "events",
                        handler = "kafka"
                    );
                    self.publish_deposit_event(cmd);
                    self.publish_deposit_withdrawal_event(cmd);
                }
                self.publish_response(cmd);
                return;
            }
            OrderCommandType::WithdrawFunds => {
                if cmd.status() == Status::Processed {
                    order_debug!(
                        "events_publish_withdraw",
                        cmd,
                        stage = "events",
                        handler = "kafka"
                    );
                    self.publish_withdraw_event(cmd);
                    self.publish_deposit_withdrawal_event(cmd);
                }
                self.publish_response(cmd);
                return;
            }
            _ => {
                // Continue with existing order book command handling
            }
        }

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
                self.publish_order_event(cmd, None);
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
                self.publish_order_event(cmd, None);
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
                // Calculate original size: filled_size + remaining_size
                let original_size =
                    cmd.events().map(|e| e.calc_filled_size()).unwrap_or(0) + cmd.size();
                // Publish balance event for the taker
                self.publish_balance_event(taker_id, cmd, &cmd.balance);
                // Publish taker order event with original size
                self.publish_order_event(cmd, Some(original_size));
                self.publish_orderbook_event(market_id, &cmd.l2_data);
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
                self.publish_deposit_withdrawal_event(cmd);
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

    #[test]
    fn test_kafka_events_handler_placed_order() {
        let handler = KafkaEventsHandler::new(
            "localhost:9092",
            "http://localhost:8081",
            Arc::new(Publications::new()),
            ReplayControl::disabled(),
        );

        let mut cmd = OrderCommand::place_order(
            common::TimeInForce::Gtc,
            1001, // user_id
            1000, // price
            100,  // size
            Side::Bid,
            MARKET_ID,
            12345, // order_id
        );
        cmd.set_status(Status::Placed);
        cmd.timestamp = 1000;

        handler.handle_processed_command(&mut cmd);

        // Wait for async Kafka send to complete
        std::thread::sleep(std::time::Duration::from_secs(2));
    }

    #[test]
    fn test_kafka_events_handler_cancelled_order() {
        let handler = KafkaEventsHandler::new(
            "localhost:9092",
            "http://localhost:8081",
            Arc::new(Publications::new()),
            ReplayControl::disabled(),
        );

        let mut cmd = OrderCommand::place_order(
            common::TimeInForce::Gtc,
            1002, // user_id
            950,  // price
            50,   // size
            Side::Ask,
            MARKET_ID,
            12346, // order_id
        );
        cmd.set_status(Status::Cancelled);
        cmd.timestamp = 1001;

        handler.handle_processed_command(&mut cmd);

        std::thread::sleep(std::time::Duration::from_secs(2));
    }

    #[test]
    fn test_kafka_events_handler_filled_order_with_trades() {
        let handler = KafkaEventsHandler::new(
            "localhost:9092",
            "http://localhost:8081",
            Arc::new(Publications::new()),
            ReplayControl::disabled(),
        );

        // Create a processed command with Filled status and trade events
        let mut filled_cmd = OrderCommand::place_order(
            common::TimeInForce::Gtc,
            1004, // user_id
            1050, // price
            200,  // size
            Side::Bid,
            MARKET_ID,
            12348, // order_id
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
        filled_cmd.attach_event(Box::new(trade1));

        handler.handle_processed_command(&mut filled_cmd);

        std::thread::sleep(std::time::Duration::from_secs(2));
    }

    #[test]
    fn test_kafka_events_handler_deposit_funds() {
        let handler = KafkaEventsHandler::new(
            "localhost:9092",
            "http://localhost:8081",
            Arc::new(Publications::new()),
            ReplayControl::disabled(),
        );

        let mut cmd = OrderCommand::deposit_funds(
            1005, // user_id
            1000, // amount
            1,    // asset_id
        );
        cmd.set_status(Status::Processed);
        cmd.timestamp = 1004;

        handler.handle_processed_command(&mut cmd);

        std::thread::sleep(std::time::Duration::from_secs(2));
    }

    #[test]
    fn test_kafka_events_handler_withdraw_funds() {
        let handler = KafkaEventsHandler::new(
            "localhost:9092",
            "http://localhost:8081",
            Arc::new(Publications::new()),
            ReplayControl::disabled(),
        );

        let mut cmd = OrderCommand::withdraw_funds(
            1006, // user_id
            500,  // amount
            1,    // asset_id
        );
        cmd.set_status(Status::Processed);
        cmd.timestamp = 1005;

        handler.handle_processed_command(&mut cmd);

        std::thread::sleep(std::time::Duration::from_secs(2));
    }
}
