//! Kafka verification utilities for the test framework
//!
//! The core publishes the outcome of every command as protobuf events on Kafka topics, each event
//! keyed by the entity it describes (`processors::events::KafkaEventsHandler`). This module reads
//! every one of those topics back, starting where they ended when the run began, and keeps what a
//! downstream reader of them keeps: per key, the event with the greatest timestamp (after a
//! restart, an older copy can follow a newer event in a partition), and every trade. It also
//! records every `(topic, key, timestamp)` read, which identifies an event and all its copies.

use crate::test_framework::types::*;
use hashbrown::{HashMap, HashSet};
use processors::events::EVENT_TOPICS;
use processors::events::trading_proto::{
    BalanceEvent, CancelOrderEvent, DepositEvent, OrderEvent, OrderbookEvent, TradeEvent,
    WithdrawEvent,
};
use prost::Message as _;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::{Offset, TopicPartitionList};
use schema_registry_converter::proto_resolver::to_index_and_data;
use schema_registry_converter::schema_registry_common::{BytesResult, get_bytes_result};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{debug, warn};

/// `BalanceEvent`s, keyed `user_id:asset_id`.
const BALANCES_TOPIC: &str = "balances";
/// `TradeEvent`s, keyed `taker_order_id:maker_order_id`.
const TRADES_TOPIC: &str = "trades";
/// `OrderbookEvent`s, keyed `market_id`.
const ORDERBOOK_TOPIC: &str = "orderbook";

/// Bound on one broker request, and on reading everything the broker holds.
const BROKER_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a `wait_for_*` call pauses before it reads again.
const RETRY_INTERVAL: Duration = Duration::from_millis(50);

/// Verifier for the state the core publishes to Kafka
///
/// Every read first takes in all the events the broker holds at that moment, so it reflects
/// everything the core has published so far.
pub struct KafkaVerifier {
    consumer: BaseConsumer,
    events: Events,
}

impl KafkaVerifier {
    /// Connect to the brokers and read each event topic from its current end
    ///
    /// Only events published after this call are seen, whatever the topics already hold from
    /// earlier runs. The topics must exist (`deploy/kafka/topics.toml`).
    pub async fn new(brokers: &str) -> TestResult<Self> {
        // librdkafka only assigns partitions to a consumer with a group; nothing is committed to it.
        // The broker answers a connection's requests in order, so each watermark query waits for
        // the fetch in flight: keep the fetch long-poll short (the default is 500 ms).
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", "xtask-integration")
            .set("enable.auto.commit", "false")
            .set("fetch.wait.max.ms", "10")
            .create()?;
        let mut events = Events::default();
        let mut assignment = TopicPartitionList::new();
        for topic in EVENT_TOPICS {
            for partition in partitions(&consumer, topic)? {
                let (_, end) = consumer.fetch_watermarks(topic, partition, BROKER_TIMEOUT)?;
                assignment.add_partition_offset(topic, partition, Offset::Offset(end))?;
                events.positions.insert((topic.to_string(), partition), end);
            }
        }
        consumer.assign(&assignment)?;

        debug!(
            "Reading Kafka at {} from offsets {:?}",
            brokers, events.positions
        );

        Ok(Self { consumer, events })
    }

    /// Get the latest balance published for a user and asset
    /// Topic `balances`, key `{user_id}:{asset_id}`
    pub async fn get_balance(&mut self, user_id: u64, asset_id: u16) -> TestResult<BalanceEvent> {
        self.catch_up()?;

        self.events
            .balance(user_id, asset_id)
            .cloned()
            .ok_or_else(|| TestError::Verification {
                message: format!(
                    "No balance published for user {} asset {}",
                    user_id, asset_id
                ),
            })
    }

    /// Get the most recent `count` trades published for a market, oldest first
    /// Topic `trades`
    pub async fn get_trades(
        &mut self,
        market_id: u32,
        count: usize,
    ) -> TestResult<Vec<TradeEvent>> {
        self.catch_up()?;
        Ok(self.events.recent_trades(market_id, count))
    }

    /// Get the latest orderbook snapshot published for a market
    /// Topic `orderbook`, key `{market_id}`
    /// Returns an empty orderbook if none has been published yet
    pub async fn get_orderbook(&mut self, market_id: u32) -> TestResult<OrderbookSnapshot> {
        self.catch_up()?;

        Ok(self
            .events
            .orderbook(market_id)
            .cloned()
            .unwrap_or_else(|| OrderbookSnapshot {
                market_id,
                bids: vec![],
                asks: vec![],
            }))
    }

    /// Wait for a balance to be published for a user and asset (with timeout)
    pub async fn wait_for_balance_update(
        &mut self,
        user_id: u64,
        asset_id: u16,
        wait_timeout: Duration,
    ) -> TestResult<BalanceEvent> {
        self.wait_for(wait_timeout, |events| {
            events.balance(user_id, asset_id).cloned()
        })
        .await
    }

    /// Wait for a trade matching the criteria to be published (with timeout)
    pub async fn wait_for_trade(
        &mut self,
        market_id: u32,
        criteria: &TradeCriteria,
        wait_timeout: Duration,
    ) -> TestResult<TradeEvent> {
        self.wait_for(wait_timeout, |events| {
            events
                .trades
                .iter()
                .find(|trade| trade.market_id == market_id && criteria.matches(trade))
                .cloned()
        })
        .await
    }

    /// Wait for an orderbook snapshot to be published for a market (with timeout)
    pub async fn wait_for_orderbook_update(
        &mut self,
        market_id: u32,
        wait_timeout: Duration,
    ) -> TestResult<OrderbookSnapshot> {
        self.wait_for(wait_timeout, |events| events.orderbook(market_id).cloned())
            .await
    }

    /// Wait until every event in `expected` has been published, at least once (with timeout)
    pub async fn wait_for_published(
        &mut self,
        expected: &HashSet<EventId>,
        wait_timeout: Duration,
    ) -> TestResult<()> {
        let missing = |events: &Events| {
            let mut missing: Vec<&EventId> = expected.difference(&events.published).collect();
            missing.sort();
            missing.into_iter().take(5).cloned().collect::<Vec<_>>()
        };
        match self
            .wait_for(wait_timeout, |events| {
                missing(events).is_empty().then_some(())
            })
            .await
        {
            Err(TestError::Timeout { .. }) => Err(TestError::Verification {
                message: format!(
                    "{} of {} expected events were not published within {wait_timeout:?}, \
                     among them {:?}",
                    expected.difference(&self.events.published).count(),
                    expected.len(),
                    missing(&self.events)
                ),
            }),
            result => result,
        }
    }

    /// Every distinct `(topic, key, timestamp)` read so far, and how many events were read in
    /// all, copies included
    pub fn published(&mut self) -> TestResult<(&HashSet<EventId>, usize)> {
        self.catch_up()?;
        Ok((&self.events.published, self.events.read))
    }

    /// The end offset of every partition of every event topic, as the broker reports it now
    pub fn end_offsets(&self) -> TestResult<BTreeMap<(String, i32), i64>> {
        self.events
            .positions
            .keys()
            .map(|(topic, partition)| {
                let (_, end) = self
                    .consumer
                    .fetch_watermarks(topic, *partition, BROKER_TIMEOUT)?;
                Ok(((topic.clone(), *partition), end))
            })
            .collect()
    }

    /// Read until `find` finds something, or `wait_timeout` passes
    async fn wait_for<T>(
        &mut self,
        wait_timeout: Duration,
        find: impl Fn(&Events) -> Option<T>,
    ) -> TestResult<T> {
        let start = Instant::now();

        loop {
            self.catch_up()?;
            if let Some(found) = find(&self.events) {
                return Ok(found);
            }
            if start.elapsed() > wait_timeout {
                return Err(TestError::Timeout {
                    timeout: wait_timeout,
                });
            }
            sleep(RETRY_INTERVAL).await;
        }
    }

    /// Take in every event the broker holds at this moment
    fn catch_up(&mut self) -> TestResult<()> {
        let mut ends = Vec::with_capacity(self.events.positions.len());
        for (topic, partition) in self.events.positions.keys() {
            let (_, end) = self
                .consumer
                .fetch_watermarks(topic, *partition, BROKER_TIMEOUT)?;
            ends.push(((topic.clone(), *partition), end));
        }

        let deadline = Instant::now() + BROKER_TIMEOUT;
        for (partition, end) in ends {
            while self.events.positions[&partition] < end {
                match self
                    .consumer
                    .poll(deadline.saturating_duration_since(Instant::now()))
                {
                    Some(Ok(message)) => self.events.apply(&message)?,
                    // A transient client error, such as a dropped connection: librdkafka
                    // recovers on its own, so keep reading until the deadline.
                    Some(Err(KafkaError::MessageConsumption(code))) => {
                        warn!("Transient Kafka consumer error: {}", code);
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => {
                        return Err(TestError::Timeout {
                            timeout: BROKER_TIMEOUT,
                        });
                    }
                }
            }
        }

        Ok(())
    }
}

/// The events read so far
#[derive(Default)]
struct Events {
    /// Offset of the next event to read, per assigned topic partition
    positions: HashMap<(String, i32), i64>,
    /// Every distinct event read
    published: HashSet<EventId>,
    /// How many events were read, copies included
    read: usize,
    /// Latest balance per message key, with its timestamp
    balances: HashMap<String, (u64, BalanceEvent)>,
    /// Every trade, in publication order
    trades: Vec<TradeEvent>,
    /// Latest orderbook snapshot per message key, with its timestamp
    orderbooks: HashMap<String, (u64, OrderbookSnapshot)>,
}

/// Keeps `event` as the latest for `key` unless an event with a greater timestamp is there already.
/// An equal timestamp is a copy of the same event.
fn keep_latest<T>(latest: &mut HashMap<String, (u64, T)>, key: String, timestamp: u64, event: T) {
    if latest.get(&key).is_none_or(|(kept, _)| *kept < timestamp) {
        latest.insert(key, (timestamp, event));
    }
}

impl Events {
    fn apply(&mut self, message: &impl Message) -> TestResult<()> {
        // The consumer has moved past this message whether or not it decodes.
        self.positions.insert(
            (message.topic().to_string(), message.partition()),
            message.offset() + 1,
        );

        let key = message
            .key()
            .and_then(|key| std::str::from_utf8(key).ok())
            .ok_or_else(|| TestError::Parse {
                message: format!(
                    "Kafka message on {} at offset {} has no UTF-8 key",
                    message.topic(),
                    message.offset()
                ),
            })?
            .to_string();
        let payload = protobuf_payload(message.payload())?;
        let payload = payload.as_slice();

        let timestamp = match message.topic() {
            BALANCES_TOPIC => {
                let event = BalanceEvent::decode(payload)?;
                let timestamp = event.timestamp;
                keep_latest(&mut self.balances, key.clone(), timestamp, event);
                timestamp
            }
            TRADES_TOPIC => {
                let event = TradeEvent::decode(payload)?;
                let timestamp = event.timestamp;
                self.trades.push(event);
                timestamp
            }
            ORDERBOOK_TOPIC => {
                let event = OrderbookEvent::decode(payload)?;
                let timestamp = event.timestamp;
                let snapshot = OrderbookSnapshot::try_from(event)?;
                keep_latest(&mut self.orderbooks, key.clone(), timestamp, snapshot);
                timestamp
            }
            "orders" => {
                OrderEvent::decode(payload)?
                    .order
                    .ok_or_else(|| TestError::Parse {
                        message: format!("OrderEvent {key} without an order"),
                    })?
                    .timestamp
            }
            "cancels" => CancelOrderEvent::decode(payload)?.timestamp,
            "deposits" => DepositEvent::decode(payload)?.timestamp,
            "withdrawals" => WithdrawEvent::decode(payload)?.timestamp,
            topic => {
                return Err(TestError::Parse {
                    message: format!("Unexpected Kafka topic {}", topic),
                });
            }
        };
        self.read += 1;
        self.published.insert(EventId {
            topic: message.topic().to_string(),
            key,
            timestamp,
        });

        Ok(())
    }

    fn balance(&self, user_id: u64, asset_id: u16) -> Option<&BalanceEvent> {
        self.balances
            .get(&format!("{}:{}", user_id, asset_id))
            .map(|(_, balance)| balance)
    }

    fn orderbook(&self, market_id: u32) -> Option<&OrderbookSnapshot> {
        self.orderbooks
            .get(&market_id.to_string())
            .map(|(_, snapshot)| snapshot)
    }

    fn recent_trades(&self, market_id: u32, count: usize) -> Vec<TradeEvent> {
        let trades: Vec<&TradeEvent> = self
            .trades
            .iter()
            .filter(|trade| trade.market_id == market_id)
            .collect();
        let skip = trades.len().saturating_sub(count);

        trades.into_iter().skip(skip).cloned().collect()
    }
}

impl TryFrom<OrderbookEvent> for OrderbookSnapshot {
    type Error = TestError;

    /// The core sends each side of the book as a JSON array of levels, best price first
    fn try_from(event: OrderbookEvent) -> TestResult<Self> {
        let levels = |side: &str| {
            serde_json::from_str(side).map_err(|e| TestError::Parse {
                message: format!(
                    "Invalid orderbook levels for market {}: {}",
                    event.market_id, e
                ),
            })
        };

        Ok(Self {
            market_id: event.market_id,
            bids: levels(&event.bids)?,
            asks: levels(&event.asks)?,
        })
    }
}

fn partitions(consumer: &BaseConsumer, topic: &str) -> TestResult<Vec<i32>> {
    let metadata = consumer.fetch_metadata(Some(topic), BROKER_TIMEOUT)?;
    let partitions: Vec<i32> = metadata
        .topics()
        .iter()
        .filter(|found| found.name() == topic && found.error().is_none())
        .flat_map(|found| found.partitions().iter().map(|partition| partition.id()))
        .collect();

    if partitions.is_empty() {
        return Err(TestError::Configuration {
            message: format!("Kafka topic {} has no partitions", topic),
        });
    }
    Ok(partitions)
}

/// The protobuf message inside a value framed for the Schema Registry: a zero byte, the schema
/// id, then the message's index path in `trading.proto`.
fn protobuf_payload(value: Option<&[u8]>) -> TestResult<Vec<u8>> {
    match get_bytes_result(value) {
        BytesResult::Valid(_schema_id, framed) if !framed.is_empty() => {
            Ok(to_index_and_data(&framed).1)
        }
        _ => Err(TestError::Parse {
            message: "Kafka value is not a Schema Registry framed protobuf message".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::Timestamp;
    use rdkafka::message::OwnedMessage;
    use schema_registry_converter::schema_registry_common::get_payload;

    /// Frame a message as the core's encoder does, with the given index path bytes
    fn framed(index_path: &[u8], message: &impl prost::Message) -> Vec<u8> {
        let mut bytes = index_path.to_vec();
        bytes.extend(message.encode_to_vec());
        get_payload(7, bytes)
    }

    fn kafka_message(topic: &str, key: &str, offset: i64, value: Vec<u8>) -> OwnedMessage {
        OwnedMessage::new(
            Some(value),
            Some(key.as_bytes().to_vec()),
            topic.to_string(),
            Timestamp::NotAvailable,
            0,
            offset,
            None,
        )
    }

    fn balance(user_id: u64, asset_id: u32, available: u64, locked: u64) -> BalanceEvent {
        balance_at(user_id, asset_id, available, locked, 1)
    }

    fn balance_at(
        user_id: u64,
        asset_id: u32,
        available: u64,
        locked: u64,
        timestamp: u64,
    ) -> BalanceEvent {
        BalanceEvent {
            user_id,
            asset_id,
            available,
            locked,
            total: available + locked,
            timestamp,
        }
    }

    fn trade(market_id: u32, price: u64) -> TradeEvent {
        TradeEvent {
            market_id,
            price,
            size: 1,
            ..Default::default()
        }
    }

    #[test]
    fn payload_is_unframed_for_first_and_nested_messages() {
        let event = balance(1, 2, 30, 4);

        // A single zero byte stands for the file's first message, [0].
        let first = protobuf_payload(Some(&framed(&[0], &event))).unwrap();
        assert_eq!(BalanceEvent::decode(first.as_slice()).unwrap(), event);

        // Otherwise a zigzag count, then zigzag indexes: [1, 3] is 2*2, 1*2, 3*2.
        let nested = protobuf_payload(Some(&framed(&[4, 2, 6], &event))).unwrap();
        assert_eq!(BalanceEvent::decode(nested.as_slice()).unwrap(), event);
    }

    #[test]
    fn payload_without_framing_is_rejected() {
        let raw = balance(1, 2, 30, 4).encode_to_vec();

        assert!(protobuf_payload(Some(&raw)).is_err());
        assert!(protobuf_payload(None).is_err());
    }

    #[test]
    fn latest_event_per_key_wins_and_trades_accumulate() {
        let mut events = Events::default();
        let orderbook = OrderbookEvent {
            market_id: 9,
            bids: r#"[{"price":100,"size":5}]"#.to_string(),
            asks: "[]".to_string(),
            timestamp: 1,
        };
        let messages = [
            kafka_message(
                BALANCES_TOPIC,
                "1:2",
                10,
                framed(&[0], &balance_at(1, 2, 50, 0, 1)),
            ),
            kafka_message(
                BALANCES_TOPIC,
                "1:2",
                11,
                framed(&[0], &balance_at(1, 2, 30, 20, 2)),
            ),
            kafka_message(TRADES_TOPIC, "5:4", 3, framed(&[0], &trade(9, 100))),
            kafka_message(TRADES_TOPIC, "6:4", 4, framed(&[0], &trade(9, 101))),
            kafka_message(ORDERBOOK_TOPIC, "9", 0, framed(&[0], &orderbook)),
        ];
        for message in &messages {
            events.apply(message).unwrap();
        }

        assert_eq!(events.balance(1, 2), Some(&balance_at(1, 2, 30, 20, 2)));
        assert_eq!(events.balance(2, 1), None);
        assert_eq!(events.trades, vec![trade(9, 100), trade(9, 101)]);
        assert_eq!(
            events.orderbook(9),
            Some(&OrderbookSnapshot {
                market_id: 9,
                bids: vec![OrderbookLevel {
                    price: 100,
                    size: 5
                }],
                asks: vec![],
            })
        );
        assert_eq!(events.positions[&(BALANCES_TOPIC.to_string(), 0)], 12);
        assert_eq!(events.positions[&(TRADES_TOPIC.to_string(), 0)], 5);
        assert_eq!(events.positions[&(ORDERBOOK_TOPIC.to_string(), 0)], 1);
    }

    #[test]
    fn a_re_published_older_copy_never_replaces_a_newer_event() {
        let mut events = Events::default();
        let newer = balance_at(1, 2, 30, 20, 9);
        for (offset, event) in [
            (0, balance_at(1, 2, 50, 0, 5)),
            (1, newer.clone()),
            // A restart re-publishes both: copies of events the partition already holds.
            (2, balance_at(1, 2, 50, 0, 5)),
            (3, newer.clone()),
        ] {
            let message = kafka_message(BALANCES_TOPIC, "1:2", offset, framed(&[0], &event));
            events.apply(&message).unwrap();
        }

        assert_eq!(
            events.balance(1, 2),
            Some(&newer),
            "the greatest timestamp wins"
        );
        assert_eq!(events.read, 4);
        assert_eq!(events.published.len(), 2, "two events, two copies of each");
    }

    #[test]
    fn every_topic_is_recorded_by_topic_key_and_timestamp() {
        let mut events = Events::default();
        let order = OrderEvent {
            order: Some(processors::events::trading_proto::Order {
                order_id: 4,
                timestamp: 13,
                ..Default::default()
            }),
            market_id: 9,
        };
        let messages = [
            ("orders", "4", framed(&[2, 2], &order)),
            (
                "cancels",
                "4",
                framed(
                    &[2, 12],
                    &CancelOrderEvent {
                        order_id: 4,
                        timestamp: 14,
                        ..Default::default()
                    },
                ),
            ),
            (
                "deposits",
                "1",
                framed(
                    &[2, 14],
                    &DepositEvent {
                        user_id: 1,
                        timestamp: 11,
                        ..Default::default()
                    },
                ),
            ),
            (
                "withdrawals",
                "1",
                framed(
                    &[2, 16],
                    &WithdrawEvent {
                        user_id: 1,
                        timestamp: 15,
                        ..Default::default()
                    },
                ),
            ),
            (TRADES_TOPIC, "5:4", framed(&[2, 4], &trade(9, 100))),
        ];
        for (offset, (topic, key, value)) in messages.into_iter().enumerate() {
            events
                .apply(&kafka_message(topic, key, offset as i64, value))
                .unwrap();
        }

        let id = |topic: &str, key: &str, timestamp| EventId {
            topic: topic.to_string(),
            key: key.to_string(),
            timestamp,
        };
        assert_eq!(
            events.published,
            HashSet::from([
                id("orders", "4", 13),
                id("cancels", "4", 14),
                id("deposits", "1", 11),
                id("withdrawals", "1", 15),
                id(TRADES_TOPIC, "5:4", 0),
            ])
        );
    }

    #[test]
    fn undecodable_message_is_reported_and_read_past() {
        let mut events = Events::default();
        let unframed = kafka_message(BALANCES_TOPIC, "1:2", 7, b"not framed".to_vec());

        assert!(events.apply(&unframed).is_err());
        assert_eq!(events.positions[&(BALANCES_TOPIC.to_string(), 0)], 8);
        assert_eq!(events.balance(1, 2), None);
    }

    #[test]
    fn recent_trades_are_the_last_of_the_market_oldest_first() {
        let events = Events {
            trades: vec![trade(9, 1), trade(8, 2), trade(9, 3), trade(9, 4)],
            ..Default::default()
        };

        assert_eq!(events.recent_trades(9, 2), vec![trade(9, 3), trade(9, 4)]);
        assert_eq!(
            events.recent_trades(9, 10),
            vec![trade(9, 1), trade(9, 3), trade(9, 4)]
        );
        assert!(events.recent_trades(7, 10).is_empty());
    }
}
