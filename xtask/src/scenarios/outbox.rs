//! The core's outbox, end to end: a journaled core re-publishes from its journal whatever a death
//! or a shutdown left undelivered, waits for Kafka at start, refuses to start on a topic that
//! no-loss cannot rest on, fails a start whose replay cannot re-publish instead of hanging,
//! answers Busy and executes nothing when a stalled broker has filled its queues, and starts from
//! its last snapshot, replaying only the commands journaled after it.
//!
//! The scenarios share one continuous ledger, in order, on the stack of
//! `xtask/tests/docker-compose.yml`: Redpanda with its Schema Registry, an Aeron
//! ArchivingMediaDriver that journals the core's commands, and the core. Each round rests a bid
//! and an ask that never cross, cancels the bid and re-places it at once, and lifts the ask with
//! a market buy: every start replays the journal back to back, and must re-decide each command
//! as the live run did.
//!
//! The oracle is the client's own ledger, built from the core's responses: every event each
//! answered command must have published, by `(topic, key, timestamp)`, and each balance's state
//! after the last command that changed it. Kafka must hold every one of those events, and its
//! latest balance per key (by timestamp) must equal the ledger's.

use crate::builders::OrderBuilder;
use crate::kafka_topics::{self, KafkaTopics};
use crate::test_framework::client::{START_TIMEOUT, TestClient};
use crate::test_framework::kafka::KafkaVerifier;
use crate::test_framework::scenario;
use crate::test_framework::stack::{self, BROKER, CORE, Paused};
use crate::test_framework::types::*;
use common::{
    CoreMarketSpecification, OrderCommand, Side, Status, TimeInForce, base_asset, quote_asset,
};
use hashbrown::{HashMap, HashSet};
use processors::events::DELIVERED_TOPIC;
use rdkafka::admin::{AdminClient, AdminOptions};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use std::ops::Range;
use std::time::{Duration, Instant};
use tracing::info;

const BIDDER: u64 = 101;
const ASKER: u64 = 102;
const BUYER: u64 = 103;
const FLOODER: u64 = 104;

/// The flood's bids rest below every other order.
const FLOOD_PRICE: u64 = 30_000;

/// Orders flooded at once while their answers are awaited: fewer than the core's ring holds
/// (1,024), so that none is answered Busy for want of a slot.
const WINDOW: u64 = 1_000;

/// Windows of orders placed with the broker paused before the scenario stops waiting for the
/// answers to stop: librdkafka's queue (100,000 events) and the events stage's channel (8,192)
/// fill at about 27,000 placements of four events each.
const MAX_STALLED_WINDOWS: u64 = 40;

/// Order pairs placed while the broker is paused: enough to leave events queued in the core, far
/// fewer than the 108,000 it takes to fill its queues and answer `Busy`.
const QUEUED_ORDER_PAIRS: u64 = 25;

/// Rounds placed while the broker is paused before a start that cannot re-publish them: 25 events
/// each, 10,000 in all, more than the 8,192 the events stage's sender channel holds, so the replay
/// stalls behind it.
const UNDELIVERED_ROUNDS: u64 = 400;

/// How long a stopping core gets to exit: a graceful shutdown with the broker paused spends the
/// 20 s delivery grace, plus up to 5 s, after its pipeline drain.
const STOP_TIMEOUT: Duration = Duration::from_secs(60);

/// What the client's responses say Kafka must hold.
#[derive(Default)]
struct Ledger {
    /// Every event an answered command published.
    expected: HashSet<EventId>,
    /// Each balance, by `(user, asset)`, after the last command that changed it, and that
    /// command's timestamp.
    balances: HashMap<(u64, u16), (Balance, u64)>,
}

impl Ledger {
    fn expect(&mut self, topic: &str, key: String, timestamp: u64) {
        self.expected.insert(EventId {
            topic: topic.to_string(),
            key,
            timestamp,
        });
    }

    /// Applies `update` to `user`'s `asset` as of the command stamped `timestamp`, which
    /// publishes that balance.
    fn change(
        &mut self,
        user: u64,
        asset: u16,
        timestamp: u64,
        update: impl FnOnce(Balance) -> Balance,
    ) {
        let (balance, at) = self
            .balances
            .entry((user, asset))
            .or_insert((Balance::new(0, 0), 0));
        *balance = update(*balance);
        *at = timestamp;
        self.expect("balances", format!("{user}:{asset}"), timestamp);
    }

    /// The timestamp of the last command answered: every answered command published events.
    fn last_timestamp(&self) -> u64 {
        self.expected
            .iter()
            .map(|event| event.timestamp)
            .max()
            .unwrap_or_default()
    }

    fn deposited(&mut self, response: &OrderCommand) -> TestResult<()> {
        expect_status(response, Status::Processed)?;
        let (user, asset, timestamp) = (
            response.user_id,
            response.market_id as u16,
            response.timestamp,
        );
        self.expect("deposits", user.to_string(), timestamp);
        self.change(user, asset, timestamp, |b| {
            Balance::new(b.available + response.size, b.locked)
        });
        Ok(())
    }

    fn placed(
        &mut self,
        response: &OrderCommand,
        market: &CoreMarketSpecification,
    ) -> TestResult<()> {
        expect_status(response, Status::Placed)?;
        let (user, timestamp) = (response.user_id, response.timestamp);
        let (locked_asset, other_asset, lock) = resting_lock(response, market);
        self.change(user, locked_asset, timestamp, |b| {
            Balance::new(b.available - lock, b.locked + lock)
        });
        self.change(user, other_asset, timestamp, |b| b);
        self.expect("orders", response.order_id.to_string(), timestamp);
        self.expect("orderbook", response.market_id.to_string(), timestamp);
        Ok(())
    }

    /// `order`, resting, cancelled by `response`.
    fn cancelled(
        &mut self,
        response: &OrderCommand,
        order: &OrderCommand,
        market: &CoreMarketSpecification,
    ) -> TestResult<()> {
        expect_status(response, Status::Cancelled)?;
        let (user, timestamp) = (response.user_id, response.timestamp);
        let (locked_asset, other_asset, lock) = resting_lock(order, market);
        self.change(user, locked_asset, timestamp, |b| {
            Balance::new(b.available + lock, b.locked - lock)
        });
        self.change(user, other_asset, timestamp, |b| b);
        self.expect("orders", order.order_id.to_string(), timestamp);
        self.expect("cancels", order.order_id.to_string(), timestamp);
        self.expect("orderbook", response.market_id.to_string(), timestamp);
        Ok(())
    }

    /// `response`, a market buy of `ask`'s size, filled entirely against `ask` at its price.
    fn filled(
        &mut self,
        response: &OrderCommand,
        ask: &OrderCommand,
        market: &CoreMarketSpecification,
    ) -> TestResult<()> {
        expect_status(response, Status::Filled)?;
        let (base, quote) = (base_asset(ask.market_id), quote_asset(ask.market_id));
        let (taker, maker, timestamp) = (response.user_id, ask.user_id, response.timestamp);
        let fee = |amount: u64, bp: u64| (amount * bp).div_ceil(10_000);
        let notional = market
            .calculate_quote_cost(ask.price, ask.size)
            .expect("scenario notional fits in u64");
        let bought = ask.size - fee(ask.size, market.taker_fee);
        let proceeds = notional - fee(notional, market.maker_fee);
        self.change(taker, base, timestamp, |b| {
            Balance::new(b.available + bought, b.locked)
        });
        self.change(taker, quote, timestamp, |b| {
            Balance::new(b.available - notional, b.locked)
        });
        self.change(maker, base, timestamp, |b| {
            Balance::new(b.available, b.locked - ask.size)
        });
        self.change(maker, quote, timestamp, |b| {
            Balance::new(b.available + proceeds, b.locked)
        });
        let trade = format!("{}:{}", response.order_id, ask.order_id);
        self.expect("trades", trade, timestamp);
        self.expect("orders", response.order_id.to_string(), timestamp);
        self.expect("orders", ask.order_id.to_string(), timestamp);
        self.expect("orderbook", ask.market_id.to_string(), timestamp);
        Ok(())
    }
}

/// What `order` locks while it rests: (that asset, the market's other asset, the amount).
fn resting_lock(order: &OrderCommand, market: &CoreMarketSpecification) -> (u16, u16, u64) {
    let (base, quote) = (base_asset(order.market_id), quote_asset(order.market_id));
    match order.side {
        Side::Bid => {
            let cost = market
                .calculate_quote_cost(order.price, order.size)
                .expect("scenario notional fits in u64");
            (quote, base, cost)
        }
        Side::Ask => (base, quote, order.size),
    }
}

fn expect_status(response: &OrderCommand, status: Status) -> TestResult<()> {
    if response.status != status {
        return Err(TestError::Assertion {
            message: format!(
                "{:?} for user {} answered {:?}, not {status:?}",
                response.command, response.user_id, response.status
            ),
        });
    }
    Ok(())
}

/// Orders sent without waiting for their answers, and what they were answered.
#[derive(Default)]
struct Flood {
    /// The client order ids sent and not answered yet.
    unanswered: HashSet<u64>,
    placed: Vec<OrderCommand>,
    busy: usize,
}

/// The scenarios' shared state.
struct Outbox {
    config: TestConfig,
    schema_registry_url: String,
    market: CoreMarketSpecification,
    /// Connected while the core runs.
    client: Option<TestClient>,
    kafka: KafkaVerifier,
    ledger: Ledger,
    order_pairs: u64,
    /// Every order resting in the book, as its placement answered.
    resting: Vec<OrderCommand>,
}

impl Outbox {
    /// Connects a client to the core, once it accepts a gateway.
    fn connect(&mut self) -> TestResult<()> {
        self.client = Some(TestClient::connect(
            0,
            self.config.default_timeout,
            &self.config.core_address,
            self.config.core_port,
            self.config.core_control_port,
        )?);
        Ok(())
    }

    fn execute(&mut self, command: OrderCommand) -> TestResult<OrderCommand> {
        self.client
            .as_mut()
            .expect("the core is running and connected")
            .send_and_recv(command)
    }

    /// Places `pairs` resting bids and asks that never cross: bids below 50,000, asks above. Each
    /// bid is then cancelled and re-placed without waiting for the cancel's response, and each ask
    /// is lifted by a market buy.
    fn place_orders(&mut self, pairs: u64) -> TestResult<()> {
        for _ in 0..pairs {
            self.order_pairs += 1;
            let limit = |user, side, price| {
                OrderBuilder::place_limit()
                    .user(user)
                    .price(price)
                    .size(1)
                    .side(side)
                    .market_id(self.config.market_id)
                    .build()
            };
            let bid = limit(BIDDER, Side::Bid, 40_000 - self.order_pairs);
            let ask = limit(ASKER, Side::Ask, 60_000 + self.order_pairs);

            let placed_bid = self.execute(bid.clone())?;
            self.ledger.placed(&placed_bid, &self.market)?;
            let placed_ask = self.execute(ask)?;
            self.ledger.placed(&placed_ask, &self.market)?;

            let cancel = OrderCommand::cancel_order(
                placed_bid.order_id,
                BIDDER,
                Side::Bid,
                self.config.market_id,
            );
            let client = self
                .client
                .as_mut()
                .expect("the core is running and connected");
            client.send(&cancel)?;
            client.send(&bid)?;
            let (cancelled, replaced) = (client.recv()?, client.recv()?);
            self.ledger
                .cancelled(&cancelled, &placed_bid, &self.market)?;
            self.ledger.placed(&replaced, &self.market)?;
            self.resting.push(replaced);

            let buy = OrderBuilder::place_market()
                .user(BUYER)
                .size(1)
                .side(Side::Bid)
                .market_id(self.config.market_id)
                .build();
            let bought = self.execute(buy)?;
            self.ledger.filled(&bought, &placed_ask, &self.market)?;
        }
        Ok(())
    }

    /// Sends limit bids of size 1 of the flooder at [`FLOOD_PRICE`], `ids` their client order ids,
    /// without waiting for the answers.
    fn flood(&mut self, flood: &mut Flood, ids: Range<u64>) -> TestResult<()> {
        let client = self
            .client
            .as_mut()
            .expect("the core is running and connected");
        for id in ids {
            let bid = OrderCommand::place_order(
                TimeInForce::Gtc,
                FLOODER,
                FLOOD_PRICE,
                1,
                Side::Bid,
                self.config.market_id,
                id,
            );
            client.send(&bid)?;
            flood.unanswered.insert(id);
        }
        Ok(())
    }

    /// Takes the flood's next answer: one per order, Placed or Busy.
    fn take_answer(&mut self, flood: &mut Flood) -> TestResult<()> {
        let answer = self
            .client
            .as_mut()
            .expect("the core is running and connected")
            .recv()?;
        let id = answer.client_order_id;
        if !flood.unanswered.remove(&id) {
            return Err(TestError::Assertion {
                message: format!("flooded order {id} answered twice, or never sent"),
            });
        }
        match answer.status {
            Status::Placed => flood.placed.push(answer),
            Status::Busy => flood.busy += 1,
            status => {
                return Err(TestError::Assertion {
                    message: format!("flooded order {id} answered {status:?}"),
                });
            }
        }
        Ok(())
    }

    fn take_answers(&mut self, flood: &mut Flood) -> TestResult<()> {
        while !flood.unanswered.is_empty() {
            self.take_answer(flood)?;
        }
        Ok(())
    }

    fn deposit(&mut self, user: u64, asset: u16, amount: u64) -> TestResult<()> {
        let response = self.execute(OrderCommand::deposit_funds(user, amount, asset))?;
        self.ledger.deposited(&response)
    }

    /// Waits until Kafka holds every event the ledger expects, then checks that its latest
    /// balance per key is the ledger's, from the same command.
    async fn verify(&mut self) -> TestResult<()> {
        self.kafka
            .wait_for_published(&self.ledger.expected, self.config.event_timeout)
            .await?;
        for (&(user, asset), &(expected, timestamp)) in &self.ledger.balances {
            let latest = self.kafka.get_balance(user, asset).await?;
            let actual = (latest.available, latest.locked, latest.timestamp);
            if actual != (expected.available, expected.locked, timestamp) {
                return Err(TestError::Verification {
                    message: format!(
                        "latest balance of user {user} asset {asset} on Kafka is (available, \
                         locked, timestamp) {actual:?}; the responses say {:?}",
                        (expected.available, expected.locked, timestamp)
                    ),
                });
            }
        }
        let expected = self.ledger.expected.len();
        let (_, read) = self.kafka.published()?;
        info!("  Kafka holds all {expected} expected events; {read} read, copies included");
        Ok(())
    }

    /// Stops the core gracefully with the client's gateway connected, as gateways always are in
    /// production, and waits for it to exit. The client goes once the core has exited.
    fn stop_core(&mut self) -> TestResult<()> {
        let client = self
            .client
            .take()
            .expect("the core is running and connected");
        stack::signal(CORE, "TERM")?;
        let exited = stack::wait_exited(CORE, STOP_TIMEOUT);
        drop(client);
        exited.map(drop)
    }

    fn start_core(&mut self) -> TestResult<()> {
        stack::start(CORE)?;
        self.connect()
    }

    /// Starts the core expecting it to refuse, and returns what it logged.
    fn start_refused(&mut self) -> TestResult<String> {
        let since = stack::now();
        stack::start(CORE)?;
        let code = stack::wait_exited(CORE, STOP_TIMEOUT)?;
        let logs = stack::logs_since(CORE, &since)?;
        if code == 0 {
            return Err(TestError::Assertion {
                message: format!("the core started, or exited cleanly:\n{logs}"),
            });
        }
        Ok(logs)
    }

    /// Applies `topics` (and `subjects`) to the stack, as `cargo xtask kafka apply` would.
    async fn apply(
        &self,
        topics: &[kafka_topics::Topic],
        subjects: &[(String, &str)],
    ) -> TestResult<()> {
        kafka_topics::apply(
            topics,
            subjects,
            &self.config.kafka_brokers,
            &self.schema_registry_url,
        )
        .await
        .map_err(|message| TestError::Configuration { message })
    }

    /// Funds the three traders and trades a few rounds, all delivered.
    async fn baseline(&mut self) -> TestResult<()> {
        let (base, quote) = (
            base_asset(self.config.market_id),
            quote_asset(self.config.market_id),
        );
        self.deposit(BIDDER, quote, 100_000_000)?;
        self.deposit(ASKER, base, 1_000_000)?;
        self.deposit(BUYER, quote, 100_000_000)?;
        self.place_orders(5)?;
        self.verify().await
    }

    /// `kill -9` with events queued behind a paused broker, and a restart at once: it waits for the
    /// media driver to release the dead core's journal, then re-publishes the events.
    async fn kill_with_events_queued(&mut self) -> TestResult<()> {
        {
            let _paused = Paused::new(BROKER)?;
            self.place_orders(QUEUED_ORDER_PAIRS)?;
            self.client = None;
            stack::signal(CORE, "KILL")?;
            stack::wait_exited(CORE, STOP_TIMEOUT)?;
        }
        let since = stack::now();
        self.start_core()?;
        let logs = stack::logs_since(CORE, &since)?;
        if !logs.contains("waiting_for_journal_release") {
            return Err(TestError::Assertion {
                message: format!("the restart began after the journal's release:\n{logs}"),
            });
        }
        // Kafka can hold every event already (a paused broker's kernel still takes them into its
        // socket buffers), so only the core's log shows that the restart re-published them.
        expect_logged(&logs, "events_republish_replay")?;
        self.verify().await
    }

    /// A graceful shutdown that cannot deliver within its grace, the broker paused: it reports
    /// the undelivered events, and the restart re-publishes them.
    async fn shutdown_past_the_grace(&mut self) -> TestResult<()> {
        let since = stack::now();
        {
            let _paused = Paused::new(BROKER)?;
            self.place_orders(QUEUED_ORDER_PAIRS)?;
            self.stop_core()?;
        }
        let logs = stack::logs_since(CORE, &since)?;
        if !logs.contains("shutdown_with_undelivered_events") {
            return Err(TestError::Assertion {
                message: format!("the shutdown reported no undelivered events:\n{logs}"),
            });
        }
        let since = stack::now();
        self.start_core()?;
        expect_logged(&stack::logs_since(CORE, &since)?, "events_republish_replay")?;
        self.verify().await
    }

    /// A start while the broker does not answer waits for it, then completes.
    async fn broker_down_at_start(&mut self) -> TestResult<()> {
        self.stop_core()?;
        let since = stack::now();
        {
            let _paused = Paused::new(BROKER)?;
            stack::start(CORE)?;
            tokio::time::sleep(Duration::from_secs(12)).await;
            let logs = stack::logs_since(CORE, &since)?;
            if !stack::is_running(CORE)?
                || !logs.contains("waiting_for_kafka")
                || logs.contains("watermark_read")
            {
                return Err(TestError::Assertion {
                    message: format!("the core did not wait for the broker:\n{logs}"),
                });
            }
        }
        self.connect()?;
        self.place_orders(2)?;
        self.verify().await
    }

    /// A clean shutdown, its gateway connected, runs the events stage's shutdown: it delivers
    /// everything and writes the watermark of the last command, which only the closing channel
    /// closes. The restart re-publishes nothing.
    async fn clean_restart_re_publishes_nothing(&mut self) -> TestResult<()> {
        self.verify().await?;
        let since = stack::now();
        self.stop_core()?;
        expect_logged(&stack::logs_since(CORE, &since)?, "flush_complete")?;
        let last = self.ledger.last_timestamp();
        let delivered = self.kafka.delivered_watermark()?;
        if delivered != last {
            return Err(TestError::Assertion {
                message: format!(
                    "the shutdown left the delivered watermark at {delivered}, not at the last \
                     command's timestamp {last}"
                ),
            });
        }
        let before = self.kafka.end_offsets()?;
        let since = stack::now();
        self.start_core()?;
        // The shutdown's snapshot holds every command: the start replays none.
        let logs = stack::logs_since(CORE, &since)?;
        expect_logged(&logs, "snapshot_loaded")?;
        expect_logged(&logs, "commands=0 ")?;
        // The core accepts gateways once its replay has drained; give whatever it re-published
        // time to land.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let after = self.kafka.end_offsets()?;
        if before != after {
            return Err(TestError::Assertion {
                message: format!("the restart re-published events: {before:?} -> {after:?}"),
            });
        }
        Ok(())
    }

    /// A start refuses, naming the topic, when `vex-core.delivered` is missing.
    async fn missing_delivered_topic_refuses(&mut self) -> TestResult<()> {
        self.stop_core()?;
        delete_topic(&self.config.kafka_brokers, DELIVERED_TOPIC).await?;
        let logs = self.start_refused()?;
        expect_logged(&logs, &format!("topic {DELIVERED_TOPIC} does not exist"))
    }

    /// A start refuses when `vex-core.delivered` is not compacted. Restoring the declared topics
    /// lets it start, and the watermark the deletion emptied re-publishes the whole journal.
    async fn non_compacted_delivered_topic_refuses(&mut self) -> TestResult<()> {
        let file = KafkaTopics::load().map_err(|message| TestError::Configuration { message })?;
        let mut topics = file.topics(true);
        let delivered = topics.last_mut().expect("the delivered topic");
        delivered
            .config
            .insert("cleanup.policy".to_string(), "delete".to_string());
        self.apply(&topics, &[]).await?;
        let logs = self.start_refused()?;
        expect_logged(
            &logs,
            &format!("topic {DELIVERED_TOPIC} has cleanup.policy=delete"),
        )?;

        self.apply(&file.topics(true), &file.subjects()).await?;
        let (_, read_before) = self.kafka.published()?;
        self.start_core()?;
        let journal = self.ledger.expected.len();
        let deadline = Instant::now() + self.config.event_timeout;
        while self.kafka.published()?.1 < read_before + journal {
            if Instant::now() > deadline {
                return Err(TestError::Assertion {
                    message: format!(
                        "an empty watermark did not re-publish the whole journal of {journal} \
                         events"
                    ),
                });
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.verify().await
    }

    /// A start whose replay cannot re-publish, the Schema Registry (which Redpanda hosts) not
    /// answering, fails within a minute instead of hanging; the next start recovers.
    async fn restart_with_the_registry_unreachable_fails_within_a_minute(
        &mut self,
    ) -> TestResult<()> {
        {
            let _paused = Paused::new(BROKER)?;
            self.place_orders(UNDELIVERED_ROUNDS)?;
            self.client = None;
            stack::signal(CORE, "KILL")?;
            stack::wait_exited(CORE, STOP_TIMEOUT)?;
        }
        // The start needs the broker to read the watermark. It then waits for the killed core's
        // journal, which gives the broker time to be paused again before the replay begins.
        let since = stack::now();
        stack::start(CORE)?;
        stack::wait_logged(CORE, &since, "watermark_read", START_TIMEOUT)?;
        {
            let _paused = Paused::new(BROKER)?;
            if stack::logs_since(CORE, &since)?.contains("recording_selected") {
                return Err(TestError::Assertion {
                    message: "the replay began before the broker was paused again".to_string(),
                });
            }
            let code = stack::wait_exited(CORE, Duration::from_secs(60))?;
            let logs = stack::logs_since(CORE, &since)?;
            if code == 0 {
                return Err(TestError::Assertion {
                    message: format!("the failed replay exited cleanly:\n{logs}"),
                });
            }
            expect_logged(&logs, "Failed to create VexCoreServer: Replay error")?;
        }
        self.start_core()?;
        self.verify().await
    }

    /// A broker that takes no events fills the core's queues behind it, and its answers stop; it
    /// then answers Busy, at once, to whatever its ring cannot hold. A Busy order is neither
    /// executed nor published, and once the broker is back every placed order is answered and
    /// published. Most of them stay in the book, in every snapshot after.
    async fn busy_when_the_broker_stalls(&mut self) -> TestResult<()> {
        let cost = self
            .market
            .calculate_quote_cost(FLOOD_PRICE, 1)
            .expect("scenario notional fits in u64");
        let before: i64 = self.kafka.end_offsets()?.values().sum();
        let quote = quote_asset(self.config.market_id);
        self.deposit(FLOODER, quote, cost * WINDOW * (MAX_STALLED_WINDOWS + 2))?;
        let mut flood = Flood::default();
        let since = stack::now();
        {
            let _paused = Paused::new(BROKER)?;
            let mut sent = 0;
            loop {
                if sent == WINDOW * MAX_STALLED_WINDOWS {
                    return Err(TestError::Assertion {
                        message: format!(
                            "{} orders placed with the broker paused, and the answers never \
                             stopped",
                            flood.placed.len()
                        ),
                    });
                }
                self.flood(&mut flood, sent..sent + WINDOW)?;
                sent += WINDOW;
                match self.take_answers(&mut flood) {
                    Ok(()) => {}
                    // The events stage waits on the full queues: the window's answers stop.
                    Err(TestError::Timeout { .. }) => break,
                    Err(e) => return Err(e),
                }
            }
            // Stopped by the stalled broker: the Kafka sender waits on a full librdkafka queue.
            if !stack::logs_since(CORE, &since)?.contains("action=\"producer_send_waiting\"") {
                return Err(TestError::Assertion {
                    message: "the answers stopped, and the core's Kafka sender never waited on a \
                              full queue"
                        .to_string(),
                });
            }
            // More than the ring holds: the rest is answered Busy, the broker still paused.
            self.flood(&mut flood, sent..sent + 2 * WINDOW)?;
            while flood.busy == 0 {
                self.take_answer(&mut flood)?;
            }
        }
        self.take_answers(&mut flood)?;
        info!(
            "  {} orders placed, {} answered Busy",
            flood.placed.len(),
            flood.busy
        );
        for placed in &flood.placed {
            self.ledger.placed(placed, &self.market)?;
        }
        self.resting.push(flood.placed[0].clone());
        self.verify().await?;
        // Kafka holds the events of every placed order (`verify`), and nothing more: the deposit's
        // two, and four for each placement.
        let published = self.kafka.end_offsets()?.values().sum::<i64>() - before;
        let expected = 2 + 4 * flood.placed.len() as i64;
        if published != expected {
            return Err(TestError::Assertion {
                message: format!(
                    "Kafka took {published} events, not the {expected} of the deposit and the \
                     placed orders: a Busy order published"
                ),
            });
        }
        Ok(())
    }

    /// A clean stop takes a snapshot, which the next start loads. After a `kill -9`, the start
    /// loads it again and replays only the two orders journaled since. Every order resting from
    /// before the snapshot and after it is in the book: each cancels.
    async fn kill_after_a_snapshot_replays_only_the_tail(&mut self) -> TestResult<()> {
        self.stop_core()?;
        self.start_core()?;
        for (user, side, price) in [(BIDDER, Side::Bid, 30_000), (ASKER, Side::Ask, 70_000)] {
            let order = OrderBuilder::place_limit()
                .user(user)
                .price(price)
                .size(1)
                .side(side)
                .market_id(self.config.market_id)
                .build();
            let placed = self.execute(order)?;
            self.ledger.placed(&placed, &self.market)?;
            self.resting.push(placed);
        }
        self.client = None;
        stack::signal(CORE, "KILL")?;
        stack::wait_exited(CORE, STOP_TIMEOUT)?;

        let since = stack::now();
        self.start_core()?;
        let logs = stack::logs_since(CORE, &since)?;
        expect_logged(&logs, "snapshot_loaded")?;
        expect_logged(&logs, "commands=2 ")?;
        for order in std::mem::take(&mut self.resting) {
            let cancel = OrderCommand::cancel_order(
                order.order_id,
                order.user_id,
                order.side,
                order.market_id,
            );
            let cancelled = self.execute(cancel)?;
            self.ledger.cancelled(&cancelled, &order, &self.market)?;
        }
        self.verify().await
    }
}

fn expect_logged(logs: &str, expected: &str) -> TestResult<()> {
    if !logs.contains(expected) {
        return Err(TestError::Assertion {
            message: format!("the core did not say {expected:?}:\n{logs}"),
        });
    }
    Ok(())
}

/// Deletes `topic`, and waits until the brokers no longer list it.
async fn delete_topic(brokers: &str, topic: &str) -> TestResult<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()?;
    for result in admin.delete_topics(&[topic], &AdminOptions::new()).await? {
        result.map_err(|(topic, code)| TestError::Configuration {
            message: format!("cannot delete topic {topic}: {code}"),
        })?;
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while admin
        .inner()
        .fetch_metadata(None, Duration::from_secs(10))?
        .topics()
        .iter()
        .any(|listed| listed.name() == topic)
    {
        if Instant::now() > deadline {
            return Err(TestError::Timeout {
                timeout: Duration::from_secs(30),
            });
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

/// Runs every scenario in order, against a stack whose core is running, and stops at the first
/// failure: each scenario starts from the state the previous one left.
pub async fn run_all(
    config: TestConfig,
    schema_registry_url: &str,
) -> TestResult<Vec<ScenarioResult>> {
    let market = vex_config::VexConfig::new(vex_config::Environment::Development)
        .symbols
        .symbols
        .get(&config.market_id)
        .cloned()
        .ok_or_else(|| TestError::Configuration {
            message: format!(
                "No development market specification for {}",
                config.market_id
            ),
        })?;
    let kafka = KafkaVerifier::new(&config.kafka_brokers).await?;
    let mut outbox = Outbox {
        config,
        schema_registry_url: schema_registry_url.to_string(),
        market,
        client: None,
        kafka,
        ledger: Ledger::default(),
        order_pairs: 0,
        resting: Vec::new(),
    };
    outbox.connect()?;

    let mut results = Vec::new();
    scenario!(results, "baseline", outbox.baseline());
    scenario!(
        results,
        "kill_with_events_queued",
        outbox.kill_with_events_queued()
    );
    scenario!(
        results,
        "shutdown_past_the_grace",
        outbox.shutdown_past_the_grace()
    );
    scenario!(
        results,
        "broker_down_at_start",
        outbox.broker_down_at_start()
    );
    scenario!(
        results,
        "clean_restart_re_publishes_nothing",
        outbox.clean_restart_re_publishes_nothing()
    );
    scenario!(
        results,
        "missing_delivered_topic_refuses",
        outbox.missing_delivered_topic_refuses()
    );
    scenario!(
        results,
        "non_compacted_delivered_topic_refuses",
        outbox.non_compacted_delivered_topic_refuses()
    );
    scenario!(
        results,
        "restart_with_the_registry_unreachable_fails_within_a_minute",
        outbox.restart_with_the_registry_unreachable_fails_within_a_minute()
    );
    scenario!(
        results,
        "busy_when_the_broker_stalls",
        outbox.busy_when_the_broker_stalls()
    );
    scenario!(
        results,
        "kill_after_a_snapshot_replays_only_the_tail",
        outbox.kill_after_a_snapshot_replays_only_the_tail()
    );
    Ok(results)
}
