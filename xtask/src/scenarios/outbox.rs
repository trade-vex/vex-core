//! The core's outbox, end to end: a journaled core re-publishes from its journal whatever a death
//! or a shutdown left undelivered, waits for Kafka at start, and refuses to start on a topic that
//! no-loss cannot rest on.
//!
//! The scenarios share one continuous ledger, in order, on the stack of
//! `xtask/tests/docker-compose.yml`: Redpanda with its Schema Registry, an Aeron
//! ArchivingMediaDriver that journals the core's commands, and the core. Every order is a limit
//! order that never crosses: replay regenerates its events exactly, which it does not for a
//! market buy (#140).
//!
//! The oracle is the client's own ledger, built from the core's responses: every event each
//! answered command must have published, by `(topic, key, timestamp)`, and each balance's state
//! after the last command that changed it. Kafka must hold every one of those events, and its
//! latest balance per key (by timestamp) must equal the ledger's.

use crate::builders::OrderBuilder;
use crate::kafka_topics::{self, KafkaTopics};
use crate::test_framework::client::TestClient;
use crate::test_framework::kafka::KafkaVerifier;
use crate::test_framework::stack::{self, BROKER, CORE, Paused};
use crate::test_framework::types::*;
use common::{CoreMarketSpecification, OrderCommand, Side, Status, base_asset, quote_asset};
use hashbrown::{HashMap, HashSet};
use processors::events::DELIVERED_TOPIC;
use rdkafka::admin::{AdminClient, AdminOptions};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use std::time::{Duration, Instant};
use tracing::{error, info};

const BIDDER: u64 = 101;
const ASKER: u64 = 102;

/// Order pairs placed while the broker is paused: enough to leave events queued in the core, far
/// fewer than the 108,000 it takes to fill its queues and answer `Busy`.
const QUEUED_ORDER_PAIRS: u64 = 25;

/// How long the core gets to accept a gateway after a start: the topic check, the replay and its
/// re-publication.
const START_TIMEOUT: Duration = Duration::from_secs(120);

/// How long a stopping core gets to exit: a graceful shutdown with the broker paused spends the
/// 20 s delivery grace, plus up to 5 s, after its pipeline drain.
const STOP_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the media driver takes to release the publications of a client that died without
/// closing them: its client liveness timeout (10 s), and a margin. A core started sooner after a
/// `kill -9` finds its journal's publication still held by the dead process, and fails to start
/// ("existing publication has different 'term-offset'"; #201).
const DEAD_CLIENT_RELEASE: Duration = Duration::from_secs(15);

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

    /// Credits `credit` to `user`'s available `asset`, then locks `lock` of it, as of the command
    /// stamped `timestamp`, which publishes that balance.
    fn change(&mut self, user: u64, asset: u16, credit: u64, lock: u64, timestamp: u64) {
        let (balance, at) = self
            .balances
            .entry((user, asset))
            .or_insert((Balance::new(0, 0), 0));
        *balance = Balance::new(balance.available + credit - lock, balance.locked + lock);
        *at = timestamp;
        self.expect("balances", format!("{user}:{asset}"), timestamp);
    }

    fn deposited(&mut self, response: &OrderCommand) -> TestResult<()> {
        expect_status(response, Status::Processed)?;
        let (user, asset, timestamp) = (
            response.user_id,
            response.market_id as u16,
            response.timestamp,
        );
        self.expect("deposits", user.to_string(), timestamp);
        self.change(user, asset, response.size, 0, timestamp);
        Ok(())
    }

    fn placed(
        &mut self,
        response: &OrderCommand,
        market: &CoreMarketSpecification,
    ) -> TestResult<()> {
        expect_status(response, Status::Placed)?;
        let (user, timestamp) = (response.user_id, response.timestamp);
        let (base, quote) = (
            base_asset(response.market_id),
            quote_asset(response.market_id),
        );
        let (locked_asset, other_asset, lock) = match response.side {
            Side::Bid => {
                let cost = market
                    .calculate_quote_cost(response.price, response.size)
                    .expect("scenario notional fits in u64");
                (quote, base, cost)
            }
            Side::Ask => (base, quote, response.size),
        };
        self.change(user, locked_asset, 0, lock, timestamp);
        self.change(user, other_asset, 0, 0, timestamp);
        self.expect("orders", response.order_id.to_string(), timestamp);
        self.expect("orderbook", response.market_id.to_string(), timestamp);
        Ok(())
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
}

impl Outbox {
    /// Connects a client to the core, retrying until the core accepts a gateway, which it does
    /// once it has replayed its journal.
    fn connect(&mut self) -> TestResult<()> {
        let start = Instant::now();
        loop {
            match TestClient::new(
                0,
                self.config.default_timeout,
                &self.config.core_address,
                self.config.core_port,
                self.config.core_control_port,
            ) {
                Ok(client) => {
                    self.client = Some(client);
                    return Ok(());
                }
                Err(e) if start.elapsed() < START_TIMEOUT => {
                    info!("  core not accepting gateways yet: {e}");
                    std::thread::sleep(Duration::from_secs(1));
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn execute(&mut self, command: OrderCommand) -> TestResult<OrderCommand> {
        self.client
            .as_mut()
            .expect("the core is running and connected")
            .send_and_recv(command)
    }

    /// Places `pairs` resting bids and asks that never cross: bids below 50,000, asks above.
    fn place_orders(&mut self, pairs: u64) -> TestResult<()> {
        for _ in 0..pairs {
            self.order_pairs += 1;
            for (user, side, price) in [
                (BIDDER, Side::Bid, 40_000 - self.order_pairs),
                (ASKER, Side::Ask, 60_000 + self.order_pairs),
            ] {
                let order = OrderBuilder::place_limit()
                    .user(user)
                    .price(price)
                    .size(1)
                    .side(side)
                    .market_id(self.config.market_id)
                    .build();
                let response = self.execute(order)?;
                self.ledger.placed(&response, &self.market)?;
            }
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

    /// Stops the core gracefully, once it has let the client's gateway go, and waits for it to
    /// exit.
    ///
    /// The core releases a gateway session's ring producer only when Aeron confirms that the
    /// session's subscription has closed, which it never does once the shutdown has closed the
    /// Aeron client. A session still open at shutdown therefore keeps the pipeline alive, and the
    /// process exits without the events handler's delivery grace or its last watermark write
    /// (the next start re-publishes what that leaves undelivered; #200). Letting the gateway go
    /// first tests the shutdown the outbox specifies.
    fn stop_core(&mut self) -> TestResult<()> {
        let since = stack::now();
        if self.client.take().is_some() {
            stack::wait_for_log(
                CORE,
                &since,
                "subscription_closed_handlers_released",
                Duration::from_secs(30),
            )?;
        }
        stack::signal(CORE, "TERM")?;
        stack::wait_exited(CORE, STOP_TIMEOUT).map(drop)
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

    /// Funds the two traders and rests a few orders, all delivered.
    async fn baseline(&mut self) -> TestResult<()> {
        let (base, quote) = (
            base_asset(self.config.market_id),
            quote_asset(self.config.market_id),
        );
        self.deposit(BIDDER, quote, 100_000_000)?;
        self.deposit(ASKER, base, 1_000_000)?;
        self.place_orders(5)?;
        self.verify().await
    }

    /// `kill -9` with events queued behind a paused broker: the restart re-publishes them.
    async fn kill_with_events_queued(&mut self) -> TestResult<()> {
        {
            let _paused = Paused::new(BROKER)?;
            self.place_orders(QUEUED_ORDER_PAIRS)?;
            self.client = None;
            stack::signal(CORE, "KILL")?;
            stack::wait_exited(CORE, STOP_TIMEOUT)?;
        }
        tokio::time::sleep(DEAD_CLIENT_RELEASE).await;
        self.start_core()?;
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
        self.start_core()?;
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

    /// A clean shutdown delivers everything and writes the watermark of its last command, which
    /// only the closing channel closes: the restart re-publishes nothing.
    async fn clean_restart_re_publishes_nothing(&mut self) -> TestResult<()> {
        self.verify().await?;
        self.stop_core()?;
        let before = self.kafka.end_offsets()?;
        self.start_core()?;
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
    };
    outbox.connect()?;

    let mut results = Vec::new();
    macro_rules! scenario {
        ($name:ident) => {
            let name = stringify!($name).to_string();
            info!("Scenario: {name}");
            let started = Instant::now();
            match outbox.$name().await {
                Ok(()) => results.push(ScenarioResult::success(name, started.elapsed())),
                Err(e) => {
                    error!("Scenario {name} failed: {e}");
                    results.push(ScenarioResult::failure(name, started.elapsed(), e));
                    return Ok(results);
                }
            }
        };
    }
    scenario!(baseline);
    scenario!(kill_with_events_queued);
    scenario!(shutdown_past_the_grace);
    scenario!(broker_down_at_start);
    scenario!(clean_restart_re_publishes_nothing);
    scenario!(missing_delivered_topic_refuses);
    scenario!(non_compacted_delivered_topic_refuses);
    Ok(results)
}
