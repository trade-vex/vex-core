use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::journaling::ReplayControl;
use common::MatcherTradeEvent;
use common::OrderCommand;
use common::OrderCommandType;
use common::Status;
use common::UserBalance;
use common::{base_asset, order_debug, quote_asset};
use hashbrown::HashMap;
use prost::Message;
use rdkafka::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{DeliveryResult, Message as _};
use rdkafka::producer::{BaseRecord, Producer, ProducerContext, ThreadedProducer};
use schema_registry_converter::async_impl::proto_raw::ProtoRawEncoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::error::SRCError;
use schema_registry_converter::schema_registry_common::{
    SchemaType, SubjectNameStrategy, SuppliedSchema,
};
use serde::Serialize;
use tokio::runtime::Runtime;
use tracing::{debug, error, info, warn};
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

/// Capacity, in events, of the channel from the events stage to the Kafka sender.
///
/// Per event the sender does about as much as the events stage (a cached schema lookup and an
/// enqueue into librdkafka's own queue), so the channel only absorbs the sender's stalls: a full
/// librdkafka queue, or a Schema Registry round trip (a topic's first event, or a retry after a
/// failed one, each bounded by [`SCHEMA_REGISTRY_TIMEOUT`]). 8,192 events is about two thousand
/// commands (a placement emits four) and a few MB at most. Ordering does not depend on it: a full
/// channel blocks the events stage and never drops.
const SENDER_CAPACITY: usize = 8192;

/// Bound on one Schema Registry call (connect and response).
///
/// The sender waits for the call, and the events stage behind it. A registry that accepts the
/// connection but never answers would otherwise hold the sender inside one attempt forever: the
/// stall would never be logged, and a shutdown could not give up on it.
const SCHEMA_REGISTRY_TIMEOUT: Duration = Duration::from_secs(2);

/// The wait before the sender first tries again to frame an event it could not frame. It doubles
/// after each failure, up to [`ENCODE_RETRY_MAX_BACKOFF`].
const ENCODE_RETRY_FIRST_BACKOFF: Duration = Duration::from_millis(10);

/// The longest wait between two attempts to frame one event; with only one event retried at a
/// time, it bounds the request rate on a registry that is down.
const ENCODE_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// How long the sender waits before offering an event to a full librdkafka queue again.
const QUEUE_FULL_BACKOFF: Duration = Duration::from_millis(10);

/// While a stall lasts, the minimum gap between two of its log lines.
const STALL_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// How long a dropped handler keeps trying to deliver the events it still holds before it gives
/// up, and logs how many it could not deliver.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(20);

/// How far past its grace a dropped handler waits for the sender thread: enough for the sender's
/// last registry call (2 s) and backoff (1 s), and for librdkafka's teardown, which normally takes
/// well under a second but can block on DNS. Past this the thread is left to process exit.
const SHUTDOWN_OVERRUN: Duration = Duration::from_secs(5);

/// One event on its way to Kafka.
struct OutboundEvent {
    topic: &'static str,
    key: String,
    /// Fully qualified protobuf message name, e.g. `trading.BalanceEvent`.
    message_name: &'static str,
    /// The prost-encoded message, not yet framed with its schema id.
    payload: Vec<u8>,
}

/// What the sender does about a failure.
#[derive(Debug, PartialEq)]
enum Verdict {
    /// Wait, then try the same event again: the cause can clear (an outage, a full queue).
    Retry,
    /// Abort the process: no retry can cure the failure, and skipping the event would lose it.
    Abort,
    /// Count the event as undelivered: the producer is discarding what it still holds, because a
    /// shutdown gave up on it.
    Discarded,
}

/// Whether the sender may try an event again after it failed to frame.
///
/// Every failure of a Schema Registry call is cached by the encoder (`cached`), and every such
/// failure is retried, whatever its `retriable` flag says. schema_registry_converter 4.7.0 never
/// looks at the HTTP status: it marks a reply it cannot parse as non-retriable (a registry
/// answering 503 with an HTML body), and it reports a registry's JSON error (a 5xx while the
/// registry's own Kafka store is unreachable, but equally a 409 for an incompatible schema) as
/// "could not get id from response". Aborting on those would crash-loop the exchange through the
/// very registry or Kafka outage that back-pressure is there to ride out, so a rejected schema
/// shows as a stall logged as `protobuf_encoding_failed`.
///
/// A failure that never reached the registry (`cached` false) comes from the schema compiled into
/// this binary: the message is not in `trading.proto`. No retry can cure that.
fn encode_failure(error: &SRCError) -> Verdict {
    if error.cached {
        Verdict::Retry
    } else {
        Verdict::Abort
    }
}

/// What the sender does when the producer refuses an event, which is then not enqueued, so waiting
/// keeps its place.
///
/// - A full librdkafka queue is how a broker outage reaches the sender: wait.
/// - An unknown topic or partition: wait too. librdkafka marks a long-lived topic as missing at
///   once when one out-of-sync broker reports it so, as can happen during a broker restart. The
///   events it already held for the topic fail their delivery and abort (see
///   [`delivery_failure`]); waiting at least spares an exchange that had none queued. A topic that
///   is really missing therefore shows as a stall, like an outage.
/// - Anything else aborts, such as the fatal state of an idempotent producer that can no longer
///   vouch for its partitions' order.
fn send_failure(error: &KafkaError) -> Verdict {
    match error {
        KafkaError::MessageProduction(
            RDKafkaErrorCode::QueueFull
            | RDKafkaErrorCode::UnknownTopic
            | RDKafkaErrorCode::UnknownPartition,
        ) => Verdict::Retry,
        _ => Verdict::Abort,
    }
}

/// What the producer does with a failed delivery report.
///
/// librdkafka retries every transient error without a time limit (`message.timeout.ms=0`), so a
/// failed report is either permanent (a missing topic, a denied ACL, an oversized message), or the
/// producer discarding events it still holds: those of a shutdown that gave up on them (counted
/// there), or, before any shutdown, a queue purged by a fatal error. That purge is how a fatal error
/// reaches this handler; the next send reports it too. Its global error event cannot tell, because
/// librdkafka gives it the underlying code and rdkafka 0.36 does not expose the fatal flag.
fn delivery_failure(error: &KafkaError, shutdown_begun: bool) -> Verdict {
    match error {
        KafkaError::MessageProduction(
            RDKafkaErrorCode::PurgeQueue | RDKafkaErrorCode::PurgeInflight,
        ) if shutdown_begun => Verdict::Discarded,
        _ => Verdict::Abort,
    }
}

/// Logs why, then aborts the process, as a panicking pipeline stage does
/// (`abort_on_handler_panic` in the server). For an event that can be neither delivered nor
/// skipped: carrying on would lose it, or publish what follows it in its place.
fn abort_exchange(action: &'static str, detail: fmt::Arguments<'_>) -> ! {
    error!(
        target: "events",
        component = "kafka_handler",
        action,
        "{detail}; aborting the exchange rather than drop a Kafka event"
    );
    std::process::abort();
}

/// Counts the failures of one stall, which can last as long as an outage, and rate-limits their
/// log lines: the first, then at most one per [`STALL_LOG_INTERVAL`].
#[derive(Default)]
struct StallLog {
    failures: u64,
    last_logged: Option<Instant>,
}

impl StallLog {
    /// Counts one failure; true if it is the one to log.
    fn failed(&mut self) -> bool {
        self.failures += 1;
        let due = self
            .last_logged
            .is_none_or(|logged| logged.elapsed() >= STALL_LOG_INTERVAL);
        if due {
            self.last_logged = Some(Instant::now());
        }
        due
    }
}

/// When the sender stops trying: never while the handler is alive, and `grace` after it is
/// dropped.
#[derive(Clone)]
struct Shutdown {
    grace: Duration,
    deadline: Arc<OnceLock<Instant>>,
}

impl Shutdown {
    fn with_grace(grace: Duration) -> Self {
        Self {
            grace,
            deadline: Arc::default(),
        }
    }

    /// Starts the grace period; only the first call counts.
    fn begin(&self) {
        let _ = self.deadline.set(Instant::now() + self.grace);
    }

    fn has_begun(&self) -> bool {
        self.deadline.get().is_some()
    }

    /// Whether the grace has run out, so the sender must give up.
    fn is_overdue(&self) -> bool {
        self.deadline
            .get()
            .is_some_and(|deadline| Instant::now() >= *deadline)
    }

    /// What is left of the grace; zero before the shutdown begins.
    fn time_left(&self) -> Duration {
        self.deadline.get().map_or(Duration::ZERO, |deadline| {
            deadline.saturating_duration_since(Instant::now())
        })
    }
}

/// The shutdown deadline passed before the event reached the producer.
#[derive(Debug)]
struct GaveUp;

/// The single, ordered path from the events stage to Kafka.
///
/// The events stage submits events in engine order into a bounded channel. One worker thread
/// owns the receiving end and finishes each event before it takes the next, so events reach the
/// producer in the order they were submitted.
struct OrderedSender {
    /// `None` only inside `drop`, which closes the channel by dropping it.
    events: Option<SyncSender<OutboundEvent>>,
    shutdown: Shutdown,
    worker: Option<JoinHandle<()>>,
}

impl OrderedSender {
    /// Runs `worker` on its own thread, fed by a channel that holds `capacity` events. `shutdown`
    /// begins when this is dropped; the worker must give up once it is overdue.
    fn spawn(
        capacity: usize,
        shutdown: Shutdown,
        worker: impl FnOnce(Receiver<OutboundEvent>) + Send + 'static,
    ) -> Self {
        let (events, receiver) = mpsc::sync_channel(capacity);
        let worker = thread::Builder::new()
            .name("kafka-sender".to_string())
            .spawn(move || {
                // Unwinding would drop the events in the channel while the exchange runs on, so a
                // panic aborts, as a panicking pipeline stage does.
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| worker(receiver))) {
                    // Dropping the payload could panic again and unwind past the abort.
                    std::mem::forget(payload);
                    abort_exchange(
                        "sender_panicked",
                        format_args!("the Kafka sender thread panicked"),
                    );
                }
            })
            .expect("failed to spawn the Kafka sender thread");
        Self {
            events: Some(events),
            shutdown,
            worker: Some(worker),
        }
    }

    /// Hands `event` to the worker, blocking while the channel is full.
    ///
    /// Blocking is the back-pressure: the events stage stalls, the ring fills and ingress answers
    /// `Busy`. Nothing is dropped to keep up.
    fn submit(&self, event: OutboundEvent) {
        let events = self
            .events
            .as_ref()
            .expect("the channel closes only in drop");
        // The worker stops receiving only when the channel closes, or by panicking. Going on
        // without it would silently stop every event, so fail the events stage instead.
        events
            .send(event)
            .expect("the Kafka sender thread has died");
    }
}

impl Drop for OrderedSender {
    /// Starts the shutdown grace and closes the channel, then waits for the worker to send what
    /// it can of everything already submitted: until it finishes, or [`SHUTDOWN_OVERRUN`] past
    /// the grace.
    fn drop(&mut self) {
        self.shutdown.begin();
        drop(self.events.take());
        let Some(worker) = self.worker.take() else {
            return;
        };
        let give_up = Instant::now() + self.shutdown.grace + SHUTDOWN_OVERRUN;
        while !worker.is_finished() {
            if Instant::now() >= give_up {
                error!(
                    target: "events",
                    component = "kafka_handler",
                    action = "sender_overran_shutdown",
                    "the Kafka sender did not finish in time; leaving it to process exit"
                );
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        // It has finished, and a panic in it aborts the process (see `spawn`): nothing to report.
        let _ = worker.join();
    }
}

/// The sender's loop: frame an event, enqueue it, and only then take the next.
///
/// Returns how many submitted events never reached the producer because the shutdown gave up
/// first; zero unless it did.
fn run_sender(
    events: Receiver<OutboundEvent>,
    shutdown: &Shutdown,
    mut frame: impl FnMut(&OutboundEvent) -> Result<Vec<u8>, SRCError>,
    mut enqueue: impl FnMut(&OutboundEvent, &[u8]) -> Result<(), GaveUp>,
) -> usize {
    let mut events = events.into_iter();
    while let Some(event) = events.next() {
        let enqueued = frame_with_retries(&event, shutdown, &mut frame)
            .and_then(|framed| enqueue(&event, &framed));
        if enqueued.is_err() {
            // The shutdown has closed the channel, so this counts what was submitted before it.
            return 1 + events.count();
        }
    }
    0
}

/// Frames `event`, trying again after every failure a retry can cure, for as long as it takes.
///
/// The event keeps its place meanwhile: the sender takes nothing else, so the stall backs up into
/// the events stage (and ingress answers `Busy`) instead of skipping the event. A failure no retry
/// can cure aborts. Gives up only once the shutdown is overdue.
fn frame_with_retries(
    event: &OutboundEvent,
    shutdown: &Shutdown,
    frame: &mut impl FnMut(&OutboundEvent) -> Result<Vec<u8>, SRCError>,
) -> Result<Vec<u8>, GaveUp> {
    let mut backoff = ENCODE_RETRY_FIRST_BACKOFF;
    let mut stall = StallLog::default();
    loop {
        let error = match frame(event) {
            Ok(framed) => {
                if stall.failures > 0 {
                    info!(
                        target: "events",
                        component = "kafka_handler",
                        action = "protobuf_encoding_recovered",
                        topic = %event.topic,
                        failures = stall.failures
                    );
                }
                return Ok(framed);
            }
            Err(error) => error,
        };
        if encode_failure(&error) == Verdict::Abort {
            abort_exchange(
                "protobuf_encoding_impossible",
                format_args!(
                    "{} for topic {} cannot be framed with the compiled schema: {error}",
                    event.message_name, event.topic
                ),
            );
        }
        if stall.failed() {
            error!(
                target: "events",
                component = "kafka_handler",
                action = "protobuf_encoding_failed",
                topic = %event.topic,
                subject = %format!("{}-value", event.topic),
                error = %error,
                failures = stall.failures,
                retry_in = ?backoff
            );
        }
        if shutdown.is_overdue() {
            return Err(GaveUp);
        }
        thread::sleep(backoff);
        backoff = (backoff * 2).min(ENCODE_RETRY_MAX_BACKOFF);
    }
}

/// The producer configuration.
///
/// - `enable.idempotence` has the broker sequence-check each partition's batches, so a retry can
///   neither reorder nor duplicate a partition. It requires `acks=all` (set explicitly here), at
///   most five in-flight requests per connection and retries, which librdkafka then defaults to
///   compatible values (unbounded retries); creating the producer fails on a conflicting setting.
/// - `message.timeout.ms=0` removes the local delivery timeout: librdkafka retries an undelivered
///   event for as long as it takes and never expires one. A stalled broker therefore fills its
///   queue and back-pressures intake instead of losing events.
fn producer_config(brokers: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("message.timeout.ms", "0")
        .set("retry.backoff.ms", "100")
        .set("request.timeout.ms", "3000");
    config
}

/// The producer's context: logs each delivery report and counts the deliveries, and aborts on a
/// failed delivery. Its callbacks run on the producer's polling thread, and on a thread that
/// flushes it.
struct DeliveryLog {
    shutdown: Shutdown,
    /// Events the broker acknowledged. Every other enqueued event is undelivered: still held by
    /// librdkafka, or discarded once a shutdown has begun.
    delivered: AtomicUsize,
}

impl ClientContext for DeliveryLog {}

impl ProducerContext for DeliveryLog {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
        let (error, message) = match result {
            Ok(message) => {
                self.delivered.fetch_add(1, Ordering::Relaxed);
                debug!(
                    target: "events",
                    component = "kafka_handler",
                    action = "event_sent",
                    topic = message.topic(),
                    partition = message.partition(),
                    offset = message.offset()
                );
                return;
            }
            Err(failed) => failed,
        };
        let topic = message.topic();
        let key = String::from_utf8_lossy(message.key().unwrap_or_default());
        match delivery_failure(error, self.shutdown.has_begun()) {
            Verdict::Discarded => debug!(
                target: "events",
                component = "kafka_handler",
                action = "event_discarded",
                topic,
                key = %key
            ),
            Verdict::Retry | Verdict::Abort => abort_exchange(
                "delivery_failed",
                format_args!("an event for topic {topic} (key {key}) was not delivered: {error}"),
            ),
        }
    }
}

type EventProducer = ThreadedProducer<DeliveryLog>;

fn create_producer(config: &ClientConfig, shutdown: &Shutdown) -> EventProducer {
    config
        .create_with_context(DeliveryLog {
            shutdown: shutdown.clone(),
            delivered: AtomicUsize::new(0),
        })
        .expect("Producer creation failed")
}

/// Waits until the shutdown is overdue for the delivery of the `enqueued` events, then logs how
/// many are undelivered and returns that count: the `not_enqueued` events the sender gave up on,
/// plus those librdkafka still holds, which dropping the producer discards. An event in flight
/// is counted although the broker may yet write it.
fn flush_at_shutdown(
    producer: &EventProducer,
    shutdown: &Shutdown,
    not_enqueued: usize,
    enqueued: usize,
) -> usize {
    // Whether the flush timed out shows in the count below.
    let _ = producer.flush(shutdown.time_left());
    let delivered = producer.context().delivered.load(Ordering::Relaxed);
    let undelivered = not_enqueued + enqueued.saturating_sub(delivered);
    if undelivered == 0 {
        debug!(
            target: "events",
            component = "kafka_handler",
            action = "flush_complete"
        );
    } else {
        error!(
            target: "events",
            component = "kafka_handler",
            action = "shutdown_with_undelivered_events",
            undelivered,
            not_enqueued,
            grace = ?shutdown.grace,
            "{undelivered} Kafka events were not delivered before shutdown; replay will not re-publish them"
        );
    }
    undelivered
}

/// The schema registered for every topic: the whole `trading.proto`.
fn trading_schema() -> SuppliedSchema {
    SuppliedSchema {
        name: None,
        schema_type: SchemaType::Protobuf,
        schema: TRADING_SCHEMA.to_string(),
        references: vec![],
        properties: None,
        tags: None,
    }
}

/// The Schema Registry encoder, with every call bounded by [`SCHEMA_REGISTRY_TIMEOUT`].
fn schema_encoder(url: &str) -> ProtoRawEncoder<'static> {
    let settings = SrSettings::new_builder(url.to_string())
        .set_timeout(SCHEMA_REGISTRY_TIMEOUT)
        .build()
        .expect("Schema Registry client creation failed");
    ProtoRawEncoder::new(settings)
}

/// Frames `event` with its Schema Registry id, registering the schema on first use.
///
/// The subject is `<topic>-value` (TopicNameStrategy), where Kafka Connect's protobuf converter
/// looks it up. The encoder caches a failed registration; it is evicted here, so that the next
/// attempt asks the registry again.
async fn frame_with_schema(
    encoder: &ProtoRawEncoder<'_>,
    schema: &SuppliedSchema,
    event: &OutboundEvent,
) -> Result<Vec<u8>, SRCError> {
    let strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
        event.topic.to_string(),
        false,
        schema.clone(),
    );
    let framed = encoder
        .encode(&event.payload, event.message_name, strategy)
        .await;
    if framed.as_ref().is_err_and(|error| error.cached) {
        encoder.remove_errors_from_cache();
    }
    framed
}

/// Enqueues one framed event on the producer, waiting out a refusal that can clear (a full queue,
/// an unknown topic; see [`send_failure`]) for as long as it lasts.
///
/// While the broker is down librdkafka keeps every event (it never times one out), so its queue
/// fills. The event keeps its place while it waits: the sender takes nothing else, so the wait
/// backs up into the events stage instead of dropping or overtaking the event. Any other refusal
/// aborts. Gives up only once the shutdown is overdue. Generic over the context so that tests can
/// observe librdkafka instead of acting on it.
fn enqueue<C>(
    producer: &ThreadedProducer<C>,
    event: &OutboundEvent,
    framed: &[u8],
    shutdown: &Shutdown,
) -> Result<(), GaveUp>
where
    C: ProducerContext<DeliveryOpaque = ()> + 'static,
{
    let mut record = BaseRecord::to(event.topic).key(&event.key).payload(framed);
    let mut stall = StallLog::default();
    loop {
        let (error, returned) = match producer.send(record) {
            Ok(()) => return Ok(()),
            Err(refused) => refused,
        };
        if send_failure(&error) == Verdict::Abort {
            abort_exchange(
                "send_failed",
                format_args!(
                    "the producer refused an event for topic {} (key {}): {error}; fatal error: {:?}",
                    event.topic,
                    event.key,
                    producer.client().fatal_error()
                ),
            );
        }
        if stall.failed() {
            warn!(
                target: "events",
                component = "kafka_handler",
                action = "producer_send_waiting",
                topic = %event.topic,
                error = %error,
                waits = stall.failures
            );
        }
        if shutdown.is_overdue() {
            return Err(GaveUp);
        }
        record = returned;
        thread::sleep(QUEUE_FULL_BACKOFF);
    }
}

/// The balance to publish for each maker in a fill chain: one entry per maker, holding the
/// snapshot R2 took after that maker's LAST fill.
///
/// R2 settles a chain in order on the maker's shard, so the last snapshot is the maker's state
/// after the command. The taker is skipped even if it appears as a maker: its own final snapshot
/// is `cmd.balance`, taken after every fill and any cancellation.
fn final_maker_balances(
    fills: Option<&MatcherTradeEvent>,
    taker_id: u64,
) -> HashMap<u64, [UserBalance; 2]> {
    let mut balances = HashMap::new();
    let mut curr_event = fills;
    while let Some(event) = curr_event {
        if event.maker_user_id != taker_id {
            balances.insert(event.maker_user_id, event.maker_balance);
        }
        curr_event = event.next_event.as_deref();
    }
    balances
}

/// Publishes each processed command's events to Kafka and its response to the gateway.
///
/// # Ordering contract (what consumers may rely on)
///
/// 1. **Per key, partition order is engine order.** The events stage hands every event, in
///    engine order, to one ordered sender: a bounded channel drained by a single thread that
///    frames each event with its schema id and enqueues it on the producer before it takes the
///    next. The producer is idempotent, so a broker retry can neither reorder nor duplicate a
///    partition. Every event about one entity carries the same key on its topic, so a sink that
///    applies a partition in offset order, last write wins, ends at the entity's latest state:
///    - `balances`: `user_id:asset_id`, one user's balance in one asset;
///    - `orders`, `cancels`: `order_id`; `orderbook`: `market_id`;
///    - history only: `trades` `taker_order_id:maker_order_id`, `deposits` and `withdrawals`
///      `user_id`.
/// 2. **`timestamp` strictly increases per command.** Every event a command produces carries the
///    command's timestamp, which journaling makes strictly increasing per command. It never
///    repeats and never steps backwards with the wall clock, and every start, with or without
///    `--replay`, continues above the last command in the journal. Order ids are derived from it,
///    so an id is never issued twice either, within a run or across a restart.
/// 3. **At most one event per key per command**, on every topic. In particular a maker hit by
///    several fills of one taker gets one `balances` event per asset, not one per fill.
/// 4. **That event holds the key's state after the command.** Balances are snapshotted in R2,
///    in sequence order with every settlement, deposits and withdrawals included, so a snapshot
///    never misses an earlier command. It can already include a later command that R1 has
///    admitted (a reservation or a deposit).
///
/// So per key the partition order and the `timestamp` order agree, `(key, timestamp)` is unique,
/// and a reader may take the latest state either from the last event in the partition or from the
/// greatest `timestamp`.
///
/// # Delivery contract: never dropped while the process runs
///
/// No event is dropped, skipped or expired while the process runs; a stall back-pressures intake
/// instead. librdkafka retries an undelivered event for as long as it takes
/// (`message.timeout.ms=0`), and the sender retries the same event after a failed Schema Registry
/// call, taking nothing else meanwhile. While Kafka or the registry is unavailable, librdkafka's
/// queue (100,000 events) and then the sender's channel ([`SENDER_CAPACITY`]) fill, the events
/// stage blocks, the ring fills and ingress answers `Busy`. Responses to commands already in the
/// ring wait too, because the events stage sends a command's response after its events. Once
/// Kafka and the registry answer, everything held is delivered in order and intake resumes.
///
/// A failure that no retry can cure aborts the process loudly, as a panicking pipeline stage
/// does, rather than skip the event: a fatal producer error, a failed delivery (a denied ACL, or
/// an event already queued for a topic a broker then reports missing), an event that the compiled
/// schema cannot frame, or a panic in the sender.
///
/// Known gap: events still queued when the process dies are not re-published on replay. That
/// covers a crash, a kill, an abort above, and a graceful shutdown that could not deliver them
/// within [`SHUTDOWN_GRACE`] (it logs how many) or whose pipeline drain timed out first. Closing
/// it means re-publishing from the journal, in the connectors work.
///
/// Not covered:
/// - A registry that rejects the schema (as incompatible, say) is retried like an outage, because
///   schema_registry_converter reports both alike; see [`encode_failure`]. Likewise a topic that
///   is really missing stalls at send rather than aborting; see [`send_failure`].
/// - Adding partitions to a topic moves keys: each partition stays ordered, but a key's events
///   from before and after the change can sit in different partitions.
/// - Without journaling (no archive control channel, as `VEX_DISABLE_ARCHIVE` sets) there is no
///   journal to continue from: timestamps, and the order ids derived from them, follow the wall
///   clock alone, so a restart on a host whose clock is behind the previous run's can repeat
///   them. The same holds for balance preload (development and test only), which runs before
///   the journal is read.
/// - The journal is what the archive recorded. If the host or media driver dies after a
///   command's events were sent but before the archive recorded the command, the restart does not
///   see it and, on a host whose clock is behind, can stamp a new command with its timestamp and
///   id.
pub struct KafkaEventsHandler {
    sender: OrderedSender,
    publications: Arc<Publications>,
    replay_control: ReplayControl,
}

impl KafkaEventsHandler {
    pub fn new(
        brokers: &str,
        schema_registry_url: &str,
        publications: Arc<Publications>,
        replay_control: ReplayControl,
    ) -> Self {
        let shutdown = Shutdown::with_grace(SHUTDOWN_GRACE);
        let producer = create_producer(&producer_config(brokers), &shutdown);
        let encoder = schema_encoder(schema_registry_url);
        let schema = trading_schema();

        // Drives the Schema Registry client, whose calls the sender blocks on.
        let rt = Runtime::new().expect("Failed to create tokio runtime");

        info!(
            target: "events",
            component = "kafka_handler",
            action = "connected",
            brokers = %brokers,
            schema_registry = %schema_registry_url
        );

        let sender = OrderedSender::spawn(SENDER_CAPACITY, shutdown.clone(), move |events| {
            let mut enqueued = 0;
            let not_enqueued = run_sender(
                events,
                &shutdown,
                |event| rt.block_on(frame_with_schema(&encoder, &schema, event)),
                |event, framed| {
                    enqueue(&producer, event, framed, &shutdown)?;
                    enqueued += 1;
                    Ok(())
                },
            );
            // The handler is dropped and the channel drained: wait for delivery, within the grace.
            flush_at_shutdown(&producer, &shutdown, not_enqueued, enqueued);
            // A timed-out registry call can leave a DNS lookup running; do not wait for it.
            rt.shutdown_background();
        });

        Self {
            sender,
            publications,
            replay_control,
        }
    }

    /// Hands one event to the ordered sender, blocking while its channel is full.
    fn publish_proto<T: Message>(
        &self,
        topic_name: &'static str,
        message_key: &str,
        full_name: &'static str,
        data: T,
    ) {
        self.sender.submit(OutboundEvent {
            topic: topic_name,
            key: message_key.to_string(),
            message_name: full_name,
            payload: data.encode_to_vec(),
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

    fn publish_order_event(&self, cmd: &OrderCommand, size: u64, filled_size: u64) {
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
            // Unreachable: Busy is only ever sent back to the gateway by ingress, for a command
            // that never entered the ring. Mapped to the nearest "not executed" value rather than
            // widening the Kafka schema.
            Status::Busy => trading_proto::Status::Rejected,
        };

        let order = trading_proto::Order {
            order_id: cmd.order_id(),
            user_id: cmd.user_id(),
            price: cmd.price(),
            size,
            filled_size,
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

    fn publish_maker_order_event(
        &self,
        event: &MatcherTradeEvent,
        cmd: &OrderCommand,
        market_id: u32,
    ) {
        let maker_side = match cmd.side().op_side() {
            common::Side::Bid => trading_proto::Side::Bid,
            common::Side::Ask => trading_proto::Side::Ask,
        };

        let status = if event.matched_order_completed {
            trading_proto::Status::Filled
        } else {
            trading_proto::Status::PartiallyFilled
        };

        let filled_size = event.maker_original_size - event.maker_remaining_size;

        let order = trading_proto::Order {
            order_id: event.matched_order_id,
            user_id: event.maker_user_id,
            price: event.price,
            size: event.maker_original_size,
            filled_size,
            side: maker_side as i32,
            time_in_force: trading_proto::TimeInForce::TifGtc as i32,
            status: status as i32,
            timestamp: cmd.timestamp(),
        };

        let order_event = trading_proto::OrderEvent {
            order: Some(order),
            market_id,
        };

        let topic_name = "orders";
        self.publish_proto(
            topic_name,
            &event.matched_order_id.to_string(),
            "trading.OrderEvent",
            order_event,
        );
        debug!(
            target: "events",
            component = "kafka_handler",
            action = "maker_order_event_published",
            maker_order_id = event.matched_order_id,
            maker_user_id = event.maker_user_id,
            market_id,
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

    /// Publishes a trade and a maker-order event per fill, then ONE balance event per maker.
    ///
    /// A maker can be hit by several fills of one taker. Publishing a balance per fill would give
    /// that key several events with the same timestamp, and the intermediate ones would tie with
    /// the final one. The taker's balance is published by the caller.
    fn publish_fills(
        &self,
        cmd: &OrderCommand,
        market_id: u32,
        taker_id: u64,
        taker_order_id: u64,
    ) {
        let mut curr_event = cmd.events();
        while let Some(event) = curr_event {
            self.publish_trade_event(event, cmd, market_id, taker_id, taker_order_id);
            self.publish_maker_order_event(event, cmd, market_id);
            curr_event = event.next_event.as_deref();
        }
        for (maker_id, balance) in final_maker_balances(cmd.events(), taker_id) {
            self.publish_balance_event(maker_id, cmd, &balance);
        }
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

    fn publish_orderbook_event(&self, cmd: &OrderCommand) {
        if let Some(snapshot) = &cmd.l2_data {
            let market_id = cmd.market_id();
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
                timestamp: cmd.timestamp(),
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

        order_debug!(
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
            // Unreachable: ingress answers a full ring with Busy and never enqueues the command.
            Status::Busy => {
                error!(
                    target: "events",
                    component = "kafka_handler",
                    action = "unexpected_busy_status",
                    order_id = cmd.order_id()
                );
            }
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
                self.publish_order_event(cmd, cmd.size(), 0);
                self.publish_orderbook_event(cmd);
            }
            Status::Cancelled => {
                order_debug!(
                    "events_publish_cancelled",
                    cmd,
                    stage = "events",
                    handler = "kafka"
                );

                if cmd.events().is_some() {
                    self.publish_fills(cmd, market_id, taker_id, taker_order_id);
                    let filled_size = cmd.events().map(|e| e.calc_filled_size()).unwrap_or(0);
                    let original_size = filled_size + cmd.size();
                    self.publish_order_event(cmd, original_size, filled_size);
                } else if cmd.command == OrderCommandType::CancelOrder {
                    let filled_size = cmd.original_size - cmd.size();
                    self.publish_order_event(cmd, cmd.original_size, filled_size);
                } else {
                    self.publish_order_event(cmd, cmd.size(), 0);
                }

                self.publish_balance_event(taker_id, cmd, &cmd.balance);
                self.publish_cancel_order_event(cmd);
                self.publish_orderbook_event(cmd);
            }
            Status::PartiallyFilled | Status::Filled => {
                order_debug!(
                    "events_publish_trade",
                    cmd,
                    stage = "events",
                    handler = "kafka"
                );
                self.publish_fills(cmd, market_id, taker_id, taker_order_id);
                self.publish_balance_event(taker_id, cmd, &cmd.balance);
                let filled_size = cmd.events().map(|e| e.calc_filled_size()).unwrap_or(0);
                let original_size = filled_size + cmd.size();
                self.publish_order_event(cmd, original_size, filled_size);
                self.publish_orderbook_event(cmd);
            }
            _ => {
                error!(
                    target: "events",
                    component = "kafka_handler",
                    action = "unexpected_status",
                    order_id = cmd.order_id(),
                    status = ?cmd.status()
                );
                self.publish_orderbook_event(cmd);
            }
        }
        // Always publish the response back to the gateway
        self.publish_response(cmd);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{L2MarketData, Side, UserBalance};
    use rdkafka::producer::PurgeConfig;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::Mutex;

    const MARKET_ID: u32 = 100_000_010; // Example market_id encoding

    /// Nothing listens on port 1, so nothing is ever delivered or answered.
    const NO_BROKER: &str = "127.0.0.1:1";

    fn fill(maker_user_id: u64, available: u64) -> MatcherTradeEvent {
        MatcherTradeEvent {
            active_order_completed: false,
            matched_order_id: 0,
            maker_user_id,
            matched_order_completed: true,
            price: 1000,
            size: 1,
            next_event: None,
            maker_balance: [UserBalance::new(available, 0), UserBalance::new(0, 0)],
            maker_remaining_size: 0,
            maker_original_size: 1,
        }
    }

    fn chain(fills: Vec<MatcherTradeEvent>) -> Option<Box<MatcherTradeEvent>> {
        fills.into_iter().rev().fold(None, |next_event, mut fill| {
            fill.next_event = next_event;
            Some(Box::new(fill))
        })
    }

    #[test]
    fn maker_hit_twice_in_one_command_gets_one_balance_with_its_final_state() {
        const TAKER: u64 = 1;
        const MAKER_A: u64 = 2;
        const MAKER_B: u64 = 3;
        // One sweep crosses maker A, then maker B, then maker A again.
        let fills = chain(vec![
            fill(MAKER_A, 10),
            fill(MAKER_B, 20),
            fill(MAKER_A, 30),
        ]);

        let balances = final_maker_balances(fills.as_deref(), TAKER);

        assert_eq!(balances.len(), 2, "one balance per maker, not one per fill");
        assert_eq!(
            balances[&MAKER_A][0],
            UserBalance::new(30, 0),
            "A's last fill wins"
        );
        assert_eq!(balances[&MAKER_B][0], UserBalance::new(20, 0));
    }

    #[test]
    fn taker_balance_is_never_published_from_a_fill() {
        const TAKER: u64 = 1;
        let fills = chain(vec![fill(TAKER, 10), fill(2, 20)]);

        let balances = final_maker_balances(fills.as_deref(), TAKER);

        assert!(
            !balances.contains_key(&TAKER),
            "the taker publishes cmd.balance"
        );
        assert_eq!(balances.len(), 1);
    }

    fn handler_with(sender: OrderedSender) -> KafkaEventsHandler {
        KafkaEventsHandler {
            sender,
            publications: Arc::new(Publications::new()),
            replay_control: ReplayControl::disabled(),
        }
    }

    fn outbound(key: &str) -> OutboundEvent {
        OutboundEvent {
            topic: "balances",
            key: key.to_string(),
            message_name: "trading.BalanceEvent",
            payload: Vec::new(),
        }
    }

    /// What the encoder returns while the registry is down.
    fn registry_down() -> SRCError {
        SRCError::retryable_with_cause("connection refused", "http call to schema registry failed")
            .into_cache()
    }

    /// A sender whose worker frames with `frame` and records each enqueued event's topic and key.
    fn recording_sender(
        capacity: usize,
        frame: impl FnMut(&OutboundEvent) -> Result<Vec<u8>, SRCError> + Send + 'static,
    ) -> (OrderedSender, Receiver<(&'static str, String)>) {
        let (enqueued_tx, enqueued) = mpsc::channel();
        let shutdown = Shutdown::with_grace(SHUTDOWN_GRACE);
        let sender = OrderedSender::spawn(capacity, shutdown.clone(), move |events| {
            run_sender(events, &shutdown, frame, |event, _| {
                enqueued_tx.send((event.topic, event.key.clone())).unwrap();
                Ok(())
            });
        });
        (sender, enqueued)
    }

    /// The topic and key of every event that `commands` publish, in order.
    fn published(commands: &mut [OrderCommand]) -> Vec<(&'static str, String)> {
        let (sender, enqueued) =
            recording_sender(SENDER_CAPACITY, |event| Ok(event.payload.clone()));
        let handler = handler_with(sender);
        for cmd in commands {
            handler.handle_processed_command(cmd);
        }
        // Dropping the handler closes the channel and waits for the sender to drain it.
        drop(handler);
        enqueued.iter().collect()
    }

    #[test]
    fn events_reach_the_producer_in_engine_order_whatever_each_encode_costs() {
        let mut framed = 0u64;
        let (sender, enqueued) = recording_sender(4, move |event| {
            // Uneven framing cost, as with a registry round trip or a cache hit: under
            // one task per event, a cheap later event would overtake a costly earlier one.
            framed += 1;
            thread::sleep(Duration::from_micros(framed * 7_919 % 500));
            Ok(event.payload.clone())
        });
        let handler = handler_with(sender);

        let (base, quote) = (base_asset(MARKET_ID), quote_asset(MARKET_ID));
        let mut expected = Vec::new();
        for user_id in 1..=50u64 {
            let mut deposit = OrderCommand::deposit_funds(user_id, 1_000, quote);
            deposit.set_status(Status::Processed);
            deposit.timestamp = 2 * user_id;
            handler.handle_processed_command(&mut deposit);
            expected.push(("deposits", user_id.to_string()));
            expected.push(("balances", format!("{user_id}:{quote}")));

            let order_id = 1_000 + user_id;
            let mut order = OrderCommand::place_order(
                common::TimeInForce::Gtc,
                user_id,
                1_000,
                1,
                Side::Bid,
                MARKET_ID,
                user_id,
            );
            order.order_id = order_id;
            order.set_status(Status::Placed);
            order.timestamp = 2 * user_id + 1;
            handler.handle_processed_command(&mut order);
            expected.push(("balances", format!("{user_id}:{base}")));
            expected.push(("balances", format!("{user_id}:{quote}")));
            expected.push(("orders", order_id.to_string()));
        }
        // Dropping the handler closes the channel and waits for the sender to drain it.
        drop(handler);

        assert_eq!(enqueued.iter().collect::<Vec<_>>(), expected);
    }

    #[test]
    fn a_failed_encode_is_retried_in_place_and_later_events_wait_behind_it() {
        let (attempts_tx, attempts) = mpsc::channel();
        let mut failures_left = 3;
        let (sender, enqueued) = recording_sender(8, move |event| {
            attempts_tx.send(event.key.clone()).unwrap();
            if event.key == "1" && failures_left > 0 {
                failures_left -= 1;
                return Err(registry_down());
            }
            Ok(event.payload.clone())
        });
        for key in ["0", "1", "2", "3"] {
            sender.submit(outbound(key));
        }
        drop(sender);

        assert_eq!(
            attempts.iter().collect::<Vec<_>>(),
            ["0", "1", "1", "1", "1", "2", "3"],
            "event 1 is tried until it frames, and nothing behind it is tried meanwhile"
        );
        assert_eq!(
            enqueued.iter().map(|(_, key)| key).collect::<Vec<_>>(),
            ["0", "1", "2", "3"],
            "and no event is skipped"
        );
    }

    #[test]
    fn a_shutdown_gives_up_after_its_grace_and_counts_the_events_it_could_not_send() {
        const GRACE: Duration = Duration::from_millis(100);
        let (not_enqueued_tx, not_enqueued) = mpsc::channel();
        let shutdown = Shutdown::with_grace(GRACE);
        let sender = OrderedSender::spawn(8, shutdown.clone(), move |events| {
            let given_up = run_sender(events, &shutdown, |_| Err(registry_down()), |_, _| Ok(()));
            not_enqueued_tx.send(given_up).unwrap();
        });
        for key in ["0", "1", "2"] {
            sender.submit(outbound(key));
        }

        let started = Instant::now();
        let (dropped_tx, dropped) = mpsc::channel();
        thread::spawn(move || {
            drop(sender);
            dropped_tx.send(()).unwrap();
        });
        dropped
            .recv_timeout(GRACE + ENCODE_RETRY_MAX_BACKOFF + Duration::from_secs(1))
            .expect("the shutdown gives up soon after its grace");
        let took = started.elapsed();

        assert!(
            took >= GRACE,
            "it keeps trying through the grace ({took:?})"
        );
        assert_eq!(
            not_enqueued.recv().unwrap(),
            3,
            "the event being retried and the two behind it"
        );
    }

    #[test]
    fn a_shutdown_flush_is_bounded_and_counts_what_librdkafka_still_holds() {
        let shutdown = Shutdown::with_grace(Duration::from_millis(100));
        let producer = create_producer(&producer_config(NO_BROKER), &shutdown);
        enqueue(&producer, &outbound("discarded"), b"0", &shutdown).unwrap();
        shutdown.begin();
        // As a fatal error during the grace would: the event's report comes back, undelivered.
        producer.purge(PurgeConfig::default().queue());
        enqueue(&producer, &outbound("held"), b"1", &shutdown).unwrap();

        let started = Instant::now();
        let undelivered = flush_at_shutdown(&producer, &shutdown, 2, 2);

        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the flush ends with the grace"
        );
        assert_eq!(
            undelivered, 4,
            "two never enqueued, one discarded and one librdkafka still holds"
        );
        // Dropping the producer discards the held event, which a begun shutdown expects.
    }

    #[test]
    fn a_sender_that_overruns_its_grace_does_not_hold_up_shutdown() {
        let (release, released) = mpsc::channel::<()>();
        let sender = OrderedSender::spawn(1, Shutdown::with_grace(Duration::ZERO), move |_| {
            // Stuck where the grace does not reach, like librdkafka's teardown on a hung DNS lookup.
            let _ = released.recv();
        });

        let started = Instant::now();
        let (dropped_tx, dropped) = mpsc::channel();
        thread::spawn(move || {
            drop(sender);
            dropped_tx.send(started.elapsed()).unwrap();
        });
        let took = dropped
            .recv_timeout(SHUTDOWN_OVERRUN + Duration::from_secs(2))
            .expect("dropping the handler must not wait for a stuck sender");
        let _ = release.send(());

        assert!(
            took >= SHUTDOWN_OVERRUN,
            "it gives the sender its overrun first ({took:?})"
        );
    }

    #[test]
    fn a_panicking_sender_aborts_the_process_instead_of_dropping_what_it_holds() {
        const CHILD: &str = "VEX_PANICKING_KAFKA_SENDER_TEST_CHILD";
        const NAME: &str = "events::tests::a_panicking_sender_aborts_the_process_instead_of_dropping_what_it_holds";

        if std::env::var_os(CHILD).is_some() {
            let sender = OrderedSender::spawn(8, Shutdown::with_grace(SHUTDOWN_GRACE), |_| {
                panic!("sender failed")
            });
            drop(sender);
            unreachable!("a panicking sender must abort the process");
        }

        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([NAME, "--exact", "--nocapture"])
            .env(CHILD, "1")
            .current_dir(std::env::temp_dir())
            .output()
            .unwrap();

        assert!(!child.status.success(), "the child must not survive");
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(
                child.status.signal(),
                Some(6),
                "killed by SIGABRT, not failed as a test: {}",
                String::from_utf8_lossy(&child.stderr)
            );
        }
    }

    #[test]
    fn dropping_an_idle_handler_does_not_wait_out_the_grace() {
        let handler = KafkaEventsHandler::new(
            NO_BROKER,
            &format!("http://{NO_BROKER}"),
            Arc::new(Publications::new()),
            ReplayControl::disabled(),
        );

        let started = Instant::now();
        drop(handler);

        assert!(started.elapsed() < SHUTDOWN_GRACE / 4);
    }

    #[test]
    fn a_full_sender_blocks_the_events_stage_instead_of_dropping() {
        let (release, released) = mpsc::channel::<()>();
        let (enqueued_tx, enqueued) = mpsc::channel();
        let shutdown = Shutdown::with_grace(SHUTDOWN_GRACE);
        let sender = Arc::new(OrderedSender::spawn(2, shutdown.clone(), move |events| {
            // Stalled, like a broker that is down, until the test releases it.
            let _ = released.recv();
            run_sender(
                events,
                &shutdown,
                |event| Ok(event.payload.clone()),
                |event, _| {
                    enqueued_tx.send(event.key.clone()).unwrap();
                    Ok(())
                },
            );
        }));
        sender.submit(outbound("0"));
        sender.submit(outbound("1"));

        let (started_tx, started) = mpsc::channel();
        let (submitted_tx, submitted) = mpsc::channel();
        let submitter = {
            let sender = Arc::clone(&sender);
            thread::spawn(move || {
                started_tx.send(()).unwrap();
                sender.submit(outbound("2"));
                submitted_tx.send(()).unwrap();
            })
        };
        started.recv().unwrap();
        let blocked = submitted.recv_timeout(Duration::from_millis(200)).is_err();
        release.send(()).unwrap();
        assert!(
            blocked,
            "a full channel must block the submitter, not drop its event"
        );

        submitted
            .recv_timeout(Duration::from_secs(5))
            .expect("the submitter resumes once the sender drains");
        submitter.join().unwrap();
        drop(Arc::into_inner(sender).expect("the submitter has finished"));
        assert_eq!(enqueued.iter().collect::<Vec<_>>(), ["0", "1", "2"]);
    }

    #[test]
    fn producer_never_expires_an_event_and_is_idempotent() {
        let config = producer_config("localhost:9092");

        assert_eq!(
            config.get("message.timeout.ms"),
            Some("0"),
            "librdkafka must retry an undelivered event for as long as it takes"
        );
        assert_eq!(config.get("enable.idempotence"), Some("true"));
        assert_eq!(config.get("acks"), Some("all"));
        // librdkafka refuses settings that conflict with idempotence when the producer is created.
        let _producer = create_producer(&config, &Shutdown::with_grace(SHUTDOWN_GRACE));
    }

    /// A producer context that records what librdkafka reports, where [`DeliveryLog`] would act
    /// on it (and abort the test process).
    #[derive(Default)]
    struct Recorder {
        failed_deliveries: Mutex<Vec<KafkaError>>,
    }

    impl ClientContext for Recorder {}

    impl ProducerContext for Recorder {
        type DeliveryOpaque = ();

        fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
            if let Err((error, _)) = result {
                self.failed_deliveries.lock().unwrap().push(error.clone());
            }
        }
    }

    fn recording_producer(config: &ClientConfig) -> ThreadedProducer<Recorder> {
        config.create_with_context(Recorder::default()).unwrap()
    }

    #[test]
    fn a_full_producer_queue_holds_the_event_back_until_it_has_room() {
        let mut config = producer_config(NO_BROKER);
        config.set("queue.buffering.max.messages", "1");
        let producer = recording_producer(&config);
        let shutdown = Shutdown::with_grace(SHUTDOWN_GRACE);
        enqueue(&producer, &outbound("0"), b"0", &shutdown).unwrap();

        thread::scope(|scope| {
            let (done_tx, done) = mpsc::channel();
            let (producer, shutdown) = (&producer, &shutdown);
            scope.spawn(move || {
                enqueue(producer, &outbound("1"), b"1", shutdown).unwrap();
                done_tx.send(()).unwrap();
            });
            assert!(
                done.recv_timeout(Duration::from_millis(300)).is_err(),
                "a full queue must hold the event back, not drop it"
            );
            // A broker would make room by acknowledging the first event; without one, discard it.
            producer.purge(PurgeConfig::default().queue());
            done.recv_timeout(Duration::from_secs(5))
                .expect("the event is enqueued once the queue has room");
        });
        assert_eq!(producer.in_flight_count(), 1, "the second event is queued");

        let overdue = Shutdown::with_grace(Duration::ZERO);
        overdue.begin();
        assert!(
            enqueue(&producer, &outbound("2"), b"2", &overdue).is_err(),
            "an overdue shutdown gives up on a full queue"
        );
    }

    #[test]
    fn a_fatal_producer_error_aborts_instead_of_dropping_the_queue() {
        use rdkafka::bindings::{rd_kafka_resp_err_t as RespErr, rd_kafka_test_fatal_error};

        let producer = recording_producer(&producer_config(NO_BROKER));
        enqueue(
            &producer,
            &outbound("queued"),
            b"0",
            &Shutdown::with_grace(SHUTDOWN_GRACE),
        )
        .unwrap();
        // SAFETY: the pointer is the live producer's handle, and the reason is a C string literal.
        let triggered = unsafe {
            rd_kafka_test_fatal_error(
                producer.client().native_ptr(),
                RespErr::RD_KAFKA_RESP_ERR_OUT_OF_ORDER_SEQUENCE_NUMBER,
                c"test".as_ptr(),
            )
        };
        assert_eq!(triggered, RespErr::RD_KAFKA_RESP_ERR_NO_ERROR);

        // librdkafka reports it twice, and the verdict on each is to abort. The queued event is
        // purged, which before any shutdown means it is lost,
        let deadline = Instant::now() + Duration::from_secs(10);
        let purged = loop {
            let purged = producer.context().failed_deliveries.lock().unwrap().clone();
            if !purged.is_empty() {
                break purged;
            }
            assert!(Instant::now() < deadline, "a fatal error purges the queue");
            thread::sleep(Duration::from_millis(10));
        };
        assert_eq!(
            purged,
            [KafkaError::MessageProduction(RDKafkaErrorCode::PurgeQueue)]
        );
        assert_eq!(delivery_failure(&purged[0], false), Verdict::Abort);
        // and the next event is refused.
        let Err((refused, _)) = producer.send(BaseRecord::to("balances").key("next").payload("1"))
        else {
            panic!("a producer in a fatal state refuses every event");
        };
        assert_eq!(
            refused,
            KafkaError::MessageProduction(RDKafkaErrorCode::Fatal)
        );
        assert_eq!(send_failure(&refused), Verdict::Abort);

        for code in [
            RDKafkaErrorCode::QueueFull,
            RDKafkaErrorCode::UnknownTopic,
            RDKafkaErrorCode::UnknownPartition,
        ] {
            assert_eq!(
                send_failure(&KafkaError::MessageProduction(code)),
                Verdict::Retry,
                "only a full queue or a topic a broker misreports is waited out: {code:?}"
            );
        }
    }

    #[test]
    fn only_a_shutdown_may_discard_an_event_and_every_other_failed_delivery_aborts() {
        for code in [
            RDKafkaErrorCode::PurgeQueue,
            RDKafkaErrorCode::PurgeInflight,
        ] {
            let purged = KafkaError::MessageProduction(code);
            assert_eq!(
                delivery_failure(&purged, true),
                Verdict::Discarded,
                "a shutdown that gave up counts these"
            );
            assert_eq!(
                delivery_failure(&purged, false),
                Verdict::Abort,
                "before a shutdown only a fatal error purges"
            );
        }
        for code in [
            RDKafkaErrorCode::Fatal,
            RDKafkaErrorCode::TopicAuthorizationFailed,
            RDKafkaErrorCode::MessageSizeTooLarge,
            RDKafkaErrorCode::UnknownTopic,
            RDKafkaErrorCode::MessageTimedOut,
        ] {
            assert_eq!(
                delivery_failure(&KafkaError::MessageProduction(code), true),
                Verdict::Abort,
                "{code:?}"
            );
        }
    }

    #[test]
    fn a_stall_logs_its_first_failure_then_at_most_once_per_interval() {
        let mut stall = StallLog::default();

        assert!(stall.failed(), "the first failure is logged");
        assert!(!stall.failed(), "an immediate repeat is not");
        stall.last_logged = Some(Instant::now() - STALL_LOG_INTERVAL);
        assert!(stall.failed(), "one interval later, the next one is");
        assert_eq!(stall.failures, 3, "every failure is counted");
    }

    /// A Schema Registry stand-in that answers its requests with `replies`, (status, body), in
    /// order, repeating the last one.
    fn fake_registry(replies: &'static [(&'static str, &'static str)]) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        thread::spawn(move || {
            for (served, stream) in listener.incoming().enumerate() {
                let (status, body) = replies[served.min(replies.len() - 1)];
                let mut request = BufReader::new(stream.unwrap());
                // Read the whole request, or the client may see a reset instead of the reply.
                let mut length = 0;
                let mut line = String::new();
                while request.read_line(&mut line).unwrap() > 2 {
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap();
                    }
                    line.clear();
                }
                request.read_exact(&mut vec![0; length]).unwrap();
                write!(
                    request.get_mut(),
                    "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        url
    }

    const UNAVAILABLE: (&str, &str) = (
        "503 Service Unavailable",
        "<html>Service Unavailable</html>",
    );
    const REGISTERED: (&str, &str) = ("200 OK", r#"{"id":7}"#);

    /// One attempt to frame `event`, with a fresh encoder.
    fn frame_once(registry_url: &str, event: &OutboundEvent) -> Result<Vec<u8>, SRCError> {
        let encoder = schema_encoder(registry_url);
        Runtime::new()
            .unwrap()
            .block_on(frame_with_schema(&encoder, &trading_schema(), event))
    }

    #[test]
    fn a_registry_error_is_retried_because_a_rejected_schema_looks_like_an_outage() {
        let rejected = frame_once(
            &fake_registry(&[(
                "409 Conflict",
                r#"{"error_code":409,"message":"Schema being registered is incompatible"}"#,
            )]),
            &outbound("k"),
        )
        .unwrap_err();
        let failing = frame_once(
            &fake_registry(&[(
                "500 Internal Server Error",
                r#"{"error_code":50002,"message":"Register operation timed out"}"#,
            )]),
            &outbound("k"),
        )
        .unwrap_err();
        let unavailable = frame_once(&fake_registry(&[UNAVAILABLE]), &outbound("k")).unwrap_err();

        assert_eq!(
            rejected, failing,
            "schema_registry_converter reports a rejected schema exactly as a failing registry"
        );
        for error in [&failing, &unavailable] {
            assert!(
                !error.retriable,
                "precondition: the converter calls it non-retriable"
            );
            assert_eq!(
                encode_failure(error),
                Verdict::Retry,
                "an outage must stall intake, not abort: {error}"
            );
        }
    }

    #[test]
    fn a_registry_that_recovers_frames_the_event_on_the_next_attempt() {
        let encoder = schema_encoder(&fake_registry(&[UNAVAILABLE, REGISTERED]));
        let schema = trading_schema();
        let rt = Runtime::new().unwrap();

        let first = rt.block_on(frame_with_schema(&encoder, &schema, &outbound("k")));
        assert!(
            first.is_err_and(|error| error.cached),
            "the outage is cached"
        );
        let second = rt.block_on(frame_with_schema(&encoder, &schema, &outbound("k")));
        assert!(
            second.is_ok(),
            "the cached failure was evicted, so the retry reaches the registry: {second:?}"
        );
    }

    #[test]
    fn an_event_the_compiled_schema_cannot_frame_aborts() {
        let registry = fake_registry(&[REGISTERED]);
        let framed = frame_once(&registry, &outbound("k")).expect("a known message frames");
        assert_eq!(
            framed[..5],
            [0, 0, 0, 0, 7],
            "magic byte, then the schema id"
        );

        let mut unknown = outbound("k");
        unknown.message_name = "trading.NoSuchEvent";
        let error = frame_once(&registry, &unknown).unwrap_err();
        assert_eq!(encode_failure(&error), Verdict::Abort, "{error}");
    }

    #[test]
    fn a_registry_that_never_answers_times_out_so_the_event_is_retried() {
        // Accepts connections (the kernel completes the handshake) but never answers.
        let registry = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", registry.local_addr().unwrap());
        let (framed_tx, framed) = mpsc::channel();
        thread::spawn(move || {
            let _ = framed_tx.send(frame_once(&url, &outbound("k")));
        });

        let result = framed
            .recv_timeout(SCHEMA_REGISTRY_TIMEOUT * 5)
            .expect("the registry call must time out, not hang the sender");
        let error = result.expect_err("an unanswered registration fails the attempt");
        assert_eq!(encode_failure(&error), Verdict::Retry);
        drop(registry);
    }

    #[test]
    fn test_kafka_events_handler_placed_order() {
        let mut cmd = OrderCommand::place_order(
            common::TimeInForce::Gtc,
            1001, // user_id
            1000, // price
            100,  // size
            Side::Bid,
            MARKET_ID,
            12345, // client_order_id
        );
        cmd.order_id = 12345;
        cmd.set_status(Status::Placed);
        cmd.timestamp = 1000;

        let (base, quote) = (base_asset(MARKET_ID), quote_asset(MARKET_ID));
        assert_eq!(
            published(&mut [cmd]),
            [
                ("balances", format!("1001:{base}")),
                ("balances", format!("1001:{quote}")),
                ("orders", "12345".to_string()),
            ]
        );
    }

    #[test]
    fn test_kafka_events_handler_cancelled_order() {
        let mut cmd = OrderCommand::place_order(
            common::TimeInForce::Gtc,
            1002, // user_id
            950,  // price
            50,   // size
            Side::Ask,
            MARKET_ID,
            12346, // client_order_id
        );
        cmd.order_id = 12346;
        cmd.set_status(Status::Cancelled);
        cmd.timestamp = 1001;

        let (base, quote) = (base_asset(MARKET_ID), quote_asset(MARKET_ID));
        assert_eq!(
            published(&mut [cmd]),
            [
                ("orders", "12346".to_string()),
                ("balances", format!("1002:{base}")),
                ("balances", format!("1002:{quote}")),
                ("cancels", "12346".to_string()),
            ]
        );
    }

    #[test]
    fn test_kafka_events_handler_filled_order_with_trades() {
        // Create a processed command with Filled status and trade events
        let mut filled_cmd = OrderCommand::place_order(
            common::TimeInForce::Gtc,
            1004, // user_id
            1050, // price
            200,  // size
            Side::Bid,
            MARKET_ID,
            12348, // client_order_id
        );
        filled_cmd.order_id = 12348;
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
            maker_remaining_size: 0,
            maker_original_size: 50,
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
            maker_remaining_size: 50,
            maker_original_size: 200,
        };
        filled_cmd.attach_event(Box::new(trade1));

        // The first fill's maker is the taker itself: its balance comes from the command.
        let (base, quote) = (base_asset(MARKET_ID), quote_asset(MARKET_ID));
        assert_eq!(
            published(&mut [filled_cmd]),
            [
                ("trades", "12348:202".to_string()),
                ("orders", "202".to_string()),
                ("trades", "12348:201".to_string()),
                ("orders", "201".to_string()),
                ("balances", format!("1003:{base}")),
                ("balances", format!("1003:{quote}")),
                ("balances", format!("1004:{base}")),
                ("balances", format!("1004:{quote}")),
                ("orders", "12348".to_string()),
            ]
        );
    }

    #[test]
    fn test_kafka_events_handler_deposit_funds() {
        let mut cmd = OrderCommand::deposit_funds(
            1005, // user_id
            1000, // amount
            1,    // asset_id
        );
        cmd.set_status(Status::Processed);
        cmd.timestamp = 1004;

        assert_eq!(
            published(&mut [cmd]),
            [
                ("deposits", "1005".to_string()),
                ("balances", "1005:1".to_string()),
            ]
        );
    }

    #[test]
    fn test_kafka_events_handler_withdraw_funds() {
        let mut cmd = OrderCommand::withdraw_funds(
            1006, // user_id
            500,  // amount
            1,    // asset_id
        );
        cmd.set_status(Status::Processed);
        cmd.timestamp = 1005;

        assert_eq!(
            published(&mut [cmd]),
            [
                ("withdrawals", "1006".to_string()),
                ("balances", "1006:1".to_string()),
            ]
        );
    }

    /// `orderbook` is keyed by market, and a latest-row reader keeps the greatest timestamp. A
    /// wall-clock stamp would leave it serving the previous run's book after a restart onto a
    /// host whose clock is behind; the journaled command timestamp continues above that run.
    #[test]
    fn an_orderbook_event_carries_its_commands_timestamp() {
        let mut cmd = OrderCommand::place_order(
            common::TimeInForce::Gtc,
            1007, // user_id
            1000, // price
            100,  // size
            Side::Bid,
            MARKET_ID,
            12349, // client_order_id
        );
        cmd.order_id = 12349;
        cmd.set_status(Status::Placed);
        cmd.timestamp = 1006;
        let mut book = L2MarketData::new();
        book.bid_prices.push(1000);
        book.bid_volumes.push(100);
        cmd.l2_data = Some(book);

        let (orderbooks_tx, orderbooks) = mpsc::channel();
        // `_enqueued` stays alive: the recorder unwraps each send to it.
        let (sender, _enqueued) = recording_sender(SENDER_CAPACITY, move |event| {
            if event.topic == "orderbook" {
                orderbooks_tx
                    .send((event.key.clone(), event.payload.clone()))
                    .unwrap();
            }
            Ok(event.payload.clone())
        });
        let handler = handler_with(sender);
        handler.handle_processed_command(&mut cmd);
        // Dropping the handler closes the channel and waits for the sender to drain it.
        drop(handler);

        let orderbooks: Vec<_> = orderbooks.try_iter().collect();
        assert_eq!(orderbooks.len(), 1);
        let (key, payload) = &orderbooks[0];
        assert_eq!(key, &MARKET_ID.to_string());
        let event = trading_proto::OrderbookEvent::decode(payload.as_slice()).unwrap();
        assert_eq!(event.market_id, MARKET_ID);
        assert_eq!(event.timestamp, 1006, "the command's timestamp");
    }
}
