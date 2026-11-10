# Kafka topics

`topics.toml` declares the Kafka topics vex-core produces and the Schema Registry compatibility of
their values. It is the one source of truth for every environment: nothing else creates the core's
topics, and brokers run with `auto.create.topics.enable=false`, so a topic deleted while the core
runs stalls it instead of coming back with default settings.

1. [What it declares](#what-it-declares)
2. [Applying it](#applying-it)
3. [What the core checks at start](#what-the-core-checks-at-start)
4. [ACLs](#acls)
5. [Changing it](#changing-it)

## What it declares

| Topics | Partitions | Settings |
|---|---|---|
| `balances`, `orders`, `trades` | 12 | replication factor 3, `min.insync.replicas=2`, `cleanup.policy=delete`, 7-day retention |
| `orderbook`, `cancels` | 6 | the same |
| `deposits`, `withdrawals` | 3 | the same |
| `vex-core.delivered` | 1 | replication factor 3, `min.insync.replicas=2`, `cleanup.policy=compact` |

- **The event topics** carry the events of `processors/src/events.rs` (`EVENT_TOPICS`; the
  contract is on `KafkaEventsHandler`). `min.insync.replicas=2` is what makes the producer's
  `acks=all` durable. Retention is how long a sink can be down and still catch up from Kafka.
- **`vex-core.delivered`** holds the delivered watermark: the timestamp of the last command whose
  events the brokers have all acknowledged, under the key `w`. The core writes it as delivery
  advances and reads it at every start; its replay re-publishes every command above it.
- **Each `<topic>-value` subject** is `BACKWARD_TRANSITIVE`. Consumers decode with the writer's
  schema, by id, so forward compatibility buys nothing, and `FULL` or `FORWARD` would reject a new
  top-level message in `trading.proto`.

Kafka Connect's own internal topics are not here: Connect creates them, with the replication
factor its worker configuration sets (vex-gateway `readmodel/`).

## Applying it

```bash
cargo xtask kafka apply --brokers <bootstrap servers> --schema-registry <url>
```

- It creates each missing topic, sets each topic's configuration to exactly the declared one
  (anything undeclared reverts to the broker's default), and sets each subject's compatibility.
  Applying it twice changes nothing.
- It refuses a topic whose partition count or replication factor differs from the file; see
  [Changing it](#changing-it).
- `--single-broker` is for a development or test cluster of one broker: replication factor and
  `min.insync.replicas` 1. The core refuses such event topics in production.
- The file is compiled into xtask, so a binary applies the file of the revision it was built
  from.

**A deploy job** runs it first when it brings up an environment, before Kafka Connect and the core
start, and again on every deploy of a new core revision, where it is a no-op unless the file
changed. It needs the revision's source and a Rust toolchain (or an `xtask` binary built from that
revision), and network access to the brokers and the registry. The brokers and the registry are
reached in plaintext, as the core reaches them.

The xtask test stacks (`xtask/tests/docker-compose.yml`) apply it the same way, with
`--single-broker`, before vex-server starts.

## What the core checks at start

With journaling configured, the core describes its topics before the pipeline starts, then reads
the watermark. It refuses to start, naming each topic at fault, if:

- any of the eight topics is missing, or the core may not describe it: Kafka leaves such a topic
  out of its listing (see [ACLs](#acls));
- `vex-core.delivered` does not have exactly one partition (the last record must be the latest
  watermark), or is not `cleanup.policy=compact` (retention must never delete the watermark);
- in production, an event topic's `min.insync.replicas` is below 2, or not reported;
- Kafka answers for a topic with an error that waiting cannot clear, such as a denied
  authorization (see [ACLs](#acls)) or an invalid topic, or describes none of a topic's settings
  (the core may not DescribeConfigs it);
- Kafka denies the core the read of the watermark (no Read on `vex-core.delivered`).

While Kafka does not answer, or reports a topic in transition (a leader election, a broker
restart, metadata not yet on every broker), the core waits, logging `waiting_for_kafka` every
10 s; see `RUNBOOK.md`, "Kafka is Down".

## ACLs

If the cluster enables ACLs:

- **The core:** Write and Describe on the seven event topics and `vex-core.delivered`;
  DescribeConfigs on all eight (the startup check); Read on `vex-core.delivered`, and Describe on
  the consumer group `vex-core.watermark-reader`: it reads the watermark with an explicit
  assignment, so it never joins that group and commits nothing, but librdkafka looks up the
  group's coordinator. Brokers older than Kafka 2.8 also need IdempotentWrite on the cluster for
  its idempotent producer.
- **Kafka Connect:** Read on the seven event topics; Read on its consumer groups
  (`connect-<connector>`); Read, Write, Create and Describe on its internal topics
  (`connect-offsets`, `connect-configs`, `connect-status`) and its group.
- **The applier:** Create, Describe, DescribeConfigs and AlterConfigs on the eight topics.

## Changing it

- **A setting** (retention, say): edit the file and apply it.
- **A partition count or a replication factor** cannot be changed in place. Adding partitions
  moves keys, so a key's later events could land in a different partition from its earlier ones.
  It is a ledger reset (`RUNBOOK.md`, "Resetting the ledger").
- **A new event topic** comes with the core change that publishes to it, in the same PR: xtask
  refuses a file that does not declare exactly the topics the core publishes to, and the core
  refuses to start until every one of them exists.
