//! Applies `deploy/kafka/topics.toml`, the source of truth for the Kafka topics vex-core produces
//! and the Schema Registry compatibility of their values.
//!
//! The file is compiled in, so a binary always applies the file of the revision it was built from.

use processors::events::{DELIVERED_TOPIC, EVENT_TOPICS};
use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, NewTopic, ResourceSpecifier, TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::Duration;

/// Where the file lives, from the repository root.
pub const TOPICS_FILE: &str = "deploy/kafka/topics.toml";

const TOPICS: &str = include_str!("../../deploy/kafka/topics.toml");

/// Bound on each admin request.
const TIMEOUT: Duration = Duration::from_secs(30);

/// `deploy/kafka/topics.toml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaTopics {
    replication_factor: i32,
    event_topics: EventTopics,
    delivered_topic: DeliveredTopic,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventTopics {
    partitions: BTreeMap<String, i32>,
    config: BTreeMap<String, String>,
    subject_compatibility: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeliveredTopic {
    name: String,
    partitions: i32,
    config: BTreeMap<String, String>,
}

/// One topic, as a cluster must have it.
#[derive(Debug, Clone, PartialEq)]
pub struct Topic {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i32,
    pub config: BTreeMap<String, String>,
}

impl KafkaTopics {
    /// The compiled-in file. Fails unless it declares exactly the topics the core publishes to.
    pub fn load() -> Result<Self, String> {
        Self::parse(TOPICS)
    }

    fn parse(text: &str) -> Result<Self, String> {
        let topics: Self =
            toml::from_str(text).map_err(|error| format!("{TOPICS_FILE}: {error}"))?;
        let mut declared: Vec<&str> = topics
            .event_topics
            .partitions
            .keys()
            .map(String::as_str)
            .collect();
        let mut published = EVENT_TOPICS.to_vec();
        declared.sort_unstable();
        published.sort_unstable();
        if declared != published {
            return Err(format!(
                "{TOPICS_FILE} declares the event topics {declared:?}; the core publishes to \
                 {published:?}"
            ));
        }
        if topics.delivered_topic.name != DELIVERED_TOPIC {
            return Err(format!(
                "{TOPICS_FILE} declares the delivered topic {}; the core uses {DELIVERED_TOPIC}",
                topics.delivered_topic.name
            ));
        }
        Ok(topics)
    }

    /// Every topic, event topics first. `single_broker`, for a development or test cluster of
    /// one broker, lowers the replication factor and `min.insync.replicas` to 1; the core refuses
    /// such event topics in production.
    pub fn topics(&self, single_broker: bool) -> Vec<Topic> {
        let events = self
            .event_topics
            .partitions
            .iter()
            .map(|(name, partitions)| (name, *partitions, &self.event_topics.config));
        let delivered = &self.delivered_topic;
        events
            .chain([(&delivered.name, delivered.partitions, &delivered.config)])
            .map(|(name, partitions, config)| {
                let mut config = config.clone();
                let mut replication_factor = self.replication_factor;
                if single_broker {
                    replication_factor = 1;
                    config.insert("min.insync.replicas".to_string(), "1".to_string());
                }
                Topic {
                    name: name.clone(),
                    partitions,
                    replication_factor,
                    config,
                }
            })
            .collect()
    }

    /// Each event topic's `<topic>-value` subject, with the compatibility it must have.
    pub fn subjects(&self) -> Vec<(String, &str)> {
        self.event_topics
            .partitions
            .keys()
            .map(|topic| {
                (
                    format!("{topic}-value"),
                    self.event_topics.subject_compatibility.as_str(),
                )
            })
            .collect()
    }
}

/// Makes the cluster at `brokers`, and the registry at `schema_registry_url`, match `topics`:
/// creates each missing topic; sets each existing topic's configuration to exactly the declared
/// one, so anything undeclared reverts to the broker's default; and sets each subject's
/// compatibility. Applying it twice changes nothing.
///
/// Refuses an existing topic whose partition count or replication factor differs from the file:
/// changing either is a ledger reset (RUNBOOK.md).
pub async fn apply(
    topics: &[Topic],
    subjects: &[(String, &str)],
    brokers: &str,
    schema_registry_url: &str,
) -> Result<(), String> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .map_err(|error| format!("cannot create a Kafka admin client: {error}"))?;
    let options = AdminOptions::new()
        .request_timeout(Some(TIMEOUT))
        .operation_timeout(Some(TIMEOUT));
    let metadata = admin
        .inner()
        .fetch_metadata(None, TIMEOUT)
        .map_err(|error| format!("cannot read the topics at {brokers}: {error}"))?;

    let mut missing = Vec::new();
    let mut existing = Vec::new();
    for topic in topics {
        let Some(found) = metadata.topics().iter().find(|t| t.name() == topic.name) else {
            missing.push(topic);
            continue;
        };
        let partitions = found.partitions().len();
        let replication_factor = found
            .partitions()
            .first()
            .map_or(0, |partition| partition.replicas().len());
        if (partitions, replication_factor)
            != (topic.partitions as usize, topic.replication_factor as usize)
        {
            return Err(format!(
                "topic {} has {partitions} partitions with replication factor \
                 {replication_factor}, where {TOPICS_FILE} declares {} with {}; changing either \
                 is a ledger reset (RUNBOOK.md, \"Resetting the ledger\")",
                topic.name, topic.partitions, topic.replication_factor
            ));
        }
        existing.push(topic);
    }

    let new_topics: Vec<NewTopic<'_>> = missing
        .iter()
        .map(|topic| {
            topic.config.iter().fold(
                NewTopic::new(
                    &topic.name,
                    topic.partitions,
                    TopicReplication::Fixed(topic.replication_factor),
                ),
                |new, (key, value)| new.set(key, value),
            )
        })
        .collect();
    if !new_topics.is_empty() {
        let created = admin
            .create_topics(&new_topics, &options)
            .await
            .map_err(|error| format!("cannot create topics: {error}"))?;
        for result in created {
            let name =
                result.map_err(|(name, code)| format!("cannot create topic {name}: {code}"))?;
            println!("Created topic {name}");
        }
    }

    let configs: Vec<AlterConfig<'_>> = existing
        .iter()
        .map(|topic| {
            topic.config.iter().fold(
                AlterConfig::new(ResourceSpecifier::Topic(&topic.name)),
                |alter, (key, value)| alter.set(key, value),
            )
        })
        .collect();
    if !configs.is_empty() {
        let altered = admin
            .alter_configs(&configs, &options)
            .await
            .map_err(|error| format!("cannot configure topics: {error}"))?;
        for result in altered {
            let topic =
                result.map_err(|(topic, code)| format!("cannot configure {topic:?}: {code}"))?;
            println!("Configured {topic:?}");
        }
    }

    let registry = reqwest::Client::new();
    for (subject, compatibility) in subjects {
        let url = format!(
            "{}/config/{subject}",
            schema_registry_url.trim_end_matches('/')
        );
        let response = registry
            .put(&url)
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .json(&serde_json::json!({ "compatibility": compatibility }))
            .send()
            .await
            .map_err(|error| format!("PUT {url}: {error}"))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(format!("PUT {url}: {status} {body}"));
        }
        println!("Set subject {subject} to {compatibility}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_file_declares_exactly_the_core_topics_with_the_settings_no_loss_rests_on() {
        let file = KafkaTopics::load().unwrap();
        let topics = file.topics(false);

        assert_eq!(topics.len(), EVENT_TOPICS.len() + 1);
        for topic in &topics {
            assert_eq!(topic.replication_factor, 3, "{}", topic.name);
            assert_eq!(topic.config["min.insync.replicas"], "2", "{}", topic.name);
        }
        let delivered = topics.last().unwrap();
        assert_eq!(delivered.name, DELIVERED_TOPIC);
        assert_eq!(delivered.partitions, 1);
        assert_eq!(delivered.config["cleanup.policy"], "compact");
        for event in &topics[..EVENT_TOPICS.len()] {
            assert!(EVENT_TOPICS.contains(&event.name.as_str()));
            assert_eq!(event.config["cleanup.policy"], "delete");
        }
        assert_eq!(
            file.subjects(),
            [
                "balances-value",
                "cancels-value",
                "deposits-value",
                "orderbook-value",
                "orders-value",
                "trades-value",
                "withdrawals-value"
            ]
            .map(|subject| (subject.to_string(), "BACKWARD_TRANSITIVE"))
        );
    }

    #[test]
    fn a_single_broker_cluster_gets_one_replica_and_nothing_else_changes() {
        let file = KafkaTopics::load().unwrap();

        for (single, full) in file.topics(true).into_iter().zip(file.topics(false)) {
            assert_eq!(single.replication_factor, 1);
            assert_eq!(single.config["min.insync.replicas"], "1");
            let mut config = single.config.clone();
            config.insert("min.insync.replicas".into(), "2".into());
            assert_eq!(
                Topic {
                    replication_factor: 3,
                    config,
                    ..single
                },
                full
            );
        }
    }

    #[test]
    fn a_file_that_disagrees_with_the_core_is_refused() {
        let renamed = TOPICS.replace("trades = 12", "fills = 12");
        let error = KafkaTopics::parse(&renamed).unwrap_err();
        assert!(error.contains("fills"), "{error}");

        let renamed = TOPICS.replace("name = \"vex-core.delivered\"", "name = \"delivered\"");
        let error = KafkaTopics::parse(&renamed).unwrap_err();
        assert!(error.contains(DELIVERED_TOPIC), "{error}");

        let unknown = format!("{TOPICS}\nretention = 1\n");
        assert!(
            KafkaTopics::parse(&unknown).is_err(),
            "unknown keys refused"
        );
    }
}
