//! The topic check a consumer runs once, before its reconnect loop.
//!
//! A topic missing at startup is otherwise invisible: `UnknownTopicOrPartition`
//! maps to a retryable `Connection`, so the reconnect wrapper loops on it
//! silently for good, and a dead-letter publish into a topic that is not there
//! fails at runtime and settles the record as a lost discard. The check is a
//! metadata probe from a client with no `group.id` and no producer, so it
//! creates nothing and requests no producer id.
//!
//! What is required of the broker depends on who owns the topic. An
//! `external()` main topic is infra's, so a missing one is a misconfiguration,
//! as is the dead-letter topic of an `external()` topology. A shove-owned main
//! topic is not probed: another process may still declare it, and the consumer
//! waits for it. The dead-letter topic is probed wherever the path publishes
//! to it, owned or not.
//!
//! The registry declares its topology before it spawns a member and calls the
//! consumer through `run_declared`, which skips this check, so it runs once
//! per start and never per member or per autoscale.

use tokio_util::sync::CancellationToken;

use super::client::{KafkaClient, TopicProbe, external_topic_missing};
use super::consumer::run_with_reconnect;
use crate::error::{Result, ShoveError};
use crate::metrics;
use crate::topology::QueueTopology;

/// What a consumer path does with the dead-letter topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Path {
    /// Settles outcomes by publishing dead letters: the standard, batch and
    /// FIFO loops.
    Publishing,
    /// Settles every outcome without a dead-letter publish.
    Broadcast,
    /// Reads from the dead-letter topic and publishes nowhere.
    DeadLetterDrain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Main,
    DeadLetter,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Required<'a> {
    name: &'a str,
    role: Role,
    external: bool,
}

impl Required<'_> {
    fn missing(&self, queue: &str) -> ShoveError {
        let what = match self.role {
            Role::Main => "topic",
            Role::DeadLetter => "dead-letter topic",
        };
        if self.external {
            return external_topic_missing(what, self.name);
        }
        metrics::record_backend_error(
            metrics::BackendLabel::Kafka,
            metrics::BackendErrorKind::Topology,
        );
        ShoveError::Topology(format!(
            "dead-letter topic `{}` of `{queue}` does not exist, so a dead letter could not be \
             published to it: declare the topology before consuming, or let the producer \
             create it with `KafkaConfig::with_producer_auto_create_topics`",
            self.name
        ))
    }
}

/// The topics `path` cannot run without. `producer_creates` is whether the
/// client's producer creates a topic it publishes to, which makes a missing
/// shove-owned dead-letter topic no loss: the first publish creates it.
fn required_topics<'a>(
    topology: &'a QueueTopology,
    path: Path,
    producer_creates: bool,
) -> Vec<Required<'a>> {
    let external = topology.external();
    let main = Required {
        name: topology.queue(),
        role: Role::Main,
        external: true,
    };
    let dead_letter = |name| Required {
        name,
        role: Role::DeadLetter,
        external,
    };
    let mut required = Vec::new();
    match (path, topology.dlq()) {
        (Path::Publishing, dlq) => {
            if external {
                required.push(main);
            }
            if let Some(name) = dlq
                && (external || !producer_creates)
            {
                required.push(dead_letter(name));
            }
        }
        (Path::Broadcast, _) => {
            if external {
                required.push(main);
            }
        }
        (Path::DeadLetterDrain, Some(name)) => {
            if external {
                required.push(dead_letter(name));
            }
        }
        (Path::DeadLetterDrain, None) => {}
    }
    required
}

async fn check(client: &KafkaClient, required: &[Required<'_>], queue: &str) -> Result<()> {
    for topic in required {
        #[cfg(feature = "test-support")]
        startup_probe::PROBES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match client.probe_topic(topic.name).await? {
            TopicProbe::Present(_) => {}
            TopicProbe::Missing => return Err(topic.missing(queue)),
            // Nothing says the topic is missing, so it stays on the retry
            // path a consumer always had.
            TopicProbe::Failed(code) => {
                return Err(ShoveError::Connection(format!(
                    "metadata for topic {} returned {code:?}",
                    topic.name
                )));
            }
        }
    }
    Ok(())
}

/// Probe what `path` needs of `topology`, once.
///
/// A missing topic is `ShoveError::Topology` naming it. A probe that cannot
/// reach the broker is `Connection`, retried under the same backoff and
/// `max_reconnect_attempts` as the consumer itself, and ended by `shutdown`.
pub(super) async fn verify(
    client: &KafkaClient,
    topology: &QueueTopology,
    path: Path,
    shutdown: &CancellationToken,
    max_reconnect_attempts: Option<u32>,
) -> Result<()> {
    let queue = topology.queue();
    let required = required_topics(topology, path, client.producer_creates_topics());
    if required.is_empty() {
        return Ok(());
    }
    let required = required.as_slice();
    // A probe blocks for up to the metadata timeout and `run_with_reconnect`
    // looks at `shutdown` only between attempts, so a stop races the probe.
    tokio::select! {
        ended = run_with_reconnect(shutdown, queue, max_reconnect_attempts, || async move {
            check(client, required, queue).await
        }) => ended,
        _ = shutdown.cancelled() => Ok(()),
    }
}

/// Test-only seam (see the `test-support` feature): how many topics the
/// startup check has probed in this process.
#[cfg(feature = "test-support")]
pub mod startup_probe {
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub(super) static PROBES: AtomicUsize = AtomicUsize::new(0);

    pub fn probes() -> usize {
        PROBES.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::TopologyBuilder;

    fn names<'a>(required: &[Required<'a>]) -> Vec<(&'a str, Role)> {
        required.iter().map(|r| (r.name, r.role)).collect()
    }

    #[test]
    fn an_owned_topic_without_a_dlq_needs_nothing() {
        let topology = TopologyBuilder::new("orders").build();
        for path in [Path::Publishing, Path::Broadcast, Path::DeadLetterDrain] {
            assert!(required_topics(&topology, path, false).is_empty());
        }
    }

    #[test]
    fn a_publishing_path_needs_the_external_topic_and_the_dlq() {
        let topology = TopologyBuilder::new("orders").external().dlq().build();
        assert_eq!(
            names(&required_topics(&topology, Path::Publishing, false)),
            [("orders", Role::Main), ("orders-dlq", Role::DeadLetter)]
        );
    }

    #[test]
    fn a_publishing_path_needs_a_named_dlq_by_its_name() {
        let topology = TopologyBuilder::new("orders")
            .dlq_named("infra-dead-letters")
            .build();
        assert_eq!(
            names(&required_topics(&topology, Path::Publishing, false)),
            [("infra-dead-letters", Role::DeadLetter)]
        );
    }

    #[test]
    fn an_owned_main_topic_is_never_required() {
        let topology = TopologyBuilder::new("orders").dlq().build();
        assert_eq!(
            names(&required_topics(&topology, Path::Publishing, false)),
            [("orders-dlq", Role::DeadLetter)]
        );
    }

    #[test]
    fn a_producer_that_creates_topics_makes_an_owned_dlq_optional_but_not_an_external_one() {
        let owned = TopologyBuilder::new("orders").dlq().build();
        assert!(required_topics(&owned, Path::Publishing, true).is_empty());
        let external = TopologyBuilder::new("orders").external().dlq().build();
        assert_eq!(
            names(&required_topics(&external, Path::Publishing, true)),
            [("orders", Role::Main), ("orders-dlq", Role::DeadLetter)]
        );
    }

    #[test]
    fn a_broadcast_path_needs_only_the_external_main_topic() {
        let topology = TopologyBuilder::new("fanout")
            .external()
            .broadcast()
            .build();
        assert_eq!(
            names(&required_topics(&topology, Path::Broadcast, false)),
            [("fanout", Role::Main)]
        );
        let owned = TopologyBuilder::new("fanout").broadcast().build();
        assert!(required_topics(&owned, Path::Broadcast, false).is_empty());
    }

    #[test]
    fn the_drain_needs_the_dlq_only_when_infra_owns_it() {
        let external = TopologyBuilder::new("orders").external().dlq().build();
        assert_eq!(
            names(&required_topics(&external, Path::DeadLetterDrain, false)),
            [("orders-dlq", Role::DeadLetter)]
        );
        let owned = TopologyBuilder::new("orders").dlq().build();
        assert!(required_topics(&owned, Path::DeadLetterDrain, false).is_empty());
    }

    #[test]
    fn a_missing_owned_dlq_names_the_topic_and_how_to_fix_it() {
        let topology = TopologyBuilder::new("orders").dlq().build();
        let required = required_topics(&topology, Path::Publishing, false);
        let message = required[0].missing("orders").to_string();
        assert!(message.contains("`orders-dlq`"), "{message}");
        assert!(message.contains("declare the topology"), "{message}");
    }
}
