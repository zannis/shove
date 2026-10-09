//! A consumer checks at startup that the topics it depends on exist, proved
//! against librdkafka's mock cluster (`rdkafka::mocking`), which needs no
//! Docker.
//!
//! A missing topic that the consumer cannot work without must end its start
//! with `ShoveError::Topology` naming the topic, before any record is
//! consumed, instead of looping silently in the reconnect wrapper: the main
//! topic of an `external()` topology, and the dead-letter topic of any
//! topology the consumer publishes to. A broker that cannot be reached must
//! not become a `Topology` error: that stays the retry path it always was.
//! A shove-owned main topic is never probed, because another process may
//! still declare it.

#![cfg(all(feature = "kafka", feature = "test-support"))]

use std::future::Future;
use std::time::Duration;

use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use serde::{Deserialize, Serialize};
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::consumer_group::ConsumerGroupConfig;
use shove::error::Result;
use shove::handler::{BatchMessageHandler, MessageHandler};
use shove::kafka::{
    BatchConsumerOptions, KafkaClient, KafkaConfig, KafkaConsumer, KafkaConsumerGroupConfig,
    startup_probe,
};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topic::Topic;
use shove::topology::{SequenceFailure, TopologyBuilder};
use shove::{SequencedTopic as _, ShoveError, SupervisorOutcome};
use tokio_util::sync::CancellationToken;

/// A startup check on a mock cluster answers in well under this; a consumer
/// that is still running past it is retrying silently.
const STARTUP_BOUND: Duration = Duration::from_secs(20);
/// Long enough for a consumer that did not refuse to be past its startup.
const STILL_RUNNING_AFTER: Duration = Duration::from_secs(4);
/// Two probes against a broker that never answers, each waiting out the
/// metadata timeout, and the backoff between them.
const UNREACHABLE_BOUND: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Order {
    id: String,
}

shove::define_topic!(
    ExternalMissing,
    Order,
    TopologyBuilder::new("startup-external-missing")
        .external()
        .build()
);

shove::define_topic!(
    ExternalDlqMissing,
    Order,
    TopologyBuilder::new("startup-external-dlq-missing")
        .external()
        .hold_queue(Duration::from_millis(200))
        .dlq()
        .build()
);

shove::define_topic!(
    OwnedDlqMissing,
    Order,
    TopologyBuilder::new("startup-owned-dlq-missing")
        .hold_queue(Duration::from_millis(200))
        .dlq()
        .build()
);

shove::define_topic!(
    OwnedMissing,
    Order,
    TopologyBuilder::new("startup-owned-missing").build()
);

shove::define_topic!(
    ExternalBroadcastMissing,
    Order,
    TopologyBuilder::new("startup-external-broadcast-missing")
        .external()
        .broadcast()
        .build()
);

shove::define_topic!(
    ExternalDlqNamedMissing,
    Order,
    TopologyBuilder::new("startup-external-named")
        .external()
        .hold_queue(Duration::from_millis(200))
        .dlq_named("startup-infra-dead-letters")
        .build()
);

shove::define_sequenced_topic!(
    FifoDlqMissing,
    Order,
    |msg: &Order| msg.id.clone(),
    TopologyBuilder::new("startup-fifo-dlq-missing")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .hold_queue(Duration::from_millis(200))
        .dlq()
        .build()
);

/// Acknowledges everything it is handed.
#[derive(Clone)]
struct Noop;

impl<T: Topic<Message = Order>> MessageHandler<T> for Noop {
    type Context = ();
    async fn handle(&self, _msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
}

impl<T: Topic<Message = Order>> BatchMessageHandler<T> for Noop {
    type Context = ();
    async fn handle_batch(&self, _messages: Vec<(Order, MessageMetadata)>, _: &()) -> Outcome {
        Outcome::Ack
    }
}

fn mock_with(topics: &[&str]) -> MockCluster<'static, DefaultProducerContext> {
    let mock = MockCluster::new(1).expect("mock cluster");
    for topic in topics {
        mock.create_topic(topic, 1, 1).expect("mock topic");
    }
    mock
}

async fn connect(bootstrap: &str) -> KafkaClient {
    connect_with(KafkaConfig::new(bootstrap)).await
}

async fn connect_with(config: KafkaConfig) -> KafkaClient {
    KafkaClient::connect_with_retry(&config, 10)
        .await
        .expect("connect to the mock cluster")
}

/// Runs `start` to its end within `STARTUP_BOUND` and returns its error,
/// which must be `Topology` and must name `named`.
async fn refused_at_startup(start: impl Future<Output = Result<()>>, named: &str) {
    let ended = tokio::time::timeout(STARTUP_BOUND, start)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "still running after {STARTUP_BOUND:?}: the consumer is retrying a missing \
                 topic instead of refusing to start"
            )
        });
    match ended {
        Err(ShoveError::Topology(message)) => assert!(
            message.contains(named),
            "the refusal must name `{named}`: {message}"
        ),
        other => panic!("expected a Topology error naming `{named}`, got {other:?}"),
    }
}

/// Starts `start` and asserts it is still running after `STILL_RUNNING_AFTER`,
/// then cancels `shutdown` and asserts it ends cleanly.
async fn keeps_running_until_shutdown(
    start: impl Future<Output = Result<()>> + Send + 'static,
    shutdown: CancellationToken,
) {
    let mut running = tokio::spawn(start);
    tokio::select! {
        ended = &mut running => panic!("ended before shutdown: {ended:?}"),
        _ = tokio::time::sleep(STILL_RUNNING_AFTER) => {}
    }
    shutdown.cancel();
    tokio::time::timeout(STARTUP_BOUND, running)
        .await
        .expect("the consumer stops on shutdown")
        .expect("consumer task joins")
        .expect("the consumer ends cleanly on shutdown");
}

#[tokio::test]
async fn direct_run_refuses_a_missing_external_topic() {
    let mock = mock_with(&[]);
    let client = connect(&mock.bootstrap_servers()).await;
    refused_at_startup(
        KafkaConsumer::new(client).run::<ExternalMissing, _>(
            Noop,
            (),
            ConsumerOptions::<Kafka>::new(),
        ),
        "startup-external-missing",
    )
    .await;
}

#[tokio::test]
async fn direct_run_refuses_a_missing_external_dead_letter_topic() {
    let mock = mock_with(&["startup-external-dlq-missing"]);
    let client = connect(&mock.bootstrap_servers()).await;
    refused_at_startup(
        KafkaConsumer::new(client).run::<ExternalDlqMissing, _>(
            Noop,
            (),
            ConsumerOptions::<Kafka>::new(),
        ),
        "startup-external-dlq-missing-dlq",
    )
    .await;
}

#[tokio::test]
async fn direct_run_refuses_a_missing_named_external_dead_letter_topic() {
    let mock = mock_with(&["startup-external-named"]);
    let client = connect(&mock.bootstrap_servers()).await;
    refused_at_startup(
        KafkaConsumer::new(client).run::<ExternalDlqNamedMissing, _>(
            Noop,
            (),
            ConsumerOptions::<Kafka>::new(),
        ),
        "startup-infra-dead-letters",
    )
    .await;
}

#[tokio::test]
async fn direct_run_refuses_a_missing_owned_dead_letter_topic() {
    let mock = mock_with(&["startup-owned-dlq-missing"]);
    let client = connect(&mock.bootstrap_servers()).await;
    refused_at_startup(
        KafkaConsumer::new(client).run::<OwnedDlqMissing, _>(
            Noop,
            (),
            ConsumerOptions::<Kafka>::new(),
        ),
        "startup-owned-dlq-missing-dlq",
    )
    .await;
}

/// A consumer that finds its dead-letter topic present starts as before.
#[tokio::test]
async fn direct_run_starts_when_the_dead_letter_topic_exists() {
    let mock = mock_with(&["startup-owned-dlq-missing", "startup-owned-dlq-missing-dlq"]);
    let client = connect(&mock.bootstrap_servers()).await;
    let shutdown = CancellationToken::new();
    let options = ConsumerOptions::<Kafka>::new().with_shutdown(shutdown.clone());
    keeps_running_until_shutdown(
        async move {
            KafkaConsumer::new(client)
                .run::<OwnedDlqMissing, _>(Noop, (), options)
                .await
        },
        shutdown,
    )
    .await;
}

/// A shove-owned main topic is another process's to declare later, so a
/// consumer that finds it missing keeps waiting for it.
#[tokio::test]
async fn direct_run_does_not_probe_an_owned_main_topic() {
    let mock = mock_with(&[]);
    let client = connect(&mock.bootstrap_servers()).await;
    let shutdown = CancellationToken::new();
    let options = ConsumerOptions::<Kafka>::new().with_shutdown(shutdown.clone());
    keeps_running_until_shutdown(
        async move {
            KafkaConsumer::new(client)
                .run::<OwnedMissing, _>(Noop, (), options)
                .await
        },
        shutdown,
    )
    .await;
}

/// With producer auto-creation on, the first dead-letter publish creates a
/// missing shove-owned dead-letter topic, so its absence at startup is not a
/// loss.
#[tokio::test]
async fn direct_run_accepts_a_missing_owned_dead_letter_topic_when_the_producer_creates_topics() {
    let mock = mock_with(&["startup-owned-dlq-missing"]);
    let client = connect_with(
        KafkaConfig::new(mock.bootstrap_servers()).with_producer_auto_create_topics(true),
    )
    .await;
    let shutdown = CancellationToken::new();
    let options = ConsumerOptions::<Kafka>::new().with_shutdown(shutdown.clone());
    keeps_running_until_shutdown(
        async move {
            KafkaConsumer::new(client)
                .run::<OwnedDlqMissing, _>(Noop, (), options)
                .await
        },
        shutdown,
    )
    .await;
}

#[tokio::test]
async fn batch_run_refuses_a_missing_external_topic() {
    let mock = mock_with(&[]);
    let client = connect(&mock.bootstrap_servers()).await;
    refused_at_startup(
        KafkaConsumer::new(client).run_batch::<ExternalMissing, _>(
            Noop,
            (),
            BatchConsumerOptions::new(),
        ),
        "startup-external-missing",
    )
    .await;
}

#[tokio::test]
async fn batch_run_refuses_a_missing_owned_dead_letter_topic() {
    let mock = mock_with(&["startup-owned-dlq-missing"]);
    let client = connect(&mock.bootstrap_servers()).await;
    refused_at_startup(
        KafkaConsumer::new(client).run_batch::<OwnedDlqMissing, _>(
            Noop,
            (),
            BatchConsumerOptions::new(),
        ),
        "startup-owned-dlq-missing-dlq",
    )
    .await;
}

#[tokio::test]
async fn fifo_run_refuses_a_missing_dead_letter_topic() {
    let mock = mock_with(&["startup-fifo-dlq-missing"]);
    let client = connect(&mock.bootstrap_servers()).await;
    refused_at_startup(
        KafkaConsumer::new(client).run_fifo::<FifoDlqMissing, _>(
            Noop,
            (),
            ConsumerOptions::<Kafka>::new(),
        ),
        "startup-fifo-dlq-missing-dlq",
    )
    .await;
}

#[tokio::test]
async fn dead_letter_drain_refuses_a_missing_external_dead_letter_topic() {
    let mock = mock_with(&["startup-external-dlq-missing"]);
    let client = connect(&mock.bootstrap_servers()).await;
    refused_at_startup(
        KafkaConsumer::new(client).run_dlq::<ExternalDlqMissing, _>(Noop, ()),
        "startup-external-dlq-missing-dlq",
    )
    .await;
}

/// The drain reads from the dead-letter topic and publishes nowhere. On a
/// shove-owned topology that topic may still be declared later, as the main
/// topic may, so the drain waits for it.
#[tokio::test]
async fn dead_letter_drain_does_not_probe_an_owned_dead_letter_topic() {
    let mock = mock_with(&[]);
    let client = connect(&mock.bootstrap_servers()).await;
    let shutdown = client.shutdown_token();
    keeps_running_until_shutdown(
        async move {
            KafkaConsumer::new(client)
                .run_dlq::<OwnedDlqMissing, _>(Noop, ())
                .await
        },
        shutdown,
    )
    .await;
}

/// A broadcast subscription settles every outcome without a dead-letter
/// publish, so only the external main topic is probed. The subscription is a
/// task, so the refusal surfaces as the task's error at the drain.
#[tokio::test]
async fn broadcast_subscription_fails_on_a_missing_external_topic() {
    let mock = mock_with(&[]);
    let broker = Broker::<Kafka>::from_client(connect(&mock.bootstrap_servers()).await);
    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<ExternalBroadcastMissing, _>(Noop, ConsumerOptions::new())
        .expect("subscribe is refused only for what it can see synchronously");
    let outcome: SupervisorOutcome = subscriber
        .run_until_timeout(tokio::time::sleep(STILL_RUNNING_AFTER), STARTUP_BOUND)
        .await;
    assert_eq!(
        (outcome.errors, outcome.panics, outcome.timed_out),
        (1, 0, false),
        "the subscription must end with an error of its own: {outcome:?}"
    );
}

/// The supervisor runs each registered consumer as a task, so a refusal at
/// startup is that task's error at the drain.
#[tokio::test]
async fn supervisor_member_fails_on_a_missing_external_topic() {
    let mock = mock_with(&[]);
    let broker = Broker::<Kafka>::from_client(connect(&mock.bootstrap_servers()).await);
    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<ExternalMissing, _>(Noop, ConsumerOptions::new())
        .expect("register");
    let outcome = supervisor
        .run_until_timeout(tokio::time::sleep(STILL_RUNNING_AFTER), STARTUP_BOUND)
        .await;
    assert_eq!(
        (outcome.errors, outcome.panics, outcome.timed_out),
        (1, 0, false),
        "the member must end with an error of its own: {outcome:?}"
    );
}

/// `register_fifo` is async and awaits the backend's spawn, so the refusal is
/// the call's own error, before any shard task exists.
#[tokio::test]
async fn supervisor_fifo_registration_refuses_a_missing_dead_letter_topic() {
    let mock = mock_with(&["startup-fifo-dlq-missing"]);
    let broker = Broker::<Kafka>::from_client(connect(&mock.bootstrap_servers()).await);
    let mut supervisor = broker.consumer_supervisor();
    let err = tokio::time::timeout(
        STARTUP_BOUND,
        supervisor.register_fifo::<FifoDlqMissing, _>(Noop, ConsumerOptions::new()),
    )
    .await
    .expect("register_fifo answers within the bound")
    .expect_err("a FIFO topic whose dead-letter topic is missing must be refused");
    assert!(
        matches!(&err, ShoveError::Topology(m) if m.contains("startup-fifo-dlq-missing-dlq")),
        "expected a Topology error naming the dead-letter topic, got {err:?}"
    );
}

/// A broker that cannot be reached says nothing about the topic, so it is not
/// a `Topology` error: the start stays on the retry path, which gives up with
/// `Connection` after the configured attempts. Two attempts, so the count is
/// not the one a reset after a slow attempt would also end on.
#[tokio::test]
async fn an_unreachable_broker_is_not_a_topology_error() {
    let client = connect("127.0.0.1:1").await;
    let options = ConsumerOptions::<Kafka>::new().with_max_reconnect_attempts(2);
    let ended = tokio::time::timeout(
        UNREACHABLE_BOUND,
        KafkaConsumer::new(client).run::<ExternalMissing, _>(Noop, (), options),
    )
    .await;
    match ended {
        Ok(Err(ShoveError::Connection(_))) => {}
        other => panic!("expected a Connection error from the exhausted retries, got {other:?}"),
    }
}

/// The registry declares its topology before it spawns a member, so its
/// members skip the check: three members make no probe, where one direct run
/// of the same topology makes two (the topic and its dead-letter topic).
#[tokio::test]
async fn registry_members_do_not_probe_again() {
    let mock = mock_with(&[
        "startup-external-dlq-missing",
        "startup-external-dlq-missing-dlq",
    ]);
    let bootstrap = mock.bootstrap_servers();

    let broker = Broker::<Kafka>::from_client(connect(&bootstrap).await);
    let mut group = broker.consumer_group();
    group
        .register::<ExternalDlqMissing, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(3..=3)),
            || Noop,
        )
        .await
        .expect("register against provisioned external topics");
    let outcome = group
        .run_until_timeout(tokio::time::sleep(STILL_RUNNING_AFTER), STARTUP_BOUND)
        .await;
    assert!(outcome.is_clean(), "{outcome:?}");
    assert_eq!(
        startup_probe::probes(),
        0,
        "a registry member probed a topology its group had declared"
    );

    let shutdown = CancellationToken::new();
    let options = ConsumerOptions::<Kafka>::new().with_shutdown(shutdown.clone());
    keeps_running_until_shutdown(
        async move {
            KafkaConsumer::new(connect(&bootstrap).await)
                .run::<ExternalDlqMissing, _>(Noop, (), options)
                .await
        },
        shutdown,
    )
    .await;
    assert_eq!(
        startup_probe::probes(),
        2,
        "a direct run probes its topic and its dead-letter topic once"
    );
}
