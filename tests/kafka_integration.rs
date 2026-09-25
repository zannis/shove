//! Integration tests for the Kafka backend.
//!
//! Migrated to `Broker<Kafka>` + `Publisher<B>` + `TopologyDeclarer<B>` +
//! `ConsumerGroup<B>`. Tests that require `run`/`run_fifo`/`run_dlq` (not yet
//! surfaced on the generic wrappers) keep a `KafkaConsumer` constructed from
//! the underlying `KafkaClient`.

#![cfg(feature = "kafka")]

use rdkafka::Message;
use serde::{Deserialize, Serialize};
use shove::RetryStrategy;
use shove::SequencedTopic as _;
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::consumer_group::ConsumerGroupConfig;
use shove::handler::MessageHandler;
use shove::kafka::{
    KafkaAutoOffsetReset, KafkaClient, KafkaConfig, KafkaConsumer, KafkaConsumerGroupConfig,
    KafkaQueueStats, KafkaTopologyDeclarer,
};
use shove::markers::Kafka;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::outcome::Outcome;
use shove::topic::Topic as _;
use shove::topology::{SequenceFailure, TopologyBuilder};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// WaitableCounter
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct WaitableCounter {
    count: Arc<AtomicU32>,
    signal: Arc<Notify>,
}

impl WaitableCounter {
    fn new() -> Self {
        Self {
            count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(Notify::new()),
        }
    }

    fn increment(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
    }

    fn get(&self) -> u32 {
        self.count.load(Ordering::Relaxed)
    }

    async fn wait_for(&self, target: u32, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            // Register the waiter before checking the count: `notify_waiters`
            // stores no permit, so an increment landing between an
            // unregistered check and the await would otherwise be lost,
            // parking this task until the deadline.
            let mut notified = std::pin::pin!(self.signal.notified());
            notified.as_mut().enable();
            if self.get() >= target {
                return true;
            }
            tokio::select! {
                _ = &mut notified => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return self.get() >= target;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SimpleMessage {
    id: String,
    content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct OrderMessage {
    order_id: String,
    amount: u64,
}

// ---------------------------------------------------------------------------
// Topic definitions
// ---------------------------------------------------------------------------

shove::define_topic!(
    WorkTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-work")
        .dlq()
        .hold_queue(Duration::from_millis(200))
        .hold_queue(Duration::from_millis(500))
        .build()
);

// A topic nobody declares: a publish to it must fail, not create it.
shove::define_topic!(
    UndeclaredTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-undeclared").build()
);

// A topic nobody declares, that a producer with topic auto-creation switched
// on may create on the first publish.
shove::define_topic!(
    AutoCreatedTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-auto-created").build()
);

shove::define_topic!(
    NoDlqTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-nodlq").build()
);

shove::define_topic!(
    DeferNoHoldTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-defer-nohold").dlq().build()
);

shove::define_sequenced_topic!(
    SeqSkipTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("kafka-seq-skip")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(2)
        .hold_queue(Duration::from_millis(200))
        .dlq()
        .build()
);

shove::define_sequenced_topic!(
    SeqFailAllTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("kafka-seq-failall")
        .sequenced(SequenceFailure::FailAll)
        .routing_shards(2)
        .hold_queue(Duration::from_millis(200))
        .dlq()
        .build()
);

shove::define_topic!(
    DrainTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-drain-fast").build()
);

shove::define_topic!(
    RetentionTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-retention")
        .with_topic_config("retention.ms", "3600000")
        .build()
);

shove::define_topic!(
    RetentionOverrideTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-retention-override")
        .with_topic_config("retention.ms", "1800000")
        .build()
);

// An infra-owned topic shove binds to but never creates, expands or alters.
// The DLQ stays shove-owned in this mode.
shove::define_topic!(
    ExternalOwnedTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-external-owned")
        .external()
        .dlq()
        .build()
);

// Eight default partitions, so the record coordinates a handler sees span
// more than one partition.
shove::define_topic!(
    CoordinatesTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-coordinates").build()
);

// A topic with history that a fresh supervisor consumer must tail, not replay.
shove::define_topic!(
    TailOnlyTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-tail-only").build()
);

// External topics for the in-place Retry and Defer contract: shove never
// produces into them, so a wait happens inside the handler's task instead.
shove::define_topic!(
    ExternalDeferTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-external-defer")
        .external()
        .hold_queue(Duration::from_millis(300))
        .allow_message_loss()
        .build()
);

shove::define_topic!(
    ExternalRetryTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-external-retry")
        .external()
        .hold_queue(Duration::from_millis(200))
        .dlq()
        .build()
);

shove::define_topic!(
    ExternalShutdownTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-external-shutdown")
        .external()
        .hold_queue(Duration::from_secs(10))
        .allow_message_loss()
        .build()
);

shove::define_topic!(
    ExternalKeepaliveTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-external-keepalive")
        .external()
        .hold_queue(Duration::from_secs(4))
        .allow_message_loss()
        .build()
);

// A long in-place wait, so a lowered `max.poll.interval.ms` passes while a
// handler waits and a record sits in the receive loop's hand.
#[cfg(feature = "test-support")]
shove::define_topic!(
    ExternalTransitionTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-external-transition")
        .external()
        .hold_queue(Duration::from_secs(25))
        .allow_message_loss()
        .build()
);

// Two partitions, so a record can arrive from one while a record from the
// other waits for the only prefetch slot.
#[cfg(feature = "test-support")]
shove::define_topic!(
    ExternalPermitWaitTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-external-permit-wait")
        .external()
        .hold_queue(Duration::from_millis(300))
        .allow_message_loss()
        .build()
);

/// A message type that derives only `Deserialize`: no `Clone`, no
/// `Serialize`. An in-place redelivery must work from the retained bytes,
/// never from a copy of the value.
#[derive(Debug, Deserialize)]
struct PlainOnly {
    id: String,
}

/// Decode-only codec for [`PlainOnly`]; the test publishes raw JSON bytes
/// through rdkafka, so encoding is never needed.
struct DecodeOnlyJson;

impl shove::Codec<PlainOnly> for DecodeOnlyJson {
    const NAME: &'static str = "json";
    fn encode(_value: &PlainOnly) -> Result<Vec<u8>, shove::ShoveError> {
        use serde::ser::Error as _;
        Err(shove::ShoveError::Serialization(serde_json::Error::custom(
            "PlainOnly is decode-only in this test",
        )))
    }
    fn decode(bytes: &[u8]) -> Result<PlainOnly, shove::ShoveError> {
        serde_json::from_slice(bytes).map_err(shove::ShoveError::Serialization)
    }
}

shove::define_topic!(
    ExternalNoCloneTopic,
    PlainOnly,
    TopologyBuilder::new("kafka-external-noclone")
        .external()
        .hold_queue(Duration::from_millis(300))
        .allow_message_loss()
        .build(),
    codec = DecodeOnlyJson
);

// A shove-owned topic whose consumer opts into the in-place retry shape.
shove::define_topic!(
    OwnedInPlaceTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-owned-inplace")
        .hold_queue(Duration::from_millis(300))
        .dlq()
        .build()
);

// An external topic nobody provisions: declaring it must fail, not create it.
shove::define_topic!(
    ExternalMissingTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-external-missing")
        .external()
        .build()
);

// ---------------------------------------------------------------------------
// Test harness: shared setup
// ---------------------------------------------------------------------------

struct TestBroker {
    _container: testcontainers::ContainerAsync<KafkaContainer>,
    client: KafkaClient,
    bootstrap_servers: String,
}

impl TestBroker {
    async fn start() -> Self {
        Self::start_with(|config| config).await
    }

    /// Start a broker and connect with the default `KafkaConfig` for it,
    /// passed through `configure` first.
    async fn start_with(configure: impl FnOnce(KafkaConfig) -> KafkaConfig) -> Self {
        Self::start_image(KafkaContainer::default(), configure).await
    }

    /// A single-broker container that can host a transactional producer.
    ///
    /// The module pins the offsets topic to one replica but leaves the
    /// transaction-state log at Kafka's defaults of three replicas and two
    /// in-sync, which a lone broker can never satisfy, so `init_transactions`
    /// hangs until it times out. Only tests that produce transactionally
    /// need this variant.
    async fn start_with_transactions() -> Self {
        use testcontainers::ImageExt;

        Self::start_image(
            KafkaContainer::default()
                .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1"),
            |config| config,
        )
        .await
    }

    async fn start_image<I>(image: I, configure: impl FnOnce(KafkaConfig) -> KafkaConfig) -> Self
    where
        I: AsyncRunner<KafkaContainer>,
    {
        let container = image
            .start()
            .await
            .expect("failed to start Kafka container");
        let port = container
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("failed to get Kafka port");
        let bootstrap_servers = format!("127.0.0.1:{port}");

        let config = configure(KafkaConfig::new(&bootstrap_servers));
        let client = KafkaClient::connect_with_retry(&config, 10)
            .await
            .expect("failed to connect to Kafka");

        Self {
            _container: container,
            client,
            bootstrap_servers,
        }
    }

    fn broker(&self) -> Broker<Kafka> {
        Broker::<Kafka>::from_client(self.client.clone())
    }

    /// A broker over a fresh client, for a "restart": a consumer group's
    /// cancellation token is its client's shutdown token, so a second group
    /// on the same client would be born cancelled.
    async fn fresh_broker(&self) -> Broker<Kafka> {
        let client =
            KafkaClient::connect_with_retry(&KafkaConfig::new(&self.bootstrap_servers), 10)
                .await
                .expect("failed to connect a fresh client");
        Broker::<Kafka>::from_client(client)
    }

    fn client(&self) -> KafkaClient {
        self.client.clone()
    }

    /// The bootstrap address, for tests that need a raw rdkafka client
    /// beside the one under test.
    fn brokers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Freeze the broker process: every request in flight hangs until
    /// [`unpause`](Self::unpause). The way to observe what a consumer does
    /// when its coordinator stops answering. Used by the frozen-shutdown
    /// test, which the `test-support` seam it reads gates.
    #[cfg(feature = "test-support")]
    async fn pause(&self) {
        self._container
            .pause()
            .await
            .expect("failed to pause the Kafka container");
    }

    #[cfg(feature = "test-support")]
    async fn unpause(&self) {
        self._container
            .unpause()
            .await
            .expect("failed to unpause the Kafka container");
    }
}

const TIMEOUT: Duration = Duration::from_secs(30);

/// `(partition, offset, timestamp_ms)` for one delivery.
type Coordinates = (Option<i32>, Option<i64>, Option<i64>);

/// Milliseconds since the Unix epoch, the unit of `MessageMetadata::timestamp_ms`.
fn epoch_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after the epoch")
            .as_millis(),
    )
    .expect("fits an i64")
}

/// Read a live topic config value straight from the broker via a raw admin
/// client, bypassing shove.
async fn live_topic_config(brokers: &str, topic: &str, key: &str) -> Option<String> {
    use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
    use rdkafka::client::DefaultClientContext;

    let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create admin client");
    let specifier = ResourceSpecifier::Topic(topic);
    let resources = admin
        .describe_configs([&specifier], &AdminOptions::new())
        .await
        .expect("describe_configs failed");
    let resource = resources
        .into_iter()
        .next()
        .expect("no resource returned")
        .expect("describe_configs returned an error for the topic");
    resource.entry_map().get(key).and_then(|e| e.value.clone())
}

/// Create `topic` with `partitions` partitions through a raw admin client,
/// standing in for the infrastructure that owns an external topic, and wait
/// until the broker's metadata shows it.
async fn provision_topic(brokers: &str, topic: &str, partitions: i32) {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;

    let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create admin client");
    let results = admin
        .create_topics(
            [&NewTopic::new(
                topic,
                partitions,
                TopicReplication::Fixed(1),
            )],
            &AdminOptions::new(),
        )
        .await
        .expect("create_topics failed");
    assert!(
        matches!(results.as_slice(), [Ok(name)] if name == topic),
        "provisioning {topic} failed: {results:?}"
    );
    let deadline = Instant::now() + TIMEOUT;
    while live_partition_count(brokers, topic) != Some(partitions as usize) {
        assert!(
            Instant::now() < deadline,
            "{topic} did not show up in metadata with {partitions} partitions"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The broker's view of `topic`: `None` when it has no such topic, else the
/// partition count. Read through a consumer-type client with no `group.id`
/// and auto-creation disabled, so the probe itself can neither create the
/// topic nor register a group.
fn live_partition_count(brokers: &str, topic: &str) -> Option<usize> {
    use rdkafka::consumer::{BaseConsumer, Consumer as _};

    let probe: BaseConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("allow.auto.create.topics", "false")
        .create()
        .expect("failed to create metadata probe");
    let metadata = probe
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .expect("failed to fetch topic metadata");
    let candidate = metadata.topics().iter().find(|t| t.name() == topic)?;
    if candidate.error().is_some() || candidate.partitions().is_empty() {
        return None;
    }
    Some(candidate.partitions().len())
}

/// The high watermark of `partition`, read through a plain consumer client:
/// the number of records the topic holds, which an external topic must keep
/// while shove retries and defers in place.
fn high_watermark(brokers: &str, topic: &str, partition: i32) -> i64 {
    use rdkafka::consumer::{BaseConsumer, Consumer as _};

    let probe: BaseConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create watermark probe");
    let (_, high) = probe
        .fetch_watermarks(topic, partition, Duration::from_secs(10))
        .expect("fetch_watermarks failed");
    high
}

/// Read up to `expected` raw payloads off `topic` with a fresh group.
/// Reads `expected` records off `topic` with a throwaway group, returning
/// each record's payload and its string headers.
async fn drain_raw_with_headers(
    brokers: &str,
    topic: &str,
    expected: usize,
    timeout: Duration,
) -> Vec<(Vec<u8>, HashMap<String, String>)> {
    use rdkafka::consumer::{Consumer as _, StreamConsumer};
    use rdkafka::message::Headers as _;

    let consumer: StreamConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("{topic}-raw-drain"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("failed to create raw consumer");
    consumer.subscribe(&[topic]).expect("subscribe should work");
    let mut out = Vec::new();
    let _ = tokio::time::timeout(timeout, async {
        while out.len() < expected {
            if let Ok(msg) = consumer.recv().await {
                let headers = msg
                    .headers()
                    .map(|hs| {
                        hs.iter()
                            .filter_map(|h| {
                                h.value.map(|v| {
                                    (h.key.to_string(), String::from_utf8_lossy(v).into_owned())
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                out.push((msg.payload().unwrap_or_default().to_vec(), headers));
            }
        }
    })
    .await;
    out
}

/// Publish raw bytes straight through rdkafka, bypassing shove's publisher.
async fn publish_raw(brokers: &str, topic: &str, payload: &[u8]) {
    use rdkafka::producer::{FutureProducer, FutureRecord};

    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create raw producer");
    producer
        .send(
            FutureRecord::to(topic).key("k").payload(payload),
            Duration::from_secs(10),
        )
        .await
        .expect("raw publish should succeed");
}

/// `publish_raw` onto one partition, for topics with more than one.
#[cfg(feature = "test-support")]
async fn publish_raw_to(brokers: &str, topic: &str, partition: i32, payload: &[u8]) {
    use rdkafka::producer::{FutureProducer, FutureRecord};

    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create raw producer");
    producer
        .send(
            FutureRecord::to(topic)
                .partition(partition)
                .key("k")
                .payload(payload),
            Duration::from_secs(10),
        )
        .await
        .expect("raw publish should succeed");
}

/// The group's committed offset on one partition, or `None` when it has
/// never committed there.
#[cfg(feature = "test-support")]
fn committed_offset(brokers: &str, group: &str, topic: &str, partition: i32) -> Option<i64> {
    use rdkafka::consumer::{BaseConsumer, Consumer as _};
    use rdkafka::{Offset, TopicPartitionList};

    let probe: BaseConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .create()
        .expect("failed to create committed-offset probe");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, partition);
    probe
        .committed_offsets(tpl, Duration::from_secs(10))
        .expect("committed_offsets")
        .find_partition(topic, partition)
        .and_then(|e| match e.offset() {
            Offset::Offset(o) => Some(o),
            _ => None,
        })
}

/// Poll until the group's lag on `topic` reads zero, the broker-side proof
/// that every consumed offset was committed.
async fn wait_for_zero_lag(client: &KafkaClient, topic: &str, group: &str, timeout: Duration) {
    use shove::kafka::{KafkaLagStatsProvider, KafkaQueueStatsProvider};

    let stats_provider = KafkaLagStatsProvider::new(client.clone());
    let deadline = Instant::now() + timeout;
    loop {
        let stats: KafkaQueueStats = stats_provider
            .get_queue_stats(topic, group, KafkaAutoOffsetReset::Earliest)
            .await
            .expect("get_queue_stats should succeed");
        if stats.messages_pending == 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "group {group} still has lag {} on {topic} after {timeout:?}",
            stats.messages_pending
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The broker's view of `group` right now: its state and its member count.
#[cfg(feature = "test-support")]
fn group_state(brokers: &str, group: &str) -> (String, usize) {
    use rdkafka::consumer::{BaseConsumer, Consumer as _};

    let probe: BaseConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create group probe");
    let list = probe
        .fetch_group_list(Some(group), Duration::from_secs(10))
        .expect("failed to fetch group list");
    list.groups()
        .iter()
        .find(|g| g.name() == group)
        .map(|g| (g.state().to_string(), g.members().len()))
        .unwrap_or_else(|| ("<absent>".to_string(), 0))
}

/// Wait until the broker reports `group` as `Stable` with at least one
/// member, so a record published afterwards lands after the group's
/// assignment was taken.
///
/// A coordinator that is moving or loading while the member joins answers the
/// probe with an error that means "ask again"; those are retried within the
/// deadline, as `tests/kafka_offset_reset_integration.rs` retries them, and
/// the last one is reported if the deadline passes. Any other error is a
/// broken probe and fails at once.
async fn wait_for_stable_group(brokers: &str, group: &str, timeout: Duration) {
    use rdkafka::consumer::{BaseConsumer, Consumer as _};
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};

    fn is_transient(code: RDKafkaErrorCode) -> bool {
        matches!(
            code,
            RDKafkaErrorCode::NotCoordinator
                | RDKafkaErrorCode::CoordinatorNotAvailable
                | RDKafkaErrorCode::CoordinatorLoadInProgress
                | RDKafkaErrorCode::OperationTimedOut
        )
    }

    let probe: BaseConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create group probe");
    let deadline = Instant::now() + timeout;
    let mut last_error: Option<KafkaError> = None;
    loop {
        match probe.fetch_group_list(Some(group), Duration::from_secs(10)) {
            Ok(list) => {
                let stable = list
                    .groups()
                    .iter()
                    .any(|g| g.name() == group && g.state() == "Stable" && !g.members().is_empty());
                if stable {
                    return;
                }
            }
            Err(KafkaError::GroupListFetch(code)) if is_transient(code) => {
                last_error = Some(KafkaError::GroupListFetch(code));
            }
            Err(e) => panic!("failed to fetch group list: {e}"),
        }
        assert!(
            Instant::now() < deadline,
            "group {group} did not become stable with a member within {timeout:?}; \
             last coordinator error: {last_error:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Poll [`live_topic_config`] until the key reads back as `expected` or the
/// timeout elapses, returning the last observed value. Config changes commit
/// on the controller before brokers apply them to their local metadata
/// snapshot, so a describe issued immediately after a create/alter can be
/// stale.
async fn wait_for_topic_config(
    brokers: &str,
    topic: &str,
    key: &str,
    expected: &str,
    timeout: Duration,
) -> Option<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let value = live_topic_config(brokers, topic, key).await;
        if value.as_deref() == Some(expected) || Instant::now() >= deadline {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ---------------------------------------------------------------------------
// Reusable handlers
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct CountingHandler {
    counter: WaitableCounter,
}

impl CountingHandler {
    fn new() -> Self {
        Self {
            counter: WaitableCounter::new(),
        }
    }
}

impl MessageHandler<WorkTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

impl MessageHandler<NoDlqTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

impl MessageHandler<DeferNoHoldTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

impl MessageHandler<ExternalOwnedTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

/// Records every delivery as `(id, retry_count, redelivered)` and returns
/// `Defer` for the very first delivery it sees, then `Ack` for everything.
#[derive(Clone)]
struct DeferOnceRecorder {
    seen: Arc<Mutex<Vec<(String, u32, bool)>>>,
    /// The coordinates of every delivery, in order, so a test can prove that
    /// a redelivery is the same record and not a copy.
    coordinates: Arc<Mutex<Vec<Coordinates>>>,
    counter: WaitableCounter,
    /// How long the first delivery runs before it returns `Defer`, so a test
    /// can hold the handler in its running state for a while.
    first_runs_for: Duration,
}

impl DeferOnceRecorder {
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
            coordinates: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
            first_runs_for: Duration::ZERO,
        }
    }

    #[cfg(feature = "test-support")]
    fn running_first_for(mut self, duration: Duration) -> Self {
        self.first_runs_for = duration;
        self
    }

    async fn ids(&self) -> Vec<String> {
        self.seen
            .lock()
            .await
            .iter()
            .map(|(id, _, _)| id.clone())
            .collect()
    }

    async fn record(&self, id: String, meta: &MessageMetadata) -> Outcome {
        self.coordinates
            .lock()
            .await
            .push((meta.partition, meta.offset, meta.timestamp_ms));
        let mut seen = self.seen.lock().await;
        let first = seen.is_empty();
        seen.push((id, meta.retry_count, meta.redelivered));
        drop(seen);
        self.counter.increment();
        if first {
            tokio::time::sleep(self.first_runs_for).await;
            Outcome::Defer
        } else {
            Outcome::Ack
        }
    }
}

macro_rules! defer_once_for {
    ($($topic:ty),+ $(,)?) => {$(
        impl MessageHandler<$topic> for DeferOnceRecorder {
            type Context = ();
            async fn handle(&self, msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
                self.record(msg.id, &meta).await
            }
        }
    )+};
}

defer_once_for!(
    ExternalDeferTopic,
    ExternalKeepaliveTopic,
    ExternalShutdownTopic,
    OwnedInPlaceTopic
);
#[cfg(feature = "test-support")]
defer_once_for!(ExternalTransitionTopic, ExternalPermitWaitTopic);

impl MessageHandler<ExternalNoCloneTopic> for DeferOnceRecorder {
    type Context = ();
    async fn handle(&self, msg: PlainOnly, meta: MessageMetadata, _: &()) -> Outcome {
        self.record(msg.id, &meta).await
    }
}

/// Returns a fixed outcome on every delivery and records each one.
#[derive(Clone)]
struct AlwaysRecorder {
    outcome: Outcome,
    seen: Arc<Mutex<Vec<(String, u32)>>>,
    counter: WaitableCounter,
}

impl AlwaysRecorder {
    fn new(outcome: Outcome) -> Self {
        Self {
            outcome,
            seen: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }
}

macro_rules! always_for {
    ($($topic:ty),+ $(,)?) => {$(
        impl MessageHandler<$topic> for AlwaysRecorder {
            type Context = ();
            async fn handle(&self, msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
                self.seen.lock().await.push((msg.id, meta.retry_count));
                self.counter.increment();
                self.outcome.clone()
            }
        }
    )+};
}

always_for!(ExternalRetryTopic, ExternalShutdownTopic);

/// Records the id of every message it is handed.
#[derive(Clone)]
struct IdRecorder {
    seen: Arc<Mutex<Vec<String>>>,
    counter: WaitableCounter,
}

impl IdRecorder {
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }
}

impl MessageHandler<TailOnlyTopic> for IdRecorder {
    type Context = ();
    async fn handle(&self, msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.seen.lock().await.push(msg.id);
        self.counter.increment();
        Outcome::Ack
    }
}

/// Keeps every `MessageMetadata` it is handed, for assertions on the record
/// coordinates Kafka fills in.
#[derive(Clone)]
struct MetadataRecorder {
    seen: Arc<Mutex<Vec<MessageMetadata>>>,
    counter: WaitableCounter,
}

impl MetadataRecorder {
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }
}

impl MessageHandler<CoordinatesTopic> for MetadataRecorder {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
        self.seen.lock().await.push(meta);
        self.counter.increment();
        Outcome::Ack
    }
}

impl MessageHandler<ExternalMissingTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

impl MessageHandler<SeqSkipTopic> for CountingHandler {
    type Context = ();
    async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.counter.increment();
        Outcome::Ack
    }
}

struct FixedOutcomeHandler(Outcome);

impl MessageHandler<WorkTopic> for FixedOutcomeHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.0.clone()
    }
}

#[derive(Clone)]
struct RetryThenAckHandler {
    retry_until: u32,
    counter: WaitableCounter,
}

impl RetryThenAckHandler {
    fn new(retry_until: u32) -> Self {
        Self {
            retry_until,
            counter: WaitableCounter::new(),
        }
    }
}

impl MessageHandler<WorkTopic> for RetryThenAckHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        let attempt = self.counter.get();
        self.counter.increment();
        if attempt < self.retry_until {
            Outcome::Retry
        } else {
            Outcome::Ack
        }
    }
}

#[derive(Clone)]
struct SlowHandler {
    delay: Duration,
    counter: WaitableCounter,
}

impl SlowHandler {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            counter: WaitableCounter::new(),
        }
    }
}

impl MessageHandler<WorkTopic> for SlowHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        tokio::time::sleep(self.delay).await;
        self.counter.increment();
        Outcome::Ack
    }
}

#[derive(Clone)]
struct DlqRecordingHandler {
    counter: WaitableCounter,
    /// The dead letter's own `(partition, offset, timestamp_ms)`.
    dead: Arc<Mutex<Vec<Coordinates>>>,
}

impl DlqRecordingHandler {
    fn new() -> Self {
        Self {
            counter: WaitableCounter::new(),
            dead: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl MessageHandler<WorkTopic> for DlqRecordingHandler {
    type Context = ();
    async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }

    async fn handle_dead(&self, _msg: SimpleMessage, meta: DeadMessageMetadata, _: &()) {
        self.dead.lock().await.push((
            meta.message.partition,
            meta.message.offset,
            meta.message.timestamp_ms,
        ));
        self.counter.increment();
    }
}

#[derive(Clone)]
struct OrderRecordingHandler {
    records: Arc<Mutex<Vec<(String, u64)>>>,
    /// Each delivery's `(partition, offset, timestamp_ms)`, in arrival order.
    coordinates: Arc<Mutex<Vec<Coordinates>>>,
    counter: WaitableCounter,
}

impl OrderRecordingHandler {
    fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(Vec::new())),
            coordinates: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }

    async fn records(&self) -> Vec<(String, u64)> {
        self.records.lock().await.clone()
    }
}

impl MessageHandler<SeqSkipTopic> for OrderRecordingHandler {
    type Context = ();
    async fn handle(&self, msg: OrderMessage, meta: MessageMetadata, _: &()) -> Outcome {
        self.records.lock().await.push((msg.order_id, msg.amount));
        self.coordinates
            .lock()
            .await
            .push((meta.partition, meta.offset, meta.timestamp_ms));
        self.counter.increment();
        Outcome::Ack
    }
}

// ===========================================================================
// Client lifecycle
// ===========================================================================

#[tokio::test]
async fn client_connect_and_shutdown() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();

    let client = tb.client();
    assert!(
        !client.shutdown_token().is_cancelled(),
        "shutdown token should not be cancelled before shutdown"
    );

    broker.close().await;

    assert!(
        client.shutdown_token().is_cancelled(),
        "shutdown token should be cancelled after shutdown"
    );
}

#[tokio::test]
async fn client_shutdown_cancels_token() {
    let tb = TestBroker::start().await;
    let client = tb.client();
    let token = client.shutdown_token();
    assert!(!token.is_cancelled());

    let broker = tb.broker();
    broker.close().await;
    assert!(token.is_cancelled());
}

// ===========================================================================
// Topology declaration
// ===========================================================================

#[tokio::test]
async fn topology_declares_standard_topic_and_dlq() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<WorkTopic>().await.unwrap();
    // If we got here without error, topology was declared successfully.
    broker.close().await;
}

#[tokio::test]
async fn topology_sets_topic_config_on_create() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<RetentionTopic>().await.unwrap();

    let value = wait_for_topic_config(
        tb.client().brokers(),
        "kafka-retention",
        "retention.ms",
        "3600000",
        TIMEOUT,
    )
    .await;
    assert_eq!(value.as_deref(), Some("3600000"));
    broker.close().await;
}

#[tokio::test]
async fn topology_reconciles_config_on_existing_topic() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();

    // First declare WITHOUT any config (declarer-level knob only, so the
    // same Topic type can be re-declared with a different desired value).
    broker.topology().declare::<WorkTopic>().await.unwrap();
    // Redeclare with a declarer-level retention (via the named helper) —
    // topic already exists, so this exercises the describe → drift → alter
    // path.
    broker
        .topology()
        .with_retention(Duration::from_secs(7200))
        .declare::<WorkTopic>()
        .await
        .unwrap();

    let value = wait_for_topic_config(
        tb.client().brokers(),
        "kafka-work",
        "retention.ms",
        "7200000",
        TIMEOUT,
    )
    .await;
    assert_eq!(value.as_deref(), Some("7200000"));
    broker.close().await;
}

#[tokio::test]
async fn topology_reconcile_preserves_unrelated_dynamic_config() {
    use rdkafka::admin::{AdminClient, AdminOptions, AlterConfig, ResourceSpecifier};
    use rdkafka::client::DefaultClientContext;

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    // Out-of-band: set an unrelated dynamic topic config, as infra might.
    let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", tb.client().brokers())
        .create()
        .expect("failed to create admin client");
    let alter =
        AlterConfig::new(ResourceSpecifier::Topic("kafka-work")).set("segment.bytes", "123456789");
    for r in admin
        .alter_configs([&alter], &AdminOptions::new())
        .await
        .expect("alter_configs failed")
    {
        r.expect("alter_configs returned an error");
    }
    // Wait for the out-of-band change to propagate to describe_configs —
    // shove's reconcile reads the live config, so it can only preserve
    // entries that are visible when it runs.
    let visible = wait_for_topic_config(
        tb.client().brokers(),
        "kafka-work",
        "segment.bytes",
        "123456789",
        TIMEOUT,
    )
    .await;
    assert_eq!(visible.as_deref(), Some("123456789"));

    // Reconcile retention via shove — segment.bytes must survive the
    // legacy-AlterConfigs merge.
    broker
        .topology()
        .with_topic_config("retention.ms", "7200000")
        .declare::<WorkTopic>()
        .await
        .unwrap();

    let brokers = tb.client().brokers().to_string();
    let retention =
        wait_for_topic_config(&brokers, "kafka-work", "retention.ms", "7200000", TIMEOUT).await;
    let segment = wait_for_topic_config(
        &brokers,
        "kafka-work",
        "segment.bytes",
        "123456789",
        TIMEOUT,
    )
    .await;
    assert_eq!(retention.as_deref(), Some("7200000"));
    assert_eq!(segment.as_deref(), Some("123456789"));
    broker.close().await;
}

#[tokio::test]
async fn topology_config_redeclare_is_idempotent() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<RetentionTopic>().await.unwrap();
    // Second declare with identical config: no drift, must succeed quietly.
    broker.topology().declare::<RetentionTopic>().await.unwrap();

    let value = wait_for_topic_config(
        tb.client().brokers(),
        "kafka-retention",
        "retention.ms",
        "3600000",
        TIMEOUT,
    )
    .await;
    assert_eq!(value.as_deref(), Some("3600000"));
    broker.close().await;
}

#[tokio::test]
async fn topology_per_topic_config_overrides_declarer_default() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    // Declarer says 9999999, the topic's builder says 1800000 — builder wins.
    broker
        .topology()
        .with_topic_config("retention.ms", "9999999")
        .declare::<RetentionOverrideTopic>()
        .await
        .unwrap();

    let value = wait_for_topic_config(
        tb.client().brokers(),
        "kafka-retention-override",
        "retention.ms",
        "1800000",
        TIMEOUT,
    )
    .await;
    assert_eq!(value.as_deref(), Some("1800000"));
    broker.close().await;
}

#[tokio::test]
async fn topology_declares_sequenced_topic_with_partitions() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();
    broker.close().await;
}

#[tokio::test]
async fn topology_idempotent() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();

    broker.topology().declare::<WorkTopic>().await.unwrap();
    broker.topology().declare::<WorkTopic>().await.unwrap(); // second call should not fail

    broker.close().await;
}

/// The broker's view of `topic`: `None` when it has no such topic, else the
/// partition count. Read through a consumer-type client with no `group.id`
/// and auto-creation disabled, so the probe itself can neither create the
/// topic nor register a group.
fn live_partition_count(brokers: &str, topic: &str) -> Option<usize> {
    use rdkafka::consumer::{BaseConsumer, Consumer as _};

    let probe: BaseConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("allow.auto.create.topics", "false")
        .create()
        .expect("failed to create metadata probe");
    let metadata = probe
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .expect("failed to fetch topic metadata");
    let candidate = metadata.topics().iter().find(|t| t.name() == topic)?;
    if candidate.error().is_some() || candidate.partitions().is_empty() {
        return None;
    }
    Some(candidate.partitions().len())
}

// ===========================================================================
// Basic publish & consume
// ===========================================================================

/// A publish never creates a topic. The test broker auto-creates topics on
/// first use, as the Apache image does by default, and a publish to a topic
/// nobody declared still fails after the produce timeout and leaves the
/// topic absent.
#[tokio::test]
async fn publish_to_an_undeclared_topic_fails_instead_of_creating_it() {
    const TOPIC: &str = "kafka-undeclared";
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let publisher = broker.publisher().await.unwrap();

    let err = publisher
        .publish::<UndeclaredTopic>(&SimpleMessage {
            id: "orphan".into(),
            content: "no topic".into(),
        })
        .await
        .expect_err("a publish must not create the topic it targets");
    assert!(
        matches!(err, shove::ShoveError::Connection(_)),
        "expected Connection, got {err:?}"
    );
    assert_eq!(
        live_partition_count(tb.client().brokers(), TOPIC),
        None,
        "the publish must leave the topic absent"
    );
    broker.close().await;
}

/// The switch: with `with_producer_auto_create_topics(true)` the same publish
/// to a topic nobody declared succeeds on the auto-creating test broker, and
/// the broker has the topic afterwards.
#[tokio::test]
async fn publish_to_an_undeclared_topic_creates_it_when_auto_creation_is_switched_on() {
    const TOPIC: &str = "kafka-auto-created";
    let tb = TestBroker::start_with(|config| config.with_producer_auto_create_topics(true)).await;
    let broker = tb.broker();
    let publisher = broker.publisher().await.unwrap();

    publisher
        .publish::<AutoCreatedTopic>(&SimpleMessage {
            id: "first".into(),
            content: "creates the topic".into(),
        })
        .await
        .expect("a producer with auto-creation switched on may create the topic it targets");
    assert!(
        live_partition_count(tb.client().brokers(), TOPIC)
            .is_some_and(|partitions| partitions >= 1),
        "the publish must leave the topic present"
    );
    broker.close().await;
}

#[tokio::test]
async fn publish_and_consume_simple_message() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "simple-1".into(),
            content: "hello".into(),
        })
        .await
        .expect("publish should succeed");

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "should receive 1 message"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    assert_eq!(handler.counter.get(), 1);
    broker.close().await;
}

#[tokio::test]
async fn publish_and_consume_with_headers() {
    #[derive(Clone)]
    struct HeaderCapture(Arc<Mutex<HashMap<String, String>>>);

    impl MessageHandler<WorkTopic> for HeaderCapture {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
            *self.0.lock().await = (*meta.headers).clone();
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    let mut headers = HashMap::new();
    headers.insert("x-trace-id".to_string(), "trace-abc-123".to_string());

    publisher
        .publish_with_headers::<WorkTopic>(
            &SimpleMessage {
                id: "hdr-1".into(),
                content: "with headers".into(),
            },
            headers,
        )
        .await
        .expect("publish_with_headers should succeed");

    let captured = Arc::new(Mutex::new(HashMap::new()));
    let handler = HeaderCapture(captured.clone());

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    let result = tokio::time::timeout(TIMEOUT, async {
        loop {
            let map = captured.lock().await;
            if !map.is_empty() {
                return map.clone();
            }
            drop(map);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    shutdown.cancel();
    handle.await.unwrap().ok();

    let headers_received = result.expect("should receive headers within timeout");
    assert_eq!(
        headers_received.get("x-trace-id").map(|s| s.as_str()),
        Some("trace-abc-123"),
    );
    broker.close().await;
}

#[tokio::test]
async fn publish_and_consume_batch() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (1..=5)
        .map(|i| SimpleMessage {
            id: format!("batch-{i}"),
            content: format!("message {i}"),
        })
        .collect();

    publisher
        .publish_batch::<WorkTopic>(&messages)
        .await
        .expect("publish_batch should succeed");

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(5, TIMEOUT).await,
        "should receive all 5 messages"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    assert_eq!(handler.counter.get(), 5);
    broker.close().await;
}

// ===========================================================================
// Rejection & DLQ
// ===========================================================================

#[tokio::test]
async fn rejected_message_lands_in_dlq() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "reject-1".into(),
            content: "reject me".into(),
        })
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                FixedOutcomeHandler(Outcome::Reject),
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1)
                    .with_max_retries(1),
            )
            .await
    });

    // Verify message arrives in DLQ via a DLQ consumer
    let dlq_handler = DlqRecordingHandler::new();
    let dhc = dlq_handler.clone();
    let dlq_consumer = KafkaConsumer::new(client.clone());
    let dlq_handle =
        tokio::spawn(async move { dlq_consumer.run_dlq::<WorkTopic, _>(dhc, ()).await });

    assert!(
        dlq_handler.counter.wait_for(1, TIMEOUT).await,
        "DLQ should receive rejected message"
    );

    shutdown.cancel();
    broker.close().await;
    handle.await.unwrap().ok();
    dlq_handle.await.unwrap().ok();
}

#[tokio::test]
async fn dlq_consumer_handles_dead_message() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let before = epoch_ms();
    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "dlq-test".into(),
            content: "dead".into(),
        })
        .await
        .unwrap();

    // Step 1: reject to get message into DLQ
    let shutdown1 = CancellationToken::new();
    let sc1 = shutdown1.clone();
    let c1 = KafkaConsumer::new(client.clone());
    let h1 = tokio::spawn(async move {
        c1.run::<WorkTopic, _>(
            FixedOutcomeHandler(Outcome::Reject),
            (),
            ConsumerOptions::<Kafka>::new()
                .with_shutdown(sc1)
                .with_prefetch_count(1),
        )
        .await
    });

    tokio::time::sleep(Duration::from_secs(10)).await;
    shutdown1.cancel();
    h1.await.unwrap().ok();

    // Step 2: consume from DLQ
    let dlq_handler = DlqRecordingHandler::new();
    let dhc = dlq_handler.clone();
    let c2 = KafkaConsumer::new(client.clone());
    let h2 = tokio::spawn(async move { c2.run_dlq::<WorkTopic, _>(dhc, ()).await });

    assert!(
        dlq_handler.counter.wait_for(1, TIMEOUT).await,
        "DLQ handler should receive 1 dead message"
    );
    assert_eq!(dlq_handler.counter.get(), 1);
    let after = epoch_ms();

    // The DLQ drain reports the dead letter's own coordinates: the record on
    // the dead-letter topic, not the original's, so a replay of the dead
    // letters names a position that exists.
    let dead = dlq_handler.dead.lock().await.clone();
    assert_eq!(dead.len(), 1, "one dead letter, one set of coordinates");
    let (partition, offset, ts) = dead[0];
    assert!(
        partition.is_some(),
        "the dead letter's partition is reported: {dead:?}"
    );
    assert_eq!(
        offset,
        Some(0),
        "the first record on a fresh dead-letter partition sits at offset 0"
    );
    let ts = ts.expect("the dead letter's broker timestamp is reported");
    assert!(
        (before - 1_000..=after + 1_000).contains(&ts),
        "timestamp {ts} outside the window {before}..={after}"
    );

    broker.close().await;
    h2.await.unwrap().ok();
}

// ===========================================================================
// Retry mechanism
// ===========================================================================

#[tokio::test]
async fn retry_then_ack_succeeds() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "retry-ack".into(),
            content: "retry then ack".into(),
        })
        .await
        .unwrap();

    let handler = RetryThenAckHandler::new(1);
    let counter = handler.counter.clone();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(60)).await,
        "should have at least 2 handler calls"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

#[tokio::test]
async fn max_retries_sends_to_dlq() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "always-retry".into(),
            content: "exhaust retries".into(),
        })
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                FixedOutcomeHandler(Outcome::Retry),
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(2)
                    .with_prefetch_count(1),
            )
            .await
    });

    let dlq_handler = DlqRecordingHandler::new();
    let dhc = dlq_handler.clone();
    let dlq_consumer = KafkaConsumer::new(client.clone());
    let dlq_handle =
        tokio::spawn(async move { dlq_consumer.run_dlq::<WorkTopic, _>(dhc, ()).await });

    assert!(
        dlq_handler
            .counter
            .wait_for(1, Duration::from_secs(60))
            .await,
        "exhausted-retry message should land in DLQ"
    );

    shutdown.cancel();
    broker.close().await;
    handle.await.unwrap().ok();
    dlq_handle.await.unwrap().ok();
}

// `max_retries = N` must allow 1 initial attempt + N retries before the
// message is dead-lettered (the documented contract). With max_retries=2 the
// handler runs exactly 3 times.
#[tokio::test]
async fn max_retries_allows_initial_plus_n_retries() {
    struct CountingRetry(WaitableCounter);
    impl MessageHandler<WorkTopic> for CountingRetry {
        type Context = ();
        async fn handle(&self, _: SimpleMessage, _: MessageMetadata, _: &()) -> Outcome {
            self.0.increment();
            Outcome::Retry
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "retry-cap".into(),
            content: "exhaust retries".into(),
        })
        .await
        .unwrap();

    let counter = WaitableCounter::new();
    let handler = CountingRetry(counter.clone());

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(2)
                    .with_prefetch_count(1),
            )
            .await
    });

    // 1 initial + 2 retries = 3 attempts, then DLQ.
    assert!(
        counter.wait_for(3, Duration::from_secs(60)).await,
        "should reach 3 attempts"
    );
    // Allow any erroneous 4th redelivery to surface before asserting.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        counter.get(),
        3,
        "max_retries=2 must allow 1 initial + 2 retries = 3 attempts before DLQ"
    );

    shutdown.cancel();
    broker.close().await;
    handle.await.unwrap().ok();
}

// ===========================================================================
// Defer mechanism
// ===========================================================================

#[tokio::test]
async fn defer_redelivers_message() {
    struct DeferThenAck(WaitableCounter);

    impl MessageHandler<WorkTopic> for DeferThenAck {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            let prev = self.0.get();
            self.0.increment();
            if prev == 0 {
                Outcome::Defer
            } else {
                Outcome::Ack
            }
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "defer-1".into(),
            content: "defer then ack".into(),
        })
        .await
        .unwrap();

    let counter = WaitableCounter::new();
    let handler = DeferThenAck(counter.clone());

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(60)).await,
        "should be called at least 2 times (1 defer + 1 ack)"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

// ===========================================================================
// Concurrent consumption
// ===========================================================================

#[tokio::test]
async fn concurrent_consume_processes_all_messages() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (1..=10)
        .map(|i| SimpleMessage {
            id: format!("cc-{i}"),
            content: format!("msg {i}"),
        })
        .collect();
    publisher
        .publish_batch::<WorkTopic>(&messages)
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(10, Duration::from_secs(60)).await,
        "should receive all 10 messages"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    assert_eq!(handler.counter.get(), 10);
    broker.close().await;
}

#[tokio::test]
async fn concurrent_consume_mixed_outcomes() {
    struct MixedHandler(WaitableCounter, WaitableCounter);

    impl MessageHandler<WorkTopic> for MixedHandler {
        type Context = ();
        async fn handle(&self, msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            if msg.id.ends_with("-reject") {
                self.1.increment();
                Outcome::Reject
            } else {
                self.0.increment();
                Outcome::Ack
            }
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..3 {
        publisher
            .publish::<WorkTopic>(&SimpleMessage {
                id: format!("ack-{i}"),
                content: "ack".into(),
            })
            .await
            .unwrap();
    }
    for i in 0..2 {
        publisher
            .publish::<WorkTopic>(&SimpleMessage {
                id: format!("{i}-reject"),
                content: "reject".into(),
            })
            .await
            .unwrap();
    }

    let ack_counter = WaitableCounter::new();
    let reject_counter = WaitableCounter::new();
    let handler = MixedHandler(ack_counter.clone(), reject_counter.clone());

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10),
            )
            .await
    });

    assert!(
        ack_counter.wait_for(3, TIMEOUT).await,
        "should ack 3 messages"
    );
    assert!(
        reject_counter.wait_for(2, TIMEOUT).await,
        "should reject 2 messages"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

#[tokio::test]
async fn graceful_shutdown_drains_inflight() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "drain-1".into(),
            content: "slow".into(),
        })
        .await
        .unwrap();

    let handler = SlowHandler::new(Duration::from_secs(2));
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let options = ConsumerOptions::<Kafka>::new()
        .with_shutdown(sc)
        .with_prefetch_count(1);
    let processing_flag = options.processing_handle();
    let handle = tokio::spawn(async move { consumer.run::<WorkTopic, _>(hc, (), options).await });

    // Wait until the handler is actively processing (Kafka consumers take
    // a few seconds for group join + rebalance before they receive messages).
    let started = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if processing_flag.load(Ordering::Acquire) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        started.is_ok(),
        "handler should start processing within timeout"
    );

    // Now cancel while the handler is still sleeping.
    shutdown.cancel();

    let result = tokio::time::timeout(Duration::from_secs(10), handle).await;
    assert!(
        result.is_ok(),
        "consumer should exit within timeout after shutdown"
    );

    assert!(
        handler.counter.get() >= 1,
        "in-flight handler should have completed"
    );
    broker.close().await;
}

// ===========================================================================
// Handler timeout
// ===========================================================================

#[tokio::test]
async fn handler_timeout_triggers_retry() {
    struct TimeoutThenAck(WaitableCounter);

    impl MessageHandler<WorkTopic> for TimeoutThenAck {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            let attempt = self.0.get();
            self.0.increment();
            if attempt == 0 {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "timeout-1".into(),
            content: "timeout".into(),
        })
        .await
        .unwrap();

    let counter = WaitableCounter::new();
    let handler = TimeoutThenAck(counter.clone());

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5)
                    .with_prefetch_count(1)
                    .with_handler_timeout(Duration::from_millis(500)),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(60)).await,
        "should retry after timeout"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

// ===========================================================================
// Sequenced (FIFO) consumption
// ===========================================================================

#[tokio::test]
async fn sequenced_consume_preserves_order() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let before = epoch_ms();
    let publisher = broker.publisher().await.unwrap();
    for i in 0..5u64 {
        publisher
            .publish::<SeqSkipTopic>(&OrderMessage {
                order_id: "key-A".into(),
                amount: i,
            })
            .await
            .unwrap();
    }

    let handler = OrderRecordingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(5, Duration::from_secs(60)).await,
        "should receive all 5 messages"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    let after = epoch_ms();

    let records = handler.records().await;
    let amounts: Vec<u64> = records.iter().map(|(_, a)| *a).collect();
    assert_eq!(amounts, vec![0, 1, 2, 3, 4], "messages should be in order");

    // The FIFO path fills the same coordinates as the standard path: one key
    // lands on one partition of a fresh topic, at offsets 0..5 in order,
    // each with the broker's timestamp.
    let coordinates = handler.coordinates.lock().await.clone();
    assert_eq!(coordinates.len(), 5, "one set of coordinates per delivery");
    let partitions: std::collections::HashSet<i32> = coordinates
        .iter()
        .map(|(p, _, _)| p.expect("Kafka reports the partition on the FIFO path"))
        .collect();
    assert_eq!(
        partitions.len(),
        1,
        "one key maps to one partition: {coordinates:?}"
    );
    let offsets: Vec<i64> = coordinates
        .iter()
        .map(|(_, o, _)| o.expect("Kafka reports the offset on the FIFO path"))
        .collect();
    assert_eq!(
        offsets,
        vec![0, 1, 2, 3, 4],
        "a fresh partition yields offsets 0..5 in order"
    );
    for (_, _, ts) in &coordinates {
        let ts = ts.expect("Kafka reports the broker timestamp on the FIFO path");
        assert!(
            (before - 1_000..=after + 1_000).contains(&ts),
            "timestamp {ts} outside the window {before}..={after}"
        );
    }
    broker.close().await;
}

#[tokio::test]
async fn sequenced_skip_continues_after_rejection() {
    struct RejectFirstHandler {
        counter: WaitableCounter,
    }

    impl MessageHandler<SeqSkipTopic> for RejectFirstHandler {
        type Context = ();
        async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.counter.increment();
            if msg.amount == 0 {
                Outcome::Reject
            } else {
                Outcome::Ack
            }
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..3u64 {
        publisher
            .publish::<SeqSkipTopic>(&OrderMessage {
                order_id: "key-B".into(),
                amount: i,
            })
            .await
            .unwrap();
    }

    let counter = WaitableCounter::new();
    let handler = RejectFirstHandler {
        counter: counter.clone(),
    };

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5),
            )
            .await
    });

    assert!(
        counter.wait_for(3, Duration::from_secs(60)).await,
        "should process all 3 messages"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// CAF-84: `SequenceFailure::FailAll` must halt the failing sequence key on
/// Kafka, not silently behave like `Skip`.
///
/// Kafka carries the sequence key as the record key, so unlike NATS this
/// needed no wire change — only that the consumer reads `on_failure()` at all.
#[tokio::test]
async fn sequenced_failall_poisons_same_key_after_reject() {
    #[derive(Clone)]
    struct PoisonHandler {
        seen: Arc<Mutex<Vec<(String, u64)>>>,
        key_b_handled: WaitableCounter,
    }

    impl MessageHandler<SeqFailAllTopic> for PoisonHandler {
        type Context = ();
        async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.seen
                .lock()
                .await
                .push((msg.order_id.clone(), msg.amount));
            if msg.order_id == "key-B" {
                self.key_b_handled.increment();
            }
            if msg.order_id == "key-A" && msg.amount == 2 {
                Outcome::Reject
            } else {
                Outcome::Ack
            }
        }
    }

    struct SeqDlqHandler(WaitableCounter);
    impl MessageHandler<SeqFailAllTopic> for SeqDlqHandler {
        type Context = ();
        async fn handle(&self, _: OrderMessage, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
        async fn handle_dead(&self, _: OrderMessage, _: DeadMessageMetadata, _: &()) {
            self.0.increment();
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<SeqFailAllTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.unwrap();
    for amount in 0..5u64 {
        publisher
            .publish::<SeqFailAllTopic>(&OrderMessage {
                order_id: "key-A".into(),
                amount,
            })
            .await
            .unwrap();
    }
    for amount in 0..3u64 {
        publisher
            .publish::<SeqFailAllTopic>(&OrderMessage {
                order_id: "key-B".into(),
                amount,
            })
            .await
            .unwrap();
    }

    let seen: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let key_b_handled = WaitableCounter::new();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handler = PoisonHandler {
        seen: seen.clone(),
        key_b_handled: key_b_handled.clone(),
    };
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqFailAllTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(0),
            )
            .await
    });

    // key-A/2 is rejected, then key-A/3 and key-A/4 are dead-lettered without
    // ever reaching the handler → exactly 3 dead messages.
    let dlq_counter = WaitableCounter::new();
    let dlq_consumer = KafkaConsumer::new(client.clone());
    let dlq_handler = SeqDlqHandler(dlq_counter.clone());
    let dlq_handle = tokio::spawn(async move {
        dlq_consumer
            .run_dlq::<SeqFailAllTopic, _>(dlq_handler, ())
            .await
    });

    assert!(
        dlq_counter.wait_for(3, Duration::from_secs(60)).await,
        "expected key-A/2 plus the two poisoned messages in the DLQ, got {}",
        dlq_counter.get()
    );

    // Kafka runs one FIFO task over the whole assignment, so key-B is strictly
    // behind key-A's dead-letters in the consume order. Wait for it rather than
    // reading `seen` off the back of the DLQ count.
    assert!(
        key_b_handled.wait_for(3, Duration::from_secs(60)).await,
        "key-B must be unaffected by key-A's poisoning, but only {} of 3 were handled",
        key_b_handled.get()
    );

    let seen = seen.lock().await.clone();
    for amount in [3u64, 4] {
        assert!(
            !seen.contains(&("key-A".to_string(), amount)),
            "key-A/{amount} reached the handler after the key was poisoned: {seen:?}"
        );
    }
    for amount in 0..3u64 {
        assert!(
            seen.contains(&("key-B".to_string(), amount)),
            "key-B/{amount} should have been handled normally: {seen:?}"
        );
    }

    // `run_dlq` takes no shutdown token — Kafka's DLQ loop stops on the
    // *client's* token, which `broker.close()` cancels. Close before awaiting,
    // or `dlq_handle` never resolves.
    shutdown.cancel();
    broker.close().await;
    handle.await.unwrap().ok();
    dlq_handle.await.unwrap().ok();
}

#[tokio::test]
async fn sequenced_multiple_keys_concurrent() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..3u64 {
        publisher
            .publish::<SeqSkipTopic>(&OrderMessage {
                order_id: "alice".into(),
                amount: i,
            })
            .await
            .unwrap();
        publisher
            .publish::<SeqSkipTopic>(&OrderMessage {
                order_id: "bob".into(),
                amount: i + 100,
            })
            .await
            .unwrap();
    }

    let handler = OrderRecordingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(6, Duration::from_secs(60)).await,
        "should receive all 6 messages"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();

    let records = handler.records().await;
    let alice: Vec<u64> = records
        .iter()
        .filter(|(k, _)| k == "alice")
        .map(|(_, a)| *a)
        .collect();
    let bob: Vec<u64> = records
        .iter()
        .filter(|(k, _)| k == "bob")
        .map(|(_, a)| *a)
        .collect();
    assert_eq!(alice, vec![0, 1, 2], "alice messages should be in order");
    assert_eq!(bob, vec![100, 101, 102], "bob messages should be in order");
    broker.close().await;
}

// ===========================================================================
// Consumer group (via Broker<Kafka> generic wrapper)
// ===========================================================================

#[tokio::test]
async fn consumer_group_processes_messages() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (1..=5)
        .map(|i| SimpleMessage {
            id: format!("cg-{i}"),
            content: format!("msg {i}"),
        })
        .collect();
    publisher
        .publish_batch::<WorkTopic>(&messages)
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let handler_clone = handler.clone();

    let config = KafkaConsumerGroupConfig::new(2..=2)
        .with_prefetch_count(5)
        .with_max_retries(5);

    let mut group = broker.consumer_group();
    group
        .register::<WorkTopic, _>(ConsumerGroupConfig::new(config), move || {
            handler_clone.clone()
        })
        .await
        .unwrap();

    let token = group.cancellation_token();
    let counter = handler.counter.clone();
    let t = token.clone();
    tokio::spawn(async move {
        counter.wait_for(5, Duration::from_secs(60)).await;
        t.cancel();
    });

    let outcome = group
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(10))
        .await;
    assert!(outcome.is_clean());

    assert_eq!(handler.counter.get(), 5);
    broker.close().await;
}

/// `external()` binds to a topic infra created. Registering a
/// group whose `max_consumers` exceeds the partition count consumes through
/// it and leaves the partition count exactly as infra set it, while the DLQ
/// is still shove's to create.
#[tokio::test]
async fn external_topic_is_never_created_or_expanded() {
    const TOPIC: &str = "kafka-external-owned";
    let tb = TestBroker::start().await;
    provision_topic(tb.brokers(), TOPIC, 3).await;

    let broker = tb.broker();
    let handler = CountingHandler::new();
    let handler_clone = handler.clone();
    let mut group = broker.consumer_group();
    group
        .register::<ExternalOwnedTopic, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(1..=8)),
            move || handler_clone.clone(),
        )
        .await
        .expect("registering against a provisioned external topic must succeed");
    assert_eq!(
        live_partition_count(tb.brokers(), TOPIC),
        Some(3),
        "declare must not expand an external topic towards max_consumers"
    );
    assert!(
        live_partition_count(tb.brokers(), "kafka-external-owned-dlq").is_some(),
        "the DLQ is shove's own topic and is still created"
    );

    let publisher = broker.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (1..=5)
        .map(|i| SimpleMessage {
            id: format!("ext-{i}"),
            content: format!("msg {i}"),
        })
        .collect();
    publisher
        .publish_batch::<ExternalOwnedTopic>(&messages)
        .await
        .unwrap();

    let token = group.cancellation_token();
    let counter = handler.counter.clone();
    let t = token.clone();
    tokio::spawn(async move {
        counter.wait_for(5, Duration::from_secs(60)).await;
        t.cancel();
    });
    let outcome = group
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(10))
        .await;
    assert!(outcome.is_clean());
    assert_eq!(handler.counter.get(), 5);

    assert_eq!(
        live_partition_count(tb.brokers(), TOPIC),
        Some(3),
        "consuming must not expand the external topic either"
    );
    broker.close().await;
}

/// Every Kafka delivery carries the record's coordinates: a partition, an
/// offset that runs contiguously from zero inside each partition of a fresh
/// topic, and a broker timestamp inside the test's own wall-clock window.
#[tokio::test]
async fn handler_sees_partition_offset_and_timestamp() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<CoordinatesTopic>()
        .await
        .unwrap();

    let now_ms = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock before the epoch")
            .as_millis() as i64
    };
    let before = now_ms();
    let publisher = broker.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (1..=12)
        .map(|i| SimpleMessage {
            id: format!("coord-{i}"),
            content: format!("msg {i}"),
        })
        .collect();
    publisher
        .publish_batch::<CoordinatesTopic>(&messages)
        .await
        .unwrap();

    let handler = MetadataRecorder::new();
    let handler_clone = handler.clone();
    let mut group = broker.consumer_group();
    group
        .register::<CoordinatesTopic, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(1..=1)),
            move || handler_clone.clone(),
        )
        .await
        .unwrap();
    let token = group.cancellation_token();
    let counter = handler.counter.clone();
    let t = token.clone();
    tokio::spawn(async move {
        counter.wait_for(12, Duration::from_secs(60)).await;
        t.cancel();
    });
    let outcome = group
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(10))
        .await;
    assert!(outcome.is_clean());
    let after = now_ms();

    let seen = handler.seen.lock().await.clone();
    assert_eq!(seen.len(), 12);
    let mut per_partition: HashMap<i32, Vec<i64>> = HashMap::new();
    for meta in &seen {
        let partition = meta.partition.expect("Kafka reports the partition");
        let offset = meta.offset.expect("Kafka reports the offset");
        let timestamp = meta
            .timestamp_ms
            .expect("Kafka reports the broker timestamp");
        // A second of margin on each side covers the clock skew between this
        // process and the broker container.
        assert!(
            (before - 1_000..=after + 1_000).contains(&timestamp),
            "timestamp {timestamp} outside the window {before}..={after}"
        );
        per_partition.entry(partition).or_default().push(offset);
    }
    for (partition, mut offsets) in per_partition {
        offsets.sort_unstable();
        let contiguous: Vec<i64> = (0..offsets.len() as i64).collect();
        assert_eq!(
            offsets, contiguous,
            "partition {partition} of a fresh topic must yield offsets 0..n once each"
        );
    }
    broker.close().await;
}

/// `ConsumerOptions::<Kafka>::with_auto_offset_reset(Latest)` reaches the
/// supervisor path: a fresh group on a topic with history starts at the tail
/// and receives only what is published after its assignment.
#[tokio::test]
async fn supervisor_with_auto_offset_reset_latest_skips_history() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<TailOnlyTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    let old: Vec<SimpleMessage> = (1..=3)
        .map(|i| SimpleMessage {
            id: format!("old-{i}"),
            content: String::new(),
        })
        .collect();
    publisher
        .publish_batch::<TailOnlyTopic>(&old)
        .await
        .unwrap();

    let handler = IdRecorder::new();
    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<TailOnlyTopic, _>(
            handler.clone(),
            ConsumerOptions::<Kafka>::new().with_auto_offset_reset(KafkaAutoOffsetReset::Latest),
        )
        .unwrap();
    let token = supervisor.cancellation_token();
    let sup_handle =
        tokio::spawn(supervisor.run_until_timeout(std::future::pending(), Duration::from_secs(10)));

    wait_for_stable_group(tb.brokers(), "kafka-tail-only-consumer", TIMEOUT).await;
    // A stable group proves the member joined, not that its fetch position
    // is resolved: a `Latest` member reads the tail when its first fetch
    // runs, after the assignment. A record published before that fetch
    // lands below the tail and is skipped as history, so one marker is not
    // enough on a slow host. The test publishes a marker every half second
    // until one arrives: each later marker lands after the one before it was
    // fetched or skipped, and the first delivered marker proves the member
    // reads the tail. The records it asserts on are published after that.
    let marker_deadline = Instant::now() + TIMEOUT;
    let mut markers = 0u32;
    loop {
        publisher
            .publish::<TailOnlyTopic>(&SimpleMessage {
                id: format!("marker-{markers}"),
                content: String::new(),
            })
            .await
            .unwrap();
        markers += 1;
        if handler
            .counter
            .wait_for(1, Duration::from_millis(500))
            .await
        {
            break;
        }
        assert!(
            Instant::now() < marker_deadline,
            "no readiness marker arrived within {TIMEOUT:?}"
        );
    }
    let new: Vec<SimpleMessage> = (1..=2)
        .map(|i| SimpleMessage {
            id: format!("new-{i}"),
            content: String::new(),
        })
        .collect();
    publisher
        .publish_batch::<TailOnlyTopic>(&new)
        .await
        .unwrap();

    // Wait for the two records by id, not by count: more than one marker
    // may have arrived.
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let seen = handler.seen.lock().await.clone();
        if seen.iter().any(|id| id == "new-1") && seen.iter().any(|id| id == "new-2") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the records published after the marker must arrive, seen {seen:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Room for a replayed history record to show up if `Latest` had not
    // reached the consumer.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let seen = handler.seen.lock().await.clone();
    let history: Vec<&String> = seen.iter().filter(|id| id.starts_with("old-")).collect();
    assert!(
        history.is_empty(),
        "a fresh group with Latest must skip the history, saw {history:?}"
    );
    let markers_seen = seen.iter().filter(|id| id.starts_with("marker-")).count();
    assert!(
        markers_seen >= 1 && seen.len() == markers_seen + 2,
        "only readiness markers and the two new records arrive: {seen:?}"
    );

    token.cancel();
    let outcome = sup_handle.await.unwrap();
    assert!(outcome.is_clean());
    broker.close().await;
}

/// On an external topic a `Defer` waits in place and hands the same record
/// back before any later record, and the topic gains no republished copy:
/// the handler sees `[1, 1, 2]` and the high watermark stays at two.
#[tokio::test]
async fn external_topic_defer_redelivers_in_place_without_producing() {
    const TOPIC: &str = "kafka-external-defer";
    let tb = TestBroker::start().await;
    provision_topic(tb.brokers(), TOPIC, 1).await;
    let broker = tb.broker();

    let handler = DeferOnceRecorder::new();
    let h = handler.clone();
    let mut group = broker.consumer_group();
    group
        .register::<ExternalDeferTopic, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(1..=1)),
            move || h.clone(),
        )
        .await
        .unwrap();
    let token = group.cancellation_token();
    let running = tokio::spawn(
        group.run_until_timeout(token.clone().cancelled_owned(), Duration::from_secs(15)),
    );
    wait_for_stable_group(tb.brokers(), "kafka-external-defer-consumer", TIMEOUT).await;

    let publisher = broker.publisher().await.unwrap();
    let msg = |id: &str| SimpleMessage {
        id: id.into(),
        content: String::new(),
    };
    publisher
        .publish::<ExternalDeferTopic>(&msg("1"))
        .await
        .unwrap();
    assert!(handler.counter.wait_for(1, TIMEOUT).await, "first delivery");
    // Published while "1" is waiting out its deferral holding the only slot.
    publisher
        .publish::<ExternalDeferTopic>(&msg("2"))
        .await
        .unwrap();
    assert!(
        handler.counter.wait_for(3, TIMEOUT).await,
        "redelivery and the second record"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        handler.ids().await,
        vec!["1".to_string(), "1".to_string(), "2".to_string()],
        "the deferred record is redelivered before the record behind it"
    );
    let seen = handler.seen.lock().await.clone();
    assert_eq!(seen[0], ("1".to_string(), 0, false));
    assert_eq!(
        seen[1],
        ("1".to_string(), 0, true),
        "a Defer keeps the retry count and marks the redelivery"
    );
    let coordinates = handler.coordinates.lock().await.clone();
    assert_eq!(
        coordinates[0],
        (Some(0), Some(0), coordinates[0].2),
        "the first delivery is the first record of the one partition"
    );
    assert!(
        coordinates[0].2.is_some(),
        "Kafka reports the broker timestamp on the first delivery"
    );
    assert_eq!(
        coordinates[1], coordinates[0],
        "the redelivery is the same record: partition, offset and timestamp unchanged"
    );
    assert_eq!(
        coordinates[2],
        (Some(0), Some(1), coordinates[2].2),
        "the record behind it has the next offset"
    );
    assert_eq!(
        high_watermark(tb.brokers(), TOPIC, 0),
        2,
        "nothing was republished into the external topic"
    );

    token.cancel();
    let outcome = running.await.unwrap();
    assert!(outcome.is_clean());
    broker.close().await;
}

/// The in-place shape is a retry strategy, not an ownership flag: a consumer
/// of a shove-owned topic opts into it with `RetryStrategy::InPlace`, and its
/// `Defer` then waits in place and hands the same record back before any
/// later record, with no copy republished into the topic.
#[tokio::test]
async fn retry_in_place_on_an_owned_topic_does_not_republish() {
    const TOPIC: &str = "kafka-owned-inplace";
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<OwnedInPlaceTopic>()
        .await
        .expect("shove declares its own topic");

    let handler = DeferOnceRecorder::new();
    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<OwnedInPlaceTopic, _>(
            handler.clone(),
            ConsumerOptions::<Kafka>::new()
                .with_concurrent_processing(false)
                .with_retry_strategy(RetryStrategy::InPlace),
        )
        .expect("register");
    let token = supervisor.cancellation_token();
    let running =
        tokio::spawn(supervisor.run_until_timeout(std::future::pending(), Duration::from_secs(15)));
    wait_for_stable_group(tb.brokers(), "kafka-owned-inplace-consumer", TIMEOUT).await;

    let publisher = broker.publisher().await.unwrap();
    let msg = |id: &str| SimpleMessage {
        id: id.into(),
        content: String::new(),
    };
    publisher
        .publish::<OwnedInPlaceTopic>(&msg("1"))
        .await
        .unwrap();
    assert!(handler.counter.wait_for(1, TIMEOUT).await, "first delivery");
    publisher
        .publish::<OwnedInPlaceTopic>(&msg("2"))
        .await
        .unwrap();
    assert!(
        handler.counter.wait_for(3, TIMEOUT).await,
        "the in-place redelivery, then the second record"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        handler.ids().await,
        vec!["1".to_string(), "1".to_string(), "2".to_string()],
        "the deferred record is redelivered before the record behind it"
    );
    // A shove-declared topic has the default partition count, so the proof
    // that nothing was republished is the sum over every partition.
    let partitions = live_partition_count(tb.brokers(), TOPIC).expect("the topic exists");
    let records: i64 = (0..partitions)
        .map(|p| high_watermark(tb.brokers(), TOPIC, i32::try_from(p).expect("partition id")))
        .sum();
    assert_eq!(
        records, 2,
        "nothing was republished into the shove-owned topic"
    );

    token.cancel();
    assert!(running.await.unwrap().is_clean());
    broker.close().await;
}

/// On an external topic a `Retry` waits the tier delay in place and counts
/// its attempts in memory: `max_retries` 2 gives three handler calls, the
/// record then goes to the shove-owned DLQ, the topic gains nothing, and the
/// offset commits.
#[tokio::test]
async fn external_topic_retry_exhausts_without_producing() {
    const TOPIC: &str = "kafka-external-retry";
    let tb = TestBroker::start().await;
    provision_topic(tb.brokers(), TOPIC, 1).await;
    let broker = tb.broker();

    let handler = AlwaysRecorder::new(Outcome::Retry);
    let h = handler.clone();
    let mut group = broker.consumer_group();
    group
        .register::<ExternalRetryTopic, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(1..=1).with_max_retries(2)),
            move || h.clone(),
        )
        .await
        .unwrap();
    let token = group.cancellation_token();
    let running = tokio::spawn(
        group.run_until_timeout(token.clone().cancelled_owned(), Duration::from_secs(15)),
    );

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<ExternalRetryTopic>(&SimpleMessage {
            id: "poison".into(),
            content: "x".into(),
        })
        .await
        .unwrap();

    assert!(
        handler.counter.wait_for(3, TIMEOUT).await,
        "initial attempt plus two retries"
    );
    let dead = drain_raw_with_headers(tb.brokers(), "kafka-external-retry-dlq", 1, TIMEOUT).await;
    assert_eq!(dead.len(), 1, "the exhausted record is dead-lettered once");
    // The retries happened in place, so the count lived only in memory until
    // the DLQ publish wrote it; a DLQ consumer's `retry_count` reads it back.
    assert_eq!(
        dead[0].1.get("Shove-Retry-Count").map(String::as_str),
        Some("2"),
        "the dead letter carries the in-memory retry count: {:?}",
        dead[0].1
    );
    assert_eq!(
        dead[0].1.get("Shove-Death-Count").map(String::as_str),
        Some("1")
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    let seen = handler.seen.lock().await.clone();
    assert_eq!(
        seen.iter().map(|(_, n)| *n).collect::<Vec<_>>(),
        vec![0, 1, 2],
        "attempts are counted in memory and shown to the handler"
    );
    assert_eq!(seen.len(), 3, "no fourth attempt after the budget is spent");
    assert_eq!(
        high_watermark(tb.brokers(), TOPIC, 0),
        1,
        "nothing was republished into the external topic"
    );
    wait_for_zero_lag(
        &tb.client(),
        TOPIC,
        "kafka-external-retry-consumer",
        TIMEOUT,
    )
    .await;

    token.cancel();
    let outcome = running.await.unwrap();
    assert!(outcome.is_clean());
    broker.close().await;
}

/// The in-place redelivery works from the retained bytes, so a message type
/// that derives only `Deserialize`, with neither `Clone` nor `Serialize`, is
/// deferred and redelivered like any other.
#[tokio::test]
async fn external_topic_defer_works_for_a_message_type_without_clone() {
    const TOPIC: &str = "kafka-external-noclone";
    let tb = TestBroker::start().await;
    provision_topic(tb.brokers(), TOPIC, 1).await;
    let broker = tb.broker();

    let handler = DeferOnceRecorder::new();
    let h = handler.clone();
    let mut group = broker.consumer_group();
    group
        .register::<ExternalNoCloneTopic, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(1..=1)),
            move || h.clone(),
        )
        .await
        .unwrap();
    let token = group.cancellation_token();
    let running = tokio::spawn(
        group.run_until_timeout(token.clone().cancelled_owned(), Duration::from_secs(15)),
    );

    publish_raw(tb.brokers(), TOPIC, br#"{"id":"plain"}"#).await;
    assert!(
        handler.counter.wait_for(2, TIMEOUT).await,
        "deferred once, then acked"
    );
    assert_eq!(
        handler.ids().await,
        vec!["plain".to_string(), "plain".to_string()]
    );
    assert_eq!(high_watermark(tb.brokers(), TOPIC, 0), 1);

    token.cancel();
    let outcome = running.await.unwrap();
    assert!(outcome.is_clean());
    broker.close().await;
}

/// A shutdown during an in-place wait returns promptly, completes nothing,
/// and leaves the record for the next member of the group.
#[tokio::test]
async fn external_topic_shutdown_during_a_wait_leaves_the_record_uncommitted() {
    const TOPIC: &str = "kafka-external-shutdown";
    let tb = TestBroker::start().await;
    provision_topic(tb.brokers(), TOPIC, 1).await;
    let broker = tb.broker();

    let deferring = AlwaysRecorder::new(Outcome::Defer);
    let h = deferring.clone();
    let mut group = broker.consumer_group();
    group
        .register::<ExternalShutdownTopic, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(1..=1)),
            move || h.clone(),
        )
        .await
        .unwrap();
    let token = group.cancellation_token();
    let running = tokio::spawn(
        group.run_until_timeout(token.clone().cancelled_owned(), Duration::from_secs(15)),
    );

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<ExternalShutdownTopic>(&SimpleMessage {
            id: "held".into(),
            content: String::new(),
        })
        .await
        .unwrap();
    assert!(
        deferring.counter.wait_for(1, TIMEOUT).await,
        "the record reached the handler"
    );

    // Cancel in the middle of the ten-second wait.
    let cancelled_at = Instant::now();
    token.cancel();
    let outcome = running.await.unwrap();
    let took = cancelled_at.elapsed();
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert!(
        took < Duration::from_secs(5),
        "shutdown must not wait out the deferral, took {took:?}"
    );

    // A fresh member of the same group, on a fresh client, is handed the
    // record again.
    let restarted = tb.fresh_broker().await;
    let acking = AlwaysRecorder::new(Outcome::Ack);
    let h = acking.clone();
    let mut group = restarted.consumer_group();
    group
        .register::<ExternalShutdownTopic, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(1..=1)),
            move || h.clone(),
        )
        .await
        .unwrap();
    let token = group.cancellation_token();
    let running = tokio::spawn(
        group.run_until_timeout(token.clone().cancelled_owned(), Duration::from_secs(15)),
    );
    assert!(
        acking.counter.wait_for(1, TIMEOUT).await,
        "the uncommitted record must be redelivered after a restart"
    );
    assert_eq!(acking.seen.lock().await[0].0, "held");
    token.cancel();
    assert!(running.await.unwrap().is_clean());
    restarted.close().await;
    broker.close().await;
}

/// A shutdown that lands during an in-place wait ends the handler task with
/// nothing completed, and the receive loop's drain then clears the consumer's
/// busy flag: the flag is what the group's scale-down reads to find an idle
/// member, so a member that exited cleanly must not read as still working.
#[tokio::test]
async fn external_topic_shutdown_during_a_wait_clears_the_processing_flag() {
    const TOPIC: &str = "kafka-external-shutdown";
    let tb = TestBroker::start().await;
    provision_topic(tb.brokers(), TOPIC, 1).await;
    let broker = tb.broker();

    let handler = DeferOnceRecorder::new();
    let shutdown = CancellationToken::new();
    let options = ConsumerOptions::<Kafka>::new().with_shutdown(shutdown.clone());
    let processing = options.processing_handle();
    let h = handler.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let running = tokio::spawn(async move {
        consumer
            .run::<ExternalShutdownTopic, _>(h, (), options)
            .await
    });
    wait_for_stable_group(tb.brokers(), "kafka-external-shutdown-consumer", TIMEOUT).await;

    broker
        .publisher()
        .await
        .unwrap()
        .publish::<ExternalShutdownTopic>(&SimpleMessage {
            id: "held".into(),
            content: String::new(),
        })
        .await
        .unwrap();
    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "the record reached the handler, which is now waiting out its deferral"
    );
    assert!(
        processing.load(Ordering::Acquire),
        "a waiting handler holds its slot, so the member reads as busy"
    );

    shutdown.cancel();
    running
        .await
        .expect("consumer task panicked")
        .expect("the consumer ends cleanly on shutdown");
    assert!(
        !processing.load(Ordering::Acquire),
        "after a clean exit the member must not read as still working"
    );
    broker.close().await;
}

/// While the only prefetch slot is held by a handler waiting in place, the
/// member stays in its group and the loop keeps polling: the record published
/// during the wait arrives right after the redelivery, and the group reads
/// as stable throughout.
///
/// This test keeps the pinned five-minute limit and a four-second wait, so
/// it pins the polling discipline (a paused assignment that keeps calling
/// `recv()`), not the eviction. The next test lowers the limit through
/// `with_max_poll_interval_for_test` and pins the eviction.
#[tokio::test]
async fn external_topic_waiting_handlers_keep_the_member_in_the_group() {
    const TOPIC: &str = "kafka-external-keepalive";
    const GROUP: &str = "kafka-external-keepalive-consumer";
    let tb = TestBroker::start().await;
    provision_topic(tb.brokers(), TOPIC, 1).await;
    let broker = tb.broker();

    let handler = DeferOnceRecorder::new();
    let h = handler.clone();
    let mut group = broker.consumer_group();
    group
        .register::<ExternalKeepaliveTopic, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(1..=1)),
            move || h.clone(),
        )
        .await
        .unwrap();
    let token = group.cancellation_token();
    let running = tokio::spawn(
        group.run_until_timeout(token.clone().cancelled_owned(), Duration::from_secs(15)),
    );
    wait_for_stable_group(tb.brokers(), GROUP, TIMEOUT).await;

    let publisher = broker.publisher().await.unwrap();
    let msg = |id: &str| SimpleMessage {
        id: id.into(),
        content: String::new(),
    };
    publisher
        .publish::<ExternalKeepaliveTopic>(&msg("a"))
        .await
        .unwrap();
    assert!(handler.counter.wait_for(1, TIMEOUT).await, "first delivery");
    // "a" now waits four seconds holding the only slot; the assignment is
    // paused and the loop keeps polling.
    publisher
        .publish::<ExternalKeepaliveTopic>(&msg("b"))
        .await
        .unwrap();
    // The receive arm puts `b` back because the one slot is held by a waiting
    // handler; the probe is the signal that this happened, in place of a
    // sleep that only made it likely.
    #[cfg(feature = "test-support")]
    {
        use shove::kafka::put_back_probe;

        let put_back_deadline = Instant::now() + TIMEOUT;
        while put_back_probe::paused_receive() == 0 {
            assert!(
                Instant::now() < put_back_deadline,
                "the record received while the slot is held must be put back by the receive arm"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    wait_for_stable_group(tb.brokers(), GROUP, Duration::from_secs(5)).await;
    assert_eq!(
        handler.counter.get(),
        1,
        "nothing is handled while the slot is held"
    );

    assert!(
        handler.counter.wait_for(3, TIMEOUT).await,
        "redelivery, then the record published during the wait"
    );
    assert_eq!(
        handler.ids().await,
        vec!["a".to_string(), "a".to_string(), "b".to_string()]
    );
    assert_eq!(high_watermark(tb.brokers(), TOPIC, 0), 2);

    token.cancel();
    assert!(running.await.unwrap().is_clean());
    broker.close().await;
}

/// The rule the docs state: a record that arrives while every slot is held
/// by a running handler is decoded and waits for a slot while the loop keeps
/// polling, and a further record that arrives during that wait is handed
/// back to the broker and the assignment paused until a slot frees. With one
/// slot, `a` on partition 0 runs for three seconds, `b` on partition 1
/// arrives meanwhile and waits for the slot, and `c` on partition 0 arrives
/// during that wait. `c` must arrive again once the slot frees: the order is
/// `[a, a, b, c]`, nothing is republished, and both partitions commit.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn external_topic_record_received_during_the_permit_wait_is_put_back_and_redelivered() {
    use shove::kafka::put_back_probe;

    const TOPIC: &str = "kafka-external-permit-wait";
    const GROUP: &str = "kafka-external-permit-wait-consumer";
    let tb = TestBroker::start().await;
    provision_topic(tb.brokers(), TOPIC, 2).await;
    let broker = tb.broker();

    let handler = DeferOnceRecorder::new().running_first_for(Duration::from_secs(3));
    let h = handler.clone();
    let mut group = broker.consumer_group();
    group
        .register::<ExternalPermitWaitTopic, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(1..=1)),
            move || h.clone(),
        )
        .await
        .unwrap();
    let token = group.cancellation_token();
    let running = tokio::spawn(
        group.run_until_timeout(token.clone().cancelled_owned(), Duration::from_secs(15)),
    );
    wait_for_stable_group(tb.brokers(), GROUP, TIMEOUT).await;

    let record = |id: &str| {
        serde_json::to_vec(&SimpleMessage {
            id: id.into(),
            content: String::new(),
        })
        .unwrap()
    };
    publish_raw_to(tb.brokers(), TOPIC, 0, &record("a")).await;
    assert!(handler.counter.wait_for(1, TIMEOUT).await, "a is running");
    // `b` waits for the slot `a` holds; `c` arrives during that wait.
    publish_raw_to(tb.brokers(), TOPIC, 1, &record("b")).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    publish_raw_to(tb.brokers(), TOPIC, 0, &record("c")).await;
    // The probe proves that `c` went back to the broker from inside the
    // permit wait, rather than inferring it from the sleep above.
    let put_back_deadline = Instant::now() + TIMEOUT;
    while put_back_probe::permit_wait() == 0 {
        assert!(
            Instant::now() < put_back_deadline,
            "the record received during the permit wait must be put back from that wait"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        handler.counter.wait_for(4, TIMEOUT).await,
        "a's redelivery, then b, then the put-back c"
    );
    assert_eq!(
        handler.ids().await,
        vec![
            "a".to_string(),
            "a".to_string(),
            "b".to_string(),
            "c".to_string()
        ]
    );
    assert_eq!(
        high_watermark(tb.brokers(), TOPIC, 0),
        2,
        "nothing was republished"
    );
    assert_eq!(high_watermark(tb.brokers(), TOPIC, 1), 1);
    wait_for_zero_lag(&tb.client(), TOPIC, GROUP, TIMEOUT).await;
    assert_eq!(committed_offset(tb.brokers(), GROUP, TOPIC, 0), Some(2));
    assert_eq!(committed_offset(tb.brokers(), GROUP, TOPIC, 1), Some(1));

    token.cancel();
    assert!(running.await.unwrap().is_clean());
    broker.close().await;
}

/// A record queued behind a running handler must not park the receive loop.
/// With one slot, "a" is still running when "b" arrives, so no handler is
/// waiting yet and the loop cannot pause up front. Before the permit was
/// acquired inside a polling `select!`, the loop then waited for it without
/// calling `recv()`, and when "a" turned into a 25 s in-place wait the member
/// was evicted at `max.poll.interval.ms`. With that limit lowered to its
/// 10 s floor through the `test-support` seam, the group must still read as
/// `Stable` with one member well past the limit, the order must be
/// `[a, a, b]`, and the topic must gain no republished copy.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn external_topic_queued_record_keeps_the_member_when_a_running_handler_starts_waiting() {
    const TOPIC: &str = "kafka-external-transition";
    const GROUP: &str = "kafka-external-transition-consumer";
    let tb = TestBroker::start().await;
    provision_topic(tb.brokers(), TOPIC, 1).await;
    let broker = tb.broker();

    // Both records sit on the topic before the member joins, so "b" is in
    // the loop's hand while "a" is still running.
    let publisher = broker.publisher().await.unwrap();
    for id in ["a", "b"] {
        publisher
            .publish::<ExternalTransitionTopic>(&SimpleMessage {
                id: id.into(),
                content: String::new(),
            })
            .await
            .unwrap();
    }

    let handler = DeferOnceRecorder::new().running_first_for(Duration::from_secs(3));
    let h = handler.clone();
    let mut group = broker.consumer_group();
    group
        .register::<ExternalTransitionTopic, _>(
            ConsumerGroupConfig::new(
                KafkaConsumerGroupConfig::new(1..=1)
                    .with_max_poll_interval_for_test(Duration::from_secs(10)),
            ),
            move || h.clone(),
        )
        .await
        .unwrap();
    let token = group.cancellation_token();
    let running = tokio::spawn(
        group.run_until_timeout(token.clone().cancelled_owned(), Duration::from_secs(15)),
    );

    assert!(handler.counter.wait_for(1, TIMEOUT).await, "first delivery");
    // "a" runs for 3 s and then waits 25 s in place; "b" waits for the slot
    // throughout. Well past the 10 s poll limit the member must still be in
    // its group, and nothing else may have been handled.
    tokio::time::sleep(Duration::from_secs(18)).await;
    assert_eq!(
        group_state(tb.brokers(), GROUP),
        ("Stable".to_string(), 1),
        "the member must stay in the group while it waits for a permit"
    );
    assert_eq!(
        handler.counter.get(),
        1,
        "nothing else is handled while the slot is held"
    );

    assert!(
        handler.counter.wait_for(3, TIMEOUT).await,
        "the redelivery of a, then b"
    );
    assert_eq!(
        handler.ids().await,
        vec!["a".to_string(), "a".to_string(), "b".to_string()]
    );
    assert_eq!(high_watermark(tb.brokers(), TOPIC, 0), 2);

    token.cancel();
    assert!(running.await.unwrap().is_clean());
    broker.close().await;
}

/// A missing external topic is a startup error, not a silent auto-create:
/// `declare` returns `Topology`, the registry path surfaces the same error,
/// and the topic is still absent afterwards, so the verification fetch
/// itself created nothing.
#[tokio::test]
async fn external_topic_missing_fails_fast_at_declare() {
    const TOPIC: &str = "kafka-external-missing";
    let tb = TestBroker::start().await;
    let broker = tb.broker();

    let err = broker
        .topology()
        .declare::<ExternalMissingTopic>()
        .await
        .expect_err("declare must refuse a topic nobody provisioned");
    assert!(
        matches!(err, shove::ShoveError::Topology(_)),
        "expected Topology, got {err:?}"
    );
    assert!(
        err.to_string().contains("external()") && err.to_string().contains("must be provisioned"),
        "{err}"
    );
    assert_eq!(
        live_partition_count(tb.brokers(), TOPIC),
        None,
        "the verification fetch must not auto-create the topic"
    );

    let mut group = broker.consumer_group();
    let err = group
        .register::<ExternalMissingTopic, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(1..=1)),
            CountingHandler::new,
        )
        .await
        .expect_err("register must refuse a topic nobody provisioned");
    assert!(
        matches!(err, shove::ShoveError::Topology(_)),
        "expected Topology, got {err:?}"
    );
    assert_eq!(live_partition_count(tb.brokers(), TOPIC), None);
    broker.close().await;
}

// Registry default handler timeout reaches a registered handler when the
// per-group config does NOT call `with_handler_timeout`. Mirrors the
// raw-consumer `handler_timeout_triggers_retry` test but exercises the
// registry pre-resolution path.
#[tokio::test]
async fn registry_default_handler_timeout_triggers_retry() {
    use shove::ConsumerGroupConfig;

    #[derive(Clone)]
    struct TimeoutThenAck(WaitableCounter);

    impl MessageHandler<WorkTopic> for TimeoutThenAck {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            let attempt = self.0.get();
            self.0.increment();
            if attempt == 0 {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "default-timeout-1".into(),
            content: "default timeout".into(),
        })
        .await
        .unwrap();

    let handler = TimeoutThenAck(WaitableCounter::new());
    let counter = handler.0.clone();
    let factory_handler = handler.clone();

    let mut group = broker
        .consumer_group()
        .with_default_handler_timeout(Duration::from_millis(500));
    group
        .register::<WorkTopic, _>(
            ConsumerGroupConfig::new(
                KafkaConsumerGroupConfig::new(1..=1)
                    .with_prefetch_count(1)
                    .with_max_retries(5),
            ),
            move || factory_handler.clone(),
        )
        .await
        .unwrap();

    let token = group.cancellation_token();
    let cancel = token.clone();
    let observer = tokio::spawn(async move {
        if counter.wait_for(2, TIMEOUT).await {
            cancel.cancel();
        }
    });

    let outcome = group
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(10))
        .await;
    observer.await.unwrap();
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert!(
        handler.0.get() >= 2,
        "expected >=2 invocations (timeout+retry) via registry default, got {}",
        handler.0.get()
    );
    broker.close().await;
}

// ===========================================================================
// Post-cancel drain latency
// ===========================================================================

/// Total records appended to `__consumer_offsets` so far, summed over its
/// partitions' high watermarks. Every OffsetCommit request the broker accepts
/// appends one record per partition it covers (and compaction never lowers a
/// high watermark), so the delta across a consume run counts the commits a
/// consumer actually issued — independent of build profile, host load, or how
/// fast the coordinator happened to answer.
///
/// Returns 0 while the topic does not exist yet (nothing has committed); any
/// actual fetch failure panics rather than returning 0, because a silently
/// zeroed instrument would let the commit-count assertion pass vacuously.
fn offsets_topic_records(brokers: &str) -> u64 {
    use rdkafka::consumer::{BaseConsumer, Consumer};

    const OFFSETS_TOPIC: &str = "__consumer_offsets";
    let probe: BaseConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("probe consumer");
    let metadata = probe
        .fetch_metadata(Some(OFFSETS_TOPIC), Duration::from_secs(10))
        .expect("fetch __consumer_offsets metadata");
    metadata
        .topics()
        .iter()
        .filter(|t| t.name() == OFFSETS_TOPIC)
        .flat_map(|t| t.partitions())
        .map(|p| {
            probe
                .fetch_watermarks(OFFSETS_TOPIC, p.id(), Duration::from_secs(10))
                .map(|(_, high)| high.max(0) as u64)
                .unwrap_or_else(|e| {
                    panic!(
                        "fetch_watermarks(__consumer_offsets, {}) failed: {e}",
                        p.id()
                    )
                })
        })
        .sum()
}

/// A consumer that acks a large corpus quickly must not stack up offset
/// commits — and must still shut down promptly.
///
/// The receive loop used to issue one fire-and-forget async offset commit per
/// iteration that advanced any offset: ~1 commit per message consumed. When
/// the consume rate outruns the coordinator's replies those pile up in
/// flight, and librdkafka's consumer close (which runs inside `Drop` and
/// cannot be aborted) waits for every one before terminating. With 150,000
/// messages the bench harness had ~49k commits in flight at cancel and
/// post-cancel drains of 5.5–20.2 s across 11 runs on an 8-core host —
/// intermittently blowing a 30 s drain budget.
///
/// Whether the pile-up (and so the slow close) manifests depends on
/// coordinator round-trip time × commit rate, which a fresh local broker
/// usually keeps under the tipping point — so the drain wall clock alone is
/// a vacuous guard here. The red/green driver is instead the number of
/// commits *issued*, counted as records appended to `__consumer_offsets`:
/// the ungated loop writes on the order of one record per message, the
/// interval-gated loop one commit round (≤ one record per partition) per
/// `ASYNC_COMMIT_INTERVAL`. The wall-clock bound stays as the symptom-level
/// backstop — on any broker slow enough to pile commits, the broken loop
/// blows it.
///
/// Known residual: the red power needs genuine handler/loop concurrency. On
/// a badly starved runner the scheduler can serialize handlers the way the
/// current-thread runtime does, batching completions and keeping even the
/// ungated loop's commit count low — the test then passes on broken code.
/// It cannot false-fail that way (the fixed path's count only shrinks); the
/// deterministic red venue is any host with a few free cores.
// Multi-thread runtime, deliberately: on the default current-thread runtime
// the spawned handlers only run when the receive loop yields, so completions
// arrive in prefetch-sized batches and the loop commits once per ~100
// messages — the per-iteration commit pattern under test never engages.
// Concurrent workers deliver completions continuously, like production.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_cancel_drain_is_bounded_after_a_large_fast_corpus() {
    const CORPUS: u64 = 150_000;
    const PUBLISH_CHUNK: u64 = 10_000;
    // Wide enough that a loaded host's LeaveGroup + close handshake cannot
    // trip it on healthy code (fixed path measured ~0.1-0.2 s), while the
    // broken pile-up measured 23.8-34.5 s at this corpus.
    const DRAIN_BOUND: Duration = Duration::from_secs(10);
    // 8 cores, ~11.5k msg/s in the unoptimized test profile locally; a
    // coverage-instrumented run on a starved CI runner can be several times
    // slower, so the budget is generous rather than calibrated tight.
    const CONSUME_BUDGET: Duration = Duration::from_secs(300);

    #[derive(Clone)]
    struct InstantAck(Arc<AtomicU64>);

    impl MessageHandler<DrainTopic> for InstantAck {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::Relaxed);
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    // Eight partitions, matching the shape the defect was observed under (a
    // single member of a group whose topic was sized for eight consumers):
    // parallel fetch across partitions is part of what pushes the consume
    // rate past the coordinator's commit-ack rate.
    let declarer = KafkaTopologyDeclarer::new(tb.client()).with_min_partitions(8);
    declarer.declare(DrainTopic::topology()).await.unwrap();

    // Prefill the topic completely BEFORE the consumer starts: the pile-up
    // needs the receive loop running at full speed, issuing commits far
    // faster than the coordinator answers them. A consumer racing the
    // publisher is throttled to publish rate, which gives the coordinator
    // room to keep up and hides the defect.
    let publisher = broker.publisher().await.unwrap();
    let mut next = 0u64;
    while next < CORPUS {
        let end = (next + PUBLISH_CHUNK).min(CORPUS);
        let chunk: Vec<SimpleMessage> = (next..end)
            .map(|i| SimpleMessage {
                id: i.to_string(),
                content: String::new(),
            })
            .collect();
        publisher.publish_batch::<DrainTopic>(&chunk).await.unwrap();
        next = end;
    }

    let commit_records_before = offsets_topic_records(tb.client.brokers());

    let processed = Arc::new(AtomicU64::new(0));
    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<DrainTopic, _>(
            InstantAck(processed.clone()),
            ConsumerOptions::<Kafka>::new()
                .with_prefetch_count(100)
                .with_concurrent_processing(true),
        )
        .unwrap();
    let token = supervisor.cancellation_token();
    let consume_started = Instant::now();
    let sup_handle =
        tokio::spawn(supervisor.run_until_timeout(std::future::pending(), Duration::from_secs(30)));

    let consume_deadline = Instant::now() + CONSUME_BUDGET;
    while processed.load(Ordering::Relaxed) < CORPUS {
        assert!(
            Instant::now() < consume_deadline,
            "consumed only {} of {CORPUS} within {CONSUME_BUDGET:?}",
            processed.load(Ordering::Relaxed)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let cancelled_at = Instant::now();
    let consume_took = consume_started.elapsed();
    token.cancel();
    // Hard cap on the join: a regressed pile-up makes rdkafka's close drain
    // in-flight commits for however long they take (unbounded on a slow
    // coordinator), and the supervisor's abort cannot preempt the blocking
    // FFI — without this the test hangs instead of failing.
    let outcome = tokio::time::timeout(Duration::from_secs(120), sup_handle)
        .await
        .expect("supervisor did not shut down within 120s of cancel — consumer close is stuck draining in-flight offset commits")
        .unwrap();
    let drain = cancelled_at.elapsed();

    let commit_records_after = offsets_topic_records(tb.client.brokers());
    assert!(
        commit_records_after >= commit_records_before,
        "__consumer_offsets high watermarks went backwards \
         ({commit_records_before} -> {commit_records_after}); the instrument is broken"
    );
    let commit_records = commit_records_after - commit_records_before;
    // The gated loop issues at most one commit round (<= 8 partition records)
    // per ASYNC_COMMIT_INTERVAL (500ms), so the fixed path's record count
    // scales with consume duration: bound it that way rather than with a
    // constant a slow run could legitimately cross. Measured here: gated 32-35
    // records over ~13s (budget ~1400); ungated 79,818 and 117,106 — two
    // orders of magnitude over any admissible duration's budget.
    let commit_record_bound = 1_000 + 32 * consume_took.as_secs();
    eprintln!(
        "consumed {CORPUS} in {consume_took:?} ({:.0} msg/s); post-cancel drain {drain:?}; \
         {commit_records} offset-commit records written (bound {commit_record_bound})",
        CORPUS as f64 / consume_took.as_secs_f64()
    );

    assert!(
        commit_records > 0,
        "no offset-commit records observed at all — the instrument is measuring nothing"
    );
    assert!(
        commit_records < commit_record_bound,
        "consumer wrote {commit_records} offset-commit records for a {CORPUS}-message corpus \
         (bound {commit_record_bound}); the receive loop is committing per iteration instead \
         of on the ASYNC_COMMIT_INTERVAL gate, which piles up in-flight commits that consumer \
         close then waits out (measured ungated: 79,818 and 117,106 records; gated: 32-35)"
    );
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert!(
        drain < DRAIN_BOUND,
        "post-cancel drain took {drain:?} (bound {DRAIN_BOUND:?}); either in-flight offset \
         commits are stacking up and consumer close is waiting them out (check the \
         commit-record count above), or the broker/host is badly starved"
    );
    broker.close().await;
}

// ===========================================================================
// Edge cases
// ===========================================================================

#[tokio::test]
async fn run_dlq_on_topic_without_dlq_fails() {
    struct Noop;
    impl MessageHandler<NoDlqTopic> for Noop {
        type Context = ();
        async fn handle(&self, _: SimpleMessage, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let client = tb.client();
    let consumer = KafkaConsumer::new(client.clone());

    let result = consumer.run_dlq::<NoDlqTopic, _>(Noop, ()).await;
    assert!(result.is_err(), "run_dlq on topic without DLQ should fail");
    tb.broker().close().await;
}

#[tokio::test]
async fn defer_without_hold_queues_redelivers() {
    struct DeferThenAck(WaitableCounter);

    impl MessageHandler<DeferNoHoldTopic> for DeferThenAck {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            let prev = self.0.get();
            self.0.increment();
            if prev == 0 {
                Outcome::Defer
            } else {
                Outcome::Ack
            }
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<DeferNoHoldTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<DeferNoHoldTopic>(&SimpleMessage {
            id: "defer-nohold".into(),
            content: "test".into(),
        })
        .await
        .unwrap();

    let counter = WaitableCounter::new();
    let handler = DeferThenAck(counter.clone());

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<DeferNoHoldTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(60)).await,
        "should be called at least 2 times"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

#[tokio::test]
async fn defer_preserves_retry_count() {
    struct DeferCheckRetry {
        counter: WaitableCounter,
        retry_counts: Arc<Mutex<Vec<u32>>>,
    }

    impl MessageHandler<WorkTopic> for DeferCheckRetry {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, meta: MessageMetadata, _: &()) -> Outcome {
            self.retry_counts.lock().await.push(meta.retry_count);
            let call = self.counter.get();
            self.counter.increment();
            match call {
                0 => Outcome::Retry, // retry_count becomes 1
                1 => Outcome::Defer, // retry_count should still be 1
                _ => Outcome::Ack,
            }
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "defer-retry".into(),
            content: "test".into(),
        })
        .await
        .unwrap();

    let counter = WaitableCounter::new();
    let retry_counts = Arc::new(Mutex::new(Vec::new()));
    let handler = DeferCheckRetry {
        counter: counter.clone(),
        retry_counts: retry_counts.clone(),
    };

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(10)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        counter.wait_for(3, Duration::from_secs(60)).await,
        "should be called 3 times"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();

    let counts = retry_counts.lock().await;
    assert_eq!(counts[0], 0, "first call: retry_count should be 0");
    assert_eq!(
        counts[1], 1,
        "second call (after Retry): retry_count should be 1"
    );
    assert_eq!(
        counts[2], 1,
        "third call (after Defer): retry_count should still be 1"
    );

    broker.close().await;
}

// ===========================================================================
// Deserialization failure
// ===========================================================================

#[tokio::test]
async fn deserialization_failure_rejects_to_dlq() {
    use rdkafka::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer as RdkafkaConsumer};

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    // Publish raw invalid JSON directly via the client routing method
    use rdkafka::message::OwnedHeaders;
    client
        .publish_with_retry(
            "kafka-work",
            None,
            OwnedHeaders::new(),
            b"not valid json",
            1,
            "test raw publish",
        )
        .await
        .expect("raw publish should succeed");

    // Start consumer — should reject the bad message to DLQ
    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    // Verify the message lands in the DLQ topic by consuming raw bytes.
    // The DLQ payload is un-deserializable (that's why it was rejected),
    // so we read directly with a BaseConsumer instead of run_dlq.
    let dlq_topic = WorkTopic::topology().dlq().expect("WorkTopic has a DLQ");
    let brokers = client.brokers().to_string();
    let received = tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", "test-dlq-verify")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("DLQ verify consumer");
        consumer
            .subscribe(&[dlq_topic])
            .expect("subscribe to DLQ topic");

        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            if std::time::Instant::now() > deadline {
                return None;
            }
            if let Some(result) = consumer.poll(Duration::from_secs(1)) {
                let msg = result.expect("DLQ message");
                let payload = msg.payload().unwrap_or_default().to_vec();
                return Some(payload);
            }
        }
    })
    .await
    .expect("spawn_blocking join");

    let payload = received.expect("malformed message should land in DLQ");
    assert_eq!(
        payload, b"not valid json",
        "DLQ should contain original payload"
    );
    assert_eq!(
        handler.counter.get(),
        0,
        "handler should not be called for bad JSON"
    );

    shutdown.cancel();
    broker.close().await;
    handle.await.unwrap().ok();
}

// ===========================================================================
// Lag stats provider
// ===========================================================================

#[tokio::test]
async fn lag_stats_provider_reports_pending_messages() {
    use shove::kafka::{KafkaAutoOffsetReset, KafkaLagStatsProvider, KafkaQueueStatsProvider};

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..5 {
        publisher
            .publish::<WorkTopic>(&SimpleMessage {
                id: format!("lag-{i}"),
                content: "test".into(),
            })
            .await
            .unwrap();
    }

    let stats_provider = KafkaLagStatsProvider::new(client.clone());
    let stats: KafkaQueueStats = stats_provider
        .get_queue_stats(
            "kafka-work",
            "kafka-work-consumer",
            KafkaAutoOffsetReset::Earliest,
        )
        .await
        .expect("get_queue_stats should succeed");

    assert!(
        stats.messages_pending >= 5,
        "should report at least 5 pending messages, got {}",
        stats.messages_pending
    );

    broker.close().await;
}

#[tokio::test]
async fn lag_stats_provider_reports_zero_after_consumption() {
    use shove::kafka::{KafkaAutoOffsetReset, KafkaLagStatsProvider, KafkaQueueStatsProvider};

    shove::define_topic!(
        LagTestTopic,
        SimpleMessage,
        TopologyBuilder::new("kafka-lag-test").dlq().build()
    );

    impl MessageHandler<LagTestTopic> for CountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.counter.increment();
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<LagTestTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..3 {
        publisher
            .publish::<LagTestTopic>(&SimpleMessage {
                id: format!("lag-zero-{i}"),
                content: "test".into(),
            })
            .await
            .unwrap();
    }

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<LagTestTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(3, TIMEOUT).await,
        "should consume all 3 messages"
    );

    // Shut down the consumer so it performs its final offset commit.
    shutdown.cancel();
    handle.await.unwrap().ok();

    // Poll until committed offsets are visible (async commit may take a moment).
    let stats_provider = KafkaLagStatsProvider::new(client.clone());
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let stats: KafkaQueueStats = stats_provider
            .get_queue_stats(
                "kafka-lag-test",
                "kafka-lag-test-consumer",
                KafkaAutoOffsetReset::Earliest,
            )
            .await
            .expect("get_queue_stats should succeed");
        if stats.messages_pending == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "lag should be 0 after consuming all messages, still got {}",
            stats.messages_pending
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    broker.close().await;
}

#[tokio::test]
async fn committed_offsets_advance_while_consumer_is_idle() {
    use shove::kafka::{KafkaAutoOffsetReset, KafkaLagStatsProvider, KafkaQueueStatsProvider};

    shove::define_topic!(
        IdleCommitTopic,
        SimpleMessage,
        TopologyBuilder::new("kafka-idle-commit").dlq().build()
    );

    impl MessageHandler<IdleCommitTopic> for CountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.counter.increment();
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<IdleCommitTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..3 {
        publisher
            .publish::<IdleCommitTopic>(&SimpleMessage {
                id: format!("idle-commit-{i}"),
                content: "test".into(),
            })
            .await
            .unwrap();
    }

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<IdleCommitTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(3, TIMEOUT).await,
        "should consume all 3 messages"
    );

    // The consumer stays running and no further messages arrive. Handler
    // completions alone must drive the offset commits — a crash or rebalance
    // in this state must not redeliver the already-processed batch.
    let stats_provider = KafkaLagStatsProvider::new(client.clone());
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let stats: KafkaQueueStats = stats_provider
            .get_queue_stats(
                "kafka-idle-commit",
                "kafka-idle-commit-consumer",
                KafkaAutoOffsetReset::Earliest,
            )
            .await
            .expect("get_queue_stats should succeed");
        if stats.messages_pending == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "offsets must be committed while the consumer is idle (no new \
             traffic, no shutdown), still got lag {}",
            stats.messages_pending
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// A transactional producer leaves a control record at the end of every
/// transaction, at an offset no consumer ever receives. Log compaction leaves
/// the same kind of hole. The tracker must commit past such a hole as soon as
/// every *delivered* record below it has completed, instead of waiting for
/// an offset that will never arrive.
///
/// Every record carries the same key so one partition holds the whole
/// sequence: data, data, control, data, data, control, data, data, control.
/// The consumer receives six records; the committed position must reach the
/// last control record (highest delivered plus one), and a second member of
/// the same group must then receive only records produced after it.
#[tokio::test]
async fn transactional_gaps_do_not_stall_commits() {
    use rdkafka::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer as RdkafkaConsumer};
    use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
    use rdkafka::util::Timeout;
    use rdkafka::{Offset, TopicPartitionList};

    shove::define_topic!(
        TxnGapsTopic,
        SimpleMessage,
        TopologyBuilder::new("kafka-txn-gaps").build()
    );

    impl MessageHandler<TxnGapsTopic> for CountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.counter.increment();
            Outcome::Ack
        }
    }

    const TOPIC: &str = "kafka-txn-gaps";
    const GROUP: &str = "kafka-txn-gaps-consumer";
    const KEY: &str = "one-partition";
    let rpc = Timeout::After(Duration::from_secs(10));

    let tb = TestBroker::start_with_transactions().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<TxnGapsTopic>().await.unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", tb.brokers())
        .set("transactional.id", "kafka-txn-gaps-producer")
        .create()
        .expect("failed to create transactional producer");
    producer
        .init_transactions(rpc)
        .expect("init_transactions failed");

    // Three transactions of two records each: offsets 0,1 | 3,4 | 6,7 are
    // data and 2, 5, 8 are control records.
    let mut produced = 0;
    for txn in 0..3 {
        producer
            .begin_transaction()
            .expect("begin_transaction failed");
        for i in 0..2 {
            let payload = serde_json::to_string(&SimpleMessage {
                id: format!("txn-{txn}-{i}"),
                content: "gap".into(),
            })
            .unwrap();
            producer
                .send(
                    FutureRecord::to(TOPIC).key(KEY).payload(&payload),
                    Timeout::After(Duration::from_secs(10)),
                )
                .await
                .expect("transactional publish failed");
            produced += 1;
        }
        producer
            .commit_transaction(rpc)
            .expect("commit_transaction failed");
    }
    assert_eq!(produced, 6);

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<TxnGapsTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10),
            )
            .await
    });
    assert!(
        handler.counter.wait_for(6, TIMEOUT).await,
        "should consume all 6 data records"
    );

    // Find the one partition the key landed on and its high watermark, then
    // poll the committed offset until it sits at the trailing control record.
    let probe: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", tb.brokers())
        .set("group.id", GROUP)
        .create()
        .expect("failed to create committed-offset probe");
    let metadata = probe
        .fetch_metadata(Some(TOPIC), Duration::from_secs(10))
        .expect("metadata");
    let partitions: Vec<i32> = metadata.topics()[0]
        .partitions()
        .iter()
        .map(|p| p.id())
        .collect();
    let (partition, high) = partitions
        .iter()
        .map(|&p| {
            let (_, high) = probe
                .fetch_watermarks(TOPIC, p, Duration::from_secs(10))
                .expect("watermarks");
            (p, high)
        })
        .find(|&(_, high)| high > 0)
        .expect("one partition holds the keyed records");
    assert_eq!(high, 9, "6 data records plus 3 control records");

    let committed_offset = || {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(TOPIC, partition);
        probe
            .committed_offsets(tpl, Duration::from_secs(10))
            .expect("committed_offsets")
            .find_partition(TOPIC, partition)
            .and_then(|e| match e.offset() {
                Offset::Offset(o) => Some(o),
                _ => None,
            })
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let committed = committed_offset();
        if committed == Some(high - 1) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "committed offset must reach the trailing control record ({}), got {committed:?}",
            high - 1
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    shutdown.cancel();
    handle.await.unwrap().ok();

    // A second member under the same group resumes at the committed position
    // and must see only what is produced from now on.
    let handler2 = CountingHandler::new();
    let hc2 = handler2.clone();
    let shutdown2 = CancellationToken::new();
    let sc2 = shutdown2.clone();
    let consumer2 = KafkaConsumer::new(client.clone());
    let handle2 = tokio::spawn(async move {
        consumer2
            .run::<TxnGapsTopic, _>(hc2, (), ConsumerOptions::<Kafka>::new().with_shutdown(sc2))
            .await
    });
    producer
        .begin_transaction()
        .expect("begin_transaction failed");
    let payload = serde_json::to_string(&SimpleMessage {
        id: "after-restart".into(),
        content: "gap".into(),
    })
    .unwrap();
    producer
        .send(
            FutureRecord::to(TOPIC).key(KEY).payload(&payload),
            Timeout::After(Duration::from_secs(10)),
        )
        .await
        .expect("transactional publish failed");
    producer
        .commit_transaction(rpc)
        .expect("commit_transaction failed");

    assert!(
        handler2.counter.wait_for(1, TIMEOUT).await,
        "the new record must arrive"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        handler2.counter.get(),
        1,
        "nothing below the committed position may be redelivered"
    );

    shutdown2.cancel();
    handle2.await.unwrap().ok();
    broker.close().await;
}

/// `with_commit_interval` widens the commit gate. With a 5 s interval the
/// first drain still commits at once (the gate opens due), so the second
/// batch's completions sit uncommitted for about one interval before the
/// gate reopens - where the default 500 ms gate would have committed them
/// within a second.
#[tokio::test]
async fn commit_interval_bounds_how_far_committed_offsets_lag() {
    use shove::kafka::{KafkaAutoOffsetReset, KafkaLagStatsProvider, KafkaQueueStatsProvider};

    shove::define_topic!(
        CommitIntervalTopic,
        SimpleMessage,
        TopologyBuilder::new("kafka-commit-interval").build()
    );

    impl MessageHandler<CommitIntervalTopic> for CountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.counter.increment();
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<CommitIntervalTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();
    let publish = |i: usize| {
        let publisher = &publisher;
        async move {
            publisher
                .publish::<CommitIntervalTopic>(&SimpleMessage {
                    id: format!("interval-{i}"),
                    content: "gate".into(),
                })
                .await
                .unwrap();
        }
    };
    for i in 0..3 {
        publish(i).await;
    }

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<CommitIntervalTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10)
                    .with_commit_interval(Duration::from_secs(5)),
            )
            .await
    });
    assert!(
        handler.counter.wait_for(3, TIMEOUT).await,
        "should consume the first batch"
    );

    let stats_provider = KafkaLagStatsProvider::new(client.clone());
    let lag = || async {
        stats_provider
            .get_queue_stats(
                "kafka-commit-interval",
                "kafka-commit-interval-consumer",
                KafkaAutoOffsetReset::Earliest,
            )
            .await
            .expect("get_queue_stats should succeed")
            .messages_pending
    };
    // The first drain commits at once: the gate is due until a commit has
    // been issued, whatever the interval.
    let deadline = Instant::now() + TIMEOUT;
    while lag().await != 0 {
        assert!(
            Instant::now() < deadline,
            "the first batch must commit on the opening drain"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // The second batch completes inside the 5 s window opened by that commit
    // and must still be uncommitted one second later.
    for i in 3..6 {
        publish(i).await;
    }
    assert!(
        handler.counter.wait_for(6, TIMEOUT).await,
        "should consume the second batch"
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        lag().await,
        3,
        "a 5 s gate must hold the second batch's commit for the interval"
    );

    // ...and commit once the interval elapses.
    let deadline = Instant::now() + TIMEOUT;
    while lag().await != 0 {
        assert!(
            Instant::now() < deadline,
            "the gate must reopen after the interval and commit the batch"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// The child half of [`shutdown_exits_the_process_while_the_broker_is_frozen`].
///
/// Re-invoked by the parent test through `current_exe()` with
/// `--ignored --exact`, so it is never run on its own. It consumes one
/// record with a commit gate too wide to ever reopen, reports that it is
/// ready, consumes a second record whose completion therefore stays
/// uncommitted, and shuts its consumer down when a line arrives on stdin.
/// The parent has frozen the broker by then, so the final synchronous commit
/// blocks: the consumer must give up at `SHUTDOWN_COMMIT_DEADLINE` and this
/// process must exit while the commit thread is still stuck.
#[tokio::test]
#[ignore = "child process of shutdown_exits_the_process_while_the_broker_is_frozen"]
async fn child_consumes_then_shuts_down_on_stdin() {
    use std::io::Write as _;

    shove::define_topic!(
        FrozenShutdownTopic,
        SimpleMessage,
        TopologyBuilder::new("kafka-frozen-shutdown").build()
    );

    impl MessageHandler<FrozenShutdownTopic> for CountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.counter.increment();
            Outcome::Ack
        }
    }

    let bootstrap = std::env::var("SHOVE_TEST_KAFKA_BOOTSTRAP")
        .expect("SHOVE_TEST_KAFKA_BOOTSTRAP is set by the parent test");
    // The parent asserts on the deadline warning, which needs a subscriber.
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(std::io::stderr)
        .try_init();

    let client = KafkaClient::connect_with_retry(&KafkaConfig::new(&bootstrap), 10)
        .await
        .expect("child failed to connect to Kafka");
    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let run = tokio::spawn(async move {
        consumer
            .run::<FrozenShutdownTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    // Wide enough that the second record's completion can only
                    // be committed by the final commit at shutdown.
                    .with_commit_interval(Duration::from_secs(3600)),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "child must receive the first record"
    );
    println!("ready");
    std::io::stdout().flush().unwrap();
    assert!(
        handler.counter.wait_for(2, TIMEOUT).await,
        "child must receive the second record"
    );
    println!("handled 2");
    std::io::stdout().flush().unwrap();

    // The parent writes a line once the broker is paused. EOF counts too.
    tokio::task::spawn_blocking(|| {
        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
    })
    .await
    .unwrap();

    let started = Instant::now();
    shutdown.cancel();
    run.await.unwrap().expect("run returns Ok after shutdown");
    println!("run returned after {} ms", started.elapsed().as_millis());
    std::io::stdout().flush().unwrap();
    // Return the way a service's `main` does. The commit thread is still
    // blocked on the frozen broker; that this process nevertheless exits, which
    // the parent waits for, is what proves the thread holds neither the
    // runtime nor the process.
}

/// The final commit and the consumer's close run on a dedicated thread with a
/// `SHUTDOWN_COMMIT_DEADLINE` bound, so a frozen coordinator cannot hold a
/// shutting-down process past that deadline. Proven with a real process
/// exit: a child consumer is driven to have an uncommitted completion, the
/// broker is paused, the child is told to shut down, and it must exit within
/// the deadline plus a margin while the broker stays paused. The parent
/// unpauses only after the exit.
// `test-support` gates the deadline seam this test reads; the Kafka coverage
// row enables it, and the schema-registry row, which compiles this binary
// without it, never runs this suite.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn shutdown_exits_the_process_while_the_broker_is_frozen() {
    use shove::kafka::shutdown_commit_deadline_for_test;
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    shove::define_topic!(
        FrozenShutdownParentTopic,
        SimpleMessage,
        TopologyBuilder::new("kafka-frozen-shutdown").build()
    );

    // The receive loop's own shutdown commit deadline, read through the
    // `test-support` seam so this test cannot drift from the constant it
    // asserts the child's elapsed time against.
    let deadline: Duration = shutdown_commit_deadline_for_test();
    const MARGIN: Duration = Duration::from_secs(15);

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<FrozenShutdownParentTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();
    let publish = |id: &'static str| {
        let publisher = &publisher;
        async move {
            publisher
                .publish::<FrozenShutdownParentTopic>(&SimpleMessage {
                    id: id.into(),
                    content: "frozen".into(),
                })
                .await
                .unwrap();
        }
    };
    publish("first").await;

    let exe = std::env::current_exe().expect("current_exe");
    let mut child = tokio::process::Command::new(exe)
        .args([
            "--ignored",
            "--exact",
            "child_consumes_then_shuts_down_on_stdin",
            "--nocapture",
        ])
        .env("SHOVE_TEST_KAFKA_BOOTSTRAP", tb.brokers())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn child test process");
    let mut stdin = child.stdin.take().expect("child stdin");
    let mut stdout = BufReader::new(child.stdout.take().expect("child stdout")).lines();
    let stderr = child.stderr.take().expect("child stderr");

    async fn wait_for_line(
        lines: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
        wanted: &str,
    ) {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let next = tokio::time::timeout_at(deadline, lines.next_line()).await;
            match next {
                Ok(Ok(Some(line))) if line.trim() == wanted => return,
                Ok(Ok(Some(_))) => continue,
                other => panic!("child did not print {wanted:?}: {other:?}"),
            }
        }
    }

    wait_for_line(&mut stdout, "ready").await;
    // The first drain committed offset 1. This completion stays uncommitted
    // behind the child's hour-long gate, so the final commit has work to do.
    publish("second").await;
    wait_for_line(&mut stdout, "handled 2").await;

    tb.pause().await;
    let exit = async {
        stdin.write_all(b"\n").await.expect("write shutdown line");
        stdin.flush().await.expect("flush shutdown line");
        drop(stdin);
        let status = tokio::time::timeout(deadline + MARGIN, child.wait()).await;
        // Drain the pipes after the exit so nothing here waits on a live child.
        let mut elapsed_ms = None;
        while let Ok(Ok(Some(line))) =
            tokio::time::timeout(Duration::from_secs(5), stdout.next_line()).await
        {
            if let Some(ms) = line.strip_prefix("run returned after ") {
                elapsed_ms = ms.trim_end_matches(" ms").parse::<u128>().ok();
            }
        }
        let mut stderr_text = String::new();
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::io::AsyncReadExt::read_to_string(&mut BufReader::new(stderr), &mut stderr_text),
        )
        .await;
        (status, elapsed_ms, stderr_text)
    }
    .await;
    // Only now may the broker run again: the assertions below are about what
    // the child managed while it was frozen.
    tb.unpause().await;

    let (status, elapsed_ms, stderr_text) = exit;
    let status = status
        .expect(
            "child must exit within the shutdown deadline plus margin while the broker is frozen",
        )
        .expect("child wait");
    assert!(
        status.success(),
        "child exited with {status}, stderr: {stderr_text}"
    );
    let elapsed_ms = elapsed_ms.expect("child reports how long run took to return");
    assert!(
        Duration::from_millis(elapsed_ms as u64) >= deadline - Duration::from_secs(1),
        "run must wait out the deadline while the commit is blocked, took {elapsed_ms} ms"
    );
    assert!(
        stderr_text.contains("did not finish within the shutdown deadline"),
        "child stderr must carry the deadline warning: {stderr_text}"
    );

    broker.close().await;
}

// ===========================================================================
// Partition expansion
// ===========================================================================

#[tokio::test]
async fn topology_expands_partitions_on_redeclare() {
    shove::define_topic!(
        ExpandTopic,
        SimpleMessage,
        TopologyBuilder::new("kafka-expand-test").build()
    );

    impl MessageHandler<ExpandTopic> for CountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.counter.increment();
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<ExpandTopic>().await.unwrap();

    // Re-declare with higher min_partitions to trigger ensure_partitions
    let declarer = KafkaTopologyDeclarer::new(client.clone()).with_min_partitions(16);
    declarer
        .declare(ExpandTopic::topology())
        .await
        .expect("re-declaring with more partitions should succeed");

    // Verify by publishing and consuming
    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<ExpandTopic>(&SimpleMessage {
            id: "expand-1".into(),
            content: "test".into(),
        })
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<ExpandTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "should receive message after partition expansion"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

// ===========================================================================
// DLQ consumer edge cases
// ===========================================================================

#[tokio::test]
async fn dlq_consumer_handles_deserialization_failure() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    // Publish raw invalid JSON directly to the DLQ topic
    use rdkafka::message::OwnedHeaders;
    client
        .publish_with_retry(
            "kafka-work-dlq",
            None,
            OwnedHeaders::new(),
            b"not valid json",
            1,
            "test raw publish to DLQ",
        )
        .await
        .expect("raw publish to DLQ should succeed");

    // Also publish a valid message to DLQ
    let valid_payload = serde_json::to_vec(&SimpleMessage {
        id: "valid-dlq".into(),
        content: "valid".into(),
    })
    .unwrap();
    client
        .publish_with_retry(
            "kafka-work-dlq",
            None,
            OwnedHeaders::new(),
            &valid_payload,
            1,
            "test valid publish to DLQ",
        )
        .await
        .expect("valid publish to DLQ should succeed");

    let dlq_handler = DlqRecordingHandler::new();
    let dhc = dlq_handler.clone();
    let dlq_consumer = KafkaConsumer::new(client.clone());
    let dlq_handle =
        tokio::spawn(async move { dlq_consumer.run_dlq::<WorkTopic, _>(dhc, ()).await });

    // The valid message should still be processed
    assert!(
        dlq_handler.counter.wait_for(1, TIMEOUT).await,
        "DLQ consumer should process valid messages even after a deserialization failure"
    );

    broker.close().await;
    dlq_handle.await.unwrap().ok();
}

// ===========================================================================
// Handler panic recovery
// ===========================================================================

#[tokio::test]
async fn handler_panic_does_not_crash_consumer() {
    struct PanicThenAck(WaitableCounter);

    impl MessageHandler<WorkTopic> for PanicThenAck {
        type Context = ();
        async fn handle(&self, msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.0.increment();
            if msg.id == "panic-me" {
                panic!("intentional test panic");
            }
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "panic-me".into(),
            content: "boom".into(),
        })
        .await
        .unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "normal".into(),
            content: "ok".into(),
        })
        .await
        .unwrap();

    let counter = WaitableCounter::new();
    let handler = PanicThenAck(counter.clone());

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1)
                    .with_max_retries(5),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(60)).await,
        "consumer should recover from panic and process messages"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

// ===========================================================================
// Sequenced defer falls back to retry
// ===========================================================================

#[tokio::test]
async fn sequenced_defer_falls_back_to_retry() {
    struct DeferThenAck {
        counter: WaitableCounter,
        retry_counts: Arc<Mutex<Vec<u32>>>,
    }

    impl MessageHandler<SeqSkipTopic> for DeferThenAck {
        type Context = ();
        async fn handle(&self, _msg: OrderMessage, meta: MessageMetadata, _: &()) -> Outcome {
            self.retry_counts.lock().await.push(meta.retry_count);
            let call = self.counter.get();
            self.counter.increment();
            if call == 0 {
                Outcome::Defer
            } else {
                Outcome::Ack
            }
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<SeqSkipTopic>(&OrderMessage {
            order_id: "defer-fifo-key".into(),
            amount: 42,
        })
        .await
        .unwrap();

    let counter = WaitableCounter::new();
    let retry_counts = Arc::new(Mutex::new(Vec::new()));
    let handler = DeferThenAck {
        counter: counter.clone(),
        retry_counts: retry_counts.clone(),
    };

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<SeqSkipTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_max_retries(5),
            )
            .await
    });

    assert!(
        counter.wait_for(2, Duration::from_secs(60)).await,
        "should be called at least 2 times"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();

    let counts = retry_counts.lock().await;
    assert_eq!(counts[0], 0, "first call should have retry_count 0");
    assert_eq!(
        counts[1], 1,
        "second call should have retry_count 1 (Defer became Retry)"
    );

    broker.close().await;
}

// ---------------------------------------------------------------------------
// run_fifo_until_timeout — drain semantics for sequenced topics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn run_fifo_until_timeout_clean_drain() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    // Publish a small batch.
    let publisher = broker.publisher().await.unwrap();
    for i in 0..5u64 {
        publisher
            .publish::<SeqSkipTopic>(&OrderMessage {
                order_id: "A".into(),
                amount: i,
            })
            .await
            .unwrap();
    }

    let counter = WaitableCounter::new();
    let counter_clone = counter.clone();

    #[derive(Clone)]
    struct CountSeqHandler(WaitableCounter);
    impl MessageHandler<SeqSkipTopic> for CountSeqHandler {
        type Context = ();
        async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.0.increment();
            Outcome::Ack
        }
    }

    let consumer = KafkaConsumer::new(client.clone());

    // Wait until the handler has acked all 5, then send the signal.
    let signal = async move {
        let _ = counter_clone.wait_for(5, Duration::from_secs(30)).await;
        // Brief grace period to let the shard finish its commit round trip.
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    let outcome = consumer
        .run_fifo_until_timeout::<SeqSkipTopic, _, _>(
            CountSeqHandler(counter.clone()),
            (),
            ConsumerOptions::<Kafka>::new().with_max_retries(3),
            signal,
            Duration::from_secs(10),
        )
        .await;

    assert!(
        outcome.is_clean(),
        "expected clean outcome, got {outcome:?}"
    );
    assert_eq!(counter.get(), 5);

    broker.close().await;
}

#[tokio::test]
async fn run_fifo_until_timeout_observes_handler_panic() {
    // Shard-level panic boundary: the Kafka FIFO consume loop spawns each
    // handler via `tokio::spawn` and awaits it through a oneshot channel.
    // When the handler panics, the task panics before sending, the receiver
    // closes, and the consume loop treats the closed channel as `Outcome::Retry`.
    // The panic never escapes `run_with_reconnect` — it always returns `Ok(())`.
    // As a result, `outcome.panics` and `outcome.errors` will both be zero.
    // This test documents and verifies that contract: the harness does not
    // crash or deadlock when handlers panic, and the outcome is clean.
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<SeqSkipTopic>(&OrderMessage {
            order_id: "A".into(),
            amount: 0,
        })
        .await
        .unwrap();

    #[derive(Clone)]
    struct PanicHandler;
    impl MessageHandler<SeqSkipTopic> for PanicHandler {
        type Context = ();
        async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            panic!("intentional test panic");
        }
    }

    let consumer = KafkaConsumer::new(client.clone());
    // max_retries=1 so the message is dispatched once (retry_count 0 < 1),
    // panics (Retry), then redelivered with retry_count=1 >= max_retries -> DLQ.
    let opts = ConsumerOptions::<Kafka>::new().with_max_retries(1);

    // Generous signal — give the shard time to pull the message, panic, and DLQ it.
    let signal = tokio::time::sleep(Duration::from_secs(10));

    let outcome = consumer
        .run_fifo_until_timeout::<SeqSkipTopic, _, _>(
            PanicHandler,
            (),
            opts,
            signal,
            Duration::from_secs(10),
        )
        .await;

    // The shard absorbs handler panics at the oneshot channel level (see above),
    // so neither panics nor errors are incremented. The harness returns cleanly.
    assert!(
        !outcome.timed_out,
        "harness must not hang on handler panics; got {outcome:?}"
    );
    assert_eq!(
        outcome.panics, 0,
        "Kafka shards absorb handler panics internally; got {outcome:?}"
    );
    assert_eq!(
        outcome.errors, 0,
        "Kafka shards absorb handler panics as Retry; got {outcome:?}"
    );

    broker.close().await;
}

#[tokio::test]
async fn run_fifo_until_timeout_flags_timeout_when_drain_overruns() {
    // Kafka's consume loop processes messages sequentially: it awaits the
    // oneshot receiver for each handler before moving to the next message.
    // A handler currently in-flight when shutdown fires keeps running until it
    // completes. With a slow handler (60 s sleep) and a short drain budget
    // (500 ms), the timeout fires before the shard finishes, the JoinSet
    // abort_all's the handle, and timed_out is set to true.
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<SeqSkipTopic>(&OrderMessage {
            order_id: "A".into(),
            amount: 0,
        })
        .await
        .unwrap();

    // Use a counter so the signal only fires once the handler is in-flight,
    // avoiding a race where the signal fires before the message is delivered.
    let started = WaitableCounter::new();
    let started_clone = started.clone();

    #[derive(Clone)]
    struct SlowHandler(WaitableCounter);
    impl MessageHandler<SeqSkipTopic> for SlowHandler {
        type Context = ();
        async fn handle(&self, _msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.0.increment();
            // Block well beyond drain timeout.
            tokio::time::sleep(Duration::from_secs(60)).await;
            Outcome::Ack
        }
    }

    let consumer = KafkaConsumer::new(client.clone());
    // max_retries=1 so the initial delivery (retry_count=0) passes the
    // max-retries check and the handler is actually dispatched.
    // Disable the handler timeout so the 60 s sleep is not interrupted before
    // the drain budget expires.
    let opts = ConsumerOptions::<Kafka>::new()
        .with_max_retries(1)
        .without_handler_timeout();

    // Signal fires only once the handler is confirmed in-flight; drain budget
    // is much shorter than the handler sleep.
    let signal = async move {
        started_clone.wait_for(1, Duration::from_secs(30)).await;
        // Small gap to make sure the handler is past its counter increment and
        // is blocked in the sleep before we fire shutdown.
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let drain = Duration::from_millis(500);

    let outcome = consumer
        .run_fifo_until_timeout::<SeqSkipTopic, _, _>(SlowHandler(started), (), opts, signal, drain)
        .await;

    // STRICT — handler ignores shutdown, drain budget runs out, shard aborted.
    assert!(outcome.timed_out, "expected timed_out, got {outcome:?}");
    assert_eq!(outcome.exit_code(), 3);

    broker.close().await;
}

// ===========================================================================
// ConsumerGroup::register_fifo
// ===========================================================================

/// Consumer group `register_fifo` drains all messages via `run_until_timeout`.
#[tokio::test]
async fn consumer_group_register_fifo_drains_via_run_until_timeout() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..5u64 {
        publisher
            .publish::<SeqSkipTopic>(&OrderMessage {
                order_id: "A".into(),
                amount: i,
            })
            .await
            .unwrap();
    }

    let handler = CountingHandler::new();
    let mut group = broker.consumer_group();
    group
        .register_fifo::<SeqSkipTopic, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::default()),
            {
                let h = handler.clone();
                move || h.clone()
            },
        )
        .await
        .unwrap();

    let counter = handler.counter.clone();
    let signal = async move {
        counter.wait_for(5, Duration::from_secs(30)).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let outcome = group
        .run_until_timeout(signal, Duration::from_secs(10))
        .await;
    assert!(outcome.is_clean(), "outcome was {outcome:?}");
    assert_eq!(handler.counter.get(), 5);

    broker.close().await;
}

// ===========================================================================
// Autoscaling vertical slice
// ===========================================================================

shove::define_topic!(
    AutoscalingTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-autoscaling").build()
);

/// Autoscaling lifecycle: slow handlers + burst → `enable_autoscaling` →
/// clean drain. Mirrors `autoscaling_scales_up_under_backlog_then_drains_clean`
/// from `inmemory_integration.rs` but drives the Kafka backend's
/// `spawn_autoscaler` against a real broker: the backlog is 20 messages of
/// 200 ms handler time behind one starting consumer — sustained pressure the
/// autoscaler reacts to well before the drain can complete. Scale-up is
/// exercised, not asserted — the generic `ConsumerGroup<B>` wrapper does not
/// surface the backend registry's `active_consumers()`; decision logic is
/// covered by the `src/autoscaler.rs` unit tests and, at the registry level,
/// by inmemory's `autoscaler_scales_up_under_backlog`. Scale-down is not
/// reached here at all: it would need the async-commit flush, stats
/// round-trips, cooldown and hysteresis to line up inside a fixed post-drain
/// window — the load-sensitive wait this test exists to remove. The retiring
/// bookkeeping it would drive is pinned by this backend's `scale_down_*`
/// unit tests; real-broker scale-down coverage is tracked separately.
#[tokio::test]
async fn autoscaling_scales_up_and_drains_clean() {
    use shove::AutoscalerConfig;
    use std::collections::HashSet;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::AtomicBool;

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<AutoscalingTopic>()
        .await
        .unwrap();

    // Kafka is at-least-once and every autoscaler scale-up forces a rebalance
    // mid-drain, which can strand handled-but-uncommitted offsets and
    // redeliver them to the partition's new owner. A raw delivery counter
    // would both overshoot 20 (flaky red) and trip the shutdown signal after
    // 20 - K distinct + K duplicate deliveries (false green), so count
    // distinct message ids instead.
    let seen = Arc::new(StdMutex::new(HashSet::new()));
    let distinct = WaitableCounter::new();
    // Raw delivery count, duplicates included — bounds redelivery below.
    let deliveries = Arc::new(AtomicU32::new(0));

    let mut group = broker.consumer_group();
    {
        let seen = seen.clone();
        let distinct = distinct.clone();
        let deliveries = deliveries.clone();
        group
            .register::<AutoscalingTopic, _>(
                ConsumerGroupConfig::new(
                    KafkaConsumerGroupConfig::new(1..=4).with_prefetch_count(1),
                ),
                move || {
                    #[derive(Clone)]
                    struct SlowHandler {
                        seen: Arc<StdMutex<HashSet<String>>>,
                        distinct: WaitableCounter,
                        deliveries: Arc<AtomicU32>,
                    }
                    impl MessageHandler<AutoscalingTopic> for SlowHandler {
                        type Context = ();
                        async fn handle(
                            &self,
                            msg: SimpleMessage,
                            _: MessageMetadata,
                            _: &(),
                        ) -> Outcome {
                            // Count the delivery before the slow part, so a
                            // redelivery cancelled mid-handle still counts.
                            self.deliveries.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            if self.seen.lock().unwrap().insert(msg.id) {
                                self.distinct.increment();
                            }
                            Outcome::Ack
                        }
                    }
                    SlowHandler {
                        seen: seen.clone(),
                        distinct: distinct.clone(),
                        deliveries: deliveries.clone(),
                    }
                },
            )
            .await
            .unwrap();
    }

    // Publish a burst large enough to build a sustained backlog.
    let publisher = broker.publisher().await.unwrap();
    for i in 0..20u32 {
        publisher
            .publish::<AutoscalingTopic>(&SimpleMessage {
                id: format!("as-{i}"),
                content: format!("burst {i}"),
            })
            .await
            .unwrap();
    }

    // Fast autoscaler config: short poll + hysteresis so scale-up fires
    // within the first second after the group gets an assignment.
    let cfg = AutoscalerConfig {
        poll_interval: Duration::from_millis(200),
        scale_up_multiplier: 1.5,
        scale_down_multiplier: 0.3,
        hysteresis_duration: Duration::from_millis(200),
        cooldown_duration: Duration::from_millis(400),
    };

    // Shut down on the observable — all 20 distinct messages handled — not on
    // a fixed wall-clock window: on a loaded runner, group join + coordinator
    // discovery + the first rebalance alone can outlast any "reasonable"
    // fixed budget (an 8 s window flaked in CI with zero messages handled).
    // The 60 s ceiling is a failure bound, not the expected duration; once
    // the group has an assignment the drain itself takes ~1-2 s. No settle
    // sleep after the count: the shutdown drain acquires every prefetch
    // permit and issues a final sync commit, so in-flight settlement is
    // already guaranteed.
    let reached_in_time = Arc::new(AtomicBool::new(false));
    let signal = {
        let distinct = distinct.clone();
        let reached_in_time = reached_in_time.clone();
        async move {
            let ok = distinct.wait_for(20, Duration::from_secs(60)).await;
            reached_in_time.store(ok, Ordering::SeqCst);
        }
    };
    // The 30 s drain budget is also a failure bound: unlike the old fixed
    // window, shutdown now fires at peak activity (4 consumers mid-handle),
    // and the drain joins them sequentially, flushing commits as it goes.
    let outcome = group
        .enable_autoscaling(cfg)
        .run_until_timeout(signal, Duration::from_secs(30))
        .await;

    // Count first, cleanliness second: in the never-got-an-assignment failure
    // this test exists to catch, the handled count is the diagnostic and an
    // unclean shutdown in that same state must not mask it (the outcome rides
    // along in the message either way).
    let handled = distinct.get();
    assert!(
        reached_in_time.load(Ordering::SeqCst),
        "all 20 published messages must be handled within the 60 s ceiling \
         (distinct handled after the shutdown drain: {handled}/20; \
         0 = the group never got an assignment, 1-19 = the drain stalled, \
         20 = handled only after the ceiling elapsed or the shutdown signal \
         was cancelled early; outcome: {outcome:?})"
    );
    assert!(
        outcome.is_clean(),
        "autoscaling group must drain cleanly (handled {handled}/20); outcome: {outcome:?}"
    );
    // Guard against unbounded systematic redelivery (e.g. broken commit
    // tracking redelivering continuously through the window), which the old
    // exact-count assert caught. Legitimate rebalance replay — a few
    // handled-but-uncommitted messages per scale-up — stays far below 3x.
    let total = deliveries.load(Ordering::Relaxed);
    assert!(
        total <= 60,
        "{total} deliveries for 20 published messages — at-least-once \
         tolerates bounded rebalance replay, not systematic redelivery"
    );

    broker.close().await;
}

// ===========================================================================
// Configurable consumer group_id — independent (fan-out) consumption
//
// Two independent services consuming the same topic must each receive every
// message. Each test drains once on the default group (committing offsets via
// `enable.auto.commit=false` manual commits), then drains again under an
// overridden group id which must rejoin a *fresh* group and re-receive all N
// — proving the override produces a distinct broker-side group rather than
// re-joining the committed default group.
// ===========================================================================

#[tokio::test]
async fn standard_group_id_override_consumes_independently() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    const N: u32 = 5;
    for i in 0..N {
        publisher
            .publish::<WorkTopic>(&SimpleMessage {
                id: format!("msg-{i}"),
                content: "x".into(),
            })
            .await
            .unwrap();
    }

    // Consumer 1: default group `{queue}-consumer`, drain + commit.
    let h1 = CountingHandler::new();
    let h1c = h1.clone();
    let sd1 = CancellationToken::new();
    let sd1c = sd1.clone();
    let c1 = KafkaConsumer::new(client.clone());
    let j1 = tokio::spawn(async move {
        c1.run::<WorkTopic, _>(
            h1c,
            (),
            ConsumerOptions::<Kafka>::new()
                .with_shutdown(sd1c)
                .with_prefetch_count(1),
        )
        .await
    });
    assert!(
        h1.counter.wait_for(N, TIMEOUT).await,
        "default consumer should receive all {N}"
    );
    // Let manual offset commits flush before tearing the consumer down.
    tokio::time::sleep(Duration::from_secs(2)).await;
    sd1.cancel();
    j1.await.unwrap().ok();

    // Consumer 2: overridden group id → independent, must also receive all N.
    let h2 = CountingHandler::new();
    let h2c = h2.clone();
    let sd2 = CancellationToken::new();
    let sd2c = sd2.clone();
    let c2 = KafkaConsumer::new(client.clone());
    let j2 = tokio::spawn(async move {
        c2.run::<WorkTopic, _>(
            h2c,
            (),
            ConsumerOptions::<Kafka>::new()
                .with_shutdown(sd2c)
                .with_prefetch_count(1)
                .with_group_id("independent-sink"),
        )
        .await
    });
    assert!(
        h2.counter.wait_for(N, TIMEOUT).await,
        "consumer with overridden group_id must independently receive all {N}"
    );
    sd2.cancel();
    j2.await.unwrap().ok();
    broker.close().await;
}

#[tokio::test]
async fn fifo_group_id_override_consumes_independently() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<SeqSkipTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    const N: u32 = 5;
    for i in 0..N {
        publisher
            .publish::<SeqSkipTopic>(&OrderMessage {
                order_id: "key-A".into(),
                amount: i as u64,
            })
            .await
            .unwrap();
    }

    // Consumer 1: default FIFO group `{queue}-fifo`, drain + commit.
    let h1 = OrderRecordingHandler::new();
    let h1c = h1.clone();
    let sd1 = CancellationToken::new();
    let sd1c = sd1.clone();
    let c1 = KafkaConsumer::new(client.clone());
    let j1 = tokio::spawn(async move {
        c1.run_fifo::<SeqSkipTopic, _>(
            h1c,
            (),
            ConsumerOptions::<Kafka>::new()
                .with_shutdown(sd1c)
                .with_max_retries(5),
        )
        .await
    });
    assert!(
        h1.counter.wait_for(N, Duration::from_secs(60)).await,
        "default FIFO consumer should receive all {N}"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    sd1.cancel();
    j1.await.unwrap().ok();

    // Consumer 2: overridden group id → must rejoin a fresh `{group}-fifo`.
    let h2 = OrderRecordingHandler::new();
    let h2c = h2.clone();
    let sd2 = CancellationToken::new();
    let sd2c = sd2.clone();
    let c2 = KafkaConsumer::new(client.clone());
    let j2 = tokio::spawn(async move {
        c2.run_fifo::<SeqSkipTopic, _>(
            h2c,
            (),
            ConsumerOptions::<Kafka>::new()
                .with_shutdown(sd2c)
                .with_max_retries(5)
                .with_group_id("independent-fifo-sink"),
        )
        .await
    });
    assert!(
        h2.counter.wait_for(N, Duration::from_secs(60)).await,
        "FIFO consumer with overridden group_id must independently receive all {N}"
    );
    sd2.cancel();
    j2.await.unwrap().ok();
    broker.close().await;
}

#[tokio::test]
async fn dlq_group_id_override_consumes_independently() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<WorkTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    const N: u32 = 3;
    for i in 0..N {
        publisher
            .publish::<WorkTopic>(&SimpleMessage {
                id: format!("dead-{i}"),
                content: "dead".into(),
            })
            .await
            .unwrap();
    }

    // Push all N messages to the DLQ by rejecting them.
    let sd_reject = CancellationToken::new();
    let sdr = sd_reject.clone();
    let reject = KafkaConsumer::new(client.clone());
    let jr = tokio::spawn(async move {
        reject
            .run::<WorkTopic, _>(
                FixedOutcomeHandler(Outcome::Reject),
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sdr)
                    .with_prefetch_count(1),
            )
            .await
    });
    tokio::time::sleep(Duration::from_secs(10)).await;
    sd_reject.cancel();
    jr.await.unwrap().ok();

    // Two DLQ drains run concurrently on the same DLQ topic. Drain 1 joins the
    // default group `{dlq}-consumer`; drain 2 overrides to `{group}-dlq`. With
    // independent groups both receive every dead message; if the override were
    // ignored they would share one group and compete, so one drain would see
    // fewer than N. Both stop on `broker.close()` (run_dlq tracks the client
    // shutdown token), so neither handle is awaited before the broker closes.
    let d1 = DlqRecordingHandler::new();
    let d1c = d1.clone();
    let cd1 = KafkaConsumer::new(client.clone());
    let jd1 = tokio::spawn(async move { cd1.run_dlq::<WorkTopic, _>(d1c, ()).await });

    let d2 = DlqRecordingHandler::new();
    let d2c = d2.clone();
    let cd2 = KafkaConsumer::new(client.clone());
    let jd2 = tokio::spawn(async move {
        cd2.run_dlq_with_options::<WorkTopic, _>(
            d2c,
            (),
            ConsumerOptions::<Kafka>::new().with_group_id("independent-dlq-sink"),
        )
        .await
    });

    assert!(
        d1.counter.wait_for(N, TIMEOUT).await,
        "default DLQ drain should receive all {N} dead messages"
    );
    assert!(
        d2.counter.wait_for(N, TIMEOUT).await,
        "DLQ drain with overridden group_id must independently receive all {N}"
    );

    broker.close().await;
    jd1.await.unwrap().ok();
    jd2.await.unwrap().ok();
}

// ===========================================================================
// broker.topology() / broker.consumer_group() ergonomic knobs
//
// The replication-factor and partition-floor knobs are reachable directly on
// the broker hub rather than only on the low-level KafkaTopologyDeclarer /
// KafkaConsumerGroupRegistry. These are smoke tests that the new builder paths
// chain and produce a working topology end-to-end (RF=1 on a single-broker
// test container).
// ===========================================================================

#[tokio::test]
async fn broker_topology_exposes_replication_and_partition_knobs() {
    shove::define_topic!(
        TopoKnobsTopic,
        SimpleMessage,
        TopologyBuilder::new("kafka-topo-knobs").build()
    );
    impl MessageHandler<TopoKnobsTopic> for CountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.counter.increment();
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();

    broker
        .topology()
        .with_replication_factor(1)
        .with_min_partitions(8)
        .declare::<TopoKnobsTopic>()
        .await
        .expect("declare via broker.topology() knobs should succeed");

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<TopoKnobsTopic>(&SimpleMessage {
            id: "topo-1".into(),
            content: "x".into(),
        })
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<TopoKnobsTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });
    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "should consume from a topic declared through broker.topology() knobs"
    );
    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

#[tokio::test]
async fn broker_consumer_group_exposes_default_replication_factor() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();

    let handler = CountingHandler::new();
    let handler_clone = handler.clone();

    // with_default_replication_factor is reachable on broker.consumer_group()
    // and applies to the topology auto-declared by register(). Register
    // before the first publish: a publish never creates the topic, so it
    // exists only once register() has declared it.
    let mut group = broker.consumer_group().with_default_replication_factor(1);
    group
        .register::<WorkTopic, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(1..=1)),
            move || handler_clone.clone(),
        )
        .await
        .expect("register with a default replication factor should succeed");

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<WorkTopic>(&SimpleMessage {
            id: "cg-rf-1".into(),
            content: "x".into(),
        })
        .await
        .unwrap();

    let token = group.cancellation_token();
    let counter = handler.counter.clone();
    let t = token.clone();
    tokio::spawn(async move {
        counter.wait_for(1, TIMEOUT).await;
        t.cancel();
    });

    let outcome = group
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(10))
        .await;
    assert!(outcome.is_clean());
    assert_eq!(handler.counter.get(), 1);
    broker.close().await;
}

// ===========================================================================
// publish_batch — sparse per-record failure reporting
// ===========================================================================

shove::define_topic!(
    BatchSparseTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-batch-sparse").dlq().build()
);

/// Kafka submits every record in a batch independently, so a strict subset can
/// fail while the records *after* it succeed. That is exactly the case where
/// "re-publish from the first failure" is wrong, and where `to_republish()`
/// has to name a sparse set rather than a suffix.
///
/// Record 1 is driven over librdkafka's producer-side `message.max.bytes`
/// (1 MB by default), so it is rejected locally with `MessageSizeTooLarge`
/// while records 0, 2 and 3 are produced normally. No broker-side config is
/// involved and nothing has to time out — the rejection is synchronous.
#[tokio::test]
async fn publish_batch_reports_sparse_indices_when_a_subset_fails() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<BatchSparseTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();

    // Comfortably past the 1 MB producer limit, and past it on its own — the
    // other three records are tiny, so only this one can be rejected for size.
    let oversized = "x".repeat(2 * 1024 * 1024);
    let messages: Vec<SimpleMessage> = (0..4)
        .map(|i| SimpleMessage {
            id: format!("sparse-{i}"),
            content: if i == 1 {
                oversized.clone()
            } else {
                format!("message {i}")
            },
        })
        .collect();

    let err = publisher
        .publish_batch::<BatchSparseTopic>(&messages)
        .await
        .expect_err("a batch containing an oversized record must not report success");

    let shove::ShoveError::PartialBatch(f) = err else {
        panic!("expected ShoveError::PartialBatch, got {err:?}");
    };

    assert_eq!(
        f.failed(),
        &[1],
        "Kafka must name the exact rejected index, not a prefix"
    );
    assert!(
        f.unattempted().is_empty(),
        "Kafka attempts every record, so nothing is unattempted: {:?}",
        f.unattempted()
    );
    assert_eq!(f.to_republish(), &[1]);
    assert_eq!(f.succeeded(), 3);

    // The invariant, asserted for this backend.
    assert_eq!(f.succeeded() + f.to_republish().len(), messages.len());
    assert!(f.to_republish().windows(2).all(|w| w[0] < w[1]));

    // Records 2 and 3 sit *after* the failure and were published fine. A
    // suffix-shaped retry would have re-produced them; the sparse set does not.
    let outstanding: Vec<&str> = f
        .to_republish()
        .iter()
        .filter_map(|&i| messages.get(i).map(|m| m.id.as_str()))
        .collect();
    assert_eq!(outstanding, vec!["sparse-1"]);

    broker.close().await;
}

/// A Kafka batch whose *every* record is oversized is not partial, so it keeps
/// returning the bare error. Guards the compatibility rule on the backend with
/// the sparsest failure shape.
#[tokio::test]
async fn publish_batch_wholly_failed_returns_the_bare_error() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<BatchSparseTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();

    let oversized = "x".repeat(2 * 1024 * 1024);
    let messages: Vec<SimpleMessage> = (0..3)
        .map(|i| SimpleMessage {
            id: format!("all-oversized-{i}"),
            content: oversized.clone(),
        })
        .collect();

    let err = publisher
        .publish_batch::<BatchSparseTopic>(&messages)
        .await
        .expect_err("an all-oversized batch must fail");

    assert!(
        !matches!(err, shove::ShoveError::PartialBatch(_)),
        "nothing succeeded, so this is not a partial batch; got {err:?}"
    );
    assert!(matches!(err, shove::ShoveError::Connection(_)));

    broker.close().await;
}

// ===========================================================================
// SBE codec — binary frames over a real broker
// ===========================================================================

#[cfg(feature = "sbe")]
mod sbe_codec {
    use super::*;
    use shove::{SbeCodec, SbeFrame, SbeHeader, SbeMessage};
    use std::sync::Mutex as StdMutex;

    struct SbeOrder;
    impl SbeMessage for SbeOrder {
        const SCHEMA_ID: u16 = 42;
        const TEMPLATE_ID: u16 = 7;
    }

    shove::define_topic!(
        SbeTopic,
        SbeFrame<SbeOrder>,
        TopologyBuilder::new("kafka-sbe").dlq().build(),
        codec = SbeCodec
    );

    fn encode_frame(price: u64, quantity: u64) -> SbeFrame<SbeOrder> {
        let header = SbeHeader {
            block_length: 16,
            template_id: SbeOrder::TEMPLATE_ID,
            schema_id: SbeOrder::SCHEMA_ID,
            version: 1,
        };
        let mut buf = header.to_bytes(SbeOrder::BYTE_ORDER).to_vec();
        buf.extend_from_slice(&price.to_le_bytes());
        buf.extend_from_slice(&quantity.to_le_bytes());
        SbeFrame::new(buf).expect("valid frame")
    }

    fn decode_fields(frame: &SbeFrame<SbeOrder>) -> (u64, u64) {
        let field = |offset: usize| {
            frame.body()[offset..offset + 8]
                .try_into()
                .map(u64::from_le_bytes)
                .expect("body holds two u64 fields")
        };
        (field(0), field(8))
    }

    #[derive(Clone, Default)]
    struct SbeCapture {
        seen: Arc<StdMutex<Vec<(u64, u64, u16)>>>,
    }

    impl SbeCapture {
        async fn wait_for(&self, target: usize, timeout: Duration) -> bool {
            let deadline = std::time::Instant::now() + timeout;
            loop {
                if self.seen.lock().unwrap().len() >= target {
                    return true;
                }
                if std::time::Instant::now() >= deadline {
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    impl MessageHandler<SbeTopic> for SbeCapture {
        type Context = ();
        async fn handle(&self, msg: SbeFrame<SbeOrder>, _: MessageMetadata, _: &()) -> Outcome {
            let (price, quantity) = decode_fields(&msg);
            self.seen
                .lock()
                .unwrap()
                .push((price, quantity, msg.header().version));
            Outcome::Ack
        }
    }

    #[tokio::test]
    async fn sbe_frame_round_trips_through_broker() {
        let tb = TestBroker::start().await;
        let broker = tb.broker();
        let client = tb.client();
        broker.topology().declare::<SbeTopic>().await.unwrap();

        broker
            .publisher()
            .await
            .unwrap()
            .publish::<SbeTopic>(&encode_frame(250_000, 12))
            .await
            .expect("publish should succeed");

        let handler = SbeCapture::default();
        let hc = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();

        let consumer = KafkaConsumer::new(client.clone());
        let handle = tokio::spawn(async move {
            consumer
                .run::<SbeTopic, _>(
                    hc,
                    (),
                    ConsumerOptions::<Kafka>::new()
                        .with_shutdown(sc)
                        .with_prefetch_count(1),
                )
                .await
        });

        assert!(
            handler.wait_for(1, TIMEOUT).await,
            "should receive the SBE frame"
        );
        shutdown.cancel();
        handle.await.unwrap().ok();

        assert_eq!(handler.seen.lock().unwrap().clone(), vec![(250_000, 12, 1)]);
        broker.close().await;
    }
}

/// The direct FIFO path refuses a commit interval the way `register_fifo`
/// does: `spawn_fifo_shards` is the one place every FIFO entry point goes
/// through, so `run_fifo` returns `Topology` before it subscribes, and a
/// supervisor-registered FIFO consumer fails its task the same way.
#[tokio::test]
async fn run_fifo_rejects_a_commit_interval() {
    let tb = TestBroker::start().await;
    let consumer = KafkaConsumer::new(tb.client());
    let err = consumer
        .run_fifo::<SeqSkipTopic, _>(
            CountingHandler::new(),
            (),
            ConsumerOptions::<Kafka>::new().with_commit_interval(Duration::from_secs(5)),
        )
        .await
        .expect_err("with_commit_interval must be rejected on the direct FIFO path");
    let shove::ShoveError::Topology(msg) = err else {
        panic!("expected ShoveError::Topology, got {err:?}");
    };
    assert!(
        msg.contains("kafka-seq-skip")
            && msg.contains("with_commit_interval")
            && msg.contains("commits each message as it settles"),
        "message must name the topic and the refused setting: {msg}"
    );
}

/// The receive loop judges rejected commits by a threshold that grows with
/// the commit interval, `fence_threshold`, and not by the fixed sixty
/// seconds alone. The probe reads the threshold the loop started with, so
/// this fails if the loop ever computed it from the constant instead of the
/// configured interval.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn a_raised_commit_interval_raises_the_receive_loops_fence_threshold() {
    use shove::kafka::fence_probe;

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<WorkTopic>().await.unwrap();
    let client = tb.client();

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<WorkTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_commit_interval(Duration::from_secs(20)),
            )
            .await
    });
    let deadline = Instant::now() + TIMEOUT;
    while fence_probe::last_threshold().is_none() {
        assert!(Instant::now() < deadline, "the receive loop started");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        fence_probe::last_threshold(),
        Some(Duration::from_secs(80)),
        "four commit intervals of 20 s, above the 60 s floor the default interval keeps"
    );
    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// The last resort of the final-commit thread, N1: when no thread can be
/// spawned for the commit or for the close, the handle is leaked rather than
/// closed on the runtime thread. This pins, against a real broker, the cost
/// the Kafka page and plan 021 step 4 state for that leak: the leaked
/// instance keeps heartbeating, so the group keeps its member past the
/// `session.timeout.ms` a crash would have freed it by. The member leaves
/// only when `max.poll.interval.ms` (five minutes) passes without a poll,
/// which this test does not wait for, or when the process exits.
// `test-support` gates the spawn switch and the timeout seam this test reads.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn a_leaked_consumer_keeps_its_group_member_past_the_session_timeout() {
    use rdkafka::consumer::{BaseConsumer, Consumer as _};
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};
    use shove::kafka::{final_commit_spawn_probe, session_timeout_for_test};

    shove::define_topic!(
        LeakedCloseTopic,
        SimpleMessage,
        TopologyBuilder::new("kafka-leaked-close").build()
    );

    impl MessageHandler<LeakedCloseTopic> for CountingHandler {
        type Context = ();
        async fn handle(&self, _msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.counter.increment();
            Outcome::Ack
        }
    }

    const GROUP: &str = "kafka-leaked-close-consumer";

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<LeakedCloseTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<LeakedCloseTopic>(&SimpleMessage {
            id: "leaked-close".into(),
            content: "test".into(),
        })
        .await
        .unwrap();

    let handler = CountingHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let run = tokio::spawn(async move {
        consumer
            .run::<LeakedCloseTopic, _>(hc, (), ConsumerOptions::<Kafka>::new().with_shutdown(sc))
            .await
    });
    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "the consumer receives the record"
    );
    wait_for_stable_group(tb.brokers(), GROUP, TIMEOUT).await;

    // From here on no thread can be had, for the final commit or for the close.
    final_commit_spawn_probe::refuse_threads(true);
    let started = Instant::now();
    shutdown.cancel();
    run.await
        .unwrap()
        .expect("run returns Ok: the missing commit is settled as a rejected one");
    // The close blocks for as long as librdkafka takes to leave the group. A
    // run that returns at once did not run it on the runtime thread.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "shutdown must return without closing the consumer here, took {:?}",
        started.elapsed()
    );

    let session_timeout = session_timeout_for_test();
    tokio::time::sleep(session_timeout + Duration::from_secs(2)).await;

    // A coordinator that is moving or loading answers "ask again"; retried
    // as `wait_for_stable_group` retries them.
    let probe: BaseConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", tb.brokers())
        .create()
        .expect("failed to create group probe");
    let deadline = Instant::now() + TIMEOUT;
    let members = loop {
        match probe.fetch_group_list(Some(GROUP), Duration::from_secs(10)) {
            Ok(list) => {
                break list
                    .groups()
                    .iter()
                    .find(|g| g.name() == GROUP)
                    .map_or(0, |g| g.members().len());
            }
            Err(KafkaError::GroupListFetch(
                RDKafkaErrorCode::NotCoordinator
                | RDKafkaErrorCode::CoordinatorNotAvailable
                | RDKafkaErrorCode::CoordinatorLoadInProgress
                | RDKafkaErrorCode::OperationTimedOut,
            )) => {
                assert!(
                    Instant::now() < deadline,
                    "the coordinator answers the group probe"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => panic!("failed to fetch group list: {e}"),
        }
    };
    assert_eq!(
        members, 1,
        "the leaked handle keeps heartbeating, so the broker keeps the member past the \
         {session_timeout:?} session timeout while the process lives"
    );
    broker.close().await;
}
