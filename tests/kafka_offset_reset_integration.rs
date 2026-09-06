//! Integration tests for `Broker::<Kafka>::reset_consumer_group_offsets`.
//!
//! The property under test is the one the API exists for: a group that has
//! **already committed** offsets can be moved — to the tail, back to the head,
//! or to a point in time — while keeping its group ID. None of that is possible
//! with `auto.offset.reset` alone, which only applies to a group with no usable
//! committed offset.

#![cfg(feature = "kafka")]

use serde::{Deserialize, Serialize};
use shove::ShoveError;
use shove::broker::Broker;
use shove::consumer_group::ConsumerGroupConfig;
use shove::handler::MessageHandler;
use shove::kafka::{
    KafkaClient, KafkaConfig, KafkaConsumerGroupConfig, KafkaOffsetReset, KafkaOffsetResetReport,
};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topic::Topic;
use shove::topology::TopologyBuilder;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
use tokio::sync::Mutex;
use tokio::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Tick {
    id: String,
}

shove::define_topic!(
    TickTopic,
    Tick,
    TopologyBuilder::new("kafka-offset-reset-ticks").build()
);

shove::define_topic!(
    TimeTravelTopic,
    Tick,
    TopologyBuilder::new("kafka-offset-reset-timetravel").build()
);

shove::define_topic!(
    ActiveGuardTopic,
    Tick,
    TopologyBuilder::new("kafka-offset-reset-active").build()
);

/// Records every message ID it sees.
#[derive(Clone, Default)]
struct RecordingHandler {
    seen: Arc<Mutex<Vec<String>>>,
}

impl RecordingHandler {
    async fn sorted_ids(&self) -> Vec<String> {
        let mut ids = self.seen.lock().await.clone();
        ids.sort();
        ids
    }

    async fn len(&self) -> usize {
        self.seen.lock().await.len()
    }

    /// Waits until at least `target` messages have arrived, then waits out
    /// `settle` so a test asserting "and nothing more" still sees any extra
    /// deliveries that were already in flight.
    async fn wait_for(&self, target: usize, timeout: Duration, settle: Duration) {
        let deadline = Instant::now() + timeout;
        while self.len().await < target && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        tokio::time::sleep(settle).await;
    }
}

macro_rules! recording_handler_for {
    ($($topic:ty),+ $(,)?) => {$(
        impl MessageHandler<$topic> for RecordingHandler {
            type Context = ();
            async fn handle(&self, msg: Tick, _meta: MessageMetadata, _: &()) -> Outcome {
                self.seen.lock().await.push(msg.id);
                Outcome::Ack
            }
        }
    )+};
}

recording_handler_for!(TickTopic, TimeTravelTopic, ActiveGuardTopic);

struct TestBroker {
    _container: testcontainers::ContainerAsync<KafkaContainer>,
    port: u16,
}

impl TestBroker {
    async fn start() -> Self {
        let container = KafkaContainer::default()
            .start()
            .await
            .expect("failed to start Kafka container");
        let port = container
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("failed to get Kafka port");
        Self {
            _container: container,
            port,
        }
    }

    /// A broker on its **own** client, against the same container.
    ///
    /// Every call must get a fresh client: `run_until_timeout` cancels the
    /// *client's* shutdown token when its signal fires, so a client that has
    /// hosted one group lifecycle is spent — a second group built from it
    /// shuts down before consuming anything. Reconnecting per round is also
    /// the faithful model of what these tests are about: a process restart
    /// rejoining the same Kafka consumer group.
    async fn broker(&self) -> Broker<Kafka> {
        let client = KafkaClient::connect_with_retry(
            &KafkaConfig::new(format!("127.0.0.1:{}", self.port)),
            10,
        )
        .await
        .expect("failed to connect to Kafka");
        Broker::<Kafka>::from_client(client)
    }
}

/// Publish `ids` to `$topic`.
macro_rules! publish {
    ($broker:expr, $topic:ty, $ids:expr) => {{
        let msgs: Vec<Tick> = $ids
            .iter()
            .map(|id: &String| Tick { id: id.clone() })
            .collect();
        $broker
            .publisher()
            .await
            .unwrap()
            .publish_batch::<$topic>(&msgs)
            .await
            .unwrap();
    }};
}

/// Runs a consumer group until `$expect` messages have landed (plus a settle
/// window), shuts it down, and returns the sorted IDs the handler saw.
///
/// Takes the `TestBroker`, not a broker: each call connects its own client and
/// closes it again, so consecutive calls model a process restart against the
/// same Kafka consumer group. See [`TestBroker::broker`] for why sharing a
/// client across rounds does not work.
macro_rules! drain {
    ($tb:expr, $topic:ty, $config:expr, $expect:expr) => {{
        let broker = $tb.broker().await;
        let handler = RecordingHandler::default();
        let h = handler.clone();
        let mut group = broker.consumer_group();
        group
            .register::<$topic, _>(ConsumerGroupConfig::new($config), move || h.clone())
            .await
            .unwrap();

        let token = group.cancellation_token();
        let watcher = handler.clone();
        let t = token.clone();
        tokio::spawn(async move {
            watcher
                .wait_for($expect, Duration::from_secs(45), Duration::from_secs(2))
                .await;
            t.cancel();
        });
        let outcome = group
            .run_until_timeout(token.cancelled_owned(), Duration::from_secs(10))
            .await;
        assert!(
            outcome.is_clean(),
            "consumer group did not shut down cleanly"
        );
        broker.close().await;
        handler.sorted_ids().await
    }};
}

/// True for the races in the coordinator-transition window right after a group
/// shuts down — the window `reset_when_inactive` exists to ride out. Two shapes
/// occur there:
///
/// - `Validation` from the inactive check: the coordinator still lists members
///   from the group that has just shut down (see the "must be inactive" note on
///   `reset_consumer_group_offsets`). Matched by its "active member(s)"
///   wording, because the reset path has one other `Validation` — the
///   `.broadcast()`-topic rejection — that is permanent misuse.
/// - `Connection` carrying a retriable coordinator or group-transition code:
///   the client asked a broker that is not (or no longer) the coordinator, the
///   coordinator is mid-election or still loading group state, or the broker's
///   own inactivity guard rejected the commit while the last member was still
///   leaving. Kafka's contract for all of them is "refresh metadata and
///   retry"; they are only ever transient. The list matches the coordinator
///   family in `examples/kafka/stress.rs` plus the commit-side codes named in
///   `src/backends/kafka/offset_reset.rs`'s advisory-guard note.
///
/// Everything else — transport failures, topology errors — is terminal and must
/// fail fast so the test never papers over a real regression.
fn is_transient_coordinator_error(err: &ShoveError) -> bool {
    match err {
        ShoveError::Validation(msg) => msg.contains("active member"),
        // The offset-reset path wraps rdkafka errors as `Connection` strings,
        // so the codes are only recognizable by name. That rendering is
        // stable: `RDKafkaErrorCode` displays as `{:?} ({description})`, so
        // the variant name is always present — and the classification test
        // below builds its fixtures through the real rdkafka types to catch
        // that format ever drifting. Deliberately NOT a blanket `Connection`
        // retry: a transport failure must still fail fast.
        ShoveError::Connection(msg) => [
            "NotCoordinator",
            "CoordinatorNotAvailable",
            "CoordinatorLoadInProgress",
            "UnknownMemberId",
            "RebalanceInProgress",
        ]
        .iter()
        .any(|code| msg.contains(code)),
        _ => false,
    }
}

/// How a `retry_transient_until` loop ended. Split from the panics in
/// `reset_when_inactive` so the deadline behaviour is unit-testable without a
/// broker.
enum RetryOutcome<T> {
    Settled(T),
    DeadlineExceeded(ShoveError),
    Terminal(ShoveError),
}

/// Drives `attempt` until it succeeds, retrying only the transient
/// coordinator-transition races (see `is_transient_coordinator_error`) with a
/// 500ms pause between attempts, bounded by `deadline`; any other error ends
/// the loop immediately.
async fn retry_transient_until<T, F, Fut>(deadline: Instant, mut attempt: F) -> RetryOutcome<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ShoveError>>,
{
    loop {
        match attempt().await {
            Ok(value) => return RetryOutcome::Settled(value),
            Err(e) if is_transient_coordinator_error(&e) => {
                // The check sits after the pause, immediately before the next
                // attempt would launch: one attempt issues several RPCs, so a
                // pre-sleep check would let an error caught just inside the
                // deadline start one more full attempt past it.
                tokio::time::sleep(Duration::from_millis(500)).await;
                if Instant::now() >= deadline {
                    return RetryOutcome::DeadlineExceeded(e);
                }
            }
            Err(e) => return RetryOutcome::Terminal(e),
        }
    }
}

/// Re-anchor, retrying only the transient coordinator-transition races (see
/// `is_transient_coordinator_error`) from a group that has just shut down,
/// bounded by a 45s deadline; any other error fails immediately.
async fn reset_when_inactive<T: Topic>(
    broker: &Broker<Kafka>,
    config: &KafkaConsumerGroupConfig,
    to: KafkaOffsetReset,
) -> KafkaOffsetResetReport {
    let deadline = Instant::now() + Duration::from_secs(45);
    let attempt = || broker.reset_consumer_group_offsets::<T>(config, to);
    match retry_transient_until(deadline, attempt).await {
        RetryOutcome::Settled(report) => report,
        RetryOutcome::DeadlineExceeded(e) => {
            panic!("coordinator never settled after 45s of retries: {e}")
        }
        RetryOutcome::Terminal(e) => panic!("offset reset failed: {e}"),
    }
}

/// The deadline must bound when an attempt may *start*, not just how long
/// transient errors keep being tolerated: one reset attempt issues several
/// RPCs (group list, fetch/commit offsets), so an attempt that begins at the
/// boundary runs unbounded past the promised 45s. Paused time makes the
/// schedule exact — attempts fire every 500ms and the loop must give up at the
/// deadline instead of launching one more.
#[tokio::test(start_paused = true)]
async fn no_retry_attempt_starts_at_or_after_the_deadline() {
    let deadline = Instant::now() + Duration::from_secs(45);
    let starts = std::sync::Mutex::new(Vec::new());

    let outcome = retry_transient_until::<(), _, _>(deadline, || {
        starts
            .lock()
            .expect("start recorder poisoned")
            .push(Instant::now());
        async {
            Err(ShoveError::Connection(
                "failed to read committed offsets for group 'g': Meta data \
                 fetch error: NotCoordinator (Broker: Not coordinator)"
                    .to_string(),
            ))
        }
    })
    .await;

    assert!(
        matches!(outcome, RetryOutcome::DeadlineExceeded(_)),
        "a never-settling transient error must end as DeadlineExceeded"
    );
    let starts = starts.lock().expect("start recorder poisoned");
    assert!(!starts.is_empty(), "the loop never attempted at all");
    let late = starts.iter().filter(|s| **s >= deadline).count();
    assert_eq!(
        late, 0,
        "{late} attempt(s) started at or after the 45s deadline"
    );
}

/// One row per (error, expected classification). The `Connection` fixtures are
/// rendered through the real `rdkafka` error types — the same ones the
/// offset-reset path wraps — so a dependency bump that changes the `Display`
/// format the predicate's substring match depends on turns up here as a red
/// test instead of a silently dead predicate.
#[test]
fn transient_coordinator_classification() {
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};

    // The wrapping applied at src/backends/kafka/offset_reset.rs's
    // committed-offsets read — the site the CI flake came through
    // (actions run 33953186608, both nextest tries, NotCoordinator).
    let committed_read = |code: RDKafkaErrorCode| {
        ShoveError::Connection(format!(
            "failed to read committed offsets for group \
             'kafka-offset-reset-timetravel-consumer': {}",
            KafkaError::MetadataFetch(code)
        ))
    };

    let cases = [
        (committed_read(RDKafkaErrorCode::NotCoordinator), true),
        (
            committed_read(RDKafkaErrorCode::CoordinatorNotAvailable),
            true,
        ),
        (
            committed_read(RDKafkaErrorCode::CoordinatorLoadInProgress),
            true,
        ),
        // The advisory inactive-check race: the group-list probe saw zero
        // members but the coordinator had not finished reaping the leaving
        // member, so its own guard rejects the commit.
        (
            ShoveError::Connection(format!(
                "failed to commit Latest offsets for group 'g' on 'q': {}. \
                 Kafka only accepts an offset reset while the group is \
                 inactive — stop every consumer in the group first.",
                KafkaError::ConsumerCommit(RDKafkaErrorCode::UnknownMemberId)
            )),
            true,
        ),
        (
            ShoveError::Connection(format!(
                "failed to fetch group list for 'g': {}",
                KafkaError::GroupListFetch(RDKafkaErrorCode::RebalanceInProgress)
            )),
            true,
        ),
        // The faithful wording of `ensure_group_inactive`'s transient refusal.
        (
            ShoveError::Validation(
                "cannot reset offsets for group 'g' on topic 'q': the group \
                 has 1 active member(s). Kafka only accepts an offset reset \
                 while the group is inactive — stop every consumer in the \
                 group, then reset before starting them again."
                    .to_string(),
            ),
            true,
        ),
        // The reset path's other Validation — a `.broadcast()` topic has no
        // group to re-anchor (src/broker.rs) — is permanent misuse: retrying
        // it can never succeed and must fail fast.
        (
            ShoveError::Validation(
                "topic 'q' declares `.broadcast()`, so it has no consumer \
                 group to re-anchor: its subscribers assign partitions \
                 manually and never commit an offset."
                    .to_string(),
            ),
            false,
        ),
        // A dead broker is terminal, not a coordinator transition.
        (
            committed_read(RDKafkaErrorCode::BrokerTransportFailure),
            false,
        ),
        (
            ShoveError::Topology("failed to build target offsets: bad state".to_string()),
            false,
        ),
    ];

    for (err, expected) in cases {
        assert_eq!(
            is_transient_coordinator_error(&err),
            expected,
            "misclassified: {err}"
        );
    }
}

fn ids(prefix: &str, count: u32) -> Vec<String> {
    (1..=count).map(|i| format!("{prefix}-{i}")).collect()
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

/// The headline case: a group with committed offsets is moved to the tail and
/// back to the head, keeping the same group ID throughout.
#[tokio::test]
async fn reset_moves_a_committed_group_to_the_tail_and_back() {
    let tb = TestBroker::start().await;
    let broker = tb.broker().await;
    broker.topology().declare::<TickTopic>().await.unwrap();

    let config = KafkaConsumerGroupConfig::new(1..=1).with_prefetch_count(50);

    // Round 1: consume a first batch so the group has real committed offsets.
    let first = ids("first", 10);
    publish!(&broker, TickTopic, first);
    let seen = drain!(&tb, TickTopic, config.clone(), 10);
    assert_eq!(seen, sorted(first), "round 1 should drain the first batch");

    // A backlog accumulates while the group is down.
    let backlog = ids("backlog", 10);
    publish!(&broker, TickTopic, backlog);

    // Re-anchor at the tail. Without this the group would resume at its
    // committed offset and crawl the backlog; a fresh group ID would also skip
    // it, but would strand the old group's offsets and lag metrics.
    let report = reset_when_inactive::<TickTopic>(&broker, &config, KafkaOffsetReset::Latest).await;
    assert_eq!(report.group_id(), "kafka-offset-reset-ticks-consumer");
    assert_eq!(report.queue(), "kafka-offset-reset-ticks");
    assert_eq!(report.partitions().len(), 8, "default partition count");
    assert!(
        !report.is_noop(),
        "the group was 10 messages behind the tail: {report:?}"
    );
    // Every partition must now sit at its high watermark, and with nothing
    // deleted those watermarks sum to the number of records published. Measured
    // in absolute offsets rather than by summing `delta()`: the backlog can land
    // entirely on partitions this group never committed, and those report
    // `delta() == None` — a real skip that per-partition deltas cannot see.
    let tail: i64 = report.partitions().iter().map(|p| p.new_offset()).sum();
    assert_eq!(
        tail, 20,
        "Latest must anchor every partition at its high watermark, 20 records published: {report:?}"
    );

    // Round 2: only messages published *after* the re-anchor arrive.
    let fresh = ids("fresh", 5);
    publish!(&broker, TickTopic, fresh);
    let seen = drain!(&tb, TickTopic, config.clone(), 5);
    assert_eq!(
        seen,
        sorted(fresh),
        "after a Latest reset the group must see only post-reset messages"
    );

    // And the inverse: Earliest replays everything still retained.
    let report =
        reset_when_inactive::<TickTopic>(&broker, &config, KafkaOffsetReset::Earliest).await;
    assert!(
        !report.is_noop(),
        "the group was at the tail, not the head: {report:?}"
    );
    let seen = drain!(&tb, TickTopic, config.clone(), 25);
    assert_eq!(
        seen.len(),
        25,
        "Earliest must replay all 25 retained messages, got {seen:?}"
    );

    // Re-anchoring where the group already sits changes nothing.
    let report = reset_when_inactive::<TickTopic>(&broker, &config, KafkaOffsetReset::Latest).await;
    assert!(
        report.is_noop(),
        "a fully drained group is already at the tail: {report:?}"
    );

    broker.close().await;
}

/// `Timestamp` re-anchors at the first record at or after a wall-clock point.
#[tokio::test]
async fn reset_to_timestamp_replays_only_records_after_that_point() {
    let tb = TestBroker::start().await;
    let broker = tb.broker().await;
    broker
        .topology()
        .declare::<TimeTravelTopic>()
        .await
        .unwrap();

    let config = KafkaConsumerGroupConfig::new(1..=1).with_prefetch_count(50);

    publish!(&broker, TimeTravelTopic, ids("before", 6));

    // Kafka stamps records with millisecond precision; leave a clear gap either
    // side so the cut point is unambiguous.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let cut = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let after = ids("after", 6);
    publish!(&broker, TimeTravelTopic, after);

    // This group has never committed, so it also covers the "no prior commit"
    // arm of the report.
    let report =
        reset_when_inactive::<TimeTravelTopic>(&broker, &config, KafkaOffsetReset::Timestamp(cut))
            .await;
    assert!(
        report.partitions().iter().all(|p| p.previous().is_none()),
        "a group that never committed has no previous offsets: {report:?}"
    );

    let seen = drain!(&tb, TimeTravelTopic, config.clone(), 6);
    assert_eq!(
        seen,
        sorted(after),
        "only records published after the cut should be delivered"
    );

    broker.close().await;
}

/// Resetting a group that still has live members is refused with an error that
/// names the problem, rather than a bare librdkafka `UNKNOWN_MEMBER_ID`.
#[tokio::test]
async fn reset_is_refused_while_the_group_has_members() {
    let tb = TestBroker::start().await;
    let broker = tb.broker().await;
    broker
        .topology()
        .declare::<ActiveGuardTopic>()
        .await
        .unwrap();

    let config = KafkaConsumerGroupConfig::new(1..=1);
    let handler = RecordingHandler::default();
    let h = handler.clone();
    let mut group = broker.consumer_group();
    group
        .register::<ActiveGuardTopic, _>(ConsumerGroupConfig::new(config.clone()), move || {
            h.clone()
        })
        .await
        .unwrap();

    let token = group.cancellation_token();
    let t = token.clone();
    let probe_broker = tb.broker().await;
    let probe_config = config.clone();
    let probe_handler = handler.clone();
    // Everything in here must end by cancelling the token: `run_until_timeout`
    // below waits on it, so a panic before the cancel would hang the test
    // instead of failing it.
    let probe = tokio::spawn(async move {
        let publisher = probe_broker.publisher().await.unwrap();
        publisher
            .publish::<ActiveGuardTopic>(&Tick { id: "live".into() })
            .await
            .unwrap();
        // A delivered message proves the member has joined and been assigned
        // partitions before the reset is attempted.
        probe_handler
            .wait_for(1, Duration::from_secs(45), Duration::ZERO)
            .await;
        let delivered = probe_handler.len().await;

        let result = probe_broker
            .reset_consumer_group_offsets::<ActiveGuardTopic>(
                &probe_config,
                KafkaOffsetReset::Latest,
            )
            .await;
        t.cancel();
        (delivered, result)
    });

    let outcome = group
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(10))
        .await;
    assert!(outcome.is_clean());

    let (delivered, result) = probe.await.unwrap();
    assert_eq!(delivered, 1, "the group member never received its message");
    let err = result.expect_err("reset must be refused while the group has members");
    assert!(
        matches!(err, ShoveError::Validation(_)),
        "expected a Validation error, got {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("active member"),
        "the error should name the active members: {msg}"
    );
    assert!(
        msg.contains("kafka-offset-reset-active-consumer"),
        "the error should name the group: {msg}"
    );

    broker.close().await;
}
