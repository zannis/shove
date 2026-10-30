#![cfg(all(feature = "nats", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: a `Reject` on a NATS topic that declares **no DLQ** moves
//! `shove_messages_discarded_total` only when the server confirms the ack that
//! retires the message.
//!
//! An ack that carries a reply subject is answered by the consumer that takes
//! it. When no consumer does, because the stream or the durable is gone, the
//! *server* answers in its place, with a `503` status that says nobody was
//! listening. async-nats's `double_ack` takes any reply for the confirmation,
//! that status included, so the ack that never reached a consumer used to pass
//! for a retirement and the discard was counted for a message the broker still
//! owns. The accounting waits for a reply without a status.
//!
//! The fixture rejects two messages in one consumer. The first is acked and
//! confirmed as usual, so the counter has to read 1 for it: a consumer that
//! stopped counting would pass the other half of the test alone. The second is
//! held in its handler while the stream is deleted under it, so its ack finds
//! no consumer, and it must not be counted.
//!
//! With a prefetch of one, the second handler can only start once the first
//! message is fully settled, which gives the test its barrier: no counter is
//! polled, because `Snapshotter::snapshot()` *drains* every counter it reads.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot. Hence its own integration binary, a single `#[test]`, and
//! exactly one snapshot taken after the consumer has stopped.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsContainer, NatsServerCmd};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::markers::Nats;
use shove::metadata::MessageMetadata;
use shove::nats::{NatsClient, NatsConfig, NatsConsumer};
use shove::outcome::Outcome;
use shove::topic::Topic as _;
use shove::topology::TopologyBuilder;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// A counter a test can await on rather than sleeping against.
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

struct TestBroker {
    _container: testcontainers::ContainerAsync<NatsContainer>,
    client: NatsClient,
}

impl TestBroker {
    async fn start() -> Self {
        let cmd = NatsServerCmd::default().with_jetstream();
        let container = NatsContainer::default()
            .with_cmd(&cmd)
            .start()
            .await
            .expect("failed to start NATS container");
        let host = container.get_host().await.expect("failed to get host");
        let port = container
            .get_host_port_ipv4(4222)
            .await
            .expect("failed to get NATS port");
        let nats_url = format!("nats://{host}:{port}");

        let client = NatsClient::connect_with_retry(&NatsConfig::new(&nats_url), 10)
            .await
            .expect("failed to connect to NATS");

        Self {
            _container: container,
            client,
        }
    }

    fn broker(&self) -> Broker<Nats> {
        Broker::<Nats>::from_client(self.client.clone())
    }

    fn client(&self) -> NatsClient {
        self.client.clone()
    }
}

// ---------------------------------------------------------------------------
// Topic and handler
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct OrderMessage {
    order_id: String,
}

// Bare topology: no DLQ, no hold queues, so the ack is what drops a rejected
// message and what the discard accounting hangs off.
shove::define_topic!(
    UnconfirmedAckTopic,
    OrderMessage,
    TopologyBuilder::new("nats-metrics-unconfirmed-ack").build()
);

const STREAM: &str = "nats-metrics-unconfirmed-ack";

/// Rejects `confirmed` at once and holds `unconfirmed` in its handler until the
/// test lets it go.
#[derive(Clone)]
struct RejectHandler {
    unconfirmed_entered: WaitableCounter,
    release_unconfirmed: Arc<Notify>,
}

impl MessageHandler<UnconfirmedAckTopic> for RejectHandler {
    type Context = ();
    async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        if msg.order_id == "unconfirmed" {
            self.unconfirmed_entered.increment();
            self.release_unconfirmed.notified().await;
        }
        Outcome::Reject
    }
}

// ---------------------------------------------------------------------------
// Snapshot helpers
// ---------------------------------------------------------------------------

type Snapshot = std::collections::HashMap<
    metrics_util::CompositeKey,
    (
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    ),
>;

/// Every `shove_messages_discarded_total` series in the snapshot, as
/// `(reason, count)` pairs, so a wrong total names the reason that produced it.
fn discarded_series(snapshot: &Snapshot) -> Vec<(String, u64)> {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_messages_discarded_total")
        .map(|(k, (_, _, value))| {
            let reason = k
                .key()
                .labels()
                .find(|l| l.key() == "reason")
                .map_or_else(|| "<unlabelled>".to_string(), |l| l.value().to_string());
            match value {
                DebugValue::Counter(n) => (reason, *n),
                other => panic!("shove_messages_discarded_total is not a counter: {other:?}"),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_reject_is_counted_as_discarded_only_when_its_ack_is_confirmed() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<UnconfirmedAckTopic>()
        .await
        .expect("declare");
    assert!(
        UnconfirmedAckTopic::topology().dlq().is_none(),
        "this fixture is the no-DLQ arm: with a DLQ the discard counter would stay at zero \
         whichever way the ack went"
    );

    let publisher = broker.publisher().await.expect("publisher");
    for order_id in ["confirmed", "unconfirmed"] {
        publisher
            .publish::<UnconfirmedAckTopic>(&OrderMessage {
                order_id: order_id.into(),
            })
            .await
            .expect("publish");
    }

    let handler = RejectHandler {
        unconfirmed_entered: WaitableCounter::new(),
        release_unconfirmed: Arc::new(Notify::new()),
    };
    let entered = handler.unconfirmed_entered.clone();
    let release = Arc::clone(&handler.release_unconfirmed);
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = NatsConsumer::new(client.clone());
    let task = tokio::spawn(async move {
        consumer
            .run::<UnconfirmedAckTopic, _>(
                handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(sc)
                    .with_max_retries(0)
                    .with_prefetch_count(1),
            )
            .await
    });

    // One processing slot: the second handler runs only once the first message
    // is settled, its ack confirmed and its discard recorded.
    assert!(
        entered.wait_for(1, Duration::from_secs(30)).await,
        "the second message must reach its handler"
    );

    // The stream and its consumer go, so the ack the held message sends finds
    // nobody to take it and the server answers with its own status.
    client
        .jetstream()
        .delete_stream(STREAM)
        .await
        .expect("delete the stream under the held message");
    release.notify_one();

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(60), task)
        .await
        .expect("the consumer stops once the held message is settled")
        .expect("consumer task panicked")
        .ok();

    // Single, draining snapshot, taken only once the consumer has stopped so
    // nothing can emit into it while it is being read.
    let snapshot = snapshotter.snapshot().into_hashmap();
    let discarded = discarded_series(&snapshot);
    let discarded_total: u64 = discarded.iter().map(|(_, n)| *n).sum();
    assert_eq!(
        discarded_total, 1,
        "only the confirmed ack counts a discard: the ack that found no consumer retired \
         nothing and the broker still owns the message; got {discarded:?}"
    );
}
