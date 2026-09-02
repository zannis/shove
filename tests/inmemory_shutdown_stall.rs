//! `with_shutdown` must cut every InMemory single-message wait for queue
//! capacity, not just the batch arms #138 already fixed.
//!
//! `InMemoryBroker::enqueue` only races the *broker* shutdown token
//! internally, so before this fix the per-consumer token had no way to
//! unblock a publish wedged on a full queue: an explicit `Reject` (or a
//! poisoned-key cascade) stuck behind a full DLQ, and a sequenced retry
//! republish stuck behind its own full shard queue, all parked `run()` /
//! `run_fifo()` until `broker.close()`. A cooperative-stop wrapper timing
//! that out aborts the task mid-publish, destroying the message instead of
//! redelivering it.
//!
//! Batch counterpart (the reference for this shape):
//! `tests/inmemory_batch.rs::shutdown_unblocks_a_dead_letter_flush_wedged_on_a_full_dlq`.

#![cfg(feature = "inmemory")]

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use shove::broker::Broker;
use shove::inmemory::{InMemoryBroker, InMemoryConfig, InMemoryConsumer};
use shove::markers::InMemory;
use shove::{
    ConsumerOptions, JsonCodec, MessageHandler, MessageMetadata, Outcome, SequenceFailure,
    SequencedTopic, Topic, TopologyBuilder,
};

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Msg {
    id: u64,
}

/// Unsequenced, DLQ, no hold queues: a `Reject` routes straight to the DLQ.
struct RejectStallTopic;
impl Topic for RejectStallTopic {
    type Message = Msg;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("inmem-shutdown-reject-stall")
                .dlq()
                .build()
        })
    }
}

/// Sequenced (1 shard), DLQ, no hold queues, one key per message so poisoning
/// one key never suppresses the others' handler calls.
struct FifoRejectStallTopic;
impl Topic for FifoRejectStallTopic {
    type Message = Msg;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("inmem-shutdown-fifo-reject-stall")
                .sequenced(SequenceFailure::Skip)
                .routing_shards(1)
                .hold_queue(Duration::from_millis(10))
                .dlq()
                .build()
        })
    }
    const SEQUENCE_KEY_FN: Option<fn(&Self::Message) -> String> = Some(Self::sequence_key);
}
impl SequencedTopic for FifoRejectStallTopic {
    fn sequence_key(msg: &Msg) -> String {
        msg.id.to_string()
    }
}

/// Sequenced (1 shard), no hold queues (zero-delay retry), no DLQ needed:
/// the wait under test is the retry republish against the shard queue itself.
struct FifoRetryStallTopic;
impl Topic for FifoRetryStallTopic {
    type Message = Msg;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("inmem-shutdown-fifo-retry-stall")
                .sequenced(SequenceFailure::Skip)
                .routing_shards(1)
                .allow_message_loss()
                .build()
        })
    }
    const SEQUENCE_KEY_FN: Option<fn(&Self::Message) -> String> = Some(Self::sequence_key);
}
impl SequencedTopic for FifoRetryStallTopic {
    fn sequence_key(msg: &Msg) -> String {
        msg.id.to_string()
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Returns a scripted outcome for every delivery and counts calls.
#[derive(Clone)]
struct ScriptedHandler {
    outcome: Outcome,
    calls: Arc<AtomicUsize>,
}

impl ScriptedHandler {
    fn new(outcome: Outcome) -> Self {
        Self {
            outcome,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn wait_for_calls(&self, n: usize, timeout: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if self.calls.load(Ordering::SeqCst) >= n {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        self.calls.load(Ordering::SeqCst) >= n
    }
}

macro_rules! impl_scripted_for {
    ($($topic:ty),* $(,)?) => {
        $(
            impl MessageHandler<$topic> for ScriptedHandler {
                type Context = ();
                async fn handle(&self, _: Msg, _: MessageMetadata, _: &()) -> Outcome {
                    self.calls.fetch_add(1, Ordering::SeqCst);
                    self.outcome.clone()
                }
            }
        )*
    };
}

impl_scripted_for!(RejectStallTopic, FifoRejectStallTopic, FifoRetryStallTopic);

/// Acks everything, recording `(id, delivery_count, retry_count)` per message.
#[derive(Clone)]
struct RecordingAckHandler {
    seen: Arc<Mutex<Vec<(u64, Option<u32>, u32)>>>,
}

impl RecordingAckHandler {
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn wait_for(&self, n: usize, timeout: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if self.seen.lock().await.len() >= n {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        self.seen.lock().await.len() >= n
    }
}

macro_rules! impl_recording_for {
    ($($topic:ty),* $(,)?) => {
        $(
            impl MessageHandler<$topic> for RecordingAckHandler {
                type Context = ();
                async fn handle(&self, msg: Msg, meta: MessageMetadata, _: &()) -> Outcome {
                    self.seen
                        .lock()
                        .await
                        .push((msg.id, meta.delivery_count, meta.retry_count));
                    Outcome::Ack
                }
            }
        )*
    };
}

impl_recording_for!(RejectStallTopic, FifoRejectStallTopic, FifoRetryStallTopic);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn small_broker() -> (InMemoryBroker, Broker<InMemory>) {
    let client = InMemoryBroker::with_config(
        InMemoryConfig::default()
            .with_default_capacity(NonZeroUsize::new(2).expect("2 is non-zero")),
    );
    let broker = Broker::<InMemory>::from_client(client.clone());
    (client, broker)
}

/// Publish `0..n` from a spawned task: shared capacity (2) cannot hold them
/// all at once, so the calls complete only as the consumer drains the queue.
fn publish_all<T>(
    publisher: shove::publisher::Publisher<InMemory>,
    n: u64,
) -> tokio::task::JoinHandle<()>
where
    T: Topic<Message = Msg>,
{
    tokio::spawn(async move {
        for id in 0..n {
            publisher
                .publish::<T>(&Msg { id })
                .await
                .expect("publish should succeed");
        }
    })
}

const TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Acceptance test for the single-message reject path: DLQ capacity (2) below
/// the pending rejects (5), nothing draining the DLQ, cancel the *consumer*
/// token — `run()` must return promptly and the wedged message must survive
/// on the main queue (marked redelivered), not vanish.
#[tokio::test]
async fn shutdown_unblocks_a_reject_wedged_on_a_full_dlq() {
    let (client, broker) = small_broker();
    broker
        .topology()
        .declare::<RejectStallTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.expect("publisher");
    let publish_handle = publish_all::<RejectStallTopic>(publisher, 5);

    let handler = ScriptedHandler::new(Outcome::Reject);
    let shutdown = CancellationToken::new();
    let consumer = InMemoryConsumer::new(client.clone());
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<RejectStallTopic, _>(
                    handler,
                    (),
                    ConsumerOptions::<InMemory>::new()
                        .with_prefetch_count(1)
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    // Two rejects land in the DLQ (capacity 2); the third handler call's
    // route then wedges on the full DLQ.
    assert!(
        handler.wait_for_calls(3, TIMEOUT).await,
        "three messages must reach the handler before the DLQ route wedges"
    );
    publish_handle.await.expect("publisher must not panic");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !handle.is_finished(),
        "the reject route should still be wedged behind the full DLQ here"
    );

    let cancelled_at = Instant::now();
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect(
            "cancelling the per-consumer shutdown token must unblock a reject \
             wedged behind a full DLQ, not hang until broker.close()",
        )
        .expect("consumer task must not panic")
        .ok();
    assert!(
        cancelled_at.elapsed() < Duration::from_secs(2),
        "shutdown took {:?} to take effect",
        cancelled_at.elapsed()
    );

    // The wedged message (id=2) must survive on the main queue, marked
    // redelivered; 3 and 4 were never popped. Nothing may be lost.
    let survivors = RecordingAckHandler::new();
    let shutdown2 = CancellationToken::new();
    let consumer2 = InMemoryConsumer::new(client.clone());
    let handle2 = tokio::spawn({
        let survivors = survivors.clone();
        let shutdown2 = shutdown2.clone();
        async move {
            consumer2
                .run::<RejectStallTopic, _>(
                    survivors,
                    (),
                    ConsumerOptions::<InMemory>::new()
                        .with_prefetch_count(1)
                        .with_shutdown(shutdown2),
                )
                .await
        }
    });
    assert!(
        survivors.wait_for(3, TIMEOUT).await,
        "exactly the messages that never reached the DLQ must survive, got {:?}",
        survivors.seen.lock().await
    );
    shutdown2.cancel();
    handle2.await.unwrap().ok();

    let seen = survivors.seen.lock().await;
    let mut ids: Vec<u64> = seen.iter().map(|(id, _, _)| *id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![2, 3, 4], "got {seen:?}");
    let wedged = seen.iter().find(|(id, _, _)| *id == 2).expect("id=2 seen");
    assert_eq!(
        wedged.1,
        Some(2),
        "the message pulled back from the wedged DLQ publish must be marked \
         redelivered, got {seen:?}"
    );

    broker.close().await;
}

/// Same wedge through the sequenced loop's terminal routing
/// (`route_reject_sequenced`): the shard task must return promptly on the
/// per-consumer token and the wedged message must survive on the shard queue.
#[tokio::test]
async fn shutdown_unblocks_a_sequenced_reject_wedged_on_a_full_dlq() {
    let (client, broker) = small_broker();
    broker
        .topology()
        .declare::<FifoRejectStallTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.expect("publisher");
    let publish_handle = publish_all::<FifoRejectStallTopic>(publisher, 5);

    let handler = ScriptedHandler::new(Outcome::Reject);
    let shutdown = CancellationToken::new();
    let consumer = InMemoryConsumer::new(client.clone());
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run_fifo::<FifoRejectStallTopic, _>(
                    handler,
                    (),
                    ConsumerOptions::<InMemory>::new().with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(
        handler.wait_for_calls(3, TIMEOUT).await,
        "three messages must reach the handler before the DLQ route wedges"
    );
    publish_handle.await.expect("publisher must not panic");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !handle.is_finished(),
        "the sequenced reject route should still be wedged behind the full DLQ here"
    );

    let cancelled_at = Instant::now();
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect(
            "cancelling the per-consumer shutdown token must unblock a sequenced \
             reject wedged behind a full DLQ, not hang until broker.close()",
        )
        .expect("shard task must not panic")
        .ok();
    assert!(
        cancelled_at.elapsed() < Duration::from_secs(2),
        "shutdown took {:?} to take effect",
        cancelled_at.elapsed()
    );

    let survivors = RecordingAckHandler::new();
    let shutdown2 = CancellationToken::new();
    let consumer2 = InMemoryConsumer::new(client.clone());
    let handle2 = tokio::spawn({
        let survivors = survivors.clone();
        let shutdown2 = shutdown2.clone();
        async move {
            consumer2
                .run_fifo::<FifoRejectStallTopic, _>(
                    survivors,
                    (),
                    ConsumerOptions::<InMemory>::new().with_shutdown(shutdown2),
                )
                .await
        }
    });
    assert!(
        survivors.wait_for(3, TIMEOUT).await,
        "exactly the messages that never reached the DLQ must survive, got {:?}",
        survivors.seen.lock().await
    );
    shutdown2.cancel();
    handle2.await.unwrap().ok();

    let seen = survivors.seen.lock().await;
    let mut ids: Vec<u64> = seen.iter().map(|(id, _, _)| *id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![2, 3, 4], "got {seen:?}");
    let wedged = seen.iter().find(|(id, _, _)| *id == 2).expect("id=2 seen");
    assert_eq!(
        wedged.1,
        Some(2),
        "the message pulled back from the wedged DLQ publish must be marked \
         redelivered, got {seen:?}"
    );

    broker.close().await;
}

/// The sibling wait on the retry path: a zero-delay `Retry` republish against
/// the shard's own full queue (publisher keeps it full) must also be cut by
/// the per-consumer token, with the republished message — retry state already
/// stamped — surviving at the front of the shard queue.
#[tokio::test]
async fn shutdown_unblocks_a_sequenced_retry_republish_wedged_on_a_full_shard_queue() {
    let (client, broker) = small_broker();
    broker
        .topology()
        .declare::<FifoRetryStallTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.expect("publisher");
    let publish_handle = publish_all::<FifoRetryStallTopic>(publisher, 5);

    let handler = ScriptedHandler::new(Outcome::Retry);
    let shutdown = CancellationToken::new();
    let consumer = InMemoryConsumer::new(client.clone());
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run_fifo::<FifoRetryStallTopic, _>(
                    handler,
                    (),
                    ConsumerOptions::<InMemory>::new()
                        .with_max_retries(5)
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    // One handler call: the zero-delay republish of id=0 then wedges on the
    // shard queue the publisher keeps full.
    assert!(
        handler.wait_for_calls(1, TIMEOUT).await,
        "the first message must reach the handler"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !handle.is_finished(),
        "the retry republish should still be wedged behind the full shard queue here"
    );

    let cancelled_at = Instant::now();
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect(
            "cancelling the per-consumer shutdown token must unblock a retry \
             republish wedged behind a full shard queue, not hang until broker.close()",
        )
        .expect("shard task must not panic")
        .ok();
    assert!(
        cancelled_at.elapsed() < Duration::from_secs(2),
        "shutdown took {:?} to take effect",
        cancelled_at.elapsed()
    );

    // Drain everything with an acking consumer. All five messages must
    // surface — id=0 with its already-stamped retry state (retry_count=1),
    // proving the republish survived rather than being dropped mid-wait.
    let survivors = RecordingAckHandler::new();
    let shutdown2 = CancellationToken::new();
    let consumer2 = InMemoryConsumer::new(client.clone());
    let handle2 = tokio::spawn({
        let survivors = survivors.clone();
        let shutdown2 = shutdown2.clone();
        async move {
            consumer2
                .run_fifo::<FifoRetryStallTopic, _>(
                    survivors,
                    (),
                    ConsumerOptions::<InMemory>::new().with_shutdown(shutdown2),
                )
                .await
        }
    });
    assert!(
        survivors.wait_for(5, TIMEOUT).await,
        "all five messages must survive, got {:?}",
        survivors.seen.lock().await
    );
    publish_handle.await.expect("publisher must not panic");
    shutdown2.cancel();
    handle2.await.unwrap().ok();

    let seen = survivors.seen.lock().await;
    let mut ids: Vec<u64> = seen.iter().map(|(id, _, _)| *id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1, 2, 3, 4], "got {seen:?}");
    let republished = seen.iter().find(|(id, _, _)| *id == 0).expect("id=0 seen");
    assert_eq!(
        republished.2, 1,
        "the wedged republish must survive with its retry state stamped, got {seen:?}"
    );

    broker.close().await;
}
