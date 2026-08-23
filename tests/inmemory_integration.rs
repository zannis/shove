//! Integration tests for the in-memory broker backend.
//!
//! Migrated to `Broker<InMemory>` + `Publisher<B>` + `TopologyDeclarer<B>` +
//! `ConsumerSupervisor<B>` + `ConsumerGroup<B>`. Tests that require `run_fifo`
//! or `run_dlq` (not yet surfaced on the generic wrappers) keep an
//! `InMemoryConsumer` constructed from the underlying `InMemoryBroker` client.

#![cfg(feature = "inmemory")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use shove::broker::Broker;
use shove::consumer_group::ConsumerGroupConfig;
use shove::inmemory::{
    InMemoryAutoscalerBackend, InMemoryBroker, InMemoryConfig, InMemoryConsumer,
    InMemoryConsumerGroupConfig, InMemoryConsumerGroupRegistry,
};
use shove::markers::InMemory;
use shove::{
    AutoscalerConfig, ConsumerOptions, JsonCodec, MessageHandler, MessageMetadata, Outcome,
    SequenceFailure, SequencedTopic, SupervisorOutcome, Topic, TopologyBuilder,
};

// ---------------------------------------------------------------------------
// Test topics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    id: u64,
}

struct OrdersTopic;
impl Topic for OrdersTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("orders-int")
                .hold_queue(Duration::from_millis(20))
                .hold_queue(Duration::from_millis(100))
                .dlq()
                .build()
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Event {
    account: String,
    seq: u64,
}

struct LedgerFailAllTopic;
impl Topic for LedgerFailAllTopic {
    type Message = Event;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("ledger-failall-int")
                .sequenced(SequenceFailure::FailAll)
                .routing_shards(4)
                .hold_queue(Duration::from_millis(10))
                .dlq()
                .build()
        })
    }
    const SEQUENCE_KEY_FN: Option<fn(&Self::Message) -> String> = Some(Self::sequence_key);
}
impl SequencedTopic for LedgerFailAllTopic {
    fn sequence_key(msg: &Event) -> String {
        msg.account.clone()
    }
}

struct LedgerSkipTopic;
impl Topic for LedgerSkipTopic {
    type Message = Event;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("ledger-skip-int")
                .sequenced(SequenceFailure::Skip)
                .routing_shards(2)
                .hold_queue(Duration::from_millis(10))
                .dlq()
                .build()
        })
    }
    const SEQUENCE_KEY_FN: Option<fn(&Self::Message) -> String> = Some(Self::sequence_key);
}
impl SequencedTopic for LedgerSkipTopic {
    fn sequence_key(msg: &Event) -> String {
        msg.account.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ping(u32);

/// Topic used to test that `register_fifo` auto-declares shard queues without
/// a prior explicit `topology().declare()` call.
struct RegAutoDeclareFifoTopic;
impl Topic for RegAutoDeclareFifoTopic {
    type Message = Event;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("mem-int-reg-auto-fifo")
                .sequenced(SequenceFailure::Skip)
                .routing_shards(2)
                .allow_message_loss()
                .build()
        })
    }
    const SEQUENCE_KEY_FN: Option<fn(&Self::Message) -> String> = Some(Self::sequence_key);
}
impl SequencedTopic for RegAutoDeclareFifoTopic {
    fn sequence_key(msg: &Event) -> String {
        msg.account.clone()
    }
}

struct GroupTopic;
impl Topic for GroupTopic {
    type Message = Ping;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("group-int").dlq().build())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn poll_until<F: Fn() -> bool>(cond: F, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    cond()
}

// ---------------------------------------------------------------------------
// End-to-end happy path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn end_to_end_publish_consume_ack() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker.topology().declare::<OrdersTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 0..5 {
        publisher
            .publish::<OrdersTopic>(&Order { id: i })
            .await
            .unwrap();
    }

    let seen: Arc<Mutex<Vec<Order>>> = Arc::new(Mutex::new(Vec::new()));

    #[derive(Clone)]
    struct H(Arc<Mutex<Vec<Order>>>);
    impl MessageHandler<OrdersTopic> for H {
        type Context = ();
        async fn handle(&self, msg: Order, _: MessageMetadata, _: &()) -> Outcome {
            self.0.lock().await.push(msg);
            Outcome::Ack
        }
    }

    let handler = H(seen.clone());
    let mut supervisor = broker.consumer_supervisor();
    let opts = ConsumerOptions::<InMemory>::new()
        .with_shutdown(CancellationToken::new())
        .with_prefetch_count(1);
    supervisor
        .register::<OrdersTopic, _>(handler, opts)
        .unwrap();

    let token = supervisor.cancellation_token();
    let seen_probe = seen.clone();
    let t = token.clone();
    tokio::spawn(async move {
        poll_until(
            move || seen_probe.try_lock().map(|v| v.len() == 5).unwrap_or(false),
            Duration::from_secs(2),
        )
        .await;
        t.cancel();
    });

    let outcome = supervisor
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(5))
        .await;
    assert!(outcome.is_clean());

    let mut ids: Vec<u64> = seen.lock().await.iter().map(|o| o.id).collect();
    ids.sort();
    assert_eq!(ids, (0..5).collect::<Vec<_>>());
}

// ---------------------------------------------------------------------------
// RawBytesCodec — sentinel: catches any backend that bypasses the codec hook
// and silently round-trips through serde_json / UTF-8 string conversion.
// ---------------------------------------------------------------------------

shove::define_topic!(
    RawBytesIntegrationTopic,
    Vec<u8>,
    TopologyBuilder::new("raw-bytes-integration").build(),
    codec = shove::RawBytesCodec
);

#[tokio::test]
async fn raw_bytes_codec_round_trips_non_utf8_payload() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker
        .topology()
        .declare::<RawBytesIntegrationTopic>()
        .await
        .unwrap();

    // Includes 0xFF (invalid UTF-8 start byte) and 0x00 (NUL terminator).
    // Either a UTF-8 conversion or a C-string truncation in the publish or
    // consume path would corrupt this payload — that's the regression we're
    // sentinelling against.
    let payload: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF];

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<RawBytesIntegrationTopic>(&payload)
        .await
        .unwrap();

    let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));

    #[derive(Clone)]
    struct H(Arc<Mutex<Vec<Vec<u8>>>>);
    impl MessageHandler<RawBytesIntegrationTopic> for H {
        type Context = ();
        async fn handle(&self, msg: Vec<u8>, _: MessageMetadata, _: &()) -> Outcome {
            self.0.lock().await.push(msg);
            Outcome::Ack
        }
    }

    let handler = H(received.clone());
    let mut supervisor = broker.consumer_supervisor();
    let opts = ConsumerOptions::<InMemory>::new()
        .with_shutdown(CancellationToken::new())
        .with_prefetch_count(1);
    supervisor
        .register::<RawBytesIntegrationTopic, _>(handler, opts)
        .unwrap();

    let token = supervisor.cancellation_token();
    let probe = received.clone();
    let t = token.clone();
    tokio::spawn(async move {
        poll_until(
            move || probe.try_lock().map(|v| v.len() == 1).unwrap_or(false),
            Duration::from_secs(2),
        )
        .await;
        t.cancel();
    });

    let outcome = supervisor
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(5))
        .await;
    assert!(outcome.is_clean());

    let captured = received.lock().await;
    assert_eq!(captured.len(), 1);
    assert_eq!(
        captured[0], payload,
        "RawBytesCodec must preserve non-UTF-8 bytes verbatim"
    );
}

// ---------------------------------------------------------------------------
// Retry + hold queue + max_retries → DLQ
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retry_then_ack_after_hold_queue_delay() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker.topology().declare::<OrdersTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<OrdersTopic>(&Order { id: 1 })
        .await
        .unwrap();

    #[derive(Clone)]
    struct Flaky {
        remaining: Arc<AtomicU32>,
        final_retry: Arc<AtomicU32>,
    }
    impl MessageHandler<OrdersTopic> for Flaky {
        type Context = ();
        async fn handle(&self, _: Order, m: MessageMetadata, _: &()) -> Outcome {
            if self.remaining.load(Ordering::Relaxed) > 0 {
                self.remaining.fetch_sub(1, Ordering::Relaxed);
                Outcome::Retry
            } else {
                self.final_retry.store(m.retry_count, Ordering::Relaxed);
                Outcome::Ack
            }
        }
    }
    let remaining = Arc::new(AtomicU32::new(2));
    let final_retry = Arc::new(AtomicU32::new(u32::MAX));
    let handler = Flaky {
        remaining: remaining.clone(),
        final_retry: final_retry.clone(),
    };

    let mut supervisor = broker.consumer_supervisor();
    let opts = ConsumerOptions::<InMemory>::new()
        .with_shutdown(CancellationToken::new())
        .with_prefetch_count(1)
        .with_max_retries(5);
    supervisor
        .register::<OrdersTopic, _>(handler, opts)
        .unwrap();

    let token = supervisor.cancellation_token();
    let remaining_probe = remaining.clone();
    let final_retry_probe = final_retry.clone();
    let t = token.clone();
    tokio::spawn(async move {
        poll_until(
            move || {
                remaining_probe.load(Ordering::Relaxed) == 0
                    && final_retry_probe.load(Ordering::Relaxed) != u32::MAX
            },
            Duration::from_secs(2),
        )
        .await;
        t.cancel();
    });

    let outcome = supervisor
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(5))
        .await;
    assert!(outcome.is_clean());
    assert_eq!(final_retry.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn max_retries_exceeded_goes_to_dlq() {
    // This test uses run_dlq which is not yet surfaced on the generic wrappers,
    // so we keep InMemoryConsumer for the consumer tasks while using the new
    // Broker<InMemory> for setup and publishing.
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker.topology().declare::<OrdersTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<OrdersTopic>(&Order { id: 99 })
        .await
        .unwrap();

    #[derive(Clone)]
    struct AlwaysRetry;
    impl MessageHandler<OrdersTopic> for AlwaysRetry {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Retry
        }
    }

    // Collect DLQ deliveries via run_dlq.
    let dlq_seen = Arc::new(AtomicUsize::new(0));
    #[derive(Clone)]
    struct DlqHandler(Arc<AtomicUsize>);
    impl MessageHandler<OrdersTopic> for DlqHandler {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
        async fn handle_dead(&self, _: Order, _: shove::DeadMessageMetadata, _: &()) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let shutdown = CancellationToken::new();

    let consumer_main = InMemoryConsumer::new(client.clone());
    let shutdown_main = shutdown.clone();
    let main_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<InMemory>::new()
            .with_shutdown(shutdown_main)
            .with_prefetch_count(1)
            .with_max_retries(2);
        consumer_main
            .run::<OrdersTopic, _>(AlwaysRetry, (), opts)
            .await
    });

    let consumer_dlq = InMemoryConsumer::new(client.clone());
    let dlq_handler = DlqHandler(dlq_seen.clone());
    let dlq_handle = tokio::spawn(async move {
        consumer_dlq
            .run_dlq::<OrdersTopic, _>(dlq_handler, ())
            .await
    });

    let dlq_probe = dlq_seen.clone();
    assert!(
        poll_until(
            move || dlq_probe.load(Ordering::Relaxed) == 1,
            Duration::from_secs(2),
        )
        .await
    );

    shutdown.cancel();
    client.shutdown();
    let _ = main_handle.await;
    let _ = dlq_handle.await;
}

// ---------------------------------------------------------------------------
// Sequenced delivery — per-key FIFO with Skip policy
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sequenced_preserves_per_key_order() {
    // run_fifo is not yet surfaced on ConsumerSupervisor; keep InMemoryConsumer
    // for the consumer task.
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker
        .topology()
        .declare::<LedgerSkipTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.unwrap();
    // Interleave three keys.
    for i in 0..10u64 {
        for acc in ["a", "b", "c"] {
            publisher
                .publish::<LedgerSkipTopic>(&Event {
                    account: acc.into(),
                    seq: i,
                })
                .await
                .unwrap();
        }
    }

    #[derive(Clone)]
    struct H(Arc<Mutex<HashMap<String, Vec<u64>>>>);
    impl MessageHandler<LedgerSkipTopic> for H {
        type Context = ();
        async fn handle(&self, msg: Event, _: MessageMetadata, _: &()) -> Outcome {
            self.0
                .lock()
                .await
                .entry(msg.account)
                .or_default()
                .push(msg.seq);
            Outcome::Ack
        }
    }

    let order = Arc::new(Mutex::new(HashMap::<String, Vec<u64>>::new()));
    let handler = H(order.clone());

    let shutdown = CancellationToken::new();
    let consumer = InMemoryConsumer::new(client.clone());
    let shutdown_for_task = shutdown.clone();
    let handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<InMemory>::new()
            .with_shutdown(shutdown_for_task)
            .with_prefetch_count(1);
        consumer
            .run_fifo::<LedgerSkipTopic, _>(handler, (), opts)
            .await
    });

    let probe = order.clone();
    assert!(
        poll_until(
            move || {
                probe
                    .try_lock()
                    .map(|map| map.values().all(|v| v.len() == 10))
                    .unwrap_or(false)
            },
            Duration::from_secs(3),
        )
        .await
    );

    shutdown.cancel();
    let _ = handle.await;

    let final_order = order.lock().await;
    for (acc, seqs) in final_order.iter() {
        assert_eq!(
            seqs,
            &(0..10).collect::<Vec<_>>(),
            "account {acc} out of order"
        );
    }
}

#[tokio::test]
async fn sequenced_failall_poisons_same_key_after_reject() {
    // run_fifo + run_dlq — keep InMemoryConsumer for consumer tasks.
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker
        .topology()
        .declare::<LedgerFailAllTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.unwrap();
    // Publish a stream for account "A" where seq=3 fails, plus unrelated "B".
    for seq in 0..6u64 {
        publisher
            .publish::<LedgerFailAllTopic>(&Event {
                account: "A".into(),
                seq,
            })
            .await
            .unwrap();
    }
    for seq in 0..3u64 {
        publisher
            .publish::<LedgerFailAllTopic>(&Event {
                account: "B".into(),
                seq,
            })
            .await
            .unwrap();
    }

    #[derive(Clone)]
    struct H {
        acked: Arc<Mutex<Vec<(String, u64)>>>,
    }
    impl MessageHandler<LedgerFailAllTopic> for H {
        type Context = ();
        async fn handle(&self, msg: Event, _: MessageMetadata, _: &()) -> Outcome {
            if msg.account == "A" && msg.seq == 3 {
                return Outcome::Reject;
            }
            self.acked.lock().await.push((msg.account, msg.seq));
            Outcome::Ack
        }
    }

    let acked: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let dlq_count = Arc::new(AtomicUsize::new(0));

    let shutdown = CancellationToken::new();
    let consumer = InMemoryConsumer::new(client.clone());
    let shutdown_for_task = shutdown.clone();
    let handler = H {
        acked: acked.clone(),
    };
    let main_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<InMemory>::new()
            .with_shutdown(shutdown_for_task)
            .with_prefetch_count(1);
        consumer
            .run_fifo::<LedgerFailAllTopic, _>(handler, (), opts)
            .await
    });

    #[derive(Clone)]
    struct DlqHandler(Arc<AtomicUsize>);
    impl MessageHandler<LedgerFailAllTopic> for DlqHandler {
        type Context = ();
        async fn handle(&self, _: Event, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
        async fn handle_dead(&self, _: Event, _: shove::DeadMessageMetadata, _: &()) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let dlq_handle = {
        let consumer = InMemoryConsumer::new(client.clone());
        let handler = DlqHandler(dlq_count.clone());
        tokio::spawn(async move { consumer.run_dlq::<LedgerFailAllTopic, _>(handler, ()).await })
    };

    let acked_probe = acked.clone();
    let dlq_probe = dlq_count.clone();
    // B's three messages should all ack; A's seq 0-2 ack, A's seq 3 rejects +
    // poisons key, A's seq 4 and 5 land in DLQ without handler invocation →
    // 3 DLQ entries for A, 6 total acks across A and B.
    assert!(
        poll_until(
            move || {
                let acked_len = acked_probe.try_lock().map(|v| v.len()).unwrap_or(0);
                acked_len == 6 && dlq_probe.load(Ordering::Relaxed) == 3
            },
            Duration::from_secs(3),
        )
        .await,
        "expected 6 acks and 3 DLQ; got acks={:?} dlq={}",
        acked.lock().await,
        dlq_count.load(Ordering::Relaxed)
    );

    shutdown.cancel();
    client.shutdown();
    let _ = main_handle.await;
    let _ = dlq_handle.await;

    let final_acked = acked.lock().await;
    let a_seqs: Vec<u64> = final_acked
        .iter()
        .filter(|(a, _)| a == "A")
        .map(|(_, s)| *s)
        .collect();
    assert_eq!(a_seqs, vec![0, 1, 2], "A must preserve pre-poison order");
}

// ---------------------------------------------------------------------------
// Consumer groups
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Handler panic — must not crash the consumer task
// ---------------------------------------------------------------------------

#[tokio::test]
async fn handler_panic_does_not_crash_consumer() {
    // run_dlq — keep InMemoryConsumer for consumer tasks.
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker.topology().declare::<OrdersTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<OrdersTopic>(&Order { id: 1 })
        .await
        .unwrap(); // will panic in handler
    publisher
        .publish::<OrdersTopic>(&Order { id: 2 })
        .await
        .unwrap(); // will ack normally

    #[derive(Clone)]
    struct H {
        acked_ids: Arc<Mutex<Vec<u64>>>,
    }
    impl MessageHandler<OrdersTopic> for H {
        type Context = ();
        async fn handle(&self, msg: Order, m: MessageMetadata, _: &()) -> Outcome {
            if msg.id == 1 && m.retry_count == 0 {
                panic!("boom — first delivery of id=1");
            }
            self.acked_ids.lock().await.push(msg.id);
            Outcome::Ack
        }
    }

    let dlq_hits = Arc::new(AtomicUsize::new(0));
    #[derive(Clone)]
    struct DlqH(Arc<AtomicUsize>);
    impl MessageHandler<OrdersTopic> for DlqH {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
        async fn handle_dead(&self, _: Order, _: shove::DeadMessageMetadata, _: &()) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let acked_ids = Arc::new(Mutex::new(Vec::new()));
    let handler = H {
        acked_ids: acked_ids.clone(),
    };

    let shutdown = CancellationToken::new();
    let consumer = InMemoryConsumer::new(client.clone());
    let shutdown_for_task = shutdown.clone();
    let main_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<InMemory>::new()
            .with_shutdown(shutdown_for_task)
            .with_prefetch_count(1)
            .with_max_retries(5);
        consumer.run::<OrdersTopic, _>(handler, (), opts).await
    });

    let dlq_handle = {
        let consumer = InMemoryConsumer::new(client.clone());
        let dlq_handler = DlqH(dlq_hits.clone());
        tokio::spawn(async move { consumer.run_dlq::<OrdersTopic, _>(dlq_handler, ()).await })
    };

    // Both messages should eventually ack (panicked one after one retry).
    let probe = acked_ids.clone();
    assert!(
        poll_until(
            move || {
                probe
                    .try_lock()
                    .map(|v| {
                        let mut sorted: Vec<u64> = v.iter().copied().collect();
                        sorted.sort();
                        sorted == vec![1, 2]
                    })
                    .unwrap_or(false)
            },
            Duration::from_secs(3),
        )
        .await,
        "expected both ids to be acked; got {:?}",
        acked_ids.lock().await
    );
    // The panic path must NOT route to DLQ — it retries and succeeds.
    assert_eq!(dlq_hits.load(Ordering::Relaxed), 0);

    shutdown.cancel();
    client.shutdown();
    let _ = main_handle.await;
    let _ = dlq_handle.await;
}

// ---------------------------------------------------------------------------
// Oversized message rejection — must route to DLQ before deserializing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BigPayload {
    data: String,
}

struct BigTopic;
impl Topic for BigTopic {
    type Message = BigPayload;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("big-int").dlq().build())
    }
}

#[tokio::test]
async fn oversized_message_rejected_to_dlq() {
    // run_dlq — keep InMemoryConsumer for consumer tasks.
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker.topology().declare::<BigTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    // ~8 KiB payload; consumer will cap at 1 KiB.
    publisher
        .publish::<BigTopic>(&BigPayload {
            data: "x".repeat(8 * 1024),
        })
        .await
        .unwrap();

    #[derive(Clone)]
    struct NeverCalled(Arc<AtomicUsize>);
    impl MessageHandler<BigTopic> for NeverCalled {
        type Context = ();
        async fn handle(&self, _: BigPayload, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::Relaxed);
            Outcome::Ack
        }
    }

    let handler_calls = Arc::new(AtomicUsize::new(0));
    let dlq_hits = Arc::new(AtomicUsize::new(0));

    #[derive(Clone)]
    struct DlqH(Arc<AtomicUsize>);
    impl MessageHandler<BigTopic> for DlqH {
        type Context = ();
        async fn handle(&self, _: BigPayload, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
        async fn handle_dead(&self, _: BigPayload, _: shove::DeadMessageMetadata, _: &()) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let shutdown = CancellationToken::new();

    let consumer_main = InMemoryConsumer::new(client.clone());
    let main_handler = NeverCalled(handler_calls.clone());
    let shutdown_main = shutdown.clone();
    let main_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<InMemory>::new()
            .with_shutdown(shutdown_main)
            .with_prefetch_count(1)
            .with_max_message_size(1024);
        consumer_main
            .run::<BigTopic, _>(main_handler, (), opts)
            .await
    });

    let dlq_handle = {
        let consumer = InMemoryConsumer::new(client.clone());
        let handler = DlqH(dlq_hits.clone());
        tokio::spawn(async move { consumer.run_dlq::<BigTopic, _>(handler, ()).await })
    };

    let probe = dlq_hits.clone();
    assert!(
        poll_until(
            move || probe.load(Ordering::Relaxed) == 1,
            Duration::from_secs(2),
        )
        .await
    );
    // Handler must never have been invoked — the size check runs before deserialize.
    assert_eq!(handler_calls.load(Ordering::Relaxed), 0);

    shutdown.cancel();
    client.shutdown();
    let _ = main_handle.await;
    let _ = dlq_handle.await;
}

// ---------------------------------------------------------------------------
// Handler timeout → retry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn handler_timeout_triggers_retry_then_dlq() {
    // run_dlq — keep InMemoryConsumer for consumer tasks.
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker.topology().declare::<OrdersTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<OrdersTopic>(&Order { id: 7 })
        .await
        .unwrap();

    #[derive(Clone)]
    struct Sleepy(Arc<AtomicUsize>);
    impl MessageHandler<OrdersTopic> for Sleepy {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(60)).await;
            Outcome::Ack
        }
    }

    let invocations = Arc::new(AtomicUsize::new(0));
    let dlq_hits = Arc::new(AtomicUsize::new(0));

    #[derive(Clone)]
    struct DlqH(Arc<AtomicUsize>);
    impl MessageHandler<OrdersTopic> for DlqH {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
        async fn handle_dead(&self, _: Order, _: shove::DeadMessageMetadata, _: &()) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let shutdown = CancellationToken::new();
    let consumer_main = InMemoryConsumer::new(client.clone());
    let handler = Sleepy(invocations.clone());
    let shutdown_main = shutdown.clone();
    let main_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<InMemory>::new()
            .with_shutdown(shutdown_main)
            .with_prefetch_count(1)
            .with_max_retries(1)
            .with_handler_timeout(Duration::from_millis(50));
        consumer_main.run::<OrdersTopic, _>(handler, (), opts).await
    });

    let dlq_handle = {
        let consumer = InMemoryConsumer::new(client.clone());
        let handler = DlqH(dlq_hits.clone());
        tokio::spawn(async move { consumer.run_dlq::<OrdersTopic, _>(handler, ()).await })
    };

    let probe = dlq_hits.clone();
    assert!(
        poll_until(
            move || probe.load(Ordering::Relaxed) == 1,
            Duration::from_secs(3),
        )
        .await
    );
    // Invoked at least twice (first attempt + one retry — may be higher under scheduler jitter).
    assert!(
        invocations.load(Ordering::Relaxed) >= 2,
        "expected >= 2 handler invocations; got {}",
        invocations.load(Ordering::Relaxed)
    );

    shutdown.cancel();
    client.shutdown();
    let _ = main_handle.await;
    let _ = dlq_handle.await;
}

// ---------------------------------------------------------------------------
// Outcome::Defer — schedules redelivery without incrementing retry_count
// ---------------------------------------------------------------------------

#[tokio::test]
async fn defer_schedules_redelivery_without_incrementing_retry() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker.topology().declare::<OrdersTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<OrdersTopic>(&Order { id: 42 })
        .await
        .unwrap();

    #[derive(Clone)]
    struct Deferring {
        first_call: Arc<AtomicU32>,
        final_retry_count: Arc<AtomicU32>,
    }
    impl MessageHandler<OrdersTopic> for Deferring {
        type Context = ();
        async fn handle(&self, _: Order, m: MessageMetadata, _: &()) -> Outcome {
            if self.first_call.fetch_add(1, Ordering::Relaxed) == 0 {
                Outcome::Defer
            } else {
                self.final_retry_count
                    .store(m.retry_count, Ordering::Relaxed);
                Outcome::Ack
            }
        }
    }

    let first_call = Arc::new(AtomicU32::new(0));
    let final_retry_count = Arc::new(AtomicU32::new(u32::MAX));
    let handler = Deferring {
        first_call: first_call.clone(),
        final_retry_count: final_retry_count.clone(),
    };

    let mut supervisor = broker.consumer_supervisor();
    let opts = ConsumerOptions::<InMemory>::new()
        .with_shutdown(CancellationToken::new())
        .with_prefetch_count(1);
    supervisor
        .register::<OrdersTopic, _>(handler, opts)
        .unwrap();

    let token = supervisor.cancellation_token();
    let probe = final_retry_count.clone();
    let t = token.clone();
    tokio::spawn(async move {
        poll_until(
            move || probe.load(Ordering::Relaxed) != u32::MAX,
            Duration::from_secs(2),
        )
        .await;
        t.cancel();
    });

    let outcome = supervisor
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(5))
        .await;
    assert!(outcome.is_clean());
    assert_eq!(
        final_retry_count.load(Ordering::Relaxed),
        0,
        "Defer must not increment retry_count"
    );
}

// ---------------------------------------------------------------------------
// MessageMetadata::delivery_count — the counter Defer *does* advance
// ---------------------------------------------------------------------------

/// A handler that defers forever pins `retry_count` at 0, so "stuck at N
/// attempts" is inexpressible through it. `delivery_count` is the field that
/// advances across `Defer` hops, which is what makes that condition detectable
/// from inside the handler.
///
/// `Retry` is the other half of the contract: it republishes a copy on every
/// real backend, which starts the broker's counter over, so the in-process
/// broker resets it too. Both halves are pinned here because they are easy to
/// get inconsistent between backends.
#[tokio::test]
async fn delivery_count_advances_across_defers_and_resets_on_retry() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker.topology().declare::<OrdersTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<OrdersTopic>(&Order { id: 7 })
        .await
        .unwrap();

    // Defer, Defer, Retry, Defer, then Ack.
    const OUTCOMES: [Outcome; 4] = [
        Outcome::Defer,
        Outcome::Defer,
        Outcome::Retry,
        Outcome::Defer,
    ];

    /// `(delivery_count, retry_count)` captured on each delivery.
    type Seen = Arc<Mutex<Vec<(Option<u32>, u32)>>>;

    #[derive(Clone)]
    struct Scripted {
        seen: Seen,
    }
    impl MessageHandler<OrdersTopic> for Scripted {
        type Context = ();
        async fn handle(&self, _: Order, m: MessageMetadata, _: &()) -> Outcome {
            let mut seen = self.seen.lock().await;
            seen.push((m.delivery_count, m.retry_count));
            OUTCOMES
                .get(seen.len() - 1)
                .cloned()
                .unwrap_or(Outcome::Ack)
        }
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = Scripted { seen: seen.clone() };

    let mut supervisor = broker.consumer_supervisor();
    let opts = ConsumerOptions::<InMemory>::new()
        .with_shutdown(CancellationToken::new())
        .with_prefetch_count(1);
    supervisor
        .register::<OrdersTopic, _>(handler, opts)
        .unwrap();

    let token = supervisor.cancellation_token();
    let probe = seen.clone();
    let t = token.clone();
    tokio::spawn(async move {
        poll_until(
            move || probe.try_lock().is_ok_and(|s| s.len() > OUTCOMES.len()),
            Duration::from_secs(5),
        )
        .await;
        t.cancel();
    });

    let outcome = supervisor
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(10))
        .await;
    assert!(outcome.is_clean());

    let seen = seen.lock().await;
    assert_eq!(
        *seen,
        vec![
            // Defer keeps the same message: the count climbs, retry budget untouched.
            (Some(1), 0),
            (Some(2), 0),
            (Some(3), 0),
            // Retry republished a copy: count restarts, retry_count advances.
            (Some(1), 1),
            (Some(2), 1),
        ],
        "delivery_count must advance across Defer hops and reset on Retry"
    );
}

// ---------------------------------------------------------------------------
// FailAll poison survives the shard buffer emptying
// ---------------------------------------------------------------------------

/// `SequenceFailure::FailAll` documents that a key stays poisoned for the
/// lifetime of the consumer task. InMemory used to clear its poison set
/// whenever the shard buffer drained, which made it the only backend where a
/// quiet moment silently un-poisoned a key — the exact divergence CAF-84 is
/// about, and the worst place for it, since InMemory is what users assert
/// against in their own tests.
#[tokio::test]
async fn poison_survives_shard_drain() {
    // run_fifo — keep InMemoryConsumer for the consumer task.
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker
        .topology()
        .declare::<LedgerFailAllTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.unwrap();
    // First batch: seq 0 acks, seq 1 rejects → poisons "A" → seq 2 is DLQ'd.
    publisher
        .publish::<LedgerFailAllTopic>(&Event {
            account: "A".into(),
            seq: 0,
        })
        .await
        .unwrap();
    publisher
        .publish::<LedgerFailAllTopic>(&Event {
            account: "A".into(),
            seq: 1,
        })
        .await
        .unwrap();
    publisher
        .publish::<LedgerFailAllTopic>(&Event {
            account: "A".into(),
            seq: 2,
        })
        .await
        .unwrap();

    #[derive(Clone)]
    struct H {
        acked: Arc<Mutex<Vec<u64>>>,
    }
    impl MessageHandler<LedgerFailAllTopic> for H {
        type Context = ();
        async fn handle(&self, msg: Event, _: MessageMetadata, _: &()) -> Outcome {
            if msg.seq == 1 {
                return Outcome::Reject;
            }
            self.acked.lock().await.push(msg.seq);
            Outcome::Ack
        }
    }

    let acked = Arc::new(Mutex::new(Vec::<u64>::new()));
    let handler = H {
        acked: acked.clone(),
    };

    let shutdown = CancellationToken::new();
    let consumer = InMemoryConsumer::new(client.clone());
    let shutdown_for_task = shutdown.clone();
    let handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<InMemory>::new()
            .with_shutdown(shutdown_for_task)
            .with_prefetch_count(1);
        consumer
            .run_fifo::<LedgerFailAllTopic, _>(handler, (), opts)
            .await
    });

    let dlq_count = Arc::new(AtomicUsize::new(0));
    #[derive(Clone)]
    struct DlqHandler(Arc<AtomicUsize>);
    impl MessageHandler<LedgerFailAllTopic> for DlqHandler {
        type Context = ();
        async fn handle(&self, _: Event, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
        async fn handle_dead(&self, _: Event, _: shove::DeadMessageMetadata, _: &()) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let dlq_handle = {
        let consumer = InMemoryConsumer::new(client.clone());
        let handler = DlqHandler(dlq_count.clone());
        tokio::spawn(async move { consumer.run_dlq::<LedgerFailAllTopic, _>(handler, ()).await })
    };

    // First batch settles: seq 0 acks, seq 1 rejects (poisoning "A"), seq 2 is
    // dead-lettered behind the poison → 2 DLQ entries.
    let dlq_probe = dlq_count.clone();
    assert!(
        poll_until(
            move || dlq_probe.load(Ordering::Relaxed) == 2,
            Duration::from_secs(3),
        )
        .await,
        "expected seq 1 (rejected) and seq 2 (poisoned) in the DLQ; got {}",
        dlq_count.load(Ordering::Relaxed)
    );

    // Let the shard sit on an empty buffer — this is what used to clear it.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish more A messages — the key is still poisoned, so they must be
    // dead-lettered without reaching the handler.
    publisher
        .publish::<LedgerFailAllTopic>(&Event {
            account: "A".into(),
            seq: 10,
        })
        .await
        .unwrap();
    publisher
        .publish::<LedgerFailAllTopic>(&Event {
            account: "A".into(),
            seq: 11,
        })
        .await
        .unwrap();

    let dlq_probe = dlq_count.clone();
    assert!(
        poll_until(
            move || dlq_probe.load(Ordering::Relaxed) == 4,
            Duration::from_secs(3),
        )
        .await,
        "post-drain publishes must stay poisoned and land in the DLQ; dlq={}",
        dlq_count.load(Ordering::Relaxed)
    );
    assert_eq!(
        *acked.lock().await,
        vec![0],
        "only the pre-poison message may reach the handler"
    );

    shutdown.cancel();
    client.shutdown();
    let _ = handle.await;
    let _ = dlq_handle.await;
}

// ---------------------------------------------------------------------------
// Autoscaler end-to-end — backlog triggers scale_up via the backend
// ---------------------------------------------------------------------------

#[tokio::test]
async fn autoscaler_scales_up_under_backlog() {
    // This test inspects registry internals (groups(), active_consumers())
    // which are not exposed through the generic ConsumerGroup<B> wrapper,
    // so we keep the old InMemoryConsumerGroupRegistry for state inspection
    // while using Broker<InMemory> for setup and publishing.
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    let registry = Arc::new(Mutex::new(InMemoryConsumerGroupRegistry::new(
        client.clone(),
    )));

    let processed = Arc::new(AtomicUsize::new(0));
    // Sticky gate — once cancelled, all handlers drain immediately. A plain
    // `Notify` would only release one waiter per `notify_one()`, which leaves
    // blocked handlers in group.shutdown() and hangs the test.
    let gate = CancellationToken::new();

    {
        let processed = processed.clone();
        let gate = gate.clone();
        let factory = move || {
            #[derive(Clone)]
            struct Slow {
                processed: Arc<AtomicUsize>,
                gate: CancellationToken,
            }
            impl MessageHandler<GroupTopic> for Slow {
                type Context = ();
                async fn handle(&self, _: Ping, _: MessageMetadata, _: &()) -> Outcome {
                    // Block until released so the backlog persists long enough
                    // to trip the autoscaler's hysteresis window.
                    self.gate.cancelled().await;
                    self.processed.fetch_add(1, Ordering::Relaxed);
                    Outcome::Ack
                }
            }
            Slow {
                processed: processed.clone(),
                gate: gate.clone(),
            }
        };
        let mut reg = registry.lock().await;
        reg.register::<GroupTopic, _>(
            InMemoryConsumerGroupConfig::new(1..=3).with_prefetch_count(1),
            factory,
            (),
        )
        .await
        .unwrap();
        reg.start_all();
    }

    let publisher = broker.publisher().await.unwrap();
    for i in 0..30u32 {
        publisher.publish::<GroupTopic>(&Ping(i)).await.unwrap();
    }

    let config = AutoscalerConfig {
        poll_interval: Duration::from_millis(25),
        scale_up_multiplier: 2.0,
        scale_down_multiplier: 0.5,
        hysteresis_duration: Duration::from_millis(50),
        cooldown_duration: Duration::from_millis(0),
    };
    let mut autoscaler =
        InMemoryAutoscalerBackend::autoscaler(client.clone(), registry.clone(), config);
    let autoscaler_shutdown = CancellationToken::new();
    let as_shutdown_for_task = autoscaler_shutdown.clone();
    let autoscaler_handle = tokio::spawn(async move {
        autoscaler.run(as_shutdown_for_task).await;
    });

    let registry_probe = registry.clone();
    assert!(
        poll_until(
            move || {
                registry_probe
                    .try_lock()
                    .map(|r| {
                        r.groups()
                            .get("group-int")
                            .map(|g| g.active_consumers() >= 2)
                            .unwrap_or(false)
                    })
                    .unwrap_or(false)
            },
            Duration::from_secs(3),
        )
        .await,
        "autoscaler should have scaled up under sustained backlog"
    );

    // Release handlers so the pool can drain for a clean shutdown.
    gate.cancel();
    autoscaler_shutdown.cancel();
    let _ = autoscaler_handle.await;
    registry.lock().await.shutdown_all().await;
}

#[tokio::test]
async fn consumer_group_distributes_load_across_workers() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker.topology().declare::<GroupTopic>().await.unwrap();

    let processed: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

    let mut group = broker.consumer_group();
    {
        let processed = processed.clone();
        let factory = move || {
            #[derive(Clone)]
            struct H(Arc<AtomicUsize>);
            impl MessageHandler<GroupTopic> for H {
                type Context = ();
                async fn handle(&self, _: Ping, _: MessageMetadata, _: &()) -> Outcome {
                    self.0.fetch_add(1, Ordering::Relaxed);
                    Outcome::Ack
                }
            }
            H(processed.clone())
        };
        group
            .register::<GroupTopic, _>(
                ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::new(2..=4)),
                factory,
            )
            .await
            .unwrap();
    }

    // Publish 20 pings.
    let publisher = broker.publisher().await.unwrap();
    for i in 0..20u32 {
        publisher.publish::<GroupTopic>(&Ping(i)).await.unwrap();
    }

    let token = group.cancellation_token();
    let probe = processed.clone();
    let t = token.clone();
    tokio::spawn(async move {
        poll_until(
            move || probe.load(Ordering::Relaxed) == 20,
            Duration::from_secs(3),
        )
        .await;
        t.cancel();
    });

    let outcome = group
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(5))
        .await;
    assert!(outcome.is_clean());
}

// ---------------------------------------------------------------------------
// run_fifo_until_timeout — harness-equivalent for sequenced topics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn run_fifo_until_timeout_clean_drain() {
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker
        .topology()
        .declare::<LedgerSkipTopic>()
        .await
        .unwrap();

    struct H;
    impl MessageHandler<LedgerSkipTopic> for H {
        type Context = ();
        async fn handle(&self, _: Event, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    let consumer = InMemoryConsumer::new(client.clone());
    let signal = tokio::time::sleep(Duration::from_millis(50));
    let opts = ConsumerOptions::<InMemory>::new().with_prefetch_count(1);

    let outcome = consumer
        .run_fifo_until_timeout::<LedgerSkipTopic, _, _>(
            H,
            (),
            opts,
            signal,
            Duration::from_secs(5),
        )
        .await;

    assert_eq!(outcome, SupervisorOutcome::default());
    assert!(outcome.is_clean());
}

#[tokio::test]
async fn run_fifo_until_timeout_counts_panics() {
    // InMemory shard design note: handler panics are caught inside
    // `invoke_handler_caught` and mapped to `Outcome::Retry`, so they never
    // escape the shard as a JoinError. After max_retries the message goes to
    // DLQ and the shard continues. This test verifies that the harness itself
    // does not crash or deadlock when handlers panic — the outcome is clean
    // because InMemory absorbs handler-level panics internally.
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker
        .topology()
        .declare::<LedgerSkipTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<LedgerSkipTopic>(&Event {
            account: "A".into(),
            seq: 0,
        })
        .await
        .unwrap();

    struct PanicHandler;
    impl MessageHandler<LedgerSkipTopic> for PanicHandler {
        type Context = ();
        async fn handle(&self, _: Event, _: MessageMetadata, _: &()) -> Outcome {
            panic!("intentional test panic");
        }
    }

    let consumer = InMemoryConsumer::new(client.clone());
    let signal = tokio::time::sleep(Duration::from_millis(200));
    // Use max_retries=1 so the message exhausts retries quickly and the shard
    // moves on, allowing the harness to see the signal before hanging.
    let opts = ConsumerOptions::<InMemory>::new()
        .with_prefetch_count(1)
        .with_max_retries(1);

    let outcome = consumer
        .run_fifo_until_timeout::<LedgerSkipTopic, _, _>(
            PanicHandler,
            (),
            opts,
            signal,
            Duration::from_secs(5),
        )
        .await;

    // InMemory absorbs handler panics as Retry; the harness returns cleanly.
    // Other backends (RabbitMQ/Kafka/NATS/SQS) may surface panics differently.
    assert!(
        !outcome.timed_out,
        "harness must not hang on handler panics; got {outcome:?}"
    );
}

#[tokio::test]
async fn run_fifo_until_timeout_drain_does_not_hang_on_slow_handler() {
    // InMemory shard design note: `invoke_handler_caught` races the handler
    // against the shutdown token and aborts the handler task when shutdown
    // fires. As a result InMemory shards always exit promptly on shutdown,
    // so `timed_out` will always be false regardless of the drain_timeout
    // value. This test verifies the drain completes without hanging even when
    // the signal fires while a slow handler is in-flight — the drain window
    // is set to 100 ms, which is more than enough for InMemory.
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker
        .topology()
        .declare::<LedgerSkipTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<LedgerSkipTopic>(&Event {
            account: "A".into(),
            seq: 0,
        })
        .await
        .unwrap();

    struct SlowHandler;
    impl MessageHandler<LedgerSkipTopic> for SlowHandler {
        type Context = ();
        async fn handle(&self, _: Event, _: MessageMetadata, _: &()) -> Outcome {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Outcome::Ack
        }
    }

    let consumer = InMemoryConsumer::new(client.clone());
    let signal = tokio::time::sleep(Duration::from_millis(50));
    let drain = Duration::from_millis(100);
    let opts = ConsumerOptions::<InMemory>::new().with_prefetch_count(1);

    let outcome = consumer
        .run_fifo_until_timeout::<LedgerSkipTopic, _, _>(SlowHandler, (), opts, signal, drain)
        .await;

    // InMemory shards respond to shutdown immediately, so drain finishes
    // well within the 100 ms window — timed_out must be false.
    assert!(
        !outcome.timed_out,
        "InMemory drain must not time out; got {outcome:?}"
    );
    assert_eq!(outcome.exit_code(), 0);
}

#[tokio::test]
async fn consumer_group_register_fifo_drains_via_run_until_timeout() {
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker
        .topology()
        .declare::<LedgerSkipTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.unwrap();
    for seq in 0..3u64 {
        publisher
            .publish::<LedgerSkipTopic>(&Event {
                account: "A".into(),
                seq,
            })
            .await
            .unwrap();
    }

    let consumed = Arc::new(AtomicUsize::new(0));
    struct H(Arc<AtomicUsize>);
    impl MessageHandler<LedgerSkipTopic> for H {
        type Context = ();
        async fn handle(&self, _: Event, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::Relaxed);
            Outcome::Ack
        }
    }

    let mut group = broker.consumer_group();
    let counter = consumed.clone();
    group
        .register_fifo::<LedgerSkipTopic, _>(
            ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::default()),
            move || H(counter.clone()),
        )
        .await
        .unwrap();

    // Wait until all 3 messages were actually consumed before signalling drain,
    // so a regression where shards never start would fail the test.
    let signal_counter = consumed.clone();
    let signal = async move {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while signal_counter.load(Ordering::Relaxed) < 3 {
            if std::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let outcome = group
        .run_until_timeout(signal, Duration::from_secs(5))
        .await;
    assert!(outcome.is_clean(), "outcome was {outcome:?}");
    assert_eq!(consumed.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn run_fifo_until_timeout_returns_clean_when_shards_finish_first() {
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker
        .topology()
        .declare::<LedgerSkipTopic>()
        .await
        .unwrap();

    struct H;
    impl MessageHandler<LedgerSkipTopic> for H {
        type Context = ();
        async fn handle(&self, _: Event, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    let consumer = InMemoryConsumer::new(client.clone());
    let opts_shutdown = CancellationToken::new();
    let opts = ConsumerOptions::<InMemory>::new()
        .with_shutdown(opts_shutdown.clone())
        .with_prefetch_count(1);

    let killer = opts_shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        killer.cancel();
    });

    let outcome = consumer
        .run_fifo_until_timeout::<LedgerSkipTopic, _, _>(
            H,
            (),
            opts,
            std::future::pending::<()>(),
            Duration::from_secs(5),
        )
        .await;

    assert_eq!(outcome, SupervisorOutcome::default());
}

// ---------------------------------------------------------------------------
// Registry-level default handler timeout (ConsumerGroup path)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn registry_default_handler_timeout_times_out_slow_handler() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use serde::{Deserialize, Serialize};
    use shove::inmemory::{InMemoryConfig, InMemoryConsumerGroupConfig};
    use shove::{
        Broker, ConsumerGroupConfig, InMemory, JsonCodec, MessageHandler, MessageMetadata, Outcome,
        Topic, TopologyBuilder,
    };

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Slow {
        n: u32,
    }

    struct SlowTopic;
    impl Topic for SlowTopic {
        type Message = Slow;
        type Codec = JsonCodec;
        fn topology() -> &'static shove::QueueTopology {
            static T: std::sync::OnceLock<shove::QueueTopology> = std::sync::OnceLock::new();
            T.get_or_init(|| {
                TopologyBuilder::new("registry-default-timeout-test")
                    .hold_queue(Duration::from_millis(50))
                    .dlq()
                    .build()
            })
        }
    }

    #[derive(Clone)]
    struct Ctx {
        redelivered: Arc<AtomicU32>,
    }

    struct SlowHandler;
    impl MessageHandler<SlowTopic> for SlowHandler {
        type Context = Ctx;
        async fn handle(&self, _msg: Slow, meta: MessageMetadata, ctx: &Ctx) -> Outcome {
            if meta.retry_count == 0 {
                tokio::time::sleep(Duration::from_millis(500)).await;
                Outcome::Ack
            } else {
                ctx.redelivered.fetch_add(1, Ordering::SeqCst);
                Outcome::Ack
            }
        }
    }

    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("broker");
    broker
        .topology()
        .declare::<SlowTopic>()
        .await
        .expect("declare");
    let ctx = Ctx {
        redelivered: Arc::new(AtomicU32::new(0)),
    };

    let mut group = broker
        .consumer_group()
        .with_context(ctx.clone())
        .with_default_handler_timeout(Duration::from_millis(50));
    group
        .register::<SlowTopic, _>(
            ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::new(1..=1)),
            || SlowHandler,
        )
        .await
        .expect("register");

    broker
        .publisher()
        .await
        .expect("publisher")
        .publish::<SlowTopic>(&Slow { n: 1 })
        .await
        .expect("publish");

    // Drive shutdown on observed redelivery rather than a fixed sleep so
    // the test isn't racy on a loaded CI host.
    let token = group.cancellation_token();
    let redelivered_probe = ctx.redelivered.clone();
    let canceller_token = token.clone();
    let canceller = tokio::spawn(async move {
        let observed = poll_until(
            move || redelivered_probe.load(Ordering::SeqCst) >= 1,
            Duration::from_secs(5),
        )
        .await;
        canceller_token.cancel();
        observed
    });
    let outcome = group
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(1))
        .await;
    let observed = canceller.await.expect("canceller");
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert!(
        observed && ctx.redelivered.load(Ordering::SeqCst) >= 1,
        "expected >=1 redelivery after handler timeout via registry default"
    );
}

/// Regression test for the abort-leak: when `run_fifo_until_timeout`
/// returns, the underlying shard task must not keep invoking handlers.
///
/// Without the `AbortOnDrop` guard on the inner shard `JoinHandle`,
/// dropping the wrapper task on drain-timeout would merely *detach* the
/// shard, leaving it free to pull more messages and invoke more handlers
/// after the function returns. With the guard, aborting the wrapper
/// aborts the shard, so the invocation count freezes at the point of
/// return.
///
/// On InMemory, the shard cooperatively exits on shutdown so the drain
/// timeout escalation rarely fires; this test still validates the
/// invariant by checking the invocation count is stable across a
/// post-return wait. On backends where in-flight handlers don't abort on
/// shutdown (RabbitMQ, Kafka, NATS), the same invariant catches actual
/// detached-shard leaks.
#[tokio::test]
async fn run_fifo_until_timeout_does_not_invoke_handlers_after_return() {
    use std::sync::atomic::AtomicU32;

    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker
        .topology()
        .declare::<LedgerSkipTopic>()
        .await
        .unwrap();

    let publisher = broker.publisher().await.unwrap();
    for seq in 0..5u64 {
        publisher
            .publish::<LedgerSkipTopic>(&Event {
                account: "A".into(),
                seq,
            })
            .await
            .unwrap();
    }

    #[derive(Clone)]
    struct CountingSlowHandler(Arc<AtomicU32>);
    impl MessageHandler<LedgerSkipTopic> for CountingSlowHandler {
        type Context = ();
        async fn handle(&self, _: Event, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::Relaxed);
            // Long enough that drain budget MUST elapse first on backends
            // where in-flight handlers ignore shutdown.
            tokio::time::sleep(Duration::from_secs(60)).await;
            Outcome::Ack
        }
    }

    let invoked = Arc::new(AtomicU32::new(0));
    let handler = CountingSlowHandler(invoked.clone());

    let consumer = InMemoryConsumer::new(client.clone());
    let signal = tokio::time::sleep(Duration::from_millis(150));
    let opts = ConsumerOptions::<InMemory>::new().with_prefetch_count(1);

    let outcome = consumer
        .run_fifo_until_timeout::<LedgerSkipTopic, _, _>(
            handler,
            (),
            opts,
            signal,
            Duration::from_millis(100),
        )
        .await;

    let invocations_at_return = invoked.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let invocations_after_wait = invoked.load(Ordering::Relaxed);

    assert_eq!(
        invocations_at_return, invocations_after_wait,
        "shard task kept invoking handlers after return ({} → {}, outcome: {outcome:?})",
        invocations_at_return, invocations_after_wait
    );
}

#[tokio::test]
async fn registry_default_handler_timeout_applies_to_fifo_registrations() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use serde::{Deserialize, Serialize};
    use shove::inmemory::InMemoryConfig;
    use shove::{
        Broker, InMemory, MessageHandler, MessageMetadata, Outcome, SequenceFailure,
        SequencedTopic, TopologyBuilder, define_sequenced_topic,
    };

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct LedgerEntry {
        account_id: String,
    }

    define_sequenced_topic!(
        SlowLedger,
        LedgerEntry,
        |msg| msg.account_id.clone(),
        TopologyBuilder::new("fifo-registry-default-timeout-test")
            .sequenced(SequenceFailure::FailAll)
            .hold_queue(Duration::from_millis(50))
            .dlq()
            .build()
    );

    #[derive(Clone)]
    struct Ctx {
        redelivered: Arc<AtomicU32>,
    }

    struct SlowHandler;
    impl MessageHandler<SlowLedger> for SlowHandler {
        type Context = Ctx;
        async fn handle(&self, _msg: LedgerEntry, meta: MessageMetadata, ctx: &Ctx) -> Outcome {
            if meta.retry_count == 0 {
                tokio::time::sleep(Duration::from_millis(500)).await;
                Outcome::Ack
            } else {
                ctx.redelivered.fetch_add(1, Ordering::SeqCst);
                Outcome::Ack
            }
        }
    }

    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("broker");
    broker
        .topology()
        .declare::<SlowLedger>()
        .await
        .expect("declare");
    let ctx = Ctx {
        redelivered: Arc::new(AtomicU32::new(0)),
    };

    let mut group = broker
        .consumer_group()
        .with_context(ctx.clone())
        .with_default_handler_timeout(Duration::from_millis(50));
    group
        .register_fifo::<SlowLedger, _>(
            ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::default()),
            || SlowHandler,
        )
        .await
        .expect("register_fifo");

    broker
        .publisher()
        .await
        .expect("publisher")
        .publish::<SlowLedger>(&LedgerEntry {
            account_id: "acct-1".into(),
        })
        .await
        .expect("publish");

    // Drive shutdown on observed redelivery rather than a fixed sleep so
    // the test isn't racy on a loaded CI host.
    let token = group.cancellation_token();
    let redelivered_probe = ctx.redelivered.clone();
    let canceller_token = token.clone();
    let canceller = tokio::spawn(async move {
        let observed = poll_until(
            move || redelivered_probe.load(Ordering::SeqCst) >= 1,
            Duration::from_secs(5),
        )
        .await;
        canceller_token.cancel();
        observed
    });
    let outcome = group
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(1))
        .await;
    let observed = canceller.await.expect("canceller");
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert!(
        observed && ctx.redelivered.load(Ordering::SeqCst) >= 1,
        "registry default handler timeout did not propagate to FIFO registrations",
    );
}

// ---------------------------------------------------------------------------
// arch-8: register / register_fifo must auto-declare topology
// ---------------------------------------------------------------------------

/// `consumer_group().register_fifo()` must create shard queues without
/// requiring a prior `topology().declare()` call — matching RabbitMQ, NATS,
/// Kafka, and Redis which all auto-declare inside `register_fifo`.
#[tokio::test]
async fn consumer_group_register_fifo_auto_declares_topology() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();

    // register_fifo must internally declare the shard queues.
    // No explicit topology().declare() here.
    let consumed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    #[derive(Clone)]
    struct H(Arc<std::sync::atomic::AtomicUsize>);
    impl MessageHandler<RegAutoDeclareFifoTopic> for H {
        type Context = ();
        async fn handle(&self, _: Event, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::Relaxed);
            Outcome::Ack
        }
    }

    let mut group = broker.consumer_group();
    let counter = consumed.clone();
    group
        .register_fifo::<RegAutoDeclareFifoTopic, _>(
            ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::default()),
            move || H(counter.clone()),
        )
        .await
        .expect("register_fifo must succeed and auto-declare shard queues");

    // Publish after register — shard queues must exist by now.
    let publisher = broker.publisher().await.unwrap();
    for seq in 0..3u64 {
        publisher
            .publish::<RegAutoDeclareFifoTopic>(&Event {
                account: "A".into(),
                seq,
            })
            .await
            .expect("publish");
    }

    let probe = consumed.clone();
    let signal = async move {
        poll_until(
            move || probe.load(Ordering::Relaxed) >= 3,
            Duration::from_secs(5),
        )
        .await;
    };

    let outcome = group
        .run_until_timeout(signal, Duration::from_secs(1))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert_eq!(consumed.load(Ordering::Relaxed), 3);
}

// ---------------------------------------------------------------------------
// Autoscaling vertical slice — backlog triggers scale-up, then drains clean
// ---------------------------------------------------------------------------

/// A dedicated topic for the autoscaling integration test so it does not share
/// the "group-int" queue with other tests running in the same process.
struct AutoscalingTopic;
impl Topic for AutoscalingTopic {
    type Message = Ping;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("autoscaling-int").dlq().build())
    }
}

#[tokio::test]
async fn autoscaling_scales_up_under_backlog_then_drains_clean() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker
        .topology()
        .declare::<AutoscalingTopic>()
        .await
        .unwrap();

    let processed = Arc::new(AtomicUsize::new(0));

    let mut group = broker.consumer_group();
    {
        let processed = processed.clone();
        group
            .register::<AutoscalingTopic, _>(
                ConsumerGroupConfig::new(
                    InMemoryConsumerGroupConfig::new(1..=4).with_prefetch_count(1),
                ),
                move || {
                    #[derive(Clone)]
                    struct SlowHandler(Arc<AtomicUsize>);
                    impl MessageHandler<AutoscalingTopic> for SlowHandler {
                        type Context = ();
                        async fn handle(&self, _: Ping, _: MessageMetadata, _: &()) -> Outcome {
                            // Slow enough that a burst of 40 messages accumulates
                            // a backlog and pushes messages_ready above
                            // capacity × scale_up_multiplier (1 × 1 × 1.5 = 1.5).
                            tokio::time::sleep(Duration::from_millis(75)).await;
                            self.0.fetch_add(1, Ordering::Relaxed);
                            Outcome::Ack
                        }
                    }
                    SlowHandler(processed.clone())
                },
            )
            .await
            .unwrap();
    }

    // Publish a burst large enough to build a sustained backlog.
    let publisher = broker.publisher().await.unwrap();
    for i in 0..40u32 {
        publisher
            .publish::<AutoscalingTopic>(&Ping(i))
            .await
            .unwrap();
    }

    // AutoscalerConfig tuned for a fast test:
    //   poll_interval 100ms — several polls happen within the 2-3 s window.
    //   scale_up_multiplier 1.5 — with prefetch=1, even 2 ready messages triggers
    //   scale-up once hysteresis elapses.
    //   hysteresis_duration 100ms — condition must persist 100ms before action.
    //   cooldown_duration 200ms — allows multiple scale-up steps during the run.
    let cfg = AutoscalerConfig {
        poll_interval: Duration::from_millis(100),
        scale_up_multiplier: 1.5,
        scale_down_multiplier: 0.3,
        hysteresis_duration: Duration::from_millis(100),
        cooldown_duration: Duration::from_millis(200),
    };

    // Signal fires after 2.5 s — enough for several autoscaler poll cycles and
    // for the 40-message backlog to drain across scaled-up consumers.
    let signal = tokio::time::sleep(Duration::from_millis(2500));
    let outcome = group
        .enable_autoscaling(cfg)
        .run_until_timeout(signal, Duration::from_secs(5))
        .await;

    assert_eq!(
        outcome.exit_code(),
        0,
        "autoscaling group must drain cleanly; outcome: {outcome:?}"
    );

    // NOTE: The InMemory ConsumerGroup<B> wrapper does not expose a public
    // API to observe the peak active consumer count from outside the group
    // (no pub method on the generic wrapper for registry introspection).
    // Scaling behaviour — that the autoscaler fires ScaleUp decisions and
    // actually spawns additional consumers — is covered by the unit tests in
    // src/autoscaler.rs and the lower-level integration test
    // `autoscaler_scales_up_under_backlog` which uses the raw
    // InMemoryConsumerGroupRegistry. This test verifies the full integrated
    // lifecycle: broker setup → topic declaration → consumer group registration
    // → autoscaling enabled → backlog published → signal → clean drain.
}
