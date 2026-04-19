//! Integration tests for the in-memory broker backend.

#![cfg(feature = "inmemory")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use shove::inmemory::{
    InMemoryAutoscalerBackend, InMemoryBroker, InMemoryConsumer, InMemoryConsumerGroupConfig,
    InMemoryConsumerGroupRegistry, InMemoryPublisher, InMemoryTopologyDeclarer,
};
use shove::{
    AutoscalerConfig, Consumer, ConsumerOptions, MessageHandler, MessageMetadata, Outcome,
    Publisher, SequenceFailure, SequencedTopic, Topic, TopologyBuilder, declare_topic,
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
    fn topology() -> &'static shove::QueueTopology {
        static T: std::sync::OnceLock<shove::QueueTopology> = std::sync::OnceLock::new();
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
    fn topology() -> &'static shove::QueueTopology {
        static T: std::sync::OnceLock<shove::QueueTopology> = std::sync::OnceLock::new();
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
    fn topology() -> &'static shove::QueueTopology {
        static T: std::sync::OnceLock<shove::QueueTopology> = std::sync::OnceLock::new();
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

struct GroupTopic;
impl Topic for GroupTopic {
    type Message = Ping;
    fn topology() -> &'static shove::QueueTopology {
        static T: std::sync::OnceLock<shove::QueueTopology> = std::sync::OnceLock::new();
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
    let broker = InMemoryBroker::new();
    let declarer = InMemoryTopologyDeclarer::new(broker.clone());
    declare_topic::<OrdersTopic>(&declarer).await.unwrap();

    let publisher = InMemoryPublisher::new(broker.clone());
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
        async fn handle(&self, msg: Order, _: MessageMetadata) -> Outcome {
            self.0.lock().await.push(msg);
            Outcome::Ack
        }
    }

    let shutdown = CancellationToken::new();
    let consumer = InMemoryConsumer::new(broker.clone());
    let handler = H(seen.clone());
    let shutdown_for_task = shutdown.clone();
    let handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(shutdown_for_task).with_prefetch_count(1);
        consumer.run::<OrdersTopic>(handler, opts).await
    });

    let seen_probe = seen.clone();
    assert!(
        poll_until(
            move || seen_probe.try_lock().map(|v| v.len() == 5).unwrap_or(false),
            Duration::from_secs(2),
        )
        .await
    );

    shutdown.cancel();
    let _ = handle.await;

    let mut ids: Vec<u64> = seen.lock().await.iter().map(|o| o.id).collect();
    ids.sort();
    assert_eq!(ids, (0..5).collect::<Vec<_>>());
}

// ---------------------------------------------------------------------------
// Retry + hold queue + max_retries → DLQ
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retry_then_ack_after_hold_queue_delay() {
    let broker = InMemoryBroker::new();
    let declarer = InMemoryTopologyDeclarer::new(broker.clone());
    declare_topic::<OrdersTopic>(&declarer).await.unwrap();

    let publisher = InMemoryPublisher::new(broker.clone());
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
        async fn handle(&self, _: Order, m: MessageMetadata) -> Outcome {
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

    let shutdown = CancellationToken::new();
    let consumer = InMemoryConsumer::new(broker.clone());
    let shutdown_for_task = shutdown.clone();
    let handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(shutdown_for_task)
            .with_prefetch_count(1)
            .with_max_retries(5);
        consumer.run::<OrdersTopic>(handler, opts).await
    });

    // Two retries: 20ms + 100ms = 120ms + margin.
    let remaining_probe = remaining.clone();
    let final_retry_probe = final_retry.clone();
    assert!(
        poll_until(
            move || remaining_probe.load(Ordering::Relaxed) == 0
                && final_retry_probe.load(Ordering::Relaxed) != u32::MAX,
            Duration::from_secs(2),
        )
        .await
    );
    assert_eq!(final_retry.load(Ordering::Relaxed), 2);

    shutdown.cancel();
    let _ = handle.await;
}

#[tokio::test]
async fn max_retries_exceeded_goes_to_dlq() {
    let broker = InMemoryBroker::new();
    let declarer = InMemoryTopologyDeclarer::new(broker.clone());
    declare_topic::<OrdersTopic>(&declarer).await.unwrap();

    let publisher = InMemoryPublisher::new(broker.clone());
    publisher
        .publish::<OrdersTopic>(&Order { id: 99 })
        .await
        .unwrap();

    #[derive(Clone)]
    struct AlwaysRetry;
    impl MessageHandler<OrdersTopic> for AlwaysRetry {
        async fn handle(&self, _: Order, _: MessageMetadata) -> Outcome {
            Outcome::Retry
        }
    }

    // Collect DLQ deliveries via run_dlq.
    let dlq_seen = Arc::new(AtomicUsize::new(0));
    #[derive(Clone)]
    struct DlqHandler(Arc<AtomicUsize>);
    impl MessageHandler<OrdersTopic> for DlqHandler {
        async fn handle(&self, _: Order, _: MessageMetadata) -> Outcome {
            Outcome::Ack
        }
        async fn handle_dead(&self, _: Order, _: shove::DeadMessageMetadata) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let shutdown = CancellationToken::new();

    let consumer_main = InMemoryConsumer::new(broker.clone());
    let shutdown_main = shutdown.clone();
    let main_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(shutdown_main)
            .with_prefetch_count(1)
            .with_max_retries(2);
        consumer_main.run::<OrdersTopic>(AlwaysRetry, opts).await
    });

    let consumer_dlq = InMemoryConsumer::new(broker.clone());
    let dlq_handler = DlqHandler(dlq_seen.clone());
    let dlq_handle =
        tokio::spawn(async move { consumer_dlq.run_dlq::<OrdersTopic>(dlq_handler).await });

    let dlq_probe = dlq_seen.clone();
    assert!(
        poll_until(
            move || dlq_probe.load(Ordering::Relaxed) == 1,
            Duration::from_secs(2),
        )
        .await
    );

    shutdown.cancel();
    broker.shutdown();
    let _ = main_handle.await;
    let _ = dlq_handle.await;
}

// ---------------------------------------------------------------------------
// Sequenced delivery — per-key FIFO with Skip policy
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sequenced_preserves_per_key_order() {
    let broker = InMemoryBroker::new();
    let declarer = InMemoryTopologyDeclarer::new(broker.clone());
    declare_topic::<LedgerSkipTopic>(&declarer).await.unwrap();

    let publisher = InMemoryPublisher::new(broker.clone());
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
        async fn handle(&self, msg: Event, _: MessageMetadata) -> Outcome {
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
    let consumer = InMemoryConsumer::new(broker.clone());
    let shutdown_for_task = shutdown.clone();
    let handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(shutdown_for_task).with_prefetch_count(1);
        consumer.run_fifo::<LedgerSkipTopic>(handler, opts).await
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
    let broker = InMemoryBroker::new();
    let declarer = InMemoryTopologyDeclarer::new(broker.clone());
    declare_topic::<LedgerFailAllTopic>(&declarer)
        .await
        .unwrap();

    let publisher = InMemoryPublisher::new(broker.clone());
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
        async fn handle(&self, msg: Event, _: MessageMetadata) -> Outcome {
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
    let consumer = InMemoryConsumer::new(broker.clone());
    let shutdown_for_task = shutdown.clone();
    let handler = H {
        acked: acked.clone(),
    };
    let main_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(shutdown_for_task).with_prefetch_count(1);
        consumer.run_fifo::<LedgerFailAllTopic>(handler, opts).await
    });

    #[derive(Clone)]
    struct DlqHandler(Arc<AtomicUsize>);
    impl MessageHandler<LedgerFailAllTopic> for DlqHandler {
        async fn handle(&self, _: Event, _: MessageMetadata) -> Outcome {
            Outcome::Ack
        }
        async fn handle_dead(&self, _: Event, _: shove::DeadMessageMetadata) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let dlq_handle = {
        let consumer = InMemoryConsumer::new(broker.clone());
        let handler = DlqHandler(dlq_count.clone());
        tokio::spawn(async move { consumer.run_dlq::<LedgerFailAllTopic>(handler).await })
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
    broker.shutdown();
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
    let broker = InMemoryBroker::new();
    let declarer = InMemoryTopologyDeclarer::new(broker.clone());
    declare_topic::<OrdersTopic>(&declarer).await.unwrap();

    let publisher = InMemoryPublisher::new(broker.clone());
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
        async fn handle(&self, msg: Order, m: MessageMetadata) -> Outcome {
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
        async fn handle(&self, _: Order, _: MessageMetadata) -> Outcome {
            Outcome::Ack
        }
        async fn handle_dead(&self, _: Order, _: shove::DeadMessageMetadata) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let acked_ids = Arc::new(Mutex::new(Vec::new()));
    let handler = H {
        acked_ids: acked_ids.clone(),
    };

    let shutdown = CancellationToken::new();
    let consumer = InMemoryConsumer::new(broker.clone());
    let shutdown_for_task = shutdown.clone();
    let main_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(shutdown_for_task)
            .with_prefetch_count(1)
            .with_max_retries(5);
        consumer.run::<OrdersTopic>(handler, opts).await
    });

    let dlq_handle = {
        let consumer = InMemoryConsumer::new(broker.clone());
        let dlq_handler = DlqH(dlq_hits.clone());
        tokio::spawn(async move { consumer.run_dlq::<OrdersTopic>(dlq_handler).await })
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
    broker.shutdown();
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
    fn topology() -> &'static shove::QueueTopology {
        static T: std::sync::OnceLock<shove::QueueTopology> = std::sync::OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("big-int").dlq().build())
    }
}

#[tokio::test]
async fn oversized_message_rejected_to_dlq() {
    let broker = InMemoryBroker::new();
    let declarer = InMemoryTopologyDeclarer::new(broker.clone());
    declare_topic::<BigTopic>(&declarer).await.unwrap();

    let publisher = InMemoryPublisher::new(broker.clone());
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
        async fn handle(&self, _: BigPayload, _: MessageMetadata) -> Outcome {
            self.0.fetch_add(1, Ordering::Relaxed);
            Outcome::Ack
        }
    }

    let handler_calls = Arc::new(AtomicUsize::new(0));
    let dlq_hits = Arc::new(AtomicUsize::new(0));

    #[derive(Clone)]
    struct DlqH(Arc<AtomicUsize>);
    impl MessageHandler<BigTopic> for DlqH {
        async fn handle(&self, _: BigPayload, _: MessageMetadata) -> Outcome {
            Outcome::Ack
        }
        async fn handle_dead(&self, _: BigPayload, _: shove::DeadMessageMetadata) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let shutdown = CancellationToken::new();

    let consumer_main = InMemoryConsumer::new(broker.clone());
    let main_handler = NeverCalled(handler_calls.clone());
    let shutdown_main = shutdown.clone();
    let main_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(shutdown_main)
            .with_prefetch_count(1)
            .with_max_message_size(1024);
        consumer_main.run::<BigTopic>(main_handler, opts).await
    });

    let dlq_handle = {
        let consumer = InMemoryConsumer::new(broker.clone());
        let handler = DlqH(dlq_hits.clone());
        tokio::spawn(async move { consumer.run_dlq::<BigTopic>(handler).await })
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
    broker.shutdown();
    let _ = main_handle.await;
    let _ = dlq_handle.await;
}

// ---------------------------------------------------------------------------
// Handler timeout → retry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn handler_timeout_triggers_retry_then_dlq() {
    let broker = InMemoryBroker::new();
    let declarer = InMemoryTopologyDeclarer::new(broker.clone());
    declare_topic::<OrdersTopic>(&declarer).await.unwrap();

    let publisher = InMemoryPublisher::new(broker.clone());
    publisher
        .publish::<OrdersTopic>(&Order { id: 7 })
        .await
        .unwrap();

    #[derive(Clone)]
    struct Sleepy(Arc<AtomicUsize>);
    impl MessageHandler<OrdersTopic> for Sleepy {
        async fn handle(&self, _: Order, _: MessageMetadata) -> Outcome {
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
        async fn handle(&self, _: Order, _: MessageMetadata) -> Outcome {
            Outcome::Ack
        }
        async fn handle_dead(&self, _: Order, _: shove::DeadMessageMetadata) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let shutdown = CancellationToken::new();
    let consumer_main = InMemoryConsumer::new(broker.clone());
    let handler = Sleepy(invocations.clone());
    let shutdown_main = shutdown.clone();
    let main_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(shutdown_main)
            .with_prefetch_count(1)
            .with_max_retries(1)
            .with_handler_timeout(Duration::from_millis(50));
        consumer_main.run::<OrdersTopic>(handler, opts).await
    });

    let dlq_handle = {
        let consumer = InMemoryConsumer::new(broker.clone());
        let handler = DlqH(dlq_hits.clone());
        tokio::spawn(async move { consumer.run_dlq::<OrdersTopic>(handler).await })
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
    broker.shutdown();
    let _ = main_handle.await;
    let _ = dlq_handle.await;
}

// ---------------------------------------------------------------------------
// Outcome::Defer — schedules redelivery without incrementing retry_count
// ---------------------------------------------------------------------------

#[tokio::test]
async fn defer_schedules_redelivery_without_incrementing_retry() {
    let broker = InMemoryBroker::new();
    let declarer = InMemoryTopologyDeclarer::new(broker.clone());
    declare_topic::<OrdersTopic>(&declarer).await.unwrap();

    let publisher = InMemoryPublisher::new(broker.clone());
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
        async fn handle(&self, _: Order, m: MessageMetadata) -> Outcome {
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

    let shutdown = CancellationToken::new();
    let consumer = InMemoryConsumer::new(broker.clone());
    let shutdown_for_task = shutdown.clone();
    let handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(shutdown_for_task).with_prefetch_count(1);
        consumer.run::<OrdersTopic>(handler, opts).await
    });

    let probe = final_retry_count.clone();
    assert!(
        poll_until(
            move || probe.load(Ordering::Relaxed) != u32::MAX,
            Duration::from_secs(2),
        )
        .await
    );
    assert_eq!(
        final_retry_count.load(Ordering::Relaxed),
        0,
        "Defer must not increment retry_count"
    );

    shutdown.cancel();
    let _ = handle.await;
}

// ---------------------------------------------------------------------------
// FailAll poison cleared once the shard buffer empties
// ---------------------------------------------------------------------------

#[tokio::test]
async fn poison_cleared_after_shard_drains() {
    let broker = InMemoryBroker::new();
    let declarer = InMemoryTopologyDeclarer::new(broker.clone());
    declare_topic::<LedgerFailAllTopic>(&declarer)
        .await
        .unwrap();

    let publisher = InMemoryPublisher::new(broker.clone());
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
        async fn handle(&self, msg: Event, _: MessageMetadata) -> Outcome {
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
    let consumer = InMemoryConsumer::new(broker.clone());
    let shutdown_for_task = shutdown.clone();
    let handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(shutdown_for_task).with_prefetch_count(1);
        consumer.run_fifo::<LedgerFailAllTopic>(handler, opts).await
    });

    // Wait until the shard has drained the first batch (seq 0 acked, 1 rejected, 2 DLQ'd).
    let probe = acked.clone();
    assert!(
        poll_until(
            move || probe.try_lock().map(|v| v.contains(&0)).unwrap_or(false),
            Duration::from_secs(2),
        )
        .await
    );

    // Give the shard a moment to notice its buffer is empty and clear poisons.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish more A messages — none should be poisoned now.
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

    let probe = acked.clone();
    assert!(
        poll_until(
            move || {
                probe
                    .try_lock()
                    .map(|v| v.contains(&10) && v.contains(&11))
                    .unwrap_or(false)
            },
            Duration::from_secs(2),
        )
        .await,
        "post-drain publishes must be handled, not DLQ'd"
    );

    shutdown.cancel();
    broker.shutdown();
    let _ = handle.await;
}

// ---------------------------------------------------------------------------
// Autoscaler end-to-end — backlog triggers scale_up via the backend
// ---------------------------------------------------------------------------

#[tokio::test]
async fn autoscaler_scales_up_under_backlog() {
    let broker = InMemoryBroker::new();
    let registry = Arc::new(Mutex::new(InMemoryConsumerGroupRegistry::new(
        broker.clone(),
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
                async fn handle(&self, _: Ping, _: MessageMetadata) -> Outcome {
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
        )
        .await
        .unwrap();
        reg.start_all();
    }

    let publisher = InMemoryPublisher::new(broker.clone());
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
        InMemoryAutoscalerBackend::autoscaler(broker.clone(), registry.clone(), config);
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
    let broker = InMemoryBroker::new();
    let registry = Arc::new(Mutex::new(InMemoryConsumerGroupRegistry::new(
        broker.clone(),
    )));

    let processed: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

    {
        let processed = processed.clone();
        let factory = move || {
            #[derive(Clone)]
            struct H(Arc<AtomicUsize>);
            impl MessageHandler<GroupTopic> for H {
                async fn handle(&self, _: Ping, _: MessageMetadata) -> Outcome {
                    self.0.fetch_add(1, Ordering::Relaxed);
                    Outcome::Ack
                }
            }
            H(processed.clone())
        };
        let mut reg = registry.lock().await;
        reg.register::<GroupTopic, _>(InMemoryConsumerGroupConfig::new(2..=4), factory)
            .await
            .unwrap();
        reg.start_all();
    }

    // Publish 20 pings.
    let publisher = InMemoryPublisher::new(broker.clone());
    for i in 0..20u32 {
        publisher.publish::<GroupTopic>(&Ping(i)).await.unwrap();
    }

    let probe = processed.clone();
    assert!(
        poll_until(
            move || probe.load(Ordering::Relaxed) == 20,
            Duration::from_secs(3),
        )
        .await
    );

    registry.lock().await.shutdown_all().await;
}
