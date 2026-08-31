//! Tier A — every flow shove exposes, measured over the in-process backend.
//!
//! The broker is removed on purpose. What is left is shove's own per-message
//! cost: encode, enqueue, dispatch, settle. That is the only thing a change to
//! this crate can actually move, which makes it the part worth watching on
//! every PR. Absolute throughput against a real broker is a different job and
//! lives in the stress harness, not here.
//!
//! Run with:
//!
//!     cargo bench --no-default-features --features inmemory --bench inmemory_flows
//!
//! No Docker, no broker, no network.
//!
//! # Coverage
//!
//! The manifest in `common::TIER_A_COVERAGE` is printed before the first group
//! and asserted complete, so a flow cannot go missing without failing the run.
//! One flow is not measurable here: batch consume (`run_batch`) exists only on
//! the Kafka backend.
//!
//! # Why the shapes differ per group
//!
//! - **Publish** is measured in chunks against a freshly built broker, because
//!   the in-process queue *blocks* publishers once a queue reaches
//!   `DEFAULT_QUEUE_CAPACITY` (10 000). An unbounded publish loop would
//!   deadlock, and draining it with a live consumer would measure the consumer
//!   too. Chunk size follows a fixed byte budget so 64 KiB payloads do not
//!   allocate gigabytes.
//! - **Consume-side flows** publish a fixed message count outside the timer,
//!   then time delivery of exactly that many messages, so the reported figure
//!   is messages per second rather than an opaque per-iteration duration.
//! - **Broadcast** is the exception: its contract is deliver-new, so a
//!   subscriber must exist before the publish. Its timed region is therefore
//!   publish *and* deliver, and is not comparable to `consume_parallel`.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use serde::{Deserialize, Serialize};
use shove::inmemory::{
    BrokerStatsProvider, InMemoryBroker, InMemoryConsumer, InMemoryConsumerGroupConfig,
    InMemoryQueueStatsProvider,
};
use shove::{
    Broker, ConsumerGroupConfig, ConsumerOptions, DeadMessageMetadata, InMemory, MessageHandler,
    MessageMetadata, Outcome, Publisher, SequenceFailure, SequencedTopic, Topic, TopologyBuilder,
    define_sequenced_topic, define_topic,
};
use tokio::runtime::Runtime;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use common::{PAYLOAD_SIZES, payload};

// ── Tuning ──────────────────────────────────────────────────────────────────

/// Messages delivered per timed iteration on every consume-side flow.
///
/// 256 keeps the largest payload (64 KiB) at 16 MiB of live queue per
/// iteration while still being large enough that per-iteration setup does not
/// dominate the reported figure.
const CONSUME_MESSAGES: u64 = 256;

/// Messages per `publish_batch` call.
const BATCH_SIZE: u64 = 64;

/// Live bytes a single publish chunk may hold before the broker is rebuilt.
/// Bounds memory at every payload size without changing what is measured.
const CHUNK_BYTE_BUDGET: usize = 16 << 20;

/// How long a consumer is given to drain after its shutdown token fires.
/// Teardown happens outside every timed region.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Publishes per broker for `payload_bytes`-sized messages.
///
/// Clamped below `DEFAULT_QUEUE_CAPACITY` so a chunk can never reach the
/// capacity at which the in-process publisher parks.
fn chunk_len(payload_bytes: usize) -> u64 {
    (CHUNK_BYTE_BUDGET / payload_bytes.max(1)).clamp(64, 4096) as u64
}

// ── Message & topics ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchMsg {
    id: u64,
    payload: String,
}

define_topic!(
    PublishTopic,
    BenchMsg,
    TopologyBuilder::new("bench-publish").build()
);

define_topic!(
    ConsumeTopic,
    BenchMsg,
    TopologyBuilder::new("bench-consume").build()
);

define_topic!(
    GroupTopic,
    BenchMsg,
    TopologyBuilder::new("bench-group").build()
);

define_topic!(
    BroadcastTopic,
    BenchMsg,
    TopologyBuilder::new("bench-broadcast").broadcast().build()
);

// Carries a DLQ, so `Outcome::Reject` exercises the real dead-letter routing
// arm rather than the no-DLQ discard arm.
define_topic!(
    DlqTopic,
    BenchMsg,
    TopologyBuilder::new("bench-dlq").dlq().build()
);

define_sequenced_topic!(
    FifoTopic,
    BenchMsg,
    fifo_key,
    TopologyBuilder::new("bench-fifo")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(4)
        .hold_queue(Duration::from_millis(50))
        .dlq()
        .build()
);

/// Spreads messages across all four shards, so the FIFO figure reflects shard
/// routing rather than one hot key.
fn fifo_key(msg: &BenchMsg) -> String {
    format!("key-{}", msg.id % 4)
}

// `ConsumerSupervisor::register` rejects a second registration of the same
// topic, so modelling N independent pollers needs N distinct topics.
define_topic!(
    SupTopicA,
    BenchMsg,
    TopologyBuilder::new("bench-sup-a").build()
);
define_topic!(
    SupTopicB,
    BenchMsg,
    TopologyBuilder::new("bench-sup-b").build()
);
define_topic!(
    SupTopicC,
    BenchMsg,
    TopologyBuilder::new("bench-sup-c").build()
);
define_topic!(
    SupTopicD,
    BenchMsg,
    TopologyBuilder::new("bench-sup-d").build()
);

// ── Handler ─────────────────────────────────────────────────────────────────

/// Counts deliveries and signals once `target` have arrived.
///
/// The handler itself does no work, so what the consume groups report is
/// framework dispatch cost and nothing else.
///
/// `handle_dead` counts into the same counter, because `run_dlq` dispatches
/// there and not to `handle` — a DLQ drain measured with a handler that only
/// implements `handle` would wait forever on a count that never moves. Each
/// instance is used for exactly one role, so the shared counter cannot
/// double-count.
#[derive(Clone)]
struct CountingHandler {
    seen: Arc<AtomicU64>,
    target: u64,
    done: Arc<Notify>,
    outcome: Outcome,
}

impl CountingHandler {
    fn new(target: u64, outcome: Outcome) -> Self {
        Self {
            seen: Arc::new(AtomicU64::new(0)),
            target,
            done: Arc::new(Notify::new()),
            outcome,
        }
    }

    /// Resolves once `target` messages have been handled.
    ///
    /// `Notify::notify_one` stores a permit when no waiter is parked, so this
    /// cannot miss a completion that lands before the await.
    async fn completed(&self) {
        self.done.notified().await;
    }

    fn record(&self) {
        if self.seen.fetch_add(1, Ordering::Relaxed).saturating_add(1) == self.target {
            self.done.notify_one();
        }
    }
}

impl<T> MessageHandler<T> for CountingHandler
where
    T: Topic<Message = BenchMsg>,
{
    type Context = ();

    async fn handle(&self, _msg: BenchMsg, _meta: MessageMetadata, _ctx: &()) -> Outcome {
        self.record();
        self.outcome.clone()
    }

    async fn handle_dead(&self, _msg: BenchMsg, _meta: DeadMessageMetadata, _ctx: &()) {
        self.record();
    }
}

// ── Fixtures ────────────────────────────────────────────────────────────────

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build bench runtime")
}

fn consumer_options(prefetch: u16, shutdown: &CancellationToken) -> ConsumerOptions<InMemory> {
    ConsumerOptions::<InMemory>::new()
        .with_shutdown(shutdown.clone())
        .with_concurrent_processing(true)
        .with_prefetch_count(prefetch)
}

/// A broker nothing has touched yet, with `T` declared.
///
/// Every measured iteration gets its own: leftover queue state would skew the
/// next iteration, and a consumer group's shutdown cascades to the client it
/// was built from, so reusing one would silently retire it.
async fn fresh_broker<T: Topic>() -> (InMemoryBroker, Broker<InMemory>) {
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker
        .topology()
        .declare::<T>()
        .await
        .expect("declare bench topology");
    (client, broker)
}

async fn publish_n<T: Topic<Message = BenchMsg>>(
    publisher: &Publisher<InMemory>,
    msg: &BenchMsg,
    count: u64,
) {
    for id in 0..count {
        let msg = BenchMsg {
            id,
            payload: msg.payload.clone(),
        };
        publisher.publish::<T>(&msg).await.expect("publish");
    }
}

/// Block until `queue` holds at least `expected` ready messages.
///
/// Used where a flow's setup is only complete once messages have physically
/// landed — the DLQ drain cannot be timed honestly while the last reject is
/// still in flight.
async fn wait_for_depth(client: &InMemoryBroker, queue: &str, expected: u64) {
    let stats = BrokerStatsProvider::new(client.clone());
    loop {
        if let Ok(s) = stats.get_queue_stats(queue).await
            && s.messages_ready >= expected
        {
            return;
        }
        tokio::task::yield_now().await;
    }
}

// ── publish_single / publish_batch ──────────────────────────────────────────

/// `Publisher::publish` and `Publisher::publish_batch`, parameterized by
/// payload size.
///
/// The broker is rebuilt between chunks *outside* the timer, so nothing here
/// measures a publisher parked on a full queue.
fn bench_publish(c: &mut Criterion) {
    common::report_coverage();

    let rt = runtime();
    let mut group = c.benchmark_group("inmemory_publish");

    for bytes in PAYLOAD_SIZES {
        let msg = BenchMsg {
            id: 0,
            payload: payload(bytes),
        };

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("publish_single", bytes), &msg, |b, msg| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let chunk = chunk_len(bytes);
                let mut elapsed = Duration::ZERO;
                let mut remaining = iters;
                while remaining > 0 {
                    let this_chunk = remaining.min(chunk);
                    let (_client, broker) = fresh_broker::<PublishTopic>().await;
                    let publisher = broker.publisher().await.expect("publisher");

                    let start = Instant::now();
                    for _ in 0..this_chunk {
                        publisher
                            .publish::<PublishTopic>(msg)
                            .await
                            .expect("publish");
                    }
                    elapsed = elapsed.saturating_add(start.elapsed());

                    remaining = remaining.saturating_sub(this_chunk);
                }
                elapsed
            });
        });

        let batch: Vec<BenchMsg> = (0..BATCH_SIZE)
            .map(|id| BenchMsg {
                id,
                payload: payload(bytes),
            })
            .collect();

        group.throughput(Throughput::Elements(BATCH_SIZE));
        group.bench_with_input(
            BenchmarkId::new("publish_batch", bytes),
            &batch,
            |b, batch| {
                b.to_async(&rt).iter_custom(|iters| async move {
                    // Chunk counted in batches, so the byte budget still holds.
                    let chunk = (chunk_len(bytes) / BATCH_SIZE).max(1);
                    let mut elapsed = Duration::ZERO;
                    let mut remaining = iters;
                    while remaining > 0 {
                        let this_chunk = remaining.min(chunk);
                        let (_client, broker) = fresh_broker::<PublishTopic>().await;
                        let publisher = broker.publisher().await.expect("publisher");

                        let start = Instant::now();
                        for _ in 0..this_chunk {
                            publisher
                                .publish_batch::<PublishTopic>(batch)
                                .await
                                .expect("publish_batch");
                        }
                        elapsed = elapsed.saturating_add(start.elapsed());

                        remaining = remaining.saturating_sub(this_chunk);
                    }
                    elapsed
                });
            },
        );
    }

    group.finish();
}

// ── consume_parallel / consume_fifo ─────────────────────────────────────────

/// Dispatch cost of `run` (concurrent) and `run_fifo` (sequenced), from
/// consumer start to the last of `CONSUME_MESSAGES` handled.
///
/// Publishing happens outside the timer. `run_fifo` carries the extra cost of
/// shard routing and per-key ordering, which is the comparison this group
/// exists to make visible.
fn bench_consume(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("inmemory_consume");
    group.throughput(Throughput::Elements(CONSUME_MESSAGES));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    for bytes in PAYLOAD_SIZES {
        let msg = BenchMsg {
            id: 0,
            payload: payload(bytes),
        };

        group.bench_with_input(
            BenchmarkId::new("consume_parallel", bytes),
            &msg,
            |b, msg| {
                b.to_async(&rt).iter_custom(|iters| async move {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let (client, broker) = fresh_broker::<ConsumeTopic>().await;
                        let publisher = broker.publisher().await.expect("publisher");
                        publish_n::<ConsumeTopic>(&publisher, msg, CONSUME_MESSAGES).await;

                        let handler = CountingHandler::new(CONSUME_MESSAGES, Outcome::Ack);
                        let signal = handler.clone();
                        let shutdown = CancellationToken::new();
                        let options = consumer_options(32, &shutdown);
                        let consumer = InMemoryConsumer::new(client);

                        let start = Instant::now();
                        let task = tokio::spawn(async move {
                            consumer
                                .run::<ConsumeTopic, CountingHandler>(handler, (), options)
                                .await
                        });
                        signal.completed().await;
                        elapsed = elapsed.saturating_add(start.elapsed());

                        shutdown.cancel();
                        let _ = task.await;
                    }
                    elapsed
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("consume_fifo", bytes), &msg, |b, msg| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iters {
                    let (client, broker) = fresh_broker::<FifoTopic>().await;
                    let publisher = broker.publisher().await.expect("publisher");
                    publish_n::<FifoTopic>(&publisher, msg, CONSUME_MESSAGES).await;

                    let handler = CountingHandler::new(CONSUME_MESSAGES, Outcome::Ack);
                    let signal = handler.clone();
                    let shutdown = CancellationToken::new();
                    let options = consumer_options(32, &shutdown);
                    let consumer = InMemoryConsumer::new(client);

                    let start = Instant::now();
                    let task = tokio::spawn(async move {
                        consumer
                            .run_fifo::<FifoTopic, CountingHandler>(handler, (), options)
                            .await
                    });
                    signal.completed().await;
                    elapsed = elapsed.saturating_add(start.elapsed());

                    shutdown.cancel();
                    let _ = task.await;
                }
                elapsed
            });
        });
    }

    group.finish();
}

// ── consumer_group ──────────────────────────────────────────────────────────

/// `Broker::consumer_group` — a coordinated group of four consumers competing
/// for one queue, timed from group start to the last message handled.
fn bench_consumer_group(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("inmemory_group");
    group.throughput(Throughput::Elements(CONSUME_MESSAGES));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    for bytes in PAYLOAD_SIZES {
        let msg = BenchMsg {
            id: 0,
            payload: payload(bytes),
        };

        group.bench_with_input(BenchmarkId::new("consumer_group", bytes), &msg, |b, msg| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iters {
                    let (_client, broker) = fresh_broker::<GroupTopic>().await;
                    let publisher = broker.publisher().await.expect("publisher");
                    publish_n::<GroupTopic>(&publisher, msg, CONSUME_MESSAGES).await;

                    let handler = CountingHandler::new(CONSUME_MESSAGES, Outcome::Ack);
                    let signal = handler.clone();
                    let mut consumer_group = broker.consumer_group();
                    consumer_group
                        .register::<GroupTopic, CountingHandler>(
                            ConsumerGroupConfig::new(
                                InMemoryConsumerGroupConfig::new(4..=4).with_prefetch_count(32),
                            ),
                            move || handler.clone(),
                        )
                        .await
                        .expect("register consumer group");

                    let stop = CancellationToken::new();
                    let start = Instant::now();
                    let task = tokio::spawn(
                        consumer_group
                            .run_until_timeout(stop.clone().cancelled_owned(), DRAIN_TIMEOUT),
                    );
                    signal.completed().await;
                    elapsed = elapsed.saturating_add(start.elapsed());

                    stop.cancel();
                    let _ = task.await;
                }
                elapsed
            });
        });
    }

    group.finish();
}

// ── supervisor ──────────────────────────────────────────────────────────────

/// `ConsumerSupervisor` — four independent pollers, one per topic, sharing a
/// completion counter.
///
/// This is the shape SQS is restricted to, but the supervisor is generic and
/// runs here too, so it is measured rather than recorded as unavailable.
fn bench_supervisor(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("inmemory_supervisor");
    group.throughput(Throughput::Elements(CONSUME_MESSAGES));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    let per_topic = CONSUME_MESSAGES / 4;

    for bytes in PAYLOAD_SIZES {
        let msg = BenchMsg {
            id: 0,
            payload: payload(bytes),
        };

        group.bench_with_input(BenchmarkId::new("supervisor", bytes), &msg, |b, msg| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iters {
                    let (_client, broker) = fresh_broker::<SupTopicA>().await;
                    broker
                        .topology()
                        .declare::<SupTopicB>()
                        .await
                        .expect("declare supervisor topic b");
                    broker
                        .topology()
                        .declare::<SupTopicC>()
                        .await
                        .expect("declare supervisor topic c");
                    broker
                        .topology()
                        .declare::<SupTopicD>()
                        .await
                        .expect("declare supervisor topic d");

                    let publisher = broker.publisher().await.expect("publisher");
                    publish_n::<SupTopicA>(&publisher, msg, per_topic).await;
                    publish_n::<SupTopicB>(&publisher, msg, per_topic).await;
                    publish_n::<SupTopicC>(&publisher, msg, per_topic).await;
                    publish_n::<SupTopicD>(&publisher, msg, per_topic).await;

                    let handler = CountingHandler::new(per_topic.saturating_mul(4), Outcome::Ack);
                    let signal = handler.clone();
                    let mut supervisor = broker.consumer_supervisor();
                    let shutdown = supervisor.cancellation_token();

                    let start = Instant::now();
                    supervisor
                        .register::<SupTopicA, CountingHandler>(
                            handler.clone(),
                            consumer_options(32, &shutdown),
                        )
                        .expect("register supervisor topic a");
                    supervisor
                        .register::<SupTopicB, CountingHandler>(
                            handler.clone(),
                            consumer_options(32, &shutdown),
                        )
                        .expect("register supervisor topic b");
                    supervisor
                        .register::<SupTopicC, CountingHandler>(
                            handler.clone(),
                            consumer_options(32, &shutdown),
                        )
                        .expect("register supervisor topic c");
                    supervisor
                        .register::<SupTopicD, CountingHandler>(
                            handler,
                            consumer_options(32, &shutdown),
                        )
                        .expect("register supervisor topic d");

                    let stop = CancellationToken::new();
                    let task = tokio::spawn(
                        supervisor.run_until_timeout(stop.clone().cancelled_owned(), DRAIN_TIMEOUT),
                    );
                    signal.completed().await;
                    elapsed = elapsed.saturating_add(start.elapsed());

                    stop.cancel();
                    let _ = task.await;
                }
                elapsed
            });
        });
    }

    group.finish();
}

// ── broadcast ───────────────────────────────────────────────────────────────

/// `Broker::broadcast_subscriber` — one ephemeral subscription per process.
///
/// Broadcast delivers only what is published *after* a subscription exists, so
/// unlike every other consume group the publish cannot be hoisted out of the
/// timer. The timed region is publish **and** deliver; the number is not
/// comparable with `consume_parallel`, which times delivery alone.
fn bench_broadcast(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("inmemory_broadcast");
    group.throughput(Throughput::Elements(CONSUME_MESSAGES));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    for bytes in PAYLOAD_SIZES {
        let msg = BenchMsg {
            id: 0,
            payload: payload(bytes),
        };

        group.bench_with_input(BenchmarkId::new("broadcast", bytes), &msg, |b, msg| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iters {
                    // A broadcast topology declares no queue of its own, so
                    // there is nothing to declare up front.
                    let client = InMemoryBroker::new();
                    let broker = Broker::<InMemory>::from_client(client.clone());
                    let publisher = broker.publisher().await.expect("publisher");

                    let handler = CountingHandler::new(CONSUME_MESSAGES, Outcome::Ack);
                    let signal = handler.clone();
                    let mut subscriber = broker.broadcast_subscriber();
                    let shutdown = subscriber.cancellation_token();
                    subscriber
                        .subscribe::<BroadcastTopic, CountingHandler>(
                            handler,
                            consumer_options(32, &shutdown),
                        )
                        .expect("subscribe");

                    let stop = CancellationToken::new();
                    let task = tokio::spawn(
                        subscriber.run_until_timeout(stop.clone().cancelled_owned(), DRAIN_TIMEOUT),
                    );

                    // Deliver-new: the subscription is registered by the
                    // spawned delivery task, so publishing before it exists
                    // would drop messages and hang the wait below.
                    while client.broadcast_subscriber_count(BroadcastTopic::topology().queue()) == 0
                    {
                        tokio::task::yield_now().await;
                    }

                    let start = Instant::now();
                    publish_n::<BroadcastTopic>(&publisher, msg, CONSUME_MESSAGES).await;
                    signal.completed().await;
                    elapsed = elapsed.saturating_add(start.elapsed());

                    stop.cancel();
                    let _ = task.await;
                }
                elapsed
            });
        });
    }

    group.finish();
}

// ── dlq_drain ───────────────────────────────────────────────────────────────

/// `run_dlq` — draining a dead-letter queue.
///
/// Getting messages *into* the DLQ (publish, consume, reject, route) happens
/// outside the timer, and the setup waits until the DLQ actually holds all of
/// them: timing a drain while the last reject is still routing would charge
/// the drain for the reject.
fn bench_dlq_drain(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("inmemory_dlq");
    group.throughput(Throughput::Elements(CONSUME_MESSAGES));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    for bytes in PAYLOAD_SIZES {
        let msg = BenchMsg {
            id: 0,
            payload: payload(bytes),
        };

        group.bench_with_input(BenchmarkId::new("dlq_drain", bytes), &msg, |b, msg| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let dlq = DlqTopic::topology()
                    .dlq()
                    .expect("bench-dlq declares a DLQ");
                let mut elapsed = Duration::ZERO;
                for _ in 0..iters {
                    let (client, broker) = fresh_broker::<DlqTopic>().await;
                    let publisher = broker.publisher().await.expect("publisher");
                    publish_n::<DlqTopic>(&publisher, msg, CONSUME_MESSAGES).await;

                    // Fill the DLQ: reject everything on the main queue.
                    let rejecter = CountingHandler::new(CONSUME_MESSAGES, Outcome::Reject);
                    let rejected = rejecter.clone();
                    let main_shutdown = CancellationToken::new();
                    let main_options = consumer_options(32, &main_shutdown);
                    let main_consumer = InMemoryConsumer::new(client.clone());
                    let main_task = tokio::spawn(async move {
                        main_consumer
                            .run::<DlqTopic, CountingHandler>(rejecter, (), main_options)
                            .await
                    });
                    rejected.completed().await;
                    wait_for_depth(&client, dlq, CONSUME_MESSAGES).await;
                    main_shutdown.cancel();
                    let _ = main_task.await;

                    let drainer = CountingHandler::new(CONSUME_MESSAGES, Outcome::Ack);
                    let drained = drainer.clone();
                    let dlq_consumer = InMemoryConsumer::new(client);

                    let start = Instant::now();
                    let dlq_task = tokio::spawn(async move {
                        dlq_consumer
                            .run_dlq::<DlqTopic, CountingHandler>(drainer, ())
                            .await
                    });
                    drained.completed().await;
                    elapsed = elapsed.saturating_add(start.elapsed());

                    // `run_dlq` takes no shutdown token, so the drain loop is
                    // aborted rather than signalled.
                    dlq_task.abort();
                    let _ = dlq_task.await;
                }
                elapsed
            });
        });
    }

    group.finish();
}

// ── route_outcome ───────────────────────────────────────────────────────────

/// Outcome settling — what the consumer does with a message *after* the
/// handler returns.
///
/// Each backend's `route_outcome` is a private function, so it is exercised
/// through the public consume path instead of called directly. `ack` is the
/// baseline; the `reject` figure is `ack` plus dead-letter routing, and the
/// gap between them is the cost of that routing.
///
/// `Retry` and `Defer` are deliberately absent: both route through a hold
/// queue whose delay is wall-clock sleep, so benchmarking them would report
/// the sleep rather than shove's routing.
///
/// Payload-invariant — settling moves an already-encoded envelope — so this
/// group runs at the smallest payload only.
fn bench_route_outcome(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("inmemory_route_outcome");
    group.throughput(Throughput::Elements(CONSUME_MESSAGES));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    let bytes = PAYLOAD_SIZES[0];
    let msg = BenchMsg {
        id: 0,
        payload: payload(bytes),
    };

    group.bench_with_input(BenchmarkId::new("ack", bytes), &msg, |b, msg| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                elapsed = elapsed.saturating_add(settle(msg, Outcome::Ack, None).await);
            }
            elapsed
        });
    });

    group.bench_with_input(BenchmarkId::new("reject", bytes), &msg, |b, msg| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let dlq = DlqTopic::topology()
                .dlq()
                .expect("bench-dlq declares a DLQ");
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                elapsed = elapsed.saturating_add(settle(msg, Outcome::Reject, Some(dlq)).await);
            }
            elapsed
        });
    });

    group.finish();
}

/// Publish `CONSUME_MESSAGES`, then time consuming them with `outcome`.
///
/// When `settled_queue` is set the timer also covers the wait for every
/// message to arrive there, so a routing arm is never credited as free just
/// because the handler returned before the routing finished.
async fn settle(msg: &BenchMsg, outcome: Outcome, settled_queue: Option<&str>) -> Duration {
    let (client, broker) = fresh_broker::<DlqTopic>().await;
    let publisher = broker.publisher().await.expect("publisher");
    publish_n::<DlqTopic>(&publisher, msg, CONSUME_MESSAGES).await;

    let handler = CountingHandler::new(CONSUME_MESSAGES, outcome);
    let signal = handler.clone();
    let shutdown = CancellationToken::new();
    let options = consumer_options(32, &shutdown);
    let consumer = InMemoryConsumer::new(client.clone());

    let start = Instant::now();
    let task = tokio::spawn(async move {
        consumer
            .run::<DlqTopic, CountingHandler>(handler, (), options)
            .await
    });
    signal.completed().await;
    if let Some(queue) = settled_queue {
        wait_for_depth(&client, queue, CONSUME_MESSAGES).await;
    }
    let elapsed = start.elapsed();

    shutdown.cancel();
    let _ = task.await;
    elapsed
}

criterion_group!(
    benches,
    bench_publish,
    bench_consume,
    bench_consumer_group,
    bench_supervisor,
    bench_broadcast,
    bench_dlq_drain,
    bench_route_outcome
);
criterion_main!(benches);
