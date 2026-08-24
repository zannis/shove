use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use dashmap::DashMap;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::error::{Result, ShoveError};
use crate::metrics;

use super::constants::DEFAULT_QUEUE_CAPACITY;

/// Internal wire format between publisher and consumer. The payload is a
/// reference-counted [`bytes::Bytes`], so envelope clones share one buffer.
#[derive(Debug, Clone)]
pub(super) struct Envelope {
    pub payload: bytes::Bytes,
    pub headers: HashMap<String, String>,
    /// Deliveries so far including the pending one, surfaced as
    /// [`MessageMetadata::delivery_count`](crate::MessageMetadata::delivery_count).
    ///
    /// Carried on the envelope rather than in `headers` because the in-process
    /// broker owns every redelivery: unlike the header-based `retry_count`, a
    /// publisher cannot forge it.
    ///
    /// The lifecycle mirrors JetStream's `num_delivered`, the backend this
    /// field exists to expose, so that handler logic written against the
    /// in-process broker holds in production: bumped on an
    /// [`Outcome::Defer`](crate::Outcome::Defer) hop (a nak in place — same
    /// message, one more attempt) and reset by an
    /// [`Outcome::Retry`](crate::Outcome::Retry) hop (a republish, which starts
    /// a fresh message the broker has never delivered).
    pub delivery_count: u32,
}

impl Envelope {
    /// A first-delivery envelope.
    pub fn new(payload: bytes::Bytes, headers: HashMap<String, String>) -> Self {
        Self {
            payload,
            headers,
            delivery_count: 1,
        }
    }

    /// Marks this envelope as being handed to a consumer one more time.
    pub fn mark_redelivery(&mut self) {
        self.delivery_count = self.delivery_count.saturating_add(1);
    }

    /// Marks this envelope as a fresh message — the in-process equivalent of
    /// the republish that `Retry` performs on every other backend.
    pub fn reset_delivery_count(&mut self) {
        self.delivery_count = 1;
    }
}

/// State of a single declared queue: main, DLQ, hold queue, or FIFO shard.
#[derive(Debug)]
pub(super) struct QueueState {
    pub buffer: Mutex<VecDeque<Envelope>>,
    pub ready: Notify,
    pub space: Notify,
    pub capacity: usize,
    pub in_flight: AtomicU64,
    /// Single-active-consumer lock. Only used by `run_fifo`; contention-free in
    /// all other paths.
    pub sac: Mutex<()>,
}

impl QueueState {
    fn new(capacity: usize) -> Self {
        Self {
            buffer: Mutex::new(VecDeque::new()),
            ready: Notify::new(),
            space: Notify::new(),
            capacity,
            in_flight: AtomicU64::new(0),
            sac: Mutex::new(()),
        }
    }
}

/// Configuration for an [`InMemoryBroker`].
#[derive(Debug, Clone)]
pub struct InMemoryConfig {
    /// Per-queue capacity. Publishers await when the queue is at capacity.
    pub default_capacity: usize,
}

impl Default for InMemoryConfig {
    fn default() -> Self {
        Self {
            default_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }
}

impl InMemoryConfig {
    /// Set the per-queue capacity. Publishers block when a queue reaches this
    /// many buffered messages. `NonZeroUsize` rules out the degenerate `0`
    /// case at compile time.
    pub fn with_default_capacity(mut self, capacity: NonZeroUsize) -> Self {
        self.default_capacity = capacity.get();
        self
    }
}

pub(super) struct BrokerInner {
    pub queues: DashMap<String, Arc<QueueState>>,
    /// Live broadcast subscriptions, keyed by topic. Each entry is one
    /// in-process subscriber's private buffer; a publish to a broadcast topic
    /// clones into every one of them.
    ///
    /// Separate from `queues` on purpose: these are not declared, not
    /// addressable by name, and disappear with their subscriber. A topic with
    /// no live subscribers has no entry at all, which is what makes
    /// deliver-new structural — there is nowhere for an earlier message to
    /// have been buffered.
    pub broadcast: DashMap<String, Vec<BroadcastSlot>>,
    pub default_capacity: usize,
    pub shutdown: CancellationToken,
}

/// One live subscriber in the broadcast registry.
///
/// Identity is the buffer's own address: the registry holds a strong reference
/// for as long as a subscription is registered, so no two live slots can share
/// one. A counter would have to be checked for wrap; an allocation cannot
/// collide with itself.
pub(super) struct BroadcastSlot {
    queue: Arc<QueueState>,
    /// Fired when the subscriber goes away.
    ///
    /// A publish copies the slots out before its first `await`, so it can still
    /// be holding a buffer whose subscriber has since left. Waiting for capacity
    /// on that buffer would never end — nothing drains it and nothing notifies
    /// `space` — so the publish races the wait against this token and moves on
    /// to the subscribers behind it.
    closed: CancellationToken,
}

/// One process's ephemeral subscription to a broadcast topic.
///
/// Holds the subscriber's private buffer and deregisters it on drop, so the
/// subscription cannot outlive the consumer loop — including when that loop is
/// aborted rather than cancelled, which is what a drain-timeout escalation
/// does.
pub(super) struct BroadcastSubscription {
    broker: InMemoryBroker,
    topic: String,
    queue: Arc<QueueState>,
    closed: CancellationToken,
}

impl BroadcastSubscription {
    pub(super) fn queue(&self) -> &Arc<QueueState> {
        &self.queue
    }
}

impl Drop for BroadcastSubscription {
    fn drop(&mut self) {
        // Released before deregistering, so a publish already parked on this
        // buffer stops waiting rather than blocking every subscriber behind it.
        self.closed.cancel();
        let now_empty = match self.broker.inner.broadcast.get_mut(&self.topic) {
            Some(mut subs) => {
                subs.retain(|slot| !Arc::ptr_eq(&slot.queue, &self.queue));
                subs.is_empty()
            }
            None => false,
        };
        // Drop the topic entry too once the last subscriber leaves, so a
        // process that subscribes and unsubscribes repeatedly does not
        // accumulate empty vectors keyed by topic.
        if now_empty {
            self.broker
                .inner
                .broadcast
                .remove_if(&self.topic, |_, subs| subs.is_empty());
        }
    }
}

/// Handle to an in-process message broker. Cheap to `Clone`; all clones share
/// the same queue state. Used to construct `InMemoryPublisher`,
/// `InMemoryConsumer`, `InMemoryTopologyDeclarer`, and consumer-group /
/// autoscaler components.
#[derive(Clone)]
pub struct InMemoryBroker {
    inner: Arc<BrokerInner>,
}

impl Default for InMemoryBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryBroker {
    pub fn new() -> Self {
        Self::with_config(InMemoryConfig::default())
    }

    pub fn with_config(config: InMemoryConfig) -> Self {
        Self {
            inner: Arc::new(BrokerInner {
                queues: DashMap::new(),
                broadcast: DashMap::new(),
                default_capacity: config.default_capacity,
                shutdown: CancellationToken::new(),
            }),
        }
    }

    /// Broker-wide shutdown token. Cancel it to stop every publisher, consumer,
    /// and pending sleep-and-republish task created from this broker.
    pub fn shutdown_token(&self) -> &CancellationToken {
        &self.inner.shutdown
    }

    /// Cancel the shutdown token. Sleeping retry tasks abort; consumer loops
    /// exit at the next select point. `publish` calls currently awaiting
    /// capacity return `Err(ShoveError::Connection)`.
    pub fn shutdown(&self) {
        self.inner.shutdown.cancel();
    }

    /// Liveness check. Returns `Err(ShoveError::Connection)` if the broker has
    /// been shut down; otherwise `Ok(())`. No I/O — the `timeout` is unused.
    pub(super) async fn ping(&self, _timeout: std::time::Duration) -> Result<()> {
        if self.inner.shutdown.is_cancelled() {
            return Err(ShoveError::Connection("client is shut down".into()));
        }
        Ok(())
    }

    pub(super) fn lookup(&self, name: &str) -> Result<Arc<QueueState>> {
        self.inner
            .queues
            .get(name)
            .map(|q| Arc::clone(&q))
            .ok_or_else(|| ShoveError::Topology(format!("queue not declared: {name}")))
    }

    pub(super) fn declare(&self, name: &str) -> Arc<QueueState> {
        let capacity = self.inner.default_capacity;
        Arc::clone(
            &*self
                .inner
                .queues
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(QueueState::new(capacity))),
        )
    }

    /// Register an ephemeral broadcast subscription to `topic` and return its
    /// private buffer. Dropping the returned handle deregisters it.
    ///
    /// The subscription starts empty and only receives what is published from
    /// this point on — deliver-new is a consequence of the registry holding
    /// live subscribers rather than a retained log, not a policy applied on top
    /// of one.
    pub(super) fn broadcast_subscribe(&self, topic: &str) -> BroadcastSubscription {
        let queue = Arc::new(QueueState::new(self.inner.default_capacity));
        let closed = CancellationToken::new();
        self.inner
            .broadcast
            .entry(topic.to_string())
            .or_default()
            .push(BroadcastSlot {
                queue: Arc::clone(&queue),
                closed: closed.clone(),
            });
        BroadcastSubscription {
            broker: self.clone(),
            topic: topic.to_string(),
            queue,
            closed,
        }
    }

    /// Deliver `env` to every live broadcast subscriber of `topic`, one clone
    /// each.
    ///
    /// A topic nobody is subscribed to is a successful no-op: broadcast is
    /// deliver-new, so a message published with no listeners was never anyone's
    /// to receive.
    ///
    /// The subscriber list is copied out before the first `await`, so the
    /// `DashMap` shard lock is never held across one — a subscriber arriving or
    /// leaving mid-publish cannot deadlock against the enqueue. A subscriber
    /// that leaves during a publish may still be handed the message, which is
    /// harmless: its buffer is about to be dropped.
    ///
    /// Backpressure is shared, as it is for every other in-process publish:
    /// `enqueue` awaits capacity, so a subscriber whose handler has stalled
    /// with a full buffer holds up delivery to the others behind it. That is
    /// the existing `InMemoryConfig::default_capacity` contract rather than
    /// something broadcast introduces — and dropping instead would be a second
    /// discard path, invisible to the discard metrics.
    ///
    /// A *departed* subscriber is the one case where that wait would never end:
    /// the copied `Arc` keeps its full buffer alive while nothing is left to
    /// drain it or notify `space`. Each wait therefore races the slot's close
    /// token, so an abandoned buffer costs the publish nothing rather than
    /// parking it — and the subscribers behind it still get the message.
    pub(super) async fn broadcast_publish(&self, topic: &str, env: Envelope) -> Result<()> {
        let subscribers: Vec<(Arc<QueueState>, CancellationToken)> =
            match self.inner.broadcast.get(topic) {
                Some(subs) => subs
                    .iter()
                    .map(|slot| (Arc::clone(&slot.queue), slot.closed.clone()))
                    .collect(),
                None => return Ok(()),
            };

        for (queue, closed) in &subscribers {
            // Each subscriber gets its own envelope; the payload is `Bytes`, so
            // the clone shares one buffer rather than copying the message.
            tokio::select! {
                biased;
                () = closed.cancelled() => continue,
                res = self.enqueue(queue, env.clone()) => res?,
            }
        }
        Ok(())
    }

    /// Number of live subscriptions to `topic`. Test/observability hook — the
    /// property that matters is that this returns to zero once subscribers go
    /// away, with nothing left to reap.
    pub fn broadcast_subscriber_count(&self, topic: &str) -> usize {
        self.inner
            .broadcast
            .get(topic)
            .map(|subs| subs.len())
            .unwrap_or(0)
    }

    /// Enqueue `env` into `queue`, awaiting space when at capacity. Returns
    /// `Err(ShoveError::Connection)` if the broker's shutdown token fires
    /// before space is available.
    pub(super) async fn enqueue(&self, queue: &QueueState, env: Envelope) -> Result<()> {
        let mut env = Some(env);
        loop {
            let notified = queue.space.notified();
            tokio::pin!(notified);
            {
                let mut buf = queue.buffer.lock().await;
                if buf.len() < queue.capacity {
                    buf.push_back(env.take().expect("env consumed only on push"));
                    drop(buf);
                    queue.ready.notify_one();
                    return Ok(());
                }
            }
            tokio::select! {
                _ = &mut notified => continue,
                _ = self.inner.shutdown.cancelled() => {
                    metrics::record_backend_error(
                        metrics::BackendLabel::InMemory,
                        metrics::BackendErrorKind::Connection,
                    );
                    return Err(ShoveError::Connection("broker shutdown".into()));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn env(body: &'static [u8]) -> Envelope {
        Envelope::new(bytes::Bytes::from_static(body), HashMap::new())
    }

    /// A subscriber that leaves with a full buffer must not be able to park the
    /// publisher.
    ///
    /// A publish copies its subscriber handles out before the first await, so it
    /// can be parked on a buffer whose subscriber has since gone. That `Arc`
    /// keeps the full buffer alive while nothing is left to drain it or notify
    /// `space`, so without a close signal the wait never ends — and every
    /// subscriber behind it in the copied list is starved along with it.
    #[tokio::test]
    async fn a_departed_full_subscriber_does_not_park_the_broadcast() {
        let broker = InMemoryBroker::with_config(InMemoryConfig {
            default_capacity: 1,
        });

        // Registration order matters: the publish walks the list in order, so
        // `doomed` is the one it parks on.
        let doomed = broker.broadcast_subscribe("cache-invalidations");
        let live = broker.broadcast_subscribe("cache-invalidations");

        broker
            .broadcast_publish("cache-invalidations", env(b"first"))
            .await
            .expect("both buffers have room for the first message");

        // Make room in the live subscriber only, so the publish below can be
        // blocked by `doomed` and nothing else.
        live.queue().buffer.lock().await.pop_front();
        live.queue().space.notify_one();

        let publishing = tokio::spawn({
            let broker = broker.clone();
            async move {
                broker
                    .broadcast_publish("cache-invalidations", env(b"second"))
                    .await
            }
        });

        // The publish must have *copied the doomed slot* before that subscriber
        // goes, or the pre-fix code would skip the buffer for the ordinary
        // reason (it was already deregistered) and the test would pass without
        // the fix. The snapshot's own `Arc` clone is the observable that says
        // so: the registry and the subscription hold one each, so a third
        // strong reference can only be the publish holding it.
        while Arc::strong_count(doomed.queue()) < 3 {
            tokio::task::yield_now().await;
        }
        // And it must actually be parked, not merely started.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !publishing.is_finished(),
            "the publish should be parked on the full buffer at this point"
        );

        drop(doomed);

        tokio::time::timeout(Duration::from_secs(5), publishing)
            .await
            .expect("publish must stop waiting on a buffer nobody is draining")
            .expect("publish task must not panic")
            .expect("delivery to the live subscriber must succeed");

        assert_eq!(
            live.queue().buffer.lock().await.len(),
            1,
            "the subscriber behind the departed one still receives the message"
        );
    }

    /// Dropping one subscription must not deregister another. Slots are keyed
    /// by buffer address, so this pins that two live subscriptions to the same
    /// topic are always distinguishable.
    #[tokio::test]
    async fn dropping_one_subscription_leaves_the_other_registered() {
        let broker = InMemoryBroker::new();
        let first = broker.broadcast_subscribe("signals");
        let second = broker.broadcast_subscribe("signals");
        assert_eq!(broker.broadcast_subscriber_count("signals"), 2);

        drop(first);
        assert_eq!(broker.broadcast_subscriber_count("signals"), 1);

        broker
            .broadcast_publish("signals", env(b"x"))
            .await
            .unwrap();
        assert_eq!(second.queue().buffer.lock().await.len(), 1);

        drop(second);
        assert_eq!(broker.broadcast_subscriber_count("signals"), 0);
    }

    #[tokio::test]
    async fn declare_is_idempotent() {
        let broker = InMemoryBroker::new();
        let q1 = broker.declare("alpha");
        let q2 = broker.declare("alpha");
        assert!(Arc::ptr_eq(&q1, &q2));
    }

    #[tokio::test]
    async fn lookup_returns_err_for_undeclared_queue() {
        let broker = InMemoryBroker::new();
        let err = broker.lookup("missing").unwrap_err();
        assert!(matches!(err, ShoveError::Topology(_)));
    }

    #[tokio::test]
    async fn enqueue_dequeue_basic() {
        let broker = InMemoryBroker::new();
        let queue = broker.declare("t");
        let env = Envelope::new(bytes::Bytes::from_static(b"hello"), HashMap::new());
        broker.enqueue(&queue, env).await.unwrap();
        let popped = queue.buffer.lock().await.pop_front().unwrap();
        assert_eq!(&popped.payload[..], b"hello");
    }

    #[tokio::test]
    async fn enqueue_awaits_capacity_then_succeeds() {
        let broker = InMemoryBroker::with_config(InMemoryConfig {
            default_capacity: 1,
        });
        let queue = broker.declare("t");

        // Fill the queue.
        broker
            .enqueue(
                &queue,
                Envelope::new(bytes::Bytes::from_static(b"first"), HashMap::new()),
            )
            .await
            .unwrap();

        // Spawn a publisher that should block on capacity.
        let broker2 = broker.clone();
        let queue2 = Arc::clone(&queue);
        let publish_task = tokio::spawn(async move {
            broker2
                .enqueue(
                    &queue2,
                    Envelope::new(bytes::Bytes::from_static(b"second"), HashMap::new()),
                )
                .await
        });

        // Yield a couple of times so the publisher has a chance to register the waiter.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(!publish_task.is_finished(), "publisher should be blocked");

        // Drain one — publisher must unblock.
        let _ = queue.buffer.lock().await.pop_front();
        queue.space.notify_one();

        publish_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn enqueue_returns_error_on_shutdown() {
        let broker = InMemoryBroker::with_config(InMemoryConfig {
            default_capacity: 1,
        });
        let queue = broker.declare("t");
        // Fill the queue so the next publish must wait.
        broker
            .enqueue(&queue, Envelope::new(bytes::Bytes::new(), HashMap::new()))
            .await
            .unwrap();

        let broker2 = broker.clone();
        let queue2 = Arc::clone(&queue);
        let publish_task = tokio::spawn(async move {
            broker2
                .enqueue(&queue2, Envelope::new(bytes::Bytes::new(), HashMap::new()))
                .await
        });

        tokio::task::yield_now().await;
        broker.shutdown();
        let res = publish_task.await.unwrap();
        assert!(matches!(res, Err(ShoveError::Connection(_))));
    }
}
