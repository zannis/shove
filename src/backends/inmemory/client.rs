use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use dashmap::DashMap;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::error::{Result, ShoveError};

use super::constants::DEFAULT_QUEUE_CAPACITY;

/// Internal wire format between publisher and consumer.
#[derive(Debug, Clone)]
pub(super) struct Envelope {
    pub payload: Vec<u8>,
    pub headers: HashMap<String, String>,
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
    pub default_capacity: usize,
    pub shutdown: CancellationToken,
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
                    return Err(ShoveError::Connection("broker shutdown".into()));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let env = Envelope {
            payload: b"hello".to_vec(),
            headers: HashMap::new(),
        };
        broker.enqueue(&queue, env).await.unwrap();
        let popped = queue.buffer.lock().await.pop_front().unwrap();
        assert_eq!(popped.payload, b"hello");
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
                Envelope {
                    payload: b"first".to_vec(),
                    headers: HashMap::new(),
                },
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
                    Envelope {
                        payload: b"second".to_vec(),
                        headers: HashMap::new(),
                    },
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
            .enqueue(
                &queue,
                Envelope {
                    payload: vec![],
                    headers: HashMap::new(),
                },
            )
            .await
            .unwrap();

        let broker2 = broker.clone();
        let queue2 = Arc::clone(&queue);
        let publish_task = tokio::spawn(async move {
            broker2
                .enqueue(
                    &queue2,
                    Envelope {
                        payload: vec![],
                        headers: HashMap::new(),
                    },
                )
                .await
        });

        tokio::task::yield_now().await;
        broker.shutdown();
        let res = publish_task.await.unwrap();
        assert!(matches!(res, Err(ShoveError::Connection(_))));
    }
}
