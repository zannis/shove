#![cfg(feature = "rabbitmq")]

use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::backends::rabbitmq::client::RabbitMqClient;
use crate::backends::rabbitmq::consumer::RabbitMqConsumer;
use crate::consumer::{Consumer, ConsumerOptions};
use crate::handler::MessageHandler;
use crate::topic::Topic;

/// Type-erased factory that spawns a single consumer task.
///
/// The closure receives a client and a per-consumer cancellation token and
/// returns the `JoinHandle` of the spawned task.
type Spawner = Arc<dyn Fn(&RabbitMqClient, CancellationToken) -> JoinHandle<()> + Send + Sync>;

/// Configuration that governs the behaviour of a [`ConsumerGroup`].
pub struct ConsumerGroupConfig {
    pub prefetch_count: u16,
    pub min_consumers: u16,
    pub max_consumers: u16,
    pub max_retries: u32,
}

impl Default for ConsumerGroupConfig {
    fn default() -> Self {
        Self {
            prefetch_count: 10,
            min_consumers: 1,
            max_consumers: 10,
            max_retries: 10,
        }
    }
}

/// A named group of identical consumers all reading from the same queue.
///
/// The group owns the concrete consumers and is responsible for scaling them
/// up and down.  It keeps a [`CancellationToken`] per consumer so that
/// individual consumers can be stopped without affecting the rest of the group.
pub struct ConsumerGroup {
    name: String,
    /// The queue that every consumer in this group reads from (derived from
    /// `T::topology()` at construction time and stored for stats lookups).
    queue: String,
    config: ConsumerGroupConfig,
    client: RabbitMqClient,
    spawner: Spawner,
    /// One entry per active consumer: (per-consumer token, task handle).
    consumers: Vec<(CancellationToken, JoinHandle<()>)>,
    /// Cancelling this token stops every consumer in the group at once.
    group_token: CancellationToken,
}

impl ConsumerGroup {
    /// Create a new consumer group.
    ///
    /// `handler_factory` is called once per consumer spawn to produce a fresh
    /// handler instance.  The factory is stored inside a type-erased closure
    /// so that the rest of the codebase does not have to carry `T`/`H` type
    /// parameters.
    ///
    /// `queue` must match `T::topology().queue()` — it is stored separately
    /// so the autoscaler can look up queue statistics without the `T` type
    /// parameter.
    pub fn new<T, H>(
        name: impl Into<String>,
        queue: impl Into<String>,
        config: ConsumerGroupConfig,
        client: RabbitMqClient,
        group_token: CancellationToken,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
    ) -> Self
    where
        T: Topic + 'static,
        H: MessageHandler<T> + Clone + 'static,
    {
        let prefetch = config.prefetch_count;
        let max_retries = config.max_retries;

        let spawner: Spawner = Arc::new(
            move |client: &RabbitMqClient, child_token: CancellationToken| {
                let handler = handler_factory();
                let consumer = RabbitMqConsumer::new(client.clone());
                let options = ConsumerOptions::new(child_token)
                    .with_prefetch_count(prefetch)
                    .with_max_retries(max_retries);

                tokio::spawn(async move {
                    // Errors are logged inside run; we swallow the Result here
                    // so the JoinHandle is `JoinHandle<()>`.
                    if let Err(e) = consumer.run::<T>(handler, options).await {
                        tracing::error!("consumer task exited with error: {e}");
                    }
                })
            },
        );

        Self {
            name: name.into(),
            queue: queue.into(),
            config,
            client,
            spawner,
            consumers: Vec::new(),
            group_token,
        }
    }

    /// Spawn `min_consumers` consumers to get the group to its minimum size.
    pub fn start(&mut self) {
        let target = self.config.min_consumers as usize;
        for _ in 0..target {
            self.spawn_one();
        }
    }

    /// Spawn one additional consumer, respecting `max_consumers`.
    ///
    /// Returns `false` when the group is already at maximum capacity.
    pub fn scale_up(&mut self) -> bool {
        if self.consumers.len() >= self.config.max_consumers as usize {
            return false;
        }
        self.spawn_one();
        true
    }

    /// Cancel the most-recently-spawned consumer, respecting `min_consumers`.
    ///
    /// Returns `false` when the group is already at minimum capacity.
    pub fn scale_down(&mut self) -> bool {
        if self.consumers.len() <= self.config.min_consumers as usize {
            return false;
        }
        if let Some((token, _handle)) = self.consumers.pop() {
            token.cancel();
            // The handle is dropped here; the task will finish on its own
            // because its cancellation token has been signalled.
        }
        true
    }

    /// Number of currently active (spawned) consumers.
    pub fn active_consumers(&self) -> usize {
        self.consumers.len()
    }

    /// The queue name this group reads from.
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// Access the group's configuration.
    pub fn config(&self) -> &ConsumerGroupConfig {
        &self.config
    }

    /// Cancel every consumer in the group and wait for all tasks to finish.
    pub async fn shutdown(&mut self) {
        self.group_token.cancel();
        for (_token, handle) in self.consumers.drain(..) {
            // Ignore join errors — the task may have panicked but we still
            // want to proceed with a clean shutdown.
            let _ = handle.await;
        }
    }

    // ---- private helpers ----

    fn spawn_one(&mut self) {
        let child_token = self.group_token.child_token();
        let handle = (self.spawner)(&self.client, child_token.clone());
        self.consumers.push((child_token, handle));
    }
}
