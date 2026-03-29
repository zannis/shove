use std::collections::HashMap;

use crate::backends::rabbitmq::client::RabbitMqClient;
use crate::backends::rabbitmq::consumer_group::{ConsumerGroup, ConsumerGroupConfig};
use crate::handler::MessageHandler;
use crate::topic::Topic;

/// Registry of all [`ConsumerGroup`]s managed by the autoscaler.
///
/// Every group shares the same underlying [`RabbitMqClient`].  Each group gets
/// its own child [`CancellationToken`] derived from the client so that the
/// whole registry can be shut down with a single cancellation.
pub struct ConsumerGroupRegistry {
    groups: HashMap<String, ConsumerGroup>,
    client: RabbitMqClient,
}

impl ConsumerGroupRegistry {
    pub fn new(client: RabbitMqClient) -> Self {
        Self {
            groups: HashMap::new(),
            client,
        }
    }

    /// Register a new consumer group.
    ///
    /// The group is created with the provided `config` and `handler_factory`
    /// but is **not** started — call [`start_all`] (or the group's own
    /// `start`) separately.
    ///
    /// [`start_all`]: Self::start_all
    pub fn register<T, H>(
        &mut self,
        name: impl Into<String>,
        queue: impl Into<String>,
        config: ConsumerGroupConfig,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
    ) where
        T: Topic + 'static,
        H: MessageHandler<T> + Clone + 'static,
    {
        let name: String = name.into();
        let group_token = self.client.shutdown_token().child_token();
        let group = ConsumerGroup::new::<T, H>(
            name.clone(),
            queue,
            config,
            self.client.clone(),
            group_token,
            handler_factory,
        );
        self.groups.insert(name, group);
    }

    /// Call [`ConsumerGroup::start`] on every registered group.
    pub fn start_all(&mut self) {
        for group in self.groups.values_mut() {
            group.start();
        }
    }

    /// Read-only access to the underlying group map.
    pub fn groups(&self) -> &HashMap<String, ConsumerGroup> {
        &self.groups
    }

    /// Mutable access to the underlying group map.
    pub fn groups_mut(&mut self) -> &mut HashMap<String, ConsumerGroup> {
        &mut self.groups
    }

    /// Shut down every consumer group and wait for all tasks to complete.
    pub async fn shutdown_all(&mut self) {
        for group in self.groups.values_mut() {
            group.shutdown().await;
        }
    }
}
