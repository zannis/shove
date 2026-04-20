use std::collections::HashMap;

use tracing::{debug, info};

use crate::backends::rabbitmq::client::RabbitMqClient;
use crate::backends::rabbitmq::consumer_group::{ConsumerGroup, ConsumerGroupConfig};
use crate::backends::rabbitmq::topology::RabbitMqTopologyDeclarer;
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::topic::Topic;
use crate::topology::TopologyDeclarer;

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
    /// Automatically declares the topology for `T` via [`RabbitMqTopologyDeclarer`]
    /// before creating the group.  The group is **not** started — call
    /// [`start_all`] (or the group's own `start`) separately.
    ///
    /// [`start_all`]: Self::start_all
    pub async fn register<T, H>(
        &mut self,
        config: ConsumerGroupConfig,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
    ) -> Result<()>
    where
        T: Topic + 'static,
        H: MessageHandler<T, Context = ()> + 'static,
    {
        let topology = T::topology();
        let name = topology.queue().to_string();

        if self.groups.contains_key(&name) {
            return Err(ShoveError::Topology(format!(
                "consumer group '{name}' is already registered"
            )));
        }

        let channel = self.client.create_channel().await?;
        let declarer = RabbitMqTopologyDeclarer::new(channel);
        declarer.declare(topology).await?;

        info!(group = %name, "registering consumer group");
        let group_token = self.client.shutdown_token().child_token();
        let group = ConsumerGroup::new::<T, H>(
            name.clone(),
            name.clone(),
            config,
            self.client.clone(),
            group_token,
            handler_factory,
        );
        self.groups.insert(name, group);
        Ok(())
    }

    /// Call [`ConsumerGroup::start`] on every registered group.
    pub fn start_all(&mut self) {
        info!(count = self.groups.len(), "starting all consumer groups");
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

    /// Return a clone of the client's shutdown token.
    ///
    /// Used by `RegistryImpl::cancellation_token` to surface a
    /// backend-independent shutdown signal.
    pub fn client_shutdown_token(&self) -> tokio_util::sync::CancellationToken {
        self.client.shutdown_token()
    }

    /// Shut down every consumer group and wait for all tasks to complete.
    pub async fn shutdown_all(&mut self) {
        info!(
            count = self.groups.len(),
            "shutting down all consumer groups"
        );
        for group in self.groups.values_mut() {
            group.shutdown().await;
        }
        debug!("all consumer groups shut down");
    }
}
