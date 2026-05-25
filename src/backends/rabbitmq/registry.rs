use std::collections::HashMap;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::backends::rabbitmq::client::RabbitMqClient;
use crate::backends::rabbitmq::consumer_group::{
    RabbitMqConsumerGroup, RabbitMqConsumerGroupConfig,
};
use crate::backends::rabbitmq::topology::RabbitMqTopologyDeclarer;
use crate::consumer::{HandlerTimeoutConfig, resolve_handler_timeout};
use crate::consumer_supervisor::ShutdownTally;
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::metrics;
use crate::topic::{SequencedTopic, Topic};

/// Registry of all [`RabbitMqConsumerGroup`]s managed by the autoscaler.
///
/// Every group shares the same underlying [`RabbitMqClient`].  Each group gets
/// its own child [`CancellationToken`] derived from the client so that the
/// whole registry can be shut down with a single cancellation.
pub struct ConsumerGroupRegistry {
    groups: HashMap<String, RabbitMqConsumerGroup>,
    client: RabbitMqClient,
    pub(super) default_handler_timeout: Option<Duration>,
}

impl ConsumerGroupRegistry {
    pub fn new(client: RabbitMqClient) -> Self {
        Self {
            groups: HashMap::new(),
            client,
            default_handler_timeout: None,
        }
    }

    /// Set the registry-level default handler timeout. Applies to every
    /// group whose `RabbitMqConsumerGroupConfig` did not explicitly call
    /// `with_handler_timeout`. Per-group explicit settings always win.
    pub fn with_default_handler_timeout(mut self, timeout: Duration) -> Self {
        assert!(
            !timeout.is_zero(),
            "default_handler_timeout must be positive"
        );
        self.default_handler_timeout = Some(timeout);
        self
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
        config: RabbitMqConsumerGroupConfig,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<()>
    where
        T: Topic + 'static,
        H: MessageHandler<T> + 'static,
    {
        let mut config = config;
        config.handler_timeout = HandlerTimeoutConfig::Set(resolve_handler_timeout(
            config.handler_timeout,
            self.default_handler_timeout,
        ));

        let topology = T::topology();
        let name = topology.queue().to_string();

        if self.groups.contains_key(&name) {
            metrics::record_backend_error(
                metrics::BackendLabel::RabbitMq,
                metrics::BackendErrorKind::Topology,
            );
            return Err(ShoveError::Topology(format!(
                "consumer group '{name}' is already registered"
            )));
        }

        let channel = self.client.create_channel().await?;
        let declarer = RabbitMqTopologyDeclarer::new(channel);
        declarer.declare(topology).await?;

        info!(group = %name, "registering consumer group");
        let group_token = self.client.shutdown_token().child_token();
        let group = RabbitMqConsumerGroup::new::<T, H>(
            name.clone(),
            name.clone(),
            config,
            self.client.clone(),
            group_token,
            handler_factory,
            ctx,
        )
        .await?;
        self.groups.insert(name, group);
        Ok(())
    }

    /// Register a new FIFO consumer group for a [`SequencedTopic`].
    ///
    /// Declares the topology for `T` before creating the group.  The group is
    /// **not** started — call [`start_all`] separately.
    ///
    /// [`start_all`]: Self::start_all
    pub async fn register_fifo<T, H>(
        &mut self,
        config: RabbitMqConsumerGroupConfig,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<()>
    where
        T: SequencedTopic + 'static,
        H: MessageHandler<T> + 'static,
    {
        let mut config = config;
        config.handler_timeout = HandlerTimeoutConfig::Set(resolve_handler_timeout(
            config.handler_timeout,
            self.default_handler_timeout,
        ));

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

        info!(group = %name, "registering FIFO consumer group");
        let group_token = self.client.shutdown_token().child_token();
        let group = RabbitMqConsumerGroup::new_fifo::<T, H>(
            name.clone(),
            self.client.clone(),
            config,
            group_token,
            handler_factory,
            ctx,
        )
        .await?;
        self.groups.insert(name, group);
        Ok(())
    }

    /// Call [`RabbitMqConsumerGroup::start`] on every registered group.
    pub fn start_all(&mut self) {
        info!(count = self.groups.len(), "starting all consumer groups");
        for group in self.groups.values_mut() {
            group.start();
        }
    }

    /// Read-only access to the underlying group map.
    pub fn groups(&self) -> &HashMap<String, RabbitMqConsumerGroup> {
        &self.groups
    }

    /// Mutable access to the underlying group map.
    pub fn groups_mut(&mut self) -> &mut HashMap<String, RabbitMqConsumerGroup> {
        &mut self.groups
    }

    /// Return a clone of the client's shutdown token.
    ///
    /// Used by `RegistryImpl::cancellation_token` to surface a
    /// backend-independent shutdown signal.
    pub fn client_shutdown_token(&self) -> CancellationToken {
        self.client.shutdown_token()
    }

    /// Shut down every consumer group and wait for all tasks to complete.
    pub async fn shutdown_all(&mut self) {
        let _ = self.shutdown_all_with_tally().await;
    }

    pub(crate) async fn shutdown_all_with_tally(&mut self) -> ShutdownTally {
        let mut tally = ShutdownTally::default();
        self.drain_all_into(&mut tally).await;
        tally
    }

    /// Drain every consumer group, accumulating errors/panics into `tally`.
    ///
    /// Caller may race this against a timeout: each per-group `drain_into`
    /// captures atomic counts before awaiting handles, so the tally retains
    /// pre-cancel state even if the outer future is dropped mid-iteration.
    pub(crate) async fn drain_all_into(&mut self, tally: &mut ShutdownTally) {
        info!(
            count = self.groups.len(),
            "shutting down all consumer groups"
        );
        for group in self.groups.values_mut() {
            group.drain_into(tally).await;
        }
        debug!(
            errors = tally.errors,
            panics = tally.panics,
            "all consumer groups shut down"
        );
    }

    /// Abort surviving consumers across every group after a drain timeout.
    pub(crate) async fn abort_all_remaining_into(&mut self, tally: &mut ShutdownTally) {
        for group in self.groups.values_mut() {
            group.abort_remaining_into(tally).await;
        }
    }
}
