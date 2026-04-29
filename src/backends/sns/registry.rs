#![cfg(feature = "aws-sns-sqs")]

use std::collections::HashMap;

use tracing::{debug, info};

use crate::backends::sns::client::SnsClient;
use crate::backends::sns::consumer_group::{SqsConsumerGroup, SqsConsumerGroupConfig};
use crate::backends::sns::topology::SnsTopologyDeclarer;
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::topic::Topic;

/// Registry of all [`SqsConsumerGroup`]s.
///
/// Every group shares the same underlying [`SnsClient`] and therefore the
/// same client-owned topic/queue registries used by publishers and
/// topology declarers built from the same client. Each group gets its own
/// child [`CancellationToken`] derived from the client so that the whole
/// registry can be shut down with a single cancellation.
pub struct SqsConsumerGroupRegistry {
    groups: HashMap<String, SqsConsumerGroup>,
    client: SnsClient,
}

impl SqsConsumerGroupRegistry {
    pub fn new(client: SnsClient) -> Self {
        Self {
            groups: HashMap::new(),
            client,
        }
    }

    /// Register a new consumer group.
    ///
    /// Automatically declares the SNS + SQS topology for `T` before creating
    /// the group.  The group is **not** started — call [`start_all`] (or the
    /// group's own `start`) separately.
    ///
    /// [`start_all`]: Self::start_all
    pub async fn register<T, H>(
        &mut self,
        config: SqsConsumerGroupConfig,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<()>
    where
        T: Topic + 'static,
        H: MessageHandler<T> + Clone + 'static,
    {
        let topology = T::topology();
        let name = topology.queue().to_string();

        if self.groups.contains_key(&name) {
            crate::metrics::record_backend_error(
                crate::metrics::BackendLabel::SnsSqs,
                crate::metrics::BackendErrorKind::Topology,
            );
            return Err(ShoveError::Topology(format!(
                "consumer group '{name}' already registered"
            )));
        }

        SnsTopologyDeclarer::new(self.client.clone())
            .declare(topology)
            .await?;

        info!(group = %name, "registering SQS consumer group");
        let group_token = self.client.shutdown_token().child_token();
        let group = SqsConsumerGroup::new::<T, H>(
            name.clone(),
            name.clone(),
            config,
            self.client.clone(),
            self.client.queue_registry().clone(),
            group_token,
            handler_factory,
            ctx,
        );
        self.groups.insert(name, group);
        Ok(())
    }

    /// Call [`SqsConsumerGroup::start`] on every registered group.
    pub fn start_all(&mut self) {
        info!(
            count = self.groups.len(),
            "starting all SQS consumer groups"
        );
        for group in self.groups.values_mut() {
            group.start();
        }
    }

    /// Read-only access to the underlying group map.
    pub fn groups(&self) -> &HashMap<String, SqsConsumerGroup> {
        &self.groups
    }

    /// Mutable access to the underlying group map.
    pub fn groups_mut(&mut self) -> &mut HashMap<String, SqsConsumerGroup> {
        &mut self.groups
    }

    /// Shut down every consumer group and wait for all tasks to complete.
    pub async fn shutdown_all(&mut self) {
        info!(
            count = self.groups.len(),
            "shutting down all SQS consumer groups"
        );
        for group in self.groups.values_mut() {
            group.shutdown().await;
        }
        debug!("all SQS consumer groups shut down");
    }
}
