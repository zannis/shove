//! `HasCoordinatedGroups` capability trait — gates `ConsumerGroup<B>` and
//! `consumer_group()` to backends with a real broker-level group
//! primitive.

use std::sync::Arc;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::autoscaler::AutoscalerConfig;
use crate::backend::{Backend, RegistryImpl};

/// Capability: this backend has a broker-level coordinated-group primitive
/// (Kafka consumer groups, RabbitMQ consistent-hash exchange, NATS
/// JetStream work queue, InMemory model, Redis Streams consumer groups).
/// SQS does not implement this trait — its "group" is N parallel
/// independent pollers, handled by `ConsumerSupervisor`.
///
/// Sealed via `Backend`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no coordinated consumer-group primitive; use `broker.consumer_supervisor()` instead.",
    note = "Kafka, RabbitMQ, NATS, InMemory, and Redis (redis-streams) implement `HasCoordinatedGroups`. SQS runs N parallel independent consumers via the supervisor."
)]
#[allow(private_interfaces, private_bounds)]
pub trait HasCoordinatedGroups: Backend {
    type ConsumerGroupConfig: Default + Clone + Send + 'static;
    type RegistryImpl: RegistryImpl<GroupConfig = Self::ConsumerGroupConfig> + Send + 'static;

    fn make_registry(client: &Self::Client) -> Self::RegistryImpl;

    /// Spawn the autoscaler loop against this group's own registry, bound to
    /// `shutdown`. The autoscaler uses `Stabilized<ThresholdStrategy>` derived
    /// from `config`. Infallible: backends whose stats client may fail (e.g.
    /// RabbitMQ management) defer that error to the first metrics poll.
    fn spawn_autoscaler(
        client: &Self::Client,
        registry: Arc<Mutex<Self::RegistryImpl>>,
        config: AutoscalerConfig,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()>;
}
