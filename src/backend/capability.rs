//! `HasCoordinatedGroups` capability trait — gates `ConsumerGroup<B>` and
//! `consumer_group()` to backends with a real broker-level group
//! primitive. See DESIGN_V2.md §4.1.

use crate::backend::{Backend, RegistryImpl};

/// Capability: this backend has a broker-level coordinated-group primitive
/// (Kafka consumer groups, RabbitMQ consistent-hash exchange, NATS
/// JetStream work queue, InMemory model). SQS does not implement this
/// trait — its "group" is N parallel independent pollers, handled by
/// `ConsumerSupervisor`.
///
/// Sealed via `Backend`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no coordinated consumer-group primitive; use `broker.consumer_supervisor()` instead.",
    note = "Only Kafka, RabbitMQ, NATS, and InMemory implement `HasCoordinatedGroups`. SQS runs N parallel independent consumers via the supervisor."
)]
#[allow(private_interfaces, private_bounds)]
pub trait HasCoordinatedGroups: Backend {
    type ConsumerGroupConfig: Default + Clone + Send + 'static;
    type RegistryImpl: RegistryImpl<GroupConfig = Self::ConsumerGroupConfig> + Send + 'static;

    fn make_registry(client: &Self::Client) -> Self::RegistryImpl;
}
