//! Capability traits — each gates one public entry point to the backends that
//! have the underlying broker primitive.
//!
//! - [`HasCoordinatedGroups`] gates `ConsumerGroup<B>` / `consumer_group()`.
//! - [`HasBroadcast`] gates `BroadcastSubscriber<B>` / `broadcast_subscriber()`.
//!
//! SQS implements neither, so both entry points are compile errors on
//! `Broker<Sqs>` rather than runtime surprises.

use std::sync::Arc;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::autoscaler::AutoscalerConfig;
use crate::backend::{Backend, BroadcastImpl, RegistryImpl};

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
    ///
    /// # Contract
    ///
    /// The returned `JoinHandle` MUST NOT resolve until every clone of
    /// `registry` held by the spawned task — and any sub-tasks it spawns — has
    /// been dropped. `ConsumerGroup::run_until_timeout` reclaims sole ownership
    /// of the registry via `Arc::try_unwrap` immediately after joining this
    /// handle; a surviving clone makes that reclaim fail. The generic
    /// `Autoscaler::run` loop satisfies this by holding the only clone inside
    /// the awaited task future and spawning no detached sub-tasks that retain it.
    fn spawn_autoscaler(
        client: &Self::Client,
        registry: Arc<Mutex<Self::RegistryImpl>>,
        config: AutoscalerConfig,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()>;
}

/// Capability: this backend can give each process its own **ephemeral**
/// subscription, so every instance of a service receives every message
/// instead of competing for one queue (see
/// [`TopologyBuilder::broadcast`](crate::topology::TopologyBuilder::broadcast)).
///
/// The bar is deliberately higher than "can fan out at all": the subscription
/// must leave *nothing* behind when the process goes away.
///
/// # Which backends implement this
///
/// This is the authoritative list; the `.broadcast()` and
/// `broadcast_subscriber()` docs defer to it.
///
/// | Backend | Implements `HasBroadcast` | Ephemeral primitive |
/// |---|---|---|
/// | **InMemory** | **yes** | a per-subscriber buffer |
/// | NATS | not yet | ephemeral pull consumer |
/// | Redis (`redis-streams`) | not yet | plain `XREAD` from `$`, no `XGROUP` |
/// | Kafka | not yet | groupless `assign()` at the latest offset |
/// | RabbitMQ | not yet | exclusive auto-delete queue on a fanout exchange |
/// | **SQS** | **never** | — |
///
/// A backend in the *not yet* rows has a primitive that satisfies the contract,
/// but no implementation on this version — `broadcast_subscriber()` does not
/// compile for it, by the same gate that excludes SQS.
///
/// SQS is different in kind, not in schedule: per-pod fan-out there means
/// creating and deleting a real queue plus an SNS subscription per process, and
/// a leaked queue costs money forever. So SQS is gated out permanently rather
/// than given a lossy approximation.
///
/// Sealed via `Backend`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `HasBroadcast`, so `.broadcast()` topologies cannot be consumed on it.",
    note = "InMemory is the only backend implementing `HasBroadcast` on this version. NATS, Redis, Kafka and RabbitMQ each have a suitable ephemeral primitive and are planned. SQS is excluded permanently: per-process fan-out there needs a real queue plus an SNS subscription whose lifecycle shove does not manage — publish to an SNS topic each instance subscribes to itself instead."
)]
#[allow(private_interfaces, private_bounds)]
pub trait HasBroadcast: Backend {
    type BroadcastImpl: BroadcastImpl + Clone + Send + Sync + 'static;

    fn make_broadcast(client: &Self::Client) -> Self::BroadcastImpl;
}
