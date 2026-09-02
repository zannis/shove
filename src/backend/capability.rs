//! Capability traits — each gates one public entry point to the backends that
//! have the underlying broker primitive.
//!
//! - [`HasCoordinatedGroups`] gates `ConsumerGroup<B>` / `consumer_group()`.
//! - [`HasBroadcast`] gates `BroadcastSubscriber<B>` / `broadcast_subscriber()`.
//! - [`HasBatchConsumption`] gates `BatchConsumer<B>` / `Broker::batch_consumer()`.
//!
//! SQS implements none of the three, so all three entry points are compile
//! errors on `Broker<Sqs>` rather than runtime surprises.

use std::sync::Arc;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::autoscaler::AutoscalerConfig;
use crate::backend::{Backend, BatchConsumerImpl, BroadcastImpl, RegistryImpl};

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
/// | **Kafka** | **yes** | groupless `assign()` at the latest offset |
/// | **NATS** | **yes** | ephemeral pull consumer on an `Interest`-retention stream |
/// | **RabbitMQ** | **yes** | exclusive auto-delete queue on a fanout exchange |
/// | **Redis** (`redis-streams`) | **yes** | plain `XREAD` from `$`, no `XGROUP` |
/// | **SQS** | **never** | — |
///
/// Every backend except SQS implements it on this version, so there is no
/// longer a *not yet* row: `broadcast_subscriber()` compiles everywhere but
/// `Broker<Sqs>`, and that gate is permanent rather than pending.
///
/// NATS is the one backend where `.broadcast()` changes how the *stream* is
/// declared: shove's default `WorkQueue` retention rejects both the
/// `AckPolicy::None` and the `DeliverPolicy::New` an ephemeral consumer needs,
/// so a broadcast topology is declared `Interest` instead. Pinning one to
/// `WorkQueue` via `nats_stream_config` is refused at declare time.
///
/// SQS is different in kind, not in schedule: per-pod fan-out there means
/// creating and deleting a real queue plus an SNS subscription per process, and
/// a leaked queue costs money forever. So SQS is gated out permanently rather
/// than given a lossy approximation.
///
/// Sealed via `Backend`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `HasBroadcast`, so `.broadcast()` topologies cannot be consumed on it.",
    note = "Every backend except SQS implements `HasBroadcast` on this version: InMemory, Kafka, NATS, RabbitMQ and Redis (redis-streams). SQS is excluded permanently: per-process fan-out there needs a real queue plus an SNS subscription whose lifecycle shove does not manage — publish to an SNS topic each instance subscribes to itself instead."
)]
#[allow(private_interfaces, private_bounds)]
pub trait HasBroadcast: Backend {
    type BroadcastImpl: BroadcastImpl + Clone + Send + Sync + 'static;

    fn make_broadcast(client: &Self::Client) -> Self::BroadcastImpl;
}

/// Capability: this backend has a batch-consumption primitive — buffering up
/// to N messages before handing them to the handler as a single call, so a
/// sink amortises its per-flush cost (one DB transaction, one HTTP request)
/// across many messages instead of paying it per message.
///
/// # Which backends implement this
///
/// This is the authoritative list; [`Broker::batch_consumer`](crate::Broker::batch_consumer)
/// and [`BatchConsumer`](crate::batch_consumer::BatchConsumer)'s docs defer to
/// it.
///
/// | Backend | Implements `HasBatchConsumption` |
/// |---|---|
/// | **Kafka** | **yes** |
/// | **InMemory** | not yet (pending) |
/// | **NATS** | not yet (pending) |
/// | **RabbitMQ** | not yet (pending) |
/// | **Redis** (`redis-streams`) | not yet (pending) |
/// | **SQS** | not yet (pending) — will land with a documented 10-message receive cap |
///
/// Unlike [`HasBroadcast`]'s permanent exclusion of SQS, every row above other
/// than Kafka is *pending*, not excluded: each backend gets this capability as
/// its own batch consumer is implemented, and `Broker::batch_consumer()`
/// starts compiling on that marker the moment it does.
///
/// Sealed via `Backend`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no batch-consumption implementation yet, so `.batch_consumer()` is unavailable.",
    note = "Only Kafka implements `HasBatchConsumption` today. InMemory, NATS, RabbitMQ, Redis and SQS are pending — SQS will land with a documented 10-message receive cap. Use a single-message consumer in the meantime."
)]
#[allow(private_interfaces, private_bounds)]
pub trait HasBatchConsumption: Backend {
    type BatchConsumerImpl: BatchConsumerImpl + Clone + Send + Sync + 'static;

    fn make_batch_consumer(client: &Self::Client) -> Self::BatchConsumerImpl;
}
