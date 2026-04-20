//! Sealed `Backend` trait layer. Binds a backend marker type to its concrete
//! internal implementation types; the public generic wrappers (`Broker<B>`,
//! `Publisher<B>`, etc.) delegate through this.
//!
//! See `DESIGN_V2.md` §4.

use crate::error::Result;

pub(crate) mod autoscaler;
pub mod capability;
pub(crate) mod consumer;
pub(crate) mod options_inner;
pub(crate) mod publisher;
pub(crate) mod registry;
pub(crate) mod topology;

pub(crate) use autoscaler::{AutoscalerBackendImpl, QueueStatsProviderImpl};
pub(crate) use consumer::ConsumerImpl;
pub(crate) use options_inner::ConsumerOptionsInner;
pub(crate) use publisher::PublisherImpl;
pub(crate) use registry::RegistryImpl;
pub(crate) use topology::TopologyImpl;

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// Binds a backend marker type to its concrete implementation types.
///
/// Sealed: not implementable outside this crate. The associated-type bounds
/// reference `pub(crate)` traits on purpose — external code can name the
/// associated types but cannot call their methods, which is the intended
/// sealed-behind-`pub(crate)` design.
#[allow(private_interfaces, private_bounds)]
pub trait Backend: sealed::Sealed + Sized + Send + Sync + 'static {
    type Config: Send;

    /// Connection handle. Cheap to clone; shared between publisher,
    /// consumer, and topology declarer. Public so `Broker::from_client`
    /// is possible; the `Backend` trait itself stays sealed.
    type Client: Clone + Send + Sync + 'static;

    type PublisherImpl: PublisherImpl + Clone + Send + Sync + 'static;
    type ConsumerImpl: ConsumerImpl + Clone + Send + Sync + 'static;
    type TopologyImpl: TopologyImpl + Send + Sync + 'static;
    type AutoscalerImpl: AutoscalerBackendImpl + Send + Sync + 'static;
    type QueueStatsImpl: QueueStatsProviderImpl + Send + Sync + 'static;

    fn connect(config: Self::Config) -> impl Future<Output = Result<Self::Client>> + Send;

    fn make_publisher(
        client: &Self::Client,
    ) -> impl Future<Output = Result<Self::PublisherImpl>> + Send;

    fn make_consumer(client: &Self::Client) -> Self::ConsumerImpl;
    fn make_declarer(client: &Self::Client) -> Self::TopologyImpl;
    fn make_autoscaler(client: &Self::Client) -> Self::AutoscalerImpl;
    fn make_stats_provider(client: &Self::Client) -> Self::QueueStatsImpl;

    /// Graceful shutdown of the underlying connection. Takes `&Self::Client`
    /// (not owned) so it works on an `Arc<Broker<B>>`. Subsequent calls
    /// after close are no-ops.
    fn close(client: &Self::Client) -> impl Future<Output = ()> + Send;
}

#[cfg(test)]
mod bounds_smoke {
    //! Compile-only assertions that `Backend` and `HasCoordinatedGroups`
    //! keep their intended bounds, plus a per-method exerciser against
    //! every `pub(crate) trait Impl` for the InMemory concrete types.
    //! Does not test behaviour — existence of the body being well-typed
    //! is the test; the monomorphized functions are never called.
    //!
    //! The trait methods themselves keep a narrow `#[allow(dead_code)]`
    //! on the trait declaration until Phase 5+ wires real call sites
    //! through `Broker<B>`/`Publisher<B>`/etc.

    use super::*;
    use crate::backend::capability::HasCoordinatedGroups;

    fn _require_backend<B: Backend>() {
        fn needs_send_sync_static<T: Send + Sync + 'static>() {}
        needs_send_sync_static::<B>();
        needs_send_sync_static::<B::Client>();
        needs_send_sync_static::<B::PublisherImpl>();
        needs_send_sync_static::<B::ConsumerImpl>();
        needs_send_sync_static::<B::TopologyImpl>();
        needs_send_sync_static::<B::AutoscalerImpl>();
        needs_send_sync_static::<B::QueueStatsImpl>();
    }

    fn _require_has_coordinated_groups<B: HasCoordinatedGroups>() {
        fn needs_default_clone<T: Default + Clone + Send + 'static>() {}
        needs_default_clone::<B::ConsumerGroupConfig>();
    }

    // Anchor the generic assertions to a concrete backend so they fire
    // at compile time instead of only at monomorphization.
    #[cfg(feature = "inmemory")]
    fn _anchor_inmemory() {
        _require_backend::<crate::markers::InMemory>();
        _require_has_coordinated_groups::<crate::markers::InMemory>();
    }

    #[cfg(feature = "kafka")]
    fn _anchor_kafka() {
        _require_backend::<crate::markers::Kafka>();
        _require_has_coordinated_groups::<crate::markers::Kafka>();
    }

    #[cfg(feature = "nats")]
    fn _anchor_nats() {
        _require_backend::<crate::markers::Nats>();
        _require_has_coordinated_groups::<crate::markers::Nats>();
    }

    #[cfg(feature = "rabbitmq")]
    fn _anchor_rabbitmq() {
        _require_backend::<crate::markers::RabbitMq>();
        _require_has_coordinated_groups::<crate::markers::RabbitMq>();
    }

    // SQS deliberately does NOT implement `HasCoordinatedGroups` — its
    // "group" is N parallel independent pollers (covered by
    // `ConsumerSupervisor<Sqs>`). Anchor only the `Backend` bound.
    #[cfg(feature = "aws-sns-sqs")]
    fn _anchor_sqs() {
        _require_backend::<crate::markers::Sqs>();
    }

    // Per-method anchoring: exercise every internal trait method once
    // against a concrete implementor so the methods aren't flagged as
    // `dead_code` before Phase 5+ wires the generic wrappers. These
    // functions are `#[cfg(test)]`-only and never called; their bodies
    // just need to type-check.
    #[cfg(feature = "inmemory")]
    async fn _anchor_publisher_impl(p: &<crate::markers::InMemory as Backend>::PublisherImpl) {
        use crate::topic::Topic;
        struct Dummy;
        impl Topic for Dummy {
            type Message = ();
            fn topology() -> &'static crate::topology::QueueTopology {
                unreachable!("anchor only")
            }
        }
        let _ = <_ as PublisherImpl>::publish::<Dummy>(p, &()).await;
        let _ = <_ as PublisherImpl>::publish_with_headers::<Dummy>(
            p,
            &(),
            std::collections::HashMap::new(),
        )
        .await;
        let _ = <_ as PublisherImpl>::publish_batch::<Dummy>(p, &[]).await;
    }

    #[cfg(feature = "inmemory")]
    async fn _anchor_consumer_impl(c: &<crate::markers::InMemory as Backend>::ConsumerImpl) {
        use crate::handler::MessageHandler;
        use crate::outcome::Outcome;
        use crate::topic::{SequencedTopic, Topic};

        struct Dummy;
        impl Topic for Dummy {
            type Message = ();
            fn topology() -> &'static crate::topology::QueueTopology {
                unreachable!("anchor only")
            }
        }
        impl SequencedTopic for Dummy {
            fn sequence_key(_m: &Self::Message) -> String {
                unreachable!("anchor only")
            }
        }
        struct DummyHandler;
        impl MessageHandler<Dummy> for DummyHandler {
            type Context = ();
            async fn handle(
                &self,
                _m: (),
                _meta: crate::metadata::MessageMetadata,
                _ctx: &(),
            ) -> Outcome {
                Outcome::Ack
            }
        }

        let options = ConsumerOptionsInner {
            max_retries: 0,
            prefetch_count: 1,
            concurrent_processing: false,
            handler_timeout: None,
            max_pending_per_key: None,
            max_message_size: None,
            shutdown: tokio_util::sync::CancellationToken::new(),
            processing: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(feature = "rabbitmq-transactional")]
            exactly_once: false,
            #[cfg(feature = "aws-sns-sqs")]
            receive_batch_size: 0,
            #[cfg(feature = "nats")]
            max_ack_pending: None,
        };
        let _ =
            <_ as ConsumerImpl>::run::<Dummy, DummyHandler>(c, DummyHandler, (), options.clone())
                .await;
        let _ = <_ as ConsumerImpl>::run_fifo::<Dummy, DummyHandler>(c, DummyHandler, (), options)
            .await;
        let _ = <_ as ConsumerImpl>::run_dlq::<Dummy, DummyHandler>(c, DummyHandler, ()).await;
    }

    #[cfg(feature = "inmemory")]
    async fn _anchor_topology_impl(t: &<crate::markers::InMemory as Backend>::TopologyImpl) {
        use crate::topic::Topic;
        struct Dummy;
        impl Topic for Dummy {
            type Message = ();
            fn topology() -> &'static crate::topology::QueueTopology {
                unreachable!("anchor only")
            }
        }
        let _ = <_ as TopologyImpl>::declare::<Dummy>(t).await;
    }

    #[cfg(feature = "inmemory")]
    async fn _anchor_autoscaler_impl(_a: &<crate::markers::InMemory as Backend>::AutoscalerImpl) {
        // AutoscalerBackendImpl has no methods in Phase 4.
    }

    #[cfg(feature = "inmemory")]
    async fn _anchor_stats_impl(s: &<crate::markers::InMemory as Backend>::QueueStatsImpl) {
        let _ = <_ as QueueStatsProviderImpl>::snapshot(s, "q").await;
    }

    #[cfg(feature = "inmemory")]
    async fn _anchor_registry_impl(
        r: &mut <crate::markers::InMemory as HasCoordinatedGroups>::RegistryImpl,
    ) {
        use crate::handler::MessageHandler;
        use crate::outcome::Outcome;
        use crate::topic::Topic;

        struct Dummy;
        impl Topic for Dummy {
            type Message = ();
            fn topology() -> &'static crate::topology::QueueTopology {
                unreachable!("anchor only")
            }
        }
        struct DummyHandler;
        impl MessageHandler<Dummy> for DummyHandler {
            type Context = ();
            async fn handle(
                &self,
                _m: (),
                _meta: crate::metadata::MessageMetadata,
                _ctx: &(),
            ) -> Outcome {
                Outcome::Ack
            }
        }
        let _ = <_ as RegistryImpl>::register::<Dummy, DummyHandler>(
            r,
            <<crate::markers::InMemory as HasCoordinatedGroups>::ConsumerGroupConfig>::default(),
            || DummyHandler,
            (),
        )
        .await;
        let _: tokio_util::sync::CancellationToken = <_ as RegistryImpl>::cancellation_token(r);
    }

    // The `run_until_timeout` consumer of the registry can't be anchored
    // with the same borrowed-registry pattern because the method takes
    // `self` by value. Exercise it against an owned registry in a
    // dedicated anchor.
    #[cfg(feature = "inmemory")]
    async fn _anchor_registry_run_until_timeout(
        r: <crate::markers::InMemory as HasCoordinatedGroups>::RegistryImpl,
    ) {
        let _: crate::consumer_supervisor::SupervisorOutcome =
            <_ as RegistryImpl>::run_until_timeout::<std::future::Pending<()>>(
                r,
                std::future::pending(),
                std::time::Duration::ZERO,
            )
            .await;
    }
}
