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
    type TopologyImpl: TopologyImpl;
    type AutoscalerImpl: AutoscalerBackendImpl + Send + Sync + 'static;
    type QueueStatsImpl: QueueStatsProviderImpl + Send + Sync + 'static;

    fn connect(config: Self::Config)
    -> impl Future<Output = Result<Self::Client>> + Send;

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
