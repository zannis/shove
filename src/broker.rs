//! Public `Broker<B>` hub. See DESIGN_V2.md §6.1.

use crate::backend::Backend;
use crate::backend::capability::HasCoordinatedGroups;
use crate::consumer_group::ConsumerGroup;
use crate::consumer_supervisor::ConsumerSupervisor;
use crate::error::Result;
use crate::publisher::Publisher;
use crate::topology_declarer::TopologyDeclarer;

pub struct Broker<B: Backend> {
    client: B::Client,
}

impl<B: Backend> Broker<B> {
    pub async fn new(config: B::Config) -> Result<Self> {
        Ok(Self {
            client: B::connect(config).await?,
        })
    }

    pub fn from_client(client: B::Client) -> Self {
        Self { client }
    }

    pub async fn publisher(&self) -> Result<Publisher<B>> {
        Ok(Publisher::new(B::make_publisher(&self.client).await?))
    }

    pub fn consumer_supervisor(&self) -> ConsumerSupervisor<B> {
        ConsumerSupervisor::new(&self.client)
    }

    pub fn topology(&self) -> TopologyDeclarer<B> {
        TopologyDeclarer::new(B::make_declarer(&self.client))
    }

    pub async fn close(&self) {
        B::close(&self.client).await
    }

    /// Return a [`QueueStatsImpl`](crate::backend::Backend::QueueStatsImpl) for
    /// reading queue depth from the underlying broker.
    pub fn queue_stats_provider(&self) -> B::QueueStatsImpl {
        B::make_stats_provider(&self.client)
    }

    /// Return a [`Backend::AutoscalerImpl`](crate::backend::Backend::AutoscalerImpl)
    /// for driving generic autoscaling through the
    /// [`AutoscalerBackend`](crate::autoscaler::AutoscalerBackend) interface.
    ///
    /// The returned value implements [`AutoscalerBackend`](crate::autoscaler::AutoscalerBackend)
    /// and can be passed directly to
    /// [`Autoscaler::new`](crate::autoscaler::Autoscaler::new).
    pub fn autoscaler(&self) -> B::AutoscalerImpl {
        B::make_autoscaler(&self.client)
    }
}

impl<B: HasCoordinatedGroups> Broker<B> {
    pub fn consumer_group(&self) -> ConsumerGroup<B> {
        ConsumerGroup::new(B::make_registry(&self.client))
    }
}
