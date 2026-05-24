//! Public `Broker<B>` hub. See DESIGN_V2.md §6.1.

use std::time::Duration;

use crate::autoscaler::{
    Autoscaler, AutoscalerConfig, ScalingStrategy, Stabilized, ThresholdStrategy,
};
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

    /// Build an [`Autoscaler`] backed by this broker, using a caller-supplied
    /// scaling strategy.
    ///
    /// Equivalent to `Autoscaler::new(B::make_autoscaler(&client), strategy,
    /// poll_interval)` — the helper is generic over every backend that
    /// implements [`Backend`], because `Backend::AutoscalerImpl` is bound to
    /// the public [`crate::autoscaler::AutoscalerBackend`] trait.
    pub fn autoscaler<S: ScalingStrategy>(
        &self,
        strategy: S,
        poll_interval: Duration,
    ) -> Autoscaler<B::AutoscalerImpl, S> {
        Autoscaler::new(B::make_autoscaler(&self.client), strategy, poll_interval)
    }

    /// Build an [`Autoscaler`] with the default
    /// [`Stabilized<ThresholdStrategy>`] strategy parameterised by
    /// [`AutoscalerConfig`].
    ///
    /// Sugar over [`autoscaler`](Self::autoscaler) that mirrors what each
    /// backend's `XxxAutoscalerBackend::autoscaler(...)` constructor produces.
    pub fn default_autoscaler(
        &self,
        config: AutoscalerConfig,
    ) -> Autoscaler<B::AutoscalerImpl, Stabilized<ThresholdStrategy>> {
        let strategy = Stabilized::new(
            ThresholdStrategy {
                scale_up_multiplier: config.scale_up_multiplier,
                scale_down_multiplier: config.scale_down_multiplier,
            },
            config.hysteresis_duration,
            config.cooldown_duration,
        );
        self.autoscaler(strategy, config.poll_interval)
    }
}

impl<B: HasCoordinatedGroups> Broker<B> {
    pub fn consumer_group(&self) -> ConsumerGroup<B> {
        ConsumerGroup::new(B::make_registry(&self.client))
    }
}
