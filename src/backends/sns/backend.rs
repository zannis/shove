//! Backend / impl-trait registrations for the SQS backend.
//!
//! Binds the Sqs marker (`crate::markers::Sqs`) to the concrete types in
//! this module via `impl Backend`, plus the five `pub(crate)` impl-trait
//! bodies that carry the real work.
//!
//! SQS deliberately does **not** implement `HasCoordinatedGroups`: the
//! "group" is N parallel independent pollers on one queue, which is
//! covered by `Broker<Sqs>::consumer_supervisor()`. A `compile_fail`
//! doctest in `tests/sqs_no_consumer_group.rs` pins this property.
//!
//! The whole `crate::backends::sns` module is gated on `pub-aws-sns`;
//! this file is further gated on `aws-sns-sqs` by `mod.rs` so the
//! SQS-specific types (`SqsConsumer`, `SqsAutoscalerBackend`, …) are
//! available.
//!
//! See DESIGN_V2.md §4 and Phase 10 of the v2 plan.

#![cfg(feature = "aws-sns-sqs")]

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::autoscale_metrics::AutoscaleMetrics;
use crate::backend::{
    AutoscalerBackendImpl, Backend, ConsumerImpl, ConsumerOptionsInner, PublisherImpl,
    QueueStatsProviderImpl, TopologyImpl, sealed,
};
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::markers::Sqs;
use crate::topic::{SequencedTopic, Topic};

use super::autoscaler::SqsAutoscalerBackend;
use super::client::{SnsClient, SnsConfig};
use super::consumer::SqsConsumer;
use super::publisher::SnsPublisher;
use super::registry::SqsConsumerGroupRegistry;
use super::stats::SqsQueueStatsProvider;
use super::topology::SnsTopologyDeclarer;

// ---------------------------------------------------------------------------
// Marker bindings
// ---------------------------------------------------------------------------

impl sealed::Sealed for Sqs {}

impl Backend for Sqs {
    type Config = SnsConfig;
    type Client = SnsClient;

    type PublisherImpl = SnsPublisher;
    type ConsumerImpl = SqsConsumer;
    type TopologyImpl = SnsTopologyDeclarer;
    type AutoscalerImpl = SqsAutoscalerBackend<SqsQueueStatsProvider>;
    type QueueStatsImpl = SqsQueueStatsProvider;

    async fn connect(config: Self::Config) -> Result<Self::Client> {
        SnsClient::new(&config).await
    }

    async fn make_publisher(client: &Self::Client) -> Result<Self::PublisherImpl> {
        Ok(SnsPublisher::new(
            client.clone(),
            client.topic_registry().clone(),
        ))
    }

    fn make_consumer(client: &Self::Client) -> Self::ConsumerImpl {
        SqsConsumer::new(client.clone(), client.queue_registry().clone())
    }

    fn make_declarer(client: &Self::Client) -> Self::TopologyImpl {
        SnsTopologyDeclarer::new(client.clone(), client.topic_registry().clone())
            .with_queue_registry(client.queue_registry().clone())
    }

    fn make_autoscaler(client: &Self::Client) -> Self::AutoscalerImpl {
        let stats_provider =
            SqsQueueStatsProvider::new(client.clone(), client.queue_registry().clone());
        let registry = Arc::new(Mutex::new(SqsConsumerGroupRegistry::new(
            client.clone(),
            client.topic_registry().clone(),
            client.queue_registry().clone(),
        )));
        SqsAutoscalerBackend::new(stats_provider, registry)
    }

    fn make_stats_provider(client: &Self::Client) -> Self::QueueStatsImpl {
        SqsQueueStatsProvider::new(client.clone(), client.queue_registry().clone())
    }

    async fn close(client: &Self::Client) {
        client.shutdown().await;
    }
}

// ---------------------------------------------------------------------------
// PublisherImpl — delegate to the existing `Publisher` inherent impl
// ---------------------------------------------------------------------------

impl PublisherImpl for SnsPublisher {
    fn publish<T: Topic>(&self, msg: &T::Message) -> impl Future<Output = Result<()>> + Send {
        <Self as crate::publisher::Publisher>::publish::<T>(self, msg)
    }

    fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<()>> + Send {
        <Self as crate::publisher::Publisher>::publish_with_headers::<T>(self, msg, headers)
    }

    fn publish_batch<T: Topic>(
        &self,
        msgs: &[T::Message],
    ) -> impl Future<Output = Result<()>> + Send {
        <Self as crate::publisher::Publisher>::publish_batch::<T>(self, msgs)
    }
}

// ---------------------------------------------------------------------------
// ConsumerImpl — delegate through the existing `Consumer` trait (Context = ())
// ---------------------------------------------------------------------------

impl ConsumerImpl for SqsConsumer {
    async fn run<T, H>(
        &self,
        handler: H,
        _ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T, Context = ()>,
    {
        let options = options.into_consumer_options();
        <Self as crate::consumer::Consumer>::run::<T>(self, handler, options).await
    }

    async fn run_fifo<T, H>(
        &self,
        handler: H,
        _ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> Result<()>
    where
        T: SequencedTopic,
        H: MessageHandler<T, Context = ()>,
    {
        let options = options.into_consumer_options();
        <Self as crate::consumer::Consumer>::run_fifo::<T>(self, handler, options).await
    }

    async fn run_dlq<T, H>(&self, handler: H, _ctx: H::Context) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T, Context = ()>,
    {
        <Self as crate::consumer::Consumer>::run_dlq::<T>(self, handler).await
    }
}

// ---------------------------------------------------------------------------
// TopologyImpl — delegate through the existing `TopologyDeclarer` impl
// ---------------------------------------------------------------------------

impl TopologyImpl for SnsTopologyDeclarer {
    async fn declare<T: Topic>(&self) -> Result<()> {
        <Self as crate::topology::TopologyDeclarer>::declare(self, T::topology()).await
    }
}

// ---------------------------------------------------------------------------
// AutoscalerBackendImpl — trait has no methods in Phase 4
// ---------------------------------------------------------------------------

impl AutoscalerBackendImpl for SqsAutoscalerBackend<SqsQueueStatsProvider> {}

// ---------------------------------------------------------------------------
// QueueStatsProviderImpl — map the SQS stats to AutoscaleMetrics
// ---------------------------------------------------------------------------

impl QueueStatsProviderImpl for SqsQueueStatsProvider {
    async fn snapshot(&self, queue: &str) -> Result<AutoscaleMetrics> {
        use super::stats::SqsQueueStatsProviderTrait;
        let stats = self.get_queue_stats(queue).await?;
        Ok(AutoscaleMetrics {
            backlog: Some(stats.messages_ready),
            inflight: Some(stats.messages_not_visible),
            throughput_per_sec: None,
            processing_latency: None,
        })
    }
}
