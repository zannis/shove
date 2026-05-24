//! Compile-only parity assertions for every shove backend.
//!
//! Each `#[cfg(feature = "<backend>")]` block declares a handful of
//! `fn _assert_*` helpers whose bodies never execute — their job is to
//! type-check the public surface the parity matrix promises. If a backend
//! ever loses an inherent method, a trait impl, or a builder, this file
//! stops compiling.
//!
//! What this file deliberately does NOT check:
//! - Runtime behaviour (covered by per-backend integration tests).
//! - Sealed pub(crate) traits like `PublisherImpl` / `ConsumerImpl` /
//!   `QueueStatsProviderImpl`. Those are bound on `Backend` directly, so
//!   the trait already enforces them.
//!
//! Convention: anchor a single `_assert_compiles` function per feature so
//! cargo's per-feature build inevitably monomorphises every assertion.

#![allow(dead_code, unreachable_code, clippy::diverging_sub_expression)]

use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::time::Duration;

use shove::{
    AutoscalerBackend, AutoscalerConfig, Codec, JsonCodec, MessageHandler, MessageMetadata,
    Outcome, QueueTopology, ScalingStrategy, Topic, TopologyBuilder,
};

// ---------------------------------------------------------------------------
// Shared dummy topic + handler used by every backend's parity assertions.
// ---------------------------------------------------------------------------

struct DummyTopic;
impl Topic for DummyTopic {
    type Message = String;
    type Codec = JsonCodec;
    fn topology() -> &'static QueueTopology {
        static T: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("parity-dummy").build())
    }
}

struct DummyHandler;
impl MessageHandler<DummyTopic> for DummyHandler {
    type Context = ();
    async fn handle(&self, _m: String, _meta: MessageMetadata, _ctx: &()) -> Outcome {
        Outcome::Ack
    }
}

// Helper bounds: cheap to write and read at each call site.
fn _impls_autoscaler_backend<T: AutoscalerBackend>() {}

// `Send + Sync + 'static` is the bound `Backend::AutoscalerImpl` carries,
// so make sure the concrete type meets it too.
fn _is_send_sync_static<T: Send + Sync + 'static>() {}

// ---------------------------------------------------------------------------
// InMemory
// ---------------------------------------------------------------------------

#[cfg(feature = "inmemory")]
mod inmemory {
    use super::*;
    use shove::Broker;
    use shove::inmemory::{
        BrokerStatsProvider, InMemory, InMemoryAutoscalerBackend, InMemoryConsumer,
        InMemoryConsumerGroup, InMemoryConsumerGroupConfig, InMemoryPublisher,
    };

    fn _autoscaler_backend_impl() {
        _impls_autoscaler_backend::<InMemoryAutoscalerBackend<BrokerStatsProvider>>();
        _is_send_sync_static::<InMemoryAutoscalerBackend<BrokerStatsProvider>>();
    }

    fn _consumer_group_config_builders(range: RangeInclusive<u16>) -> InMemoryConsumerGroupConfig {
        InMemoryConsumerGroupConfig::new(range)
            .with_prefetch_count(10)
            .with_max_retries(7)
            .with_handler_timeout(Duration::from_secs(30))
            .with_concurrent_processing(true)
    }

    fn _consumer_group_scale(g: &mut InMemoryConsumerGroup) -> (bool, bool) {
        (g.scale_up(), g.scale_down())
    }

    async fn _publisher_inherent(p: &InMemoryPublisher) {
        let _ = p.publish::<DummyTopic>(&"x".into()).await;
        let _ = p
            .publish_with_headers::<DummyTopic>(&"x".into(), HashMap::new())
            .await;
        let _ = p.publish_batch::<DummyTopic>(&[]).await;
    }

    async fn _consumer_inherent(c: &InMemoryConsumer) {
        // Inherent run* on InMemoryConsumer take ConsumerOptions<InMemory>.
        let options = shove::ConsumerOptions::<InMemory>::new();
        let _ = c
            .run::<DummyTopic, DummyHandler>(DummyHandler, (), options.clone())
            .await;
        // run_dlq doesn't take options.
        let _ = c
            .run_dlq::<DummyTopic, DummyHandler>(DummyHandler, ())
            .await;
    }

    async fn _broker_autoscaler(broker: &Broker<InMemory>) {
        let _ = broker.default_autoscaler(AutoscalerConfig::default());
        let _ = broker.autoscaler(_unreachable_strategy(), Duration::from_secs(5));
    }
}

// ---------------------------------------------------------------------------
// Kafka
// ---------------------------------------------------------------------------

#[cfg(feature = "kafka")]
mod kafka {
    use super::*;
    use shove::Broker;
    use shove::kafka::{
        Kafka, KafkaAutoscalerBackend, KafkaConsumer, KafkaConsumerGroup, KafkaConsumerGroupConfig,
        KafkaLagStatsProvider, KafkaPublisher,
    };

    fn _autoscaler_backend_impl() {
        _impls_autoscaler_backend::<KafkaAutoscalerBackend<KafkaLagStatsProvider>>();
        _is_send_sync_static::<KafkaAutoscalerBackend<KafkaLagStatsProvider>>();
    }

    fn _consumer_group_config_builders(range: RangeInclusive<u16>) -> KafkaConsumerGroupConfig {
        KafkaConsumerGroupConfig::new(range)
            .with_prefetch_count(10)
            .with_max_retries(7)
            .with_handler_timeout(Duration::from_secs(30))
            .with_concurrent_processing(true)
    }

    fn _consumer_group_scale(g: &mut KafkaConsumerGroup) -> (bool, bool) {
        (g.scale_up(), g.scale_down())
    }

    async fn _publisher_inherent(p: &KafkaPublisher) {
        let _ = p.publish::<DummyTopic>(&"x".into()).await;
        let _ = p
            .publish_with_headers::<DummyTopic>(&"x".into(), HashMap::new())
            .await;
        let _ = p.publish_batch::<DummyTopic>(&[]).await;
    }

    async fn _consumer_inherent(c: &KafkaConsumer) {
        let options = shove::ConsumerOptions::<Kafka>::new();
        let _ = c
            .run::<DummyTopic, DummyHandler>(DummyHandler, (), options.clone())
            .await;
        let _ = c
            .run_dlq::<DummyTopic, DummyHandler>(DummyHandler, ())
            .await;
    }

    async fn _broker_autoscaler(broker: &Broker<Kafka>) {
        let _ = broker.default_autoscaler(AutoscalerConfig::default());
        let _ = broker.autoscaler(_unreachable_strategy(), Duration::from_secs(5));
    }
}

// ---------------------------------------------------------------------------
// NATS
// ---------------------------------------------------------------------------

#[cfg(feature = "nats")]
mod nats {
    use super::*;
    use shove::Broker;
    use shove::nats::{
        JetStreamStatsProvider, Nats, NatsAutoscalerBackend, NatsConsumer, NatsConsumerGroup,
        NatsConsumerGroupConfig, NatsPublisher,
    };

    fn _autoscaler_backend_impl() {
        _impls_autoscaler_backend::<NatsAutoscalerBackend<JetStreamStatsProvider>>();
        _is_send_sync_static::<NatsAutoscalerBackend<JetStreamStatsProvider>>();
    }

    fn _consumer_group_config_builders(range: RangeInclusive<u16>) -> NatsConsumerGroupConfig {
        NatsConsumerGroupConfig::new(range)
            .with_prefetch_count(10)
            .with_max_retries(7)
            .with_handler_timeout(Duration::from_secs(30))
            .with_concurrent_processing(true)
    }

    fn _consumer_group_scale(g: &mut NatsConsumerGroup) -> (bool, bool) {
        (g.scale_up(), g.scale_down())
    }

    async fn _publisher_inherent(p: &NatsPublisher) {
        let _ = p.publish::<DummyTopic>(&"x".into()).await;
        let _ = p
            .publish_with_headers::<DummyTopic>(&"x".into(), HashMap::new())
            .await;
        let _ = p.publish_batch::<DummyTopic>(&[]).await;
    }

    async fn _consumer_inherent(c: &NatsConsumer) {
        let options = shove::ConsumerOptions::<Nats>::new();
        let _ = c
            .run::<DummyTopic, DummyHandler>(DummyHandler, (), options.clone())
            .await;
        let _ = c
            .run_dlq::<DummyTopic, DummyHandler>(DummyHandler, ())
            .await;
    }

    async fn _broker_autoscaler(broker: &Broker<Nats>) {
        let _ = broker.default_autoscaler(AutoscalerConfig::default());
        let _ = broker.autoscaler(_unreachable_strategy(), Duration::from_secs(5));
    }
}

// ---------------------------------------------------------------------------
// RabbitMQ
// ---------------------------------------------------------------------------

#[cfg(feature = "rabbitmq")]
mod rabbitmq {
    use super::*;
    use shove::Broker;
    use shove::rabbitmq::{
        ConsumerGroup as RabbitMqConsumerGroup, ConsumerGroupConfig as RabbitMqConsumerGroupConfig,
        ManagementClient, RabbitMq, RabbitMqAutoscalerBackend, RabbitMqConsumer, RabbitMqPublisher,
    };

    fn _autoscaler_backend_impl() {
        _impls_autoscaler_backend::<RabbitMqAutoscalerBackend<ManagementClient>>();
        _is_send_sync_static::<RabbitMqAutoscalerBackend<ManagementClient>>();
    }

    fn _consumer_group_config_builders(range: RangeInclusive<u16>) -> RabbitMqConsumerGroupConfig {
        RabbitMqConsumerGroupConfig::new(range)
            .with_prefetch_count(10)
            .with_max_retries(7)
            .with_handler_timeout(Duration::from_secs(30))
            .with_concurrent_processing(true)
    }

    fn _consumer_group_scale(g: &mut RabbitMqConsumerGroup) -> (bool, bool) {
        (g.scale_up(), g.scale_down())
    }

    async fn _publisher_inherent(p: &RabbitMqPublisher) {
        let _ = p.publish::<DummyTopic>(&"x".into()).await;
        let _ = p
            .publish_with_headers::<DummyTopic>(&"x".into(), HashMap::new())
            .await;
        let _ = p.publish_batch::<DummyTopic>(&[]).await;
    }

    async fn _consumer_inherent(c: &RabbitMqConsumer) {
        let options = shove::ConsumerOptions::<RabbitMq>::new();
        let _ = c
            .run::<DummyTopic, DummyHandler>(DummyHandler, (), options.clone())
            .await;
        let _ = c
            .run_dlq::<DummyTopic, DummyHandler>(DummyHandler, ())
            .await;
    }

    async fn _broker_autoscaler(broker: &Broker<RabbitMq>) {
        let _ = broker.default_autoscaler(AutoscalerConfig::default());
        let _ = broker.autoscaler(_unreachable_strategy(), Duration::from_secs(5));
    }
}

// ---------------------------------------------------------------------------
// Redis
// ---------------------------------------------------------------------------

#[cfg(feature = "redis-streams")]
mod redis {
    use super::*;
    use shove::Broker;
    use shove::redis::{
        Redis, RedisAutoscalerBackend, RedisConsumer, RedisConsumerGroup, RedisConsumerGroupConfig,
        RedisPublisher, XlenStatsProvider,
    };

    fn _autoscaler_backend_impl() {
        _impls_autoscaler_backend::<RedisAutoscalerBackend<XlenStatsProvider>>();
        _is_send_sync_static::<RedisAutoscalerBackend<XlenStatsProvider>>();
    }

    fn _consumer_group_config_builders(range: RangeInclusive<u16>) -> RedisConsumerGroupConfig {
        RedisConsumerGroupConfig::new(range)
            .with_prefetch_count(10)
            .with_max_retries(7)
            .with_handler_timeout(Duration::from_secs(30))
            .with_concurrent_processing(true)
    }

    fn _consumer_group_scale(g: &mut RedisConsumerGroup) -> (bool, bool) {
        (g.scale_up(), g.scale_down())
    }

    async fn _publisher_inherent(p: &RedisPublisher) {
        let _ = p.publish::<DummyTopic>(&"x".into()).await;
        let _ = p
            .publish_with_headers::<DummyTopic>(&"x".into(), HashMap::new())
            .await;
        let _ = p.publish_batch::<DummyTopic>(&[]).await;
    }

    async fn _consumer_inherent(c: &RedisConsumer) {
        let options = shove::ConsumerOptions::<Redis>::new();
        let _ = c
            .run::<DummyTopic, DummyHandler>(DummyHandler, (), options.clone())
            .await;
        let _ = c
            .run_dlq::<DummyTopic, DummyHandler>(DummyHandler, ())
            .await;
    }

    async fn _broker_autoscaler(broker: &Broker<Redis>) {
        let _ = broker.default_autoscaler(AutoscalerConfig::default());
        let _ = broker.autoscaler(_unreachable_strategy(), Duration::from_secs(5));
    }
}

// ---------------------------------------------------------------------------
// SQS
//
// SQS deliberately opts out of `HasCoordinatedGroups` (its "group" is N
// parallel independent pollers handled by `ConsumerSupervisor<Sqs>`). The
// SqsConsumerGroup struct itself still exists and exposes scale_up/scale_down
// for users who drive the backend-specific registry directly — assert that.
// ---------------------------------------------------------------------------

#[cfg(feature = "aws-sns-sqs")]
mod sqs {
    use super::*;
    use shove::Broker;
    use shove::sns::{
        SnsPublisher, Sqs, SqsAutoscalerBackend, SqsConsumer, SqsConsumerGroup,
        SqsConsumerGroupConfig, SqsQueueStatsProvider,
    };

    fn _autoscaler_backend_impl() {
        _impls_autoscaler_backend::<SqsAutoscalerBackend<SqsQueueStatsProvider>>();
        _is_send_sync_static::<SqsAutoscalerBackend<SqsQueueStatsProvider>>();
    }

    fn _consumer_group_config_builders(range: RangeInclusive<u16>) -> SqsConsumerGroupConfig {
        SqsConsumerGroupConfig::new(range)
            .with_prefetch_count(10)
            .with_max_retries(7)
            .with_handler_timeout(Duration::from_secs(30))
            .with_concurrent_processing(true)
    }

    fn _consumer_group_scale(g: &mut SqsConsumerGroup) -> (bool, bool) {
        (g.scale_up(), g.scale_down())
    }

    async fn _publisher_inherent(p: &SnsPublisher) {
        let _ = p.publish::<DummyTopic>(&"x".into()).await;
        let _ = p
            .publish_with_headers::<DummyTopic>(&"x".into(), HashMap::new())
            .await;
        let _ = p.publish_batch::<DummyTopic>(&[]).await;
    }

    async fn _consumer_inherent(c: &SqsConsumer) {
        let options = shove::ConsumerOptions::<Sqs>::new();
        let _ = c
            .run::<DummyTopic, DummyHandler>(DummyHandler, (), options.clone())
            .await;
        let _ = c
            .run_dlq::<DummyTopic, DummyHandler>(DummyHandler, ())
            .await;
    }

    async fn _broker_autoscaler(broker: &Broker<Sqs>) {
        let _ = broker.default_autoscaler(AutoscalerConfig::default());
        let _ = broker.autoscaler(_unreachable_strategy(), Duration::from_secs(5));
    }
}

// ---------------------------------------------------------------------------
// Strategy placeholder — never constructed; used only inside `_broker_autoscaler`
// helpers above so the compiler exercises the generic bound on
// `Broker::<B>::autoscaler::<S: ScalingStrategy>(...)`.
// ---------------------------------------------------------------------------

fn _unreachable_strategy() -> NoopStrategy {
    unreachable!("parity assertions are compile-only")
}

struct NoopStrategy;
impl ScalingStrategy for NoopStrategy {
    fn evaluate(
        &mut self,
        _group: &str,
        _metrics: &shove::ScalingMetrics,
    ) -> shove::ScalingDecision {
        shove::ScalingDecision::Hold
    }

    fn gc(&mut self, _active: &[impl AsRef<str>]) {}
}

// ---------------------------------------------------------------------------
// Pin Codec to ensure JsonCodec satisfies the trait under all features.
// ---------------------------------------------------------------------------

fn _codec_compiles<C: Codec<String>>() {}
fn _json_codec_compiles() {
    _codec_compiles::<JsonCodec>();
}

// Quiet the unused-import warning when no backend feature is enabled.
#[allow(unused)]
async fn _touch_imports() {
    let _: HashMap<String, String> = HashMap::new();
}
