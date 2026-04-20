# Changelog

## 0.8.0 — shove v2 (breaking)

Collapses the per-backend public API behind a single generic `Broker<B>` facade parameterized by a sealed `Backend` marker. Every backend-specific type (`KafkaPublisher`, `NatsConsumerGroupRegistry`, `SqsConsumer`, …) is now reached through the same wrappers: `Broker<B>`, `Publisher<B>`, `TopologyDeclarer<B>`, `ConsumerSupervisor<B>`, `ConsumerGroup<B>`, `ConsumerOptions<B>`.

### Breaking changes

- `pub trait Publisher`, `pub trait Consumer`, `pub trait TopologyDeclarer`, and the `declare_topic` free function are removed. Use the generic wrappers instead.
- Per-backend re-exports (`shove::kafka::KafkaPublisher`, `shove::nats::NatsConsumerGroup`, etc.) are downgraded to `pub(crate)` where the generic API covers them. Each `shove::<backend>` module now re-exports just the marker type plus the config struct (plus `ManagementConfig` for RabbitMQ).
- `MessageHandler<T>` gains an associated `Context` type for harness-injected dependency injection. Existing handlers must declare `type Context = ();`. The harness (`ConsumerSupervisor` / `ConsumerGroup`) threads the context to every `handle` / `handle_dead` call.
- `ConsumerOptions` is now `ConsumerOptions<B: Backend>`; backend-specific setters (`with_receive_batch_size`, `with_max_ack_pending`, `with_exactly_once`) live on feature-gated inherent impl blocks keyed on the marker.
- `AutoscaleMetrics { backlog, inflight, throughput_per_sec, processing_latency }` replaces five per-backend stats structs.
- `SupervisorOutcome { errors, panics, timed_out }` replaces ad-hoc `i32` exit codes; `exit_code()` computes the canonical process exit code.
- SQS deliberately does **not** implement `HasCoordinatedGroups`. `Broker<Sqs>::consumer_group()` is a compile error; SQS uses `Broker<Sqs>::consumer_supervisor()` instead. The property is pinned by a `compile_fail` doctest on the `Sqs` marker.
- `ShoveAuditHandler<P: Publisher>` → `ShoveAuditHandler<B: Backend>`. Construct via `ShoveAuditHandler::for_publisher(&publisher)`.

### Additions

- `Broker::from_client(client)` — wrap an existing backend client without reconnecting.
- `Broker::close(&self)` — graceful shutdown; idempotent.
- `MessageHandlerExt::audited(audit)` — fluent audit wrapping: `MyHandler::new(state).audited(audit.clone())`.
- `ConsumerOptions::preset(n)` — shorthand for `ConsumerOptions::new().with_prefetch_count(n)`.
- `ConsumerOptions::<B>::with_concurrent_processing(bool)` — opts into per-consumer concurrency.
- `Default` impl for `NatsConfig`, `RabbitMqConfig`, `KafkaConfig` (localhost endpoints). `InMemoryConfig` already had one; `SnsConfig` stays explicit because there is no safe default region.
- `#[cfg_attr(docsrs, doc(cfg(feature = "...")))]` annotations across the public surface so docs.rs renders feature badges.

### Migration examples

**Broker setup (RabbitMQ):**
```rust,no_run
// Before
let client = RabbitMqClient::connect(&RabbitMqConfig::new(uri)).await?;
let publisher = RabbitMqPublisher::new(client.clone()).await?;
let channel = client.create_channel().await?;
RabbitMqTopologyDeclarer::new(channel)
    .declare(MyTopic::topology())
    .await?;

// After
use shove::{Broker, rabbitmq::{RabbitMq, RabbitMqConfig}};
let broker = Broker::<RabbitMq>::new(RabbitMqConfig::new(uri)).await?;
broker.topology().declare::<MyTopic>().await?;
let publisher = broker.publisher().await?;
```

**Consumer group:**
```rust,no_run
// Before
let mut registry = KafkaConsumerGroupRegistry::new(client.clone());
registry.register::<MyTopic, _>(
    KafkaConsumerGroupConfig::new(1..=4),
    || MyHandler::new(),
).await?;
registry.start_all();

// After
use shove::ConsumerGroupConfig;
let mut group = broker.consumer_group();
group.register::<MyTopic, _>(
    ConsumerGroupConfig::new(1..=4),
    || MyHandler::new(),
).await?;
let outcome = group
    .run_until_timeout(tokio::signal::ctrl_c(), Duration::from_secs(30))
    .await;
std::process::exit(outcome.exit_code());
```

**Audit wrapping:**
```rust,no_run
// Before
let audit = ShoveAuditHandler::new(publisher.clone());
let wrapped = Audited::new(MyHandler::new(state), audit);

// After
use shove::{MessageHandlerExt, ShoveAuditHandler};
let audit = ShoveAuditHandler::for_publisher(&publisher);
let wrapped = MyHandler::new(state).audited(audit);
```

See `README.md` and `DESIGN_V2.md` for the complete new public surface.
