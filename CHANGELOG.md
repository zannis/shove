# Changelog

## Unreleased

### Added

- `RabbitMqConsumer::run_fifo_until_timeout`, `KafkaConsumer::run_fifo_until_timeout`, `NatsConsumer::run_fifo_until_timeout`, `InMemoryConsumer::run_fifo_until_timeout`, and `SqsConsumer::run_fifo_until_timeout`. FIFO-equivalent of `ConsumerGroup::run_until_timeout` for sequenced topics — races a `signal` future against shards finishing on their own, cancels `options.shutdown` on signal, drains within `drain_timeout`, and returns a `SupervisorOutcome`. Process-level exit-code rollups across coordinated groups and sequenced topics now use one type:

  ```rust,no_run
  let outcome = consumer
      .run_fifo_until_timeout::<MyTopic, _, _>(handler, ctx, opts, signal, Duration::from_secs(30))
      .await;
  std::process::exit(outcome.exit_code());
  ```

  Handler panics are absorbed at the `tokio::spawn` + `oneshot` boundary on every backend — they map to `Outcome::Retry` and don't increment `outcome.panics`, landing in the configured retry/DLQ flow instead. `outcome.panics > 0` reflects panics in shard-infrastructure code that escape the in-shard handler-spawn boundary; aborted-during-drain shards do **not** count as panics (cancellation is ignored by the harness's tally), so a clean drain timeout returns `timed_out=true, panics=0, errors=0`.

- `ConsumerSupervisor::register_fifo<T: SequencedTopic, H>` and `ConsumerGroup::register_fifo<T: SequencedTopic, H>`. Sequenced topics now register on the same harness as regular topics and drain through the same `run_until_timeout`, returning one `SupervisorOutcome` for the entire harness:

  ```rust,no_run
  let mut sup = broker.consumer_supervisor().with_context(ctx);
  sup.register::<RegularTopic, _>(handler_a, options_a)?;
  sup.register_fifo::<SequencedTopic, _>(handler_b, options_b).await?;
  let outcome = sup.run_until_timeout(signal, Duration::from_secs(30)).await;
  ```

  `ConsumerGroup::register_fifo` takes only the handler factory — FIFO concurrency is fixed by the topology's `routing_shards`, so there is no `ConsumerGroupConfig` parameter and the autoscaler is bypassed (more replicas would break per-key ordering). The existing `register` rejection messages now point at `register_fifo` instead of the older `run_fifo` advice; per-shard panics surface as `SupervisorOutcome::panics` via a new internal `panic_count` counter on each backend's group struct.

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
