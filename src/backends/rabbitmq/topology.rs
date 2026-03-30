use lapin::Channel;
use lapin::options::{ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions};
use lapin::types::{AMQPValue, FieldTable};

use crate::error::{Result, ShoveError};
use crate::topology::{QueueTopology, TopologyDeclarer};

const X_DEAD_LETTER_EXCHANGE: &str = "x-dead-letter-exchange";
const X_DEAD_LETTER_ROUTING_KEY: &str = "x-dead-letter-routing-key";
const X_MESSAGE_TTL: &str = "x-message-ttl";
const X_SINGLE_ACTIVE_CONSUMER: &str = "x-single-active-consumer";

/// Declares RabbitMQ broker resources for a topic's topology.
///
/// All declarations are idempotent — safe to call on every startup.
pub struct RabbitMqTopologyDeclarer {
    channel: Channel,
}

impl RabbitMqTopologyDeclarer {
    pub fn new(channel: Channel) -> Self {
        Self { channel }
    }

    async fn declare_queue(&self, name: &str, args: FieldTable) -> Result<()> {
        self.channel
            .queue_declare(
                name.into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                args,
            )
            .await
            .map_err(|e| ShoveError::Topology(format!("failed to declare queue '{name}': {e}")))?;
        Ok(())
    }

    async fn declare_unsequenced(&self, topology: &QueueTopology) -> Result<()> {
        // 1. Declare DLQ (if present) — plain durable queue
        if let Some(dlq) = topology.dlq() {
            self.declare_queue(dlq, FieldTable::default()).await?;
        }

        // 2. Declare main queue with dead-letter routing to DLQ
        let mut main_args = FieldTable::default();
        if let Some(dlq) = topology.dlq() {
            main_args.insert(
                X_DEAD_LETTER_EXCHANGE.into(),
                AMQPValue::LongString("".into()),
            );
            main_args.insert(
                X_DEAD_LETTER_ROUTING_KEY.into(),
                AMQPValue::LongString(dlq.into()),
            );
        }
        self.declare_queue(topology.queue(), main_args).await?;

        // 3. Declare hold queues with TTL → route back to main queue on expiry
        for hq in topology.hold_queues() {
            let mut args = FieldTable::default();
            args.insert(
                X_MESSAGE_TTL.into(),
                AMQPValue::LongLongInt(hq.delay().as_millis() as i64),
            );
            args.insert(
                X_DEAD_LETTER_EXCHANGE.into(),
                AMQPValue::LongString("".into()),
            );
            args.insert(
                X_DEAD_LETTER_ROUTING_KEY.into(),
                AMQPValue::LongString(topology.queue().into()),
            );
            self.declare_queue(hq.name(), args).await?;
        }

        Ok(())
    }

    async fn declare_sequenced(&self, topology: &QueueTopology) -> Result<()> {
        let seq = topology
            .sequencing()
            .expect("declare_sequenced called without sequencing config");

        // 1. Declare DLQ + hold queues (shared with unsequenced)
        if let Some(dlq) = topology.dlq() {
            self.declare_queue(dlq, FieldTable::default()).await?;
        }
        for hq in topology.hold_queues() {
            let mut args = FieldTable::default();
            args.insert(
                X_MESSAGE_TTL.into(),
                AMQPValue::LongLongInt(hq.delay().as_millis() as i64),
            );
            args.insert(
                X_DEAD_LETTER_EXCHANGE.into(),
                AMQPValue::LongString("".into()),
            );
            // Hold queues route back to main queue on expiry
            // For sequenced topics, this goes to the main queue which is NOT consumed
            // (consumers read sub-queues). The main queue acts as a staging area.
            args.insert(
                X_DEAD_LETTER_ROUTING_KEY.into(),
                AMQPValue::LongString(topology.queue().into()),
            );
            self.declare_queue(hq.name(), args).await?;
        }

        // 2. Declare consistent-hash exchange
        self.channel
            .exchange_declare(
                seq.exchange().into(),
                lapin::ExchangeKind::Custom("x-consistent-hash".to_string()),
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| {
                ShoveError::Topology(format!(
                    "failed to declare exchange '{}': {e}",
                    seq.exchange()
                ))
            })?;

        // 3. Declare N sub-queues with single-active-consumer, bind to hash exchange
        for i in 0..seq.routing_shards() {
            let sub_queue = format!("{}-seq-{i}", topology.queue());

            let mut args = FieldTable::default();
            args.insert(X_SINGLE_ACTIVE_CONSUMER.into(), AMQPValue::Boolean(true));
            if let Some(dlq) = topology.dlq() {
                args.insert(
                    X_DEAD_LETTER_EXCHANGE.into(),
                    AMQPValue::LongString("".into()),
                );
                args.insert(
                    X_DEAD_LETTER_ROUTING_KEY.into(),
                    AMQPValue::LongString(dlq.into()),
                );
            }
            self.declare_queue(&sub_queue, args).await?;

            // Bind to hash exchange — routing weight "1" for even distribution
            self.channel
                .queue_bind(
                    sub_queue.as_str().into(),
                    seq.exchange().into(),
                    "1".into(),
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(|e| {
                    ShoveError::Topology(format!(
                        "failed to bind '{sub_queue}' to '{}': {e}",
                        seq.exchange()
                    ))
                })?;
        }

        Ok(())
    }
}

impl TopologyDeclarer for RabbitMqTopologyDeclarer {
    async fn declare(&self, topology: &QueueTopology) -> Result<()> {
        if topology.sequencing().is_some() {
            self.declare_sequenced(topology).await
        } else {
            self.declare_unsequenced(topology).await
        }
    }
}
