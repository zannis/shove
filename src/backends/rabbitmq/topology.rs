use lapin::Channel;
use lapin::options::{ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions};
use lapin::types::{AMQPValue, FieldTable};

use crate::error::{Result, ShoveError};
use crate::topology::QueueTopology;

const X_DEAD_LETTER_EXCHANGE: &str = "x-dead-letter-exchange";
const X_DEAD_LETTER_ROUTING_KEY: &str = "x-dead-letter-routing-key";
const X_MESSAGE_TTL: &str = "x-message-ttl";
const X_SINGLE_ACTIVE_CONSUMER: &str = "x-single-active-consumer";

fn with_dlq_routing(args: &mut FieldTable, dlq: &str) {
    args.insert(
        X_DEAD_LETTER_EXCHANGE.into(),
        AMQPValue::LongString("".into()),
    );
    args.insert(
        X_DEAD_LETTER_ROUTING_KEY.into(),
        AMQPValue::LongString(dlq.into()),
    );
}

fn hold_queue_args(route_back_to: &str, ttl_ms: i64) -> FieldTable {
    let mut args = FieldTable::default();
    args.insert(X_MESSAGE_TTL.into(), AMQPValue::LongLongInt(ttl_ms));
    with_dlq_routing(&mut args, route_back_to);
    args
}

/// The fanout exchange a `.broadcast()` topology publishes to, and that every
/// instance binds its own ephemeral queue to.
///
/// Derived from the topic name so a publisher and its subscribers agree without
/// configuration, and deliberately *not* equal to the queue name: a broadcast
/// topology declares no queue at all, and the whole deployment caveat below
/// turns on the two names being distinguishable.
pub(crate) fn broadcast_exchange(queue: &str) -> String {
    format!("{queue}-fanout")
}

/// Declare the fanout exchange for a broadcast topology.
///
/// Idempotent, and called from two places on purpose: the topology declarer, so
/// a publisher-only process has somewhere to publish, and each subscriber's own
/// channel, so a subscriber-only process can bind without depending on whether
/// anyone declared the topology first.
///
/// Durable and **not** auto-delete. A publisher must be able to publish across
/// a window where no instance is subscribed; a fanout with no bindings discards
/// what it receives, which is precisely the deliver-new contract rather than a
/// failure.
pub(crate) async fn declare_broadcast_exchange(channel: &Channel, exchange: &str) -> Result<()> {
    channel
        .exchange_declare(
            exchange.into(),
            lapin::ExchangeKind::Fanout,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| {
            ShoveError::Topology(format!(
                "failed to declare broadcast exchange '{exchange}': {e}"
            ))
        })
}

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
        if let Some(dlq) = topology.dlq() {
            self.declare_queue(dlq, FieldTable::default()).await?;
        }

        let mut main_args = FieldTable::default();
        if let Some(dlq) = topology.dlq() {
            with_dlq_routing(&mut main_args, dlq);
        }
        self.declare_queue(topology.queue(), main_args).await?;

        for hq in topology.hold_queues() {
            let args = hold_queue_args(topology.queue(), hq.delay().as_millis() as i64);
            self.declare_queue(hq.name(), args).await?;
        }

        Ok(())
    }

    async fn declare_sequenced(&self, topology: &QueueTopology) -> Result<()> {
        let seq = topology.sequencing().ok_or(ShoveError::Topology(
            "declare_sequenced called without sequencing config".into(),
        ))?;

        if let Some(dlq) = topology.dlq() {
            self.declare_queue(dlq, FieldTable::default()).await?;
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

            // Per-shard hold queues dead-letter back to this sub-queue
            for hq in topology.shard_hold_queue_names(i) {
                let args = hold_queue_args(&sub_queue, hq.delay().as_millis() as i64);
                self.declare_queue(hq.name(), args).await?;
            }

            let mut args = FieldTable::default();
            args.insert(X_SINGLE_ACTIVE_CONSUMER.into(), AMQPValue::Boolean(true));
            if let Some(dlq) = topology.dlq() {
                with_dlq_routing(&mut args, dlq);
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

impl RabbitMqTopologyDeclarer {
    pub async fn declare(&self, topology: &QueueTopology) -> Result<()> {
        if topology.broadcast() {
            // The exchange, and nothing else. A broadcast topology has no
            // shared queue, no DLQ and no hold queues, and each subscriber
            // declares its own queue when its delivery loop starts — so a queue
            // declared here would be an addressable, permanently empty one that
            // nothing ever reads, which is the residue AC6 rules out.
            return declare_broadcast_exchange(
                &self.channel,
                &broadcast_exchange(topology.queue()),
            )
            .await;
        }
        if topology.sequencing().is_some() {
            self.declare_sequenced(topology).await
        } else {
            self.declare_unsequenced(topology).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broadcast_exchange_is_derived_from_the_topic_name() {
        assert_eq!(
            broadcast_exchange("cache-invalidations"),
            "cache-invalidations-fanout"
        );
    }

    // The deployment caveat in `docs/pages/concepts/broadcast.mdx` is only
    // meaningful if the two routes are distinguishable: an older publisher
    // sends to the default exchange keyed by the queue name, a new one sends
    // to this exchange. If they collided the caveat would be describing
    // nothing.
    #[test]
    fn broadcast_exchange_never_collides_with_the_queue_name() {
        let queue = "cache-invalidations";
        assert_ne!(broadcast_exchange(queue), queue);
    }
}
