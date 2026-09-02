//! Backend marker types. One zero-sized struct per backend, each under
//! the existing Cargo feature.

#[cfg(feature = "kafka")]
#[cfg_attr(docsrs, doc(cfg(feature = "kafka")))]
pub struct Kafka;

#[cfg(feature = "nats")]
#[cfg_attr(docsrs, doc(cfg(feature = "nats")))]
pub struct Nats;

#[cfg(feature = "rabbitmq")]
#[cfg_attr(docsrs, doc(cfg(feature = "rabbitmq")))]
pub struct RabbitMq;

/// AWS SQS backend marker.
///
/// SQS has no broker-level coordinated-group primitive — N consumers
/// polling one queue is independent polling, covered by
/// `Broker<Sqs>::consumer_supervisor()`. The `compile_fail` doctest below
/// pins that property: if someone ever adds
/// `impl HasCoordinatedGroups for Sqs`, this doctest starts compiling and
/// fails the build.
///
/// ```compile_fail
/// # #[cfg(feature = "aws-sns-sqs")]
/// # async fn _x() -> shove::error::Result<()> {
/// use shove::{Broker, Sqs};
/// use shove::sns::SnsConfig;
///
/// let broker = Broker::<Sqs>::new(SnsConfig {
///     region: None,
///     endpoint_url: None,
/// }).await?;
/// // error: no method named `consumer_group` for `Broker<Sqs>`
/// let _ = broker.consumer_group();
/// # Ok(())
/// # }
/// ```
///
/// SQS likewise has no ephemeral per-instance subscription: per-pod fan-out
/// would mean creating and deleting a real queue plus an SNS subscription per
/// process, and a leaked queue costs money for as long as nobody notices. So
/// `Sqs` does not implement
/// [`HasBroadcast`](crate::backend::capability::HasBroadcast) either, and
/// `broadcast_subscriber()` is a compile error rather than a lossy
/// approximation:
///
/// ```compile_fail
/// # #[cfg(feature = "aws-sns-sqs")]
/// # async fn _x() -> shove::error::Result<()> {
/// use shove::{Broker, Sqs};
/// use shove::sns::SnsConfig;
///
/// let broker = Broker::<Sqs>::new(SnsConfig {
///     region: None,
///     endpoint_url: None,
/// }).await?;
/// // error: no method named `broadcast_subscriber` for `Broker<Sqs>`
/// let _ = broker.broadcast_subscriber();
/// # Ok(())
/// # }
/// ```
///
/// SQS has no batch-consumption implementation either — see
/// [`HasBatchConsumption`](crate::backend::capability::HasBatchConsumption)
/// for the authoritative list, where unlike `HasBroadcast` every other row is
/// *pending* rather than permanently excluded. `batch_consumer()` is
/// nevertheless a compile error on `Broker<Sqs>` today, same as the other two:
///
/// ```compile_fail
/// # #[cfg(feature = "aws-sns-sqs")]
/// # async fn _x() -> shove::error::Result<()> {
/// use shove::{Broker, Sqs};
/// use shove::sns::SnsConfig;
///
/// let broker = Broker::<Sqs>::new(SnsConfig {
///     region: None,
///     endpoint_url: None,
/// }).await?;
/// // error: no method named `batch_consumer` for `Broker<Sqs>`
/// let _ = broker.batch_consumer();
/// # Ok(())
/// # }
/// ```
///
/// The control for the tests above. A `compile_fail` doctest passes on *any*
/// compile error (see the note on [`NotSequenced`](crate::topic::NotSequenced)),
/// so on its own each would keep passing if the imports rotted or `SnsConfig`
/// changed shape — it would have become a typo test asserting nothing about
/// capabilities. This twin is byte-identical except for the final method, which
/// `Broker<Sqs>` *does* have. It compiling is what pins each failure above to
/// its missing capability trait and nothing else.
///
/// ```rust,no_run
/// # #[cfg(feature = "aws-sns-sqs")]
/// # async fn _x() -> shove::error::Result<()> {
/// use shove::{Broker, Sqs};
/// use shove::sns::SnsConfig;
///
/// let broker = Broker::<Sqs>::new(SnsConfig {
///     region: None,
///     endpoint_url: None,
/// }).await?;
/// // The supported path for SQS: N independent pollers, not a fan-out.
/// let _ = broker.consumer_supervisor();
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "aws-sns-sqs")]
#[cfg_attr(docsrs, doc(cfg(feature = "aws-sns-sqs")))]
pub struct Sqs;

#[cfg(feature = "inmemory")]
#[cfg_attr(docsrs, doc(cfg(feature = "inmemory")))]
pub struct InMemory;

#[cfg(feature = "redis-streams")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis-streams")))]
pub struct Redis;
