//! Backend marker types. One zero-sized struct per backend, each under
//! the existing Cargo feature. See DESIGN_V2.md §7.

#[cfg(feature = "kafka")]
pub struct Kafka;

#[cfg(feature = "nats")]
pub struct Nats;

#[cfg(feature = "rabbitmq")]
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
#[cfg(feature = "aws-sns-sqs")]
pub struct Sqs;

#[cfg(feature = "inmemory")]
pub struct InMemory;
