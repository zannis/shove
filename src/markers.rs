//! Backend marker types. One zero-sized struct per backend, each under
//! the existing Cargo feature. See DESIGN_V2.md §7.

#[cfg(feature = "kafka")]
pub struct Kafka;

#[cfg(feature = "nats")]
pub struct Nats;

#[cfg(feature = "rabbitmq")]
pub struct RabbitMq;

#[cfg(feature = "aws-sns-sqs")]
pub struct Sqs;

#[cfg(feature = "inmemory")]
pub struct InMemory;
