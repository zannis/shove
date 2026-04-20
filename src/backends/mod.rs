#[cfg(feature = "rabbitmq")]
#[cfg_attr(docsrs, doc(cfg(feature = "rabbitmq")))]
pub mod rabbitmq;

#[cfg(feature = "pub-aws-sns")]
#[cfg_attr(docsrs, doc(cfg(feature = "pub-aws-sns")))]
pub mod sns;

#[cfg(feature = "nats")]
#[cfg_attr(docsrs, doc(cfg(feature = "nats")))]
pub mod nats;

#[cfg(feature = "kafka")]
#[cfg_attr(docsrs, doc(cfg(feature = "kafka")))]
pub mod kafka;

#[cfg(feature = "inmemory")]
#[cfg_attr(docsrs, doc(cfg(feature = "inmemory")))]
pub mod inmemory;
