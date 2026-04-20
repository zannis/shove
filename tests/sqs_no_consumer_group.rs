#![cfg(feature = "aws-sns-sqs")]
//! Compile-time test: `Broker<Sqs>` must not expose `consumer_group()`.
//!
//! SQS has no broker-level group primitive — N consumers polling one queue
//! is independent polling, covered by `ConsumerSupervisor<Sqs>`. The
//! `compile_fail` doctest below pins this property; it passes by *failing
//! to compile*. If someone adds `impl HasCoordinatedGroups for Sqs` the
//! doctest starts compiling and this test fails.

/// ```compile_fail
/// use shove::{Broker, Sqs};
/// use shove::sns::SnsConfig;
/// # async fn _x() -> shove::error::Result<()> {
/// let broker = Broker::<Sqs>::new(SnsConfig {
///     region: None,
///     endpoint_url: None,
/// }).await?;
/// // error: no method `consumer_group` for `Broker<Sqs>`
/// let _ = broker.consumer_group();
/// # Ok(())
/// # }
/// ```
fn _compile_fail_doctest() {}
