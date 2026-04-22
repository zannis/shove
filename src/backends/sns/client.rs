use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::backends::sns::topology::TopicRegistry;
use crate::error::Result;

/// AWS SNS connection configuration.
#[derive(Clone)]
pub struct SnsConfig {
    /// AWS region (e.g. "us-east-1"). None uses the SDK default chain.
    pub region: Option<String>,
    /// Custom endpoint URL (e.g. "http://localhost:4566" for LocalStack).
    pub endpoint_url: Option<String>,
}

impl Debug for SnsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnsConfig")
            .field("region", &self.region)
            .field("endpoint_url", &self.endpoint_url)
            .finish()
    }
}

/// AWS SNS client with shutdown coordination.
///
/// Wraps the AWS SDK `sns::Client` (and optionally `sqs::Client` when the
/// `aws-sns-sqs` feature is enabled). Both clients are `Clone` and internally
/// manage HTTP/2 connection pooling.
///
/// Also owns the [`TopicRegistry`] (and [`QueueRegistry`] when `aws-sns-sqs`
/// is enabled) so that `Backend::Client` is self-contained — callers no
/// longer need to construct or pass registries separately.
#[derive(Clone)]
pub struct SnsClient {
    sns_client: aws_sdk_sns::Client,
    #[cfg(feature = "aws-sns-sqs")]
    sqs_client: aws_sdk_sqs::Client,
    topic_registry: Arc<TopicRegistry>,
    #[cfg(feature = "aws-sns-sqs")]
    queue_registry: Arc<crate::backends::sns::topology::QueueRegistry>,
    shutdown_token: CancellationToken,
}

impl SnsClient {
    /// Create a new SNS client from configuration.
    ///
    /// Loads AWS credentials from the default provider chain (env vars,
    /// config files, IAM role, etc.) and applies optional region/endpoint
    /// overrides from the provided `SnsConfig`.
    pub async fn new(config: &SnsConfig) -> Result<Self> {
        let mut aws_config = aws_config::from_env();

        if let Some(region) = &config.region {
            aws_config = aws_config.region(aws_config::Region::new(region.clone()));
        }
        if let Some(endpoint) = &config.endpoint_url {
            aws_config = aws_config.endpoint_url(endpoint);
        }

        let aws_config = aws_config.load().await;

        let sns_client = aws_sdk_sns::Client::new(&aws_config);
        #[cfg(feature = "aws-sns-sqs")]
        let sqs_client = aws_sdk_sqs::Client::new(&aws_config);

        Ok(Self {
            sns_client,
            #[cfg(feature = "aws-sns-sqs")]
            sqs_client,
            topic_registry: Arc::new(TopicRegistry::new()),
            #[cfg(feature = "aws-sns-sqs")]
            queue_registry: Arc::new(crate::backends::sns::topology::QueueRegistry::new()),
            shutdown_token: CancellationToken::new(),
        })
    }

    /// Create a mock SNS client with default configuration.
    #[cfg(test)]
    pub(crate) fn mock() -> Self {
        let behavior_version = aws_config::BehaviorVersion::latest();
        let sns_conf = aws_sdk_sns::config::Config::builder()
            .behavior_version(behavior_version)
            .region(aws_config::Region::new("us-east-1"))
            .build();
        let sns_client = aws_sdk_sns::Client::from_conf(sns_conf);

        #[cfg(feature = "aws-sns-sqs")]
        let sqs_client = {
            let sqs_conf = aws_sdk_sqs::config::Config::builder()
                .behavior_version(behavior_version)
                .region(aws_config::Region::new("us-east-1"))
                .build();
            aws_sdk_sqs::Client::from_conf(sqs_conf)
        };

        Self {
            sns_client,
            #[cfg(feature = "aws-sns-sqs")]
            sqs_client,
            topic_registry: Arc::new(TopicRegistry::new()),
            #[cfg(feature = "aws-sns-sqs")]
            queue_registry: Arc::new(crate::backends::sns::topology::QueueRegistry::new()),
            shutdown_token: CancellationToken::new(),
        }
    }

    /// Return a reference to the underlying AWS SDK SNS client.
    pub(crate) fn inner(&self) -> &aws_sdk_sns::Client {
        &self.sns_client
    }

    /// Return a reference to the underlying AWS SDK SQS client.
    #[cfg(feature = "aws-sns-sqs")]
    pub(crate) fn sqs(&self) -> &aws_sdk_sqs::Client {
        &self.sqs_client
    }

    /// Client-owned SNS topic-ARN registry.
    ///
    /// Shared by every publisher, consumer, topology declarer, and consumer
    /// group registry built from this client so that topology declaration
    /// and publish lookups always agree — there is no way to supply an
    /// alternative registry and create a divergent view.
    pub fn topic_registry(&self) -> &Arc<TopicRegistry> {
        &self.topic_registry
    }

    /// Client-owned SQS queue-URL registry.
    #[cfg(feature = "aws-sns-sqs")]
    pub fn queue_registry(&self) -> &Arc<crate::backends::sns::topology::QueueRegistry> {
        &self.queue_registry
    }

    /// Return a clone of the shutdown [`CancellationToken`].
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

    /// Initiate a graceful shutdown.
    pub async fn shutdown(&self) {
        self.shutdown_token.cancel();
        tokio::time::sleep(crate::SHUTDOWN_GRACE).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_debug_shows_fields() {
        let config = SnsConfig {
            region: Some("us-east-1".into()),
            endpoint_url: Some("http://localhost:4566".into()),
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("us-east-1"));
        assert!(debug.contains("localhost:4566"));
    }

    #[test]
    fn config_debug_none_fields() {
        let config = SnsConfig {
            region: None,
            endpoint_url: None,
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("None"));
    }

    #[test]
    fn config_clone() {
        let config = SnsConfig {
            region: Some("eu-west-1".into()),
            endpoint_url: None,
        };
        let cloned = config.clone();
        assert_eq!(cloned.region, config.region);
        assert_eq!(cloned.endpoint_url, config.endpoint_url);
    }
}
