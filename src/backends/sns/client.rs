use std::fmt::{Debug, Formatter};

use tokio_util::sync::CancellationToken;

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
#[derive(Clone)]
pub struct SnsClient {
    sns_client: aws_sdk_sns::Client,
    #[cfg(feature = "aws-sns-sqs")]
    sqs_client: aws_sdk_sqs::Client,
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
            shutdown_token: CancellationToken::new(),
        })
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
