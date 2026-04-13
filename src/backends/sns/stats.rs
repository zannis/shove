use std::sync::Arc;

use tracing::debug;

use crate::backends::sns::client::SnsClient;
use crate::backends::sns::topology::QueueRegistry;
use crate::error::{Result, ShoveError};

/// Queue statistics from SQS GetQueueAttributes.
#[derive(Debug, Clone, Default)]
pub struct SqsQueueStats {
    pub messages_ready: u64,
    pub messages_not_visible: u64,
}

/// Abstraction over SQS queue attributes for fetching stats.
pub trait SqsQueueStatsProviderTrait: Send + Sync {
    fn get_queue_stats(
        &self,
        queue_name: &str,
    ) -> impl Future<Output = Result<SqsQueueStats>> + Send;
}

/// Fetches SQS queue statistics for autoscaling decisions.
pub struct SqsQueueStatsProvider {
    client: SnsClient,
    queue_registry: Arc<QueueRegistry>,
}

impl SqsQueueStatsProvider {
    pub fn new(client: SnsClient, queue_registry: Arc<QueueRegistry>) -> Self {
        Self {
            client,
            queue_registry,
        }
    }
}

impl SqsQueueStatsProviderTrait for SqsQueueStatsProvider {
    async fn get_queue_stats(&self, queue_name: &str) -> Result<SqsQueueStats> {
        let queue_url =
            self.queue_registry.get(queue_name).await.ok_or_else(|| {
                ShoveError::Topology(format!("no SQS queue URL for '{queue_name}'"))
            })?;

        let result = self
            .client
            .sqs()
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
            .attribute_names(
                aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessagesNotVisible,
            )
            .send()
            .await
            .map_err(|e| {
                ShoveError::Connection(format!(
                    "failed to get SQS queue attributes for '{queue_name}': {e}"
                ))
            })?;

        let ready = result
            .attributes()
            .and_then(|m| {
                m.get(&aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
            })
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let not_visible = result
            .attributes()
            .and_then(|m| {
                m.get(
                    &aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessagesNotVisible,
                )
            })
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        debug!(
            queue_name,
            messages_ready = ready,
            messages_not_visible = not_visible,
            "fetched SQS queue stats"
        );

        Ok(SqsQueueStats {
            messages_ready: ready,
            messages_not_visible: not_visible,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_stats_defaults() {
        let stats = SqsQueueStats::default();
        assert_eq!(stats.messages_ready, 0);
        assert_eq!(stats.messages_not_visible, 0);
    }
}
