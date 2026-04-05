use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::backends::sns::client::SnsClient;
use crate::error::{Result, ShoveError};
use crate::topology::{QueueTopology, TopologyDeclarer};

/// Default SQS maxReceiveCount for redrive policies.
/// Messages that exceed this receive count are moved to the DLQ.
#[cfg(feature = "aws-sns-sqs")]
const DEFAULT_MAX_RECEIVE_COUNT: u32 = 10;

/// Registry mapping queue names to SNS topic ARNs.
///
/// Populated by the topology declarer or pre-configured ARNs.
/// Shared between the declarer and publisher via `Arc`.
pub struct TopicRegistry {
    arns: RwLock<HashMap<String, String>>,
}

impl Default for TopicRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicRegistry {
    pub fn new() -> Self {
        Self {
            arns: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_overrides(overrides: HashMap<String, String>) -> Self {
        Self {
            arns: RwLock::new(overrides),
        }
    }

    pub async fn get(&self, queue_name: &str) -> Option<String> {
        self.arns.read().await.get(queue_name).cloned()
    }

    pub async fn insert(&self, queue_name: String, arn: String) {
        self.arns.write().await.insert(queue_name, arn);
    }
}

/// Registry mapping queue names to SQS queue URLs.
#[cfg(feature = "aws-sns-sqs")]
pub struct QueueRegistry {
    urls: RwLock<HashMap<String, String>>,
}

#[cfg(feature = "aws-sns-sqs")]
impl Default for QueueRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "aws-sns-sqs")]
impl QueueRegistry {
    pub fn new() -> Self {
        Self {
            urls: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get(&self, queue_name: &str) -> Option<String> {
        self.urls.read().await.get(queue_name).cloned()
    }

    pub async fn insert(&self, queue_name: String, url: String) {
        self.urls.write().await.insert(queue_name, url);
    }
}

/// Returns the SNS topic name for a given queue topology.
///
/// Sequenced topics get a `.fifo` suffix to create FIFO SNS topics.
fn sns_topic_name(topology: &QueueTopology) -> String {
    if topology.sequencing().is_some() {
        format!("{}.fifo", topology.queue())
    } else {
        topology.queue().to_string()
    }
}

/// Declares SNS topics for a topic's topology.
///
/// Creates standard SNS topics for unsequenced topics and FIFO SNS topics
/// (with content-based deduplication) for sequenced topics. All create
/// operations are idempotent — safe to call on every startup.
pub struct SnsTopologyDeclarer {
    client: SnsClient,
    topic_registry: Arc<TopicRegistry>,
    #[cfg(feature = "aws-sns-sqs")]
    queue_registry: Option<Arc<QueueRegistry>>,
}

impl SnsTopologyDeclarer {
    pub fn new(client: SnsClient, registry: Arc<TopicRegistry>) -> Self {
        Self {
            client,
            topic_registry: registry,
            #[cfg(feature = "aws-sns-sqs")]
            queue_registry: None,
        }
    }

    /// Enable SQS queue creation alongside SNS topics.
    #[cfg(feature = "aws-sns-sqs")]
    pub fn with_queue_registry(mut self, registry: Arc<QueueRegistry>) -> Self {
        self.queue_registry = Some(registry);
        self
    }

    async fn declare_standard(&self, topology: &QueueTopology) -> Result<()> {
        let topic_name = sns_topic_name(topology);

        debug!(topic_name, "declaring standard SNS topic");

        let result = self
            .client
            .inner()
            .create_topic()
            .name(&topic_name)
            .send()
            .await
            .map_err(|e| {
                ShoveError::Topology(format!("failed to create SNS topic '{topic_name}': {e}"))
            })?;

        let arn = result
            .topic_arn()
            .ok_or_else(|| {
                ShoveError::Topology(format!(
                    "SNS topic '{topic_name}' created but no ARN returned"
                ))
            })?
            .to_string();

        info!(topic_name, arn, "standard SNS topic declared");
        self.topic_registry
            .insert(topology.queue().to_string(), arn)
            .await;

        Ok(())
    }

    async fn declare_fifo(&self, topology: &QueueTopology) -> Result<()> {
        let topic_name = sns_topic_name(topology);

        debug!(topic_name, "declaring FIFO SNS topic");

        let result = self
            .client
            .inner()
            .create_topic()
            .name(&topic_name)
            .attributes("FifoTopic", "true")
            .attributes("ContentBasedDeduplication", "true")
            .send()
            .await
            .map_err(|e| {
                ShoveError::Topology(format!(
                    "failed to create FIFO SNS topic '{topic_name}': {e}"
                ))
            })?;

        let arn = result
            .topic_arn()
            .ok_or_else(|| {
                ShoveError::Topology(format!(
                    "FIFO SNS topic '{topic_name}' created but no ARN returned"
                ))
            })?
            .to_string();

        info!(topic_name, arn, "FIFO SNS topic declared");
        self.topic_registry
            .insert(topology.queue().to_string(), arn)
            .await;

        Ok(())
    }

    /// Create an SQS queue and return its (url, arn).
    #[cfg(feature = "aws-sns-sqs")]
    async fn create_sqs_queue(
        &self,
        name: &str,
        fifo: bool,
        dlq_arn: Option<&str>,
        max_receive_count: u32,
    ) -> Result<(String, String)> {
        let mut req = self.client.sqs().create_queue().queue_name(name);

        if fifo {
            req = req
                .attributes(aws_sdk_sqs::types::QueueAttributeName::FifoQueue, "true")
                .attributes(
                    aws_sdk_sqs::types::QueueAttributeName::ContentBasedDeduplication,
                    "true",
                );
        }

        if let Some(arn) = dlq_arn {
            let redrive = serde_json::json!({
                "deadLetterTargetArn": arn,
                "maxReceiveCount": max_receive_count,
            })
            .to_string();
            req = req.attributes(
                aws_sdk_sqs::types::QueueAttributeName::RedrivePolicy,
                redrive,
            );
        }

        let result = req.send().await.map_err(|e| {
            ShoveError::Topology(format!("failed to create SQS queue '{name}': {e}"))
        })?;

        let url = result
            .queue_url()
            .ok_or_else(|| {
                ShoveError::Topology(format!("SQS queue '{name}' created but no URL returned"))
            })?
            .to_string();

        // Fetch the ARN
        let attrs = self
            .client
            .sqs()
            .get_queue_attributes()
            .queue_url(&url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
            .send()
            .await
            .map_err(|e| {
                ShoveError::Topology(format!(
                    "failed to get attributes for SQS queue '{name}': {e}"
                ))
            })?;

        let arn = attrs
            .attributes()
            .and_then(|m| m.get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn))
            .ok_or_else(|| {
                ShoveError::Topology(format!("SQS queue '{name}' has no ARN attribute"))
            })?
            .clone();

        info!(name, url, arn, "SQS queue declared");
        Ok((url, arn))
    }

    /// Set the SQS queue policy to allow SNS delivery and subscribe it to the topic.
    #[cfg(feature = "aws-sns-sqs")]
    async fn subscribe_sqs_to_sns(
        &self,
        topic_arn: &str,
        queue_arn: &str,
        queue_url: &str,
        filter_policy: Option<String>,
    ) -> Result<()> {
        // Allow SNS to send messages to the SQS queue
        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": { "Service": "sns.amazonaws.com" },
                "Action": "sqs:SendMessage",
                "Resource": queue_arn,
                "Condition": {
                    "ArnEquals": { "aws:SourceArn": topic_arn }
                }
            }]
        })
        .to_string();

        self.client
            .sqs()
            .set_queue_attributes()
            .queue_url(queue_url)
            .attributes(aws_sdk_sqs::types::QueueAttributeName::Policy, policy)
            .send()
            .await
            .map_err(|e| {
                ShoveError::Topology(format!(
                    "failed to set SQS queue policy for '{queue_url}': {e}"
                ))
            })?;

        // Subscribe SQS to SNS
        let mut sub_req = self
            .client
            .inner()
            .subscribe()
            .topic_arn(topic_arn)
            .protocol("sqs")
            .endpoint(queue_arn)
            .attributes("RawMessageDelivery", "true");

        if let Some(fp) = filter_policy {
            sub_req = sub_req.attributes("FilterPolicy", fp);
        }

        sub_req.send().await.map_err(|e| {
            ShoveError::Topology(format!(
                "failed to subscribe SQS queue '{queue_arn}' to SNS topic '{topic_arn}': {e}"
            ))
        })?;

        Ok(())
    }

    /// Declare a standard (unsequenced) SQS queue and subscribe it to the SNS topic.
    #[cfg(feature = "aws-sns-sqs")]
    async fn declare_sqs_unsequenced(
        &self,
        topology: &QueueTopology,
        topic_arn: &str,
    ) -> Result<()> {
        let queue_name = topology.queue();

        // Create DLQ first if requested
        let dlq_arn = if let Some(dlq_name) = topology.dlq() {
            let (dlq_url, arn) = self.create_sqs_queue(dlq_name, false, None, 0).await?;
            // Register the DLQ URL
            if let Some(ref reg) = self.queue_registry {
                reg.insert(dlq_name.to_string(), dlq_url).await;
            }
            Some(arn)
        } else {
            None
        };

        // Create the main queue with optional redrive to DLQ
        let (url, arn) = self
            .create_sqs_queue(
                queue_name,
                false,
                dlq_arn.as_deref(),
                DEFAULT_MAX_RECEIVE_COUNT,
            )
            .await?;

        // Subscribe to SNS
        self.subscribe_sqs_to_sns(topic_arn, &arn, &url, None)
            .await?;

        // Register the main queue URL
        if let Some(ref reg) = self.queue_registry {
            reg.insert(queue_name.to_string(), url).await;
        }

        Ok(())
    }

    /// Declare FIFO shard queues and subscribe each to the SNS topic.
    #[cfg(feature = "aws-sns-sqs")]
    async fn declare_sqs_sequenced(&self, topology: &QueueTopology, topic_arn: &str) -> Result<()> {
        let queue_name = topology.queue();
        let shards = topology
            .sequencing()
            .map(|s| s.routing_shards())
            .unwrap_or(8);

        // Derive the DLQ registry key (without .fifo suffix for registry lookups)
        let dlq_registry_key = topology
            .dlq()
            .unwrap_or(&format!("{queue_name}-dlq"))
            .to_string();

        // Create FIFO DLQ (actual AWS name must have .fifo suffix)
        let dlq_aws_name = format!("{dlq_registry_key}.fifo");
        let (dlq_url, dlq_arn) = self.create_sqs_queue(&dlq_aws_name, true, None, 0).await?;

        // Register DLQ URL using the key without .fifo
        if let Some(ref reg) = self.queue_registry {
            reg.insert(dlq_registry_key, dlq_url).await;
        }

        // Create N FIFO shard queues
        for i in 0..shards {
            let shard_registry_key = format!("{queue_name}-seq-{i}");
            let shard_aws_name = format!("{shard_registry_key}.fifo");
            let (url, arn) = self
                .create_sqs_queue(
                    &shard_aws_name,
                    true,
                    Some(&dlq_arn),
                    DEFAULT_MAX_RECEIVE_COUNT,
                )
                .await?;

            // Filter policy: only receive messages for this shard
            let filter = serde_json::json!({ "shard": [i.to_string()] }).to_string();

            self.subscribe_sqs_to_sns(topic_arn, &arn, &url, Some(filter))
                .await?;

            if let Some(ref reg) = self.queue_registry {
                reg.insert(shard_registry_key, url).await;
            }
        }

        Ok(())
    }
}

impl TopologyDeclarer for SnsTopologyDeclarer {
    async fn declare(&self, topology: &QueueTopology) -> Result<()> {
        // If the registry already has an ARN for this queue (pre-configured),
        // validate it exists and skip creation.
        if let Some(arn) = self.topic_registry.get(topology.queue()).await {
            debug!(
                queue = topology.queue(),
                arn, "using pre-configured SNS topic ARN"
            );

            self.client
                .inner()
                .get_topic_attributes()
                .topic_arn(&arn)
                .send()
                .await
                .map_err(|e| {
                    ShoveError::Topology(format!(
                        "pre-configured SNS topic ARN '{arn}' is invalid: {e}"
                    ))
                })?;

            // Still declare SQS if queue_registry is set and queue not yet registered
            #[cfg(feature = "aws-sns-sqs")]
            if let Some(ref qr) = self.queue_registry
                && qr.get(topology.queue()).await.is_none()
            {
                if topology.sequencing().is_some() {
                    self.declare_sqs_sequenced(topology, &arn).await?;
                } else {
                    self.declare_sqs_unsequenced(topology, &arn).await?;
                }
            }

            return Ok(());
        }

        if topology.sequencing().is_some() {
            self.declare_fifo(topology).await?;
        } else {
            self.declare_standard(topology).await?;
        }

        // Declare SQS queues if a queue_registry is configured and queue not yet registered
        #[cfg(feature = "aws-sns-sqs")]
        if let Some(ref qr) = self.queue_registry
            && qr.get(topology.queue()).await.is_none()
        {
            let topic_arn = self
                .topic_registry
                .get(topology.queue())
                .await
                .expect("topic ARN must be in registry after declare");

            if topology.sequencing().is_some() {
                self.declare_sqs_sequenced(topology, &topic_arn).await?;
            } else {
                self.declare_sqs_unsequenced(topology, &topic_arn).await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::TopologyBuilder;
    use std::time::Duration;

    #[test]
    fn sns_topic_name_standard() {
        let topology = TopologyBuilder::new("order-settlement").build();
        assert_eq!(sns_topic_name(&topology), "order-settlement");
    }

    #[test]
    fn sns_topic_name_fifo() {
        let topology = TopologyBuilder::new("order-settlement")
            .sequenced(crate::topology::SequenceFailure::Skip)
            .hold_queue(Duration::from_secs(5))
            .dlq()
            .build();
        assert_eq!(sns_topic_name(&topology), "order-settlement.fifo");
    }

    #[tokio::test]
    async fn registry_insert_and_get() {
        let registry = TopicRegistry::new();
        registry
            .insert("orders".into(), "arn:aws:sns:us-east-1:123:orders".into())
            .await;
        let arn = registry.get("orders").await;
        assert_eq!(arn, Some("arn:aws:sns:us-east-1:123:orders".to_string()));
    }

    #[tokio::test]
    async fn registry_get_missing() {
        let registry = TopicRegistry::new();
        assert_eq!(registry.get("nonexistent").await, None);
    }

    #[tokio::test]
    async fn registry_with_overrides() {
        let mut overrides = HashMap::new();
        overrides.insert("orders".into(), "arn:aws:sns:us-east-1:123:orders".into());
        let registry = TopicRegistry::with_overrides(overrides);
        assert_eq!(
            registry.get("orders").await,
            Some("arn:aws:sns:us-east-1:123:orders".to_string())
        );
    }

    #[tokio::test]
    async fn registry_insert_overwrites() {
        let registry = TopicRegistry::new();
        registry.insert("orders".into(), "arn:old".into()).await;
        registry.insert("orders".into(), "arn:new".into()).await;
        assert_eq!(registry.get("orders").await, Some("arn:new".to_string()));
    }

    #[cfg(feature = "aws-sns-sqs")]
    #[tokio::test]
    async fn queue_registry_insert_and_get() {
        let registry = QueueRegistry::new();
        registry
            .insert(
                "orders".into(),
                "https://sqs.us-east-1.amazonaws.com/123/orders".into(),
            )
            .await;
        let url = registry.get("orders").await;
        assert_eq!(
            url,
            Some("https://sqs.us-east-1.amazonaws.com/123/orders".to_string())
        );
    }

    #[cfg(feature = "aws-sns-sqs")]
    #[tokio::test]
    async fn queue_registry_get_missing() {
        let registry = QueueRegistry::new();
        assert_eq!(registry.get("nonexistent").await, None);
    }
}
