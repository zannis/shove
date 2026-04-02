use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::backends::sns::client::SnsClient;
use crate::error::{Result, ShoveError};
use crate::topology::{QueueTopology, TopologyDeclarer};

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
    registry: Arc<TopicRegistry>,
}

impl SnsTopologyDeclarer {
    pub fn new(client: SnsClient, registry: Arc<TopicRegistry>) -> Self {
        Self { client, registry }
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
        self.registry
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
        self.registry
            .insert(topology.queue().to_string(), arn)
            .await;

        Ok(())
    }
}

impl TopologyDeclarer for SnsTopologyDeclarer {
    async fn declare(&self, topology: &QueueTopology) -> Result<()> {
        // If the registry already has an ARN for this queue (pre-configured),
        // validate it exists and skip creation.
        if let Some(arn) = self.registry.get(topology.queue()).await {
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

            return Ok(());
        }

        if topology.sequencing().is_some() {
            self.declare_fifo(topology).await
        } else {
            self.declare_standard(topology).await
        }
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
}
