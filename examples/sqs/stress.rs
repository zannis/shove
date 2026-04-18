//! Stress benchmarks for the SNS/SQS backend via LocalStack.
//!
//! Requires Docker and `LOCALSTACK_AUTH_TOKEN` env var. Run with:
//!
//!     cargo run -q --example sqs_stress --features aws-sns-sqs
//!     cargo run -q --example sqs_stress --features aws-sns-sqs -- --tier moderate
//!     cargo run -q --example sqs_stress --features aws-sns-sqs -- --handler fast

#[path = "../common/stress_test.rs"]
mod harness;

use std::sync::Arc;
use std::time::Duration;

use shove::sns::*;
use shove::{Publisher, Topic, declare_topic};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStack;

use harness::{
    StressGroupHandle, StressTestBroker, StressTestHandler, StressTestMsg, StressTestTopic,
    require_docker,
};

struct SqsStressTestBroker {
    client: SnsClient,
    publisher: SnsPublisher,
    topic_registry: Arc<TopicRegistry>,
    queue_registry: Arc<QueueRegistry>,
    sqs_client: aws_sdk_sqs::Client,
    _container: testcontainers::ContainerAsync<LocalStack>,
}

struct SqsGroupHandle {
    registry: SqsConsumerGroupRegistry,
}

impl StressTestBroker for SqsStressTestBroker {
    type GroupHandle = SqsGroupHandle;

    const NAME: &'static str = "sqs";
    const PREFETCH_CAP: u16 = 10;

    async fn setup() -> Self {
        require_docker();

        let auth_token = std::env::var("LOCALSTACK_AUTH_TOKEN").unwrap_or_else(|_| {
            eprintln!(
                "LOCALSTACK_AUTH_TOKEN is not set. \
                 Set it with: export LOCALSTACK_AUTH_TOKEN=<token>"
            );
            std::process::exit(1);
        });

        // SAFETY: called before any concurrent env access in this process.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
            std::env::set_var("AWS_REGION", "us-east-1");
        }

        eprintln!("starting LocalStack container...");
        let container = LocalStack::default()
            .with_env_var("LOCALSTACK_AUTH_TOKEN", auth_token)
            .start()
            .await
            .expect("failed to start LocalStack container");

        let port = container
            .get_host_port_ipv4(4566)
            .await
            .expect("failed to get LocalStack port");
        let endpoint_url = format!("http://localhost:{port}");

        tokio::time::sleep(Duration::from_secs(2)).await;

        let sns_config = SnsConfig {
            region: Some("us-east-1".into()),
            endpoint_url: Some(endpoint_url.clone()),
        };
        let client = SnsClient::new(&sns_config)
            .await
            .expect("failed to create SnsClient");

        let topic_registry = Arc::new(TopicRegistry::new());
        let queue_registry = Arc::new(QueueRegistry::new());

        let declarer = SnsTopologyDeclarer::new(client.clone(), topic_registry.clone())
            .with_queue_registry(queue_registry.clone());
        declare_topic::<StressTestTopic>(&declarer)
            .await
            .expect("failed to declare StressTestTopic topology");

        let publisher = SnsPublisher::new(client.clone(), topic_registry.clone());

        let aws_cfg = aws_config::from_env()
            .region(aws_config::Region::new("us-east-1"))
            .endpoint_url(&endpoint_url)
            .load()
            .await;
        let sqs_client = aws_sdk_sqs::Client::new(&aws_cfg);

        eprintln!("LocalStack ready at {endpoint_url}");

        Self {
            client,
            publisher,
            topic_registry,
            queue_registry,
            sqs_client,
            _container: container,
        }
    }

    async fn purge(&self) {
        let queue_url = self
            .queue_registry
            .get(StressTestTopic::topology().queue())
            .await;
        if let Some(url) = queue_url {
            let _ = self.sqs_client.purge_queue().queue_url(url).send().await;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn publish_batch(&self, messages: &[StressTestMsg]) {
        for chunk in messages.chunks(500) {
            self.publisher
                .publish_batch::<StressTestTopic>(chunk)
                .await
                .expect("publish_batch failed");
        }
    }

    async fn run_consumer_group<F>(
        &self,
        consumers: u16,
        prefetch: u16,
        concurrent: bool,
        factory: F,
    ) -> Result<Self::GroupHandle, String>
    where
        F: Fn() -> StressTestHandler + Send + Sync + 'static,
    {
        let mut registry = SqsConsumerGroupRegistry::new(
            self.client.clone(),
            self.topic_registry.clone(),
            self.queue_registry.clone(),
        );
        registry
            .register::<StressTestTopic, StressTestHandler>(
                SqsConsumerGroupConfig::new(consumers..=consumers)
                    .with_prefetch_count(prefetch)
                    .with_concurrent_processing(concurrent),
                factory,
            )
            .await
            .map_err(|e| e.to_string())?;
        registry.start_all();
        Ok(SqsGroupHandle { registry })
    }

    async fn shutdown(self) {
        self.client.shutdown().await;
    }
}

impl StressGroupHandle for SqsGroupHandle {
    async fn shutdown_all(mut self) {
        self.registry.shutdown_all().await;
    }
}

#[tokio::main]
async fn main() {
    harness::run_all_scenarios::<SqsStressTestBroker>().await;
}
