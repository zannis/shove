//! Stress benchmarks for the RabbitMQ backend.
//!
//! Requires Docker. Run with:
//!
//!     cargo run -q --example rabbitmq_stress --features rabbitmq
//!     cargo run -q --example rabbitmq_stress --features rabbitmq -- --tier moderate
//!     cargo run -q --example rabbitmq_stress --features rabbitmq -- --handler fast

#[path = "../common/stress_test.rs"]
mod harness;

use std::time::Duration;

use shove::rabbitmq::*;
use shove::topology::TopologyDeclarer;
use shove::{Publisher, Topic};
use testcontainers::core::ExecCommand;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;

use harness::{
    StressGroupHandle, StressTestBroker, StressTestHandler, StressTestMsg, StressTestTopic,
    require_docker,
};

struct RabbitMqStressTestBroker {
    client: RabbitMqClient,
    publisher: RabbitMqPublisher,
    mgmt_config: ManagementConfig,
    _container: testcontainers::ContainerAsync<RabbitMq>,
}

struct RabbitMqGroupHandle {
    registry: ConsumerGroupRegistry,
}

impl StressTestBroker for RabbitMqStressTestBroker {
    type GroupHandle = RabbitMqGroupHandle;

    const NAME: &'static str = "rabbitmq";

    async fn setup() -> Self {
        require_docker();

        eprintln!("starting RabbitMQ container...");
        let container = RabbitMq::default().start().await.unwrap();
        let host = container.get_host().await.unwrap().to_string();
        let amqp_port = container.get_host_port_ipv4(5672).await.unwrap();
        let mgmt_port = container.get_host_port_ipv4(15672).await.unwrap();

        let mut result = container
            .exec(ExecCommand::new([
                "rabbitmq-plugins",
                "enable",
                "rabbitmq_consistent_hash_exchange",
            ]))
            .await
            .unwrap();
        let _ = result.stdout_to_vec().await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        let uri = format!("amqp://guest:guest@{host}:{amqp_port}");
        let client = RabbitMqClient::connect(&RabbitMqConfig::new(&uri))
            .await
            .unwrap();

        let channel = client.create_channel().await.unwrap();
        RabbitMqTopologyDeclarer::new(channel)
            .declare(StressTestTopic::topology())
            .await
            .unwrap();

        let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();
        let mgmt_config =
            ManagementConfig::new(format!("http://{host}:{mgmt_port}"), "guest", "guest");

        eprintln!("RabbitMQ ready at {uri}");

        Self {
            client,
            publisher,
            mgmt_config,
            _container: container,
        }
    }

    async fn purge(&self) {
        let http = reqwest::Client::new();
        let url = format!(
            "{}/api/queues/%2F/{}/contents",
            self.mgmt_config.base_url,
            StressTestTopic::topology().queue()
        );
        let _ = http
            .delete(&url)
            .basic_auth(&self.mgmt_config.username, Some(&self.mgmt_config.password))
            .send()
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    async fn publish_batch(&self, messages: &[StressTestMsg]) {
        for chunk in messages.chunks(1000) {
            self.publisher
                .publish_batch::<StressTestTopic>(chunk)
                .await
                .unwrap();
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
        let mut registry = ConsumerGroupRegistry::new(self.client.clone());
        registry
            .register::<StressTestTopic, StressTestHandler>(
                ConsumerGroupConfig::new(consumers..=consumers)
                    .with_prefetch_count(prefetch)
                    .with_concurrent_processing(concurrent),
                factory,
            )
            .await
            .map_err(|e| e.to_string())?;
        registry.start_all();
        Ok(RabbitMqGroupHandle { registry })
    }

    async fn shutdown(self) {
        self.client.shutdown().await;
    }
}

impl StressGroupHandle for RabbitMqGroupHandle {
    async fn shutdown_all(mut self) {
        self.registry.shutdown_all().await;
    }
}

#[tokio::main]
async fn main() {
    harness::run_all_scenarios::<RabbitMqStressTestBroker>().await;
}
