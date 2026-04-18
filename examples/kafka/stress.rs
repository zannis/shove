//! Stress benchmarks for the Kafka backend.
//!
//! Requires Docker. Run with:
//!
//!     cargo run -q --example kafka_stress --features kafka
//!     cargo run -q --example kafka_stress --features kafka -- --tier moderate
//!     cargo run -q --example kafka_stress --features kafka -- --handler fast

#[path = "../common/stress_test.rs"]
mod harness;

use shove::kafka::*;
use shove::topology::TopologyDeclarer;
use shove::{Publisher, Topic};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka};

use harness::{
    StressGroupHandle, StressTestBroker, StressTestHandler, StressTestMsg, StressTestTopic,
    require_docker,
};

struct KafkaStressTestBroker {
    client: KafkaClient,
    publisher: KafkaPublisher,
    _container: testcontainers::ContainerAsync<Kafka>,
}

struct KafkaGroupHandle {
    registry: KafkaConsumerGroupRegistry,
}

impl StressTestBroker for KafkaStressTestBroker {
    type GroupHandle = KafkaGroupHandle;

    const NAME: &'static str = "kafka";

    async fn setup() -> Self {
        require_docker();

        eprintln!("starting Kafka container...");
        let container = Kafka::default().start().await.unwrap();
        let port = container
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .unwrap();
        let bootstrap_servers = format!("127.0.0.1:{port}");
        let client = KafkaClient::connect_with_retry(&KafkaConfig::new(&bootstrap_servers), 10)
            .await
            .unwrap();

        KafkaTopologyDeclarer::new(client.clone())
            .declare(StressTestTopic::topology())
            .await
            .unwrap();

        let publisher = KafkaPublisher::new(client.clone()).await.unwrap();

        eprintln!("Kafka ready at {bootstrap_servers}");

        Self {
            client,
            publisher,
            _container: container,
        }
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
        let mut registry = KafkaConsumerGroupRegistry::new(self.client.clone());
        registry
            .register::<StressTestTopic, StressTestHandler>(
                KafkaConsumerGroupConfig::new(consumers..=consumers)
                    .with_prefetch_count(prefetch)
                    .with_concurrent_processing(concurrent),
                factory,
            )
            .await
            .map_err(|e| e.to_string())?;
        registry.start_all();
        Ok(KafkaGroupHandle { registry })
    }

    async fn shutdown(self) {
        self.client.shutdown().await;
    }
}

impl StressGroupHandle for KafkaGroupHandle {
    async fn shutdown_all(mut self) {
        self.registry.shutdown_all().await;
    }
}

#[tokio::main]
async fn main() {
    harness::run_all_scenarios::<KafkaStressTestBroker>().await;
}
