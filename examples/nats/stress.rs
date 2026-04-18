//! Stress benchmarks for the NATS JetStream backend.
//!
//! Requires Docker. Run with:
//!
//!     cargo run -q --example nats_stress --features nats
//!     cargo run -q --example nats_stress --features nats -- --tier moderate
//!     cargo run -q --example nats_stress --features nats -- --handler fast

#[path = "../common/stress_test.rs"]
mod harness;

use std::time::Duration;

use shove::nats::*;
use shove::topology::TopologyDeclarer;
use shove::{Publisher, Topic};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};

use harness::{
    StressGroupHandle, StressTestBroker, StressTestHandler, StressTestMsg, StressTestTopic,
    require_docker,
};

struct NatsStressTestBroker {
    client: NatsClient,
    publisher: NatsPublisher,
    _container: testcontainers::ContainerAsync<Nats>,
}

struct NatsGroupHandle {
    registry: NatsConsumerGroupRegistry,
}

impl StressTestBroker for NatsStressTestBroker {
    type GroupHandle = NatsGroupHandle;

    const NAME: &'static str = "nats";

    async fn setup() -> Self {
        require_docker();

        eprintln!("starting NATS container...");
        let cmd = NatsServerCmd::default().with_jetstream();
        let container = Nats::default().with_cmd(&cmd).start().await.unwrap();
        let host = container.get_host().await.unwrap().to_string();
        let port = container.get_host_port_ipv4(4222).await.unwrap();

        let url = format!("nats://{host}:{port}");
        let client = NatsClient::connect_with_retry(&NatsConfig::new(&url), 10)
            .await
            .unwrap();

        NatsTopologyDeclarer::new(client.clone())
            .declare(StressTestTopic::topology())
            .await
            .unwrap();

        let publisher = NatsPublisher::new(client.clone()).await.unwrap();

        eprintln!("NATS JetStream ready at {url}");

        Self {
            client,
            publisher,
            _container: container,
        }
    }

    async fn purge(&self) {
        let js = self.client.jetstream();
        let queue = StressTestTopic::topology().queue();

        if let Ok(stream) = js.get_stream(queue).await {
            let consumer_name = format!("{queue}-consumer");
            let _ = stream.delete_consumer(&consumer_name).await;
            let _ = stream.purge().await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
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
        let mut registry = NatsConsumerGroupRegistry::new(self.client.clone());
        registry
            .register::<StressTestTopic, StressTestHandler>(
                NatsConsumerGroupConfig::new(consumers..=consumers)
                    .with_prefetch_count(prefetch)
                    .with_concurrent_processing(concurrent),
                factory,
            )
            .await
            .map_err(|e| e.to_string())?;
        registry.start_all();
        Ok(NatsGroupHandle { registry })
    }

    async fn shutdown(self) {
        self.client.shutdown().await;
    }
}

impl StressGroupHandle for NatsGroupHandle {
    async fn shutdown_all(mut self) {
        self.registry.shutdown_all().await;
    }
}

#[tokio::main]
async fn main() {
    harness::run_all_scenarios::<NatsStressTestBroker>().await;
}
