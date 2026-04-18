//! Stress benchmarks for the in-memory backend.
//!
//!     cargo run -q --release --example inmemory_stress --features inmemory
//!     cargo run -q --release --example inmemory_stress --features inmemory -- --tier moderate
//!     cargo run -q --release --example inmemory_stress --features inmemory -- --handler fast
//!
//! No containers, no external deps — useful as a ceiling for framework
//! overhead under different handler profiles.

#[path = "../common/stress_test.rs"]
mod harness;

use shove::inmemory::*;
use shove::topology::TopologyDeclarer;
use shove::{Publisher, Topic};

use harness::{
    StressGroupHandle, StressTestBroker, StressTestHandler, StressTestMsg, StressTestTopic,
};

struct InMemoryStressTestBroker {
    broker: InMemoryBroker,
    publisher: InMemoryPublisher,
}

struct InMemoryGroupHandle {
    registry: InMemoryConsumerGroupRegistry,
}

impl StressTestBroker for InMemoryStressTestBroker {
    type GroupHandle = InMemoryGroupHandle;

    const NAME: &'static str = "inmemory";

    async fn setup() -> Self {
        let broker = InMemoryBroker::new();
        InMemoryTopologyDeclarer::new(broker.clone())
            .declare(StressTestTopic::topology())
            .await
            .unwrap();
        let publisher = InMemoryPublisher::new(broker.clone());
        Self { broker, publisher }
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
        _concurrent: bool,
        factory: F,
    ) -> Result<Self::GroupHandle, String>
    where
        F: Fn() -> StressTestHandler + Send + Sync + 'static,
    {
        let mut registry = InMemoryConsumerGroupRegistry::new(self.broker.clone());
        registry
            .register::<StressTestTopic, StressTestHandler>(
                InMemoryConsumerGroupConfig::new(consumers..=consumers)
                    .with_prefetch_count(prefetch),
                factory,
            )
            .await
            .map_err(|e| e.to_string())?;
        registry.start_all();
        Ok(InMemoryGroupHandle { registry })
    }

    async fn shutdown(self) {
        self.broker.shutdown();
    }
}

impl StressGroupHandle for InMemoryGroupHandle {
    async fn shutdown_all(mut self) {
        self.registry.shutdown_all().await;
    }
}

#[tokio::main]
async fn main() {
    harness::run_all_scenarios::<InMemoryStressTestBroker>().await;
}
