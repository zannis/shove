#![cfg(feature = "nats")]
//! Concurrent `publish_batch` callers sharing one client must all complete.
//!
//! async-nats caps a JetStream context at 5,000 in-flight publish acks and,
//! by default, blocks a publish until a permit frees up. A permit is only
//! released once its ack future is polled to completion or dropped, so a
//! batch publisher that submits every record before awaiting any ack holds
//! its permits hostage: eight callers of 1,000 records each need 8,000
//! permits, the 5,001st submission waits forever, and nothing ever polls the
//! acks that would release it. This is the shape of the benchmark harness's
//! drain fill, which deadlocked on every NATS cell.

use serde::{Deserialize, Serialize};
use shove::broker::Broker;
use shove::markers::Nats;
use shove::nats::{NatsClient, NatsConfig};
use shove::topology::TopologyBuilder;
use std::time::Duration;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsContainer, NatsServerCmd};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Payload {
    id: u64,
}

shove::define_topic!(
    InflightTopic,
    Payload,
    TopologyBuilder::new("nats-inflight").build()
);

const PRODUCERS: usize = 8;
const BATCH: usize = 1000;
const DEADLINE: Duration = Duration::from_secs(60);

#[tokio::test]
async fn concurrent_batches_beyond_the_ack_budget_all_complete() {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = NatsContainer::default()
        .with_cmd(&cmd)
        .start()
        .await
        .expect("failed to start NATS container");
    let host = container.get_host().await.expect("failed to get host");
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("failed to get NATS port");
    let client =
        NatsClient::connect_with_retry(&NatsConfig::new(format!("nats://{host}:{port}")), 10)
            .await
            .expect("failed to connect to NATS");
    let broker = Broker::<Nats>::from_client(client.clone());
    broker.topology().declare::<InflightTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();

    let mut tasks = tokio::task::JoinSet::new();
    for producer in 0..PRODUCERS {
        let publisher = publisher.clone();
        tasks.spawn(async move {
            let messages: Vec<Payload> = (0..BATCH)
                .map(|i| Payload {
                    id: (producer * BATCH + i) as u64,
                })
                .collect();
            publisher.publish_batch::<InflightTopic>(&messages).await
        });
    }

    let results = tokio::time::timeout(DEADLINE, async {
        let mut results = Vec::with_capacity(PRODUCERS);
        while let Some(joined) = tasks.join_next().await {
            results.push(joined.expect("publisher task panicked"));
        }
        results
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "{PRODUCERS} concurrent publish_batch callers of {BATCH} records did not all \
             return within {DEADLINE:?}: the in-flight ack budget deadlocked them"
        )
    });
    for result in results {
        result.expect("publish_batch failed");
    }

    let stream = client
        .jetstream()
        .get_stream("nats-inflight")
        .await
        .expect("stream exists");
    assert_eq!(
        stream.cached_info().state.messages,
        (PRODUCERS * BATCH) as u64,
        "every acked record is on the stream"
    );
}
