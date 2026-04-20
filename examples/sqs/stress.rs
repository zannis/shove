//! Stress benchmarks for the SNS/SQS backend.
//!
//! Reads the endpoint from `LOCALSTACK_ENDPOINT` (e.g.
//! `http://localhost:4566`) if set; otherwise uses the default AWS endpoint.
//! Region defaults to `us-east-1` but respects `AWS_REGION`.
//!
//!     cargo run -q --example sqs_stress --features aws-sns-sqs
//!     LOCALSTACK_ENDPOINT=http://localhost:4566 cargo run -q --example sqs_stress --features aws-sns-sqs -- --tier moderate

#[path = "../common/stress_test.rs"]
mod harness;

use shove::sns::SnsConfig;
use shove::{Broker, ConsumerOptions, Sqs};

use harness::{HarnessConfig, run_supervisor_scenarios};

/// SQS caps `ReceiveMessage` batches at 10.
const SQS_PREFETCH_CAP: u16 = 10;

/// Outer publish chunk size — the SNS publisher internally re-chunks to the
/// 10-entry SNS batch limit. 500 matched the original harness's outer batch
/// size; smaller values reduce peak memory.
const SQS_PUBLISH_CHUNK: usize = 500;

#[tokio::main]
async fn main() {
    let endpoint = std::env::var("LOCALSTACK_ENDPOINT").ok();
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

    let hcfg = HarnessConfig::<Sqs>::new("sqs")
        .with_prefetch_cap(SQS_PREFETCH_CAP)
        .with_publish_chunk_size(SQS_PUBLISH_CHUNK);

    run_supervisor_scenarios(
        hcfg,
        move || {
            let cfg = SnsConfig {
                region: Some(region.clone()),
                endpoint_url: endpoint.clone(),
            };
            async move { Broker::<Sqs>::new(cfg).await.expect("connect SNS/SQS") }
        },
        |prefetch, concurrent| {
            ConsumerOptions::<Sqs>::new()
                .with_prefetch_count(prefetch)
                .with_concurrent_processing(concurrent)
        },
    )
    .await;
}
