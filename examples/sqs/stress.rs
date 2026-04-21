//! Stress benchmarks for the SNS/SQS backend.
//!
//! Spins up a LocalStack testcontainer for the lifetime of the process.
//! Requires a running Docker daemon and the `LOCALSTACK_AUTH_TOKEN`
//! environment variable.
//!
//!     LOCALSTACK_AUTH_TOKEN=... cargo run -q --example sqs_stress --features aws-sns-sqs
//!     LOCALSTACK_AUTH_TOKEN=... cargo run -q --example sqs_stress --features aws-sns-sqs -- --tier moderate

#[path = "../common/stress_test.rs"]
mod harness;

use shove::sns::SnsConfig;
use shove::{Broker, ConsumerOptions, Sqs};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStack;

use harness::{HarnessConfig, run_supervisor_scenarios};

/// SQS caps `ReceiveMessage` batches at 10.
const SQS_PREFETCH_CAP: u16 = 10;

/// Outer publish chunk size — the SNS publisher internally re-chunks to the
/// 10-entry SNS batch limit. 500 matched the original harness's outer batch
/// size; smaller values reduce peak memory.
const SQS_PUBLISH_CHUNK: usize = 500;

#[tokio::main]
async fn main() {
    let auth_token = match std::env::var("LOCALSTACK_AUTH_TOKEN") {
        Ok(t) => t,
        Err(_) => {
            eprintln!(
                "LOCALSTACK_AUTH_TOKEN is not set. This example requires a LocalStack Pro auth \
                 token:\n\n    export LOCALSTACK_AUTH_TOKEN=...\n"
            );
            std::process::exit(1);
        }
    };

    // SAFETY: called before any concurrent env access in this process.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_REGION", "us-east-1");
    }

    let container = LocalStack::default()
        .with_env_var("LOCALSTACK_AUTH_TOKEN", auth_token)
        .start()
        .await
        .expect("failed to start LocalStack container");
    let port = container
        .get_host_port_ipv4(4566)
        .await
        .expect("failed to read LocalStack port");
    let endpoint = format!("http://localhost:{port}");

    let hcfg = HarnessConfig::<Sqs>::new("sqs")
        .with_prefetch_cap(SQS_PREFETCH_CAP)
        .with_publish_chunk_size(SQS_PUBLISH_CHUNK);

    run_supervisor_scenarios(
        hcfg,
        move || {
            let endpoint = endpoint.clone();
            let cfg = SnsConfig {
                region: Some("us-east-1".into()),
                endpoint_url: Some(endpoint),
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

    drop(container);
}
