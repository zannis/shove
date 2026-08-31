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

use shove::sns::{SnsConfig, SqsConsumer};
use shove::{Backend, ConsumerOptions, Sqs};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStack;

use harness::{DlqDrainFn, HarnessConfig, StressTestTopic, run_supervisor_scenarios};

/// SQS caps `ReceiveMessage` batches at 10.
const SQS_PREFETCH_CAP: u16 = 10;

/// Outer publish chunk size — the SNS publisher internally re-chunks to the
/// 10-entry SNS batch limit. 500 matched the original harness's outer batch
/// size; smaller values reduce peak memory.
const SQS_PUBLISH_CHUNK: usize = 500;

const QUEUE_NAME: &str = "shove-stress-bench";
const DLQ_NAME: &str = "shove-stress-bench-dlq";

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

    wait_until_ready(&endpoint).await;

    let purge_endpoint = endpoint.clone();
    let purge: harness::PurgeFn = Box::new(move || {
        let endpoint = purge_endpoint.clone();
        Box::pin(async move {
            // `purge_queue` drains without deleting — avoids SQS's "wait 60 s
            // before recreating a queue with the same name" rule that breaks
            // the next scenario's topology declare. LocalStack enforces
            // `purge_queue`'s own 60 s rate limit so the call may fail; we
            // ignore errors and let the next scenario re-publish on top of
            // whatever is left (the harness still counts ok once
            // `scenario.messages` are processed).
            let aws_cfg = aws_config::from_env()
                .region(aws_config::Region::new("us-east-1"))
                .endpoint_url(&endpoint)
                .load()
                .await;
            let sqs = aws_sdk_sqs::Client::new(&aws_cfg);
            for name in [QUEUE_NAME, DLQ_NAME, &format!("{QUEUE_NAME}.fifo")] {
                if let Ok(url) = sqs.get_queue_url().queue_name(name).send().await
                    && let Some(u) = url.queue_url()
                {
                    let _ = sqs.purge_queue().queue_url(u).send().await;
                }
            }
        })
    });

    let dlq_drain: DlqDrainFn<Sqs> = Box::new(|client, handler| {
        Box::pin(async move {
            let consumer: SqsConsumer = <Sqs as Backend>::make_consumer(&client);
            let _ = consumer.run_dlq::<StressTestTopic, _>(handler, ()).await;
        })
    });

    let hcfg = HarnessConfig::<Sqs>::new("sqs")
        .with_prefetch_cap(SQS_PREFETCH_CAP)
        .with_publish_chunk_size(SQS_PUBLISH_CHUNK)
        .with_purge(purge)
        .with_broker(
            "AWS SQS (LocalStack)",
            "localstack latest",
            "docker localstack",
        )
        // The decisive flag: these numbers measure LocalStack, not AWS, so
        // they must never be published as an absolute SQS throughput claim.
        // Enforcing it here rather than in prose is the point.
        .not_representative()
        .with_dlq_drain(dlq_drain);

    run_supervisor_scenarios(
        hcfg,
        move || {
            let endpoint = endpoint.clone();
            let cfg = SnsConfig {
                region: Some("us-east-1".into()),
                endpoint_url: Some(endpoint),
            };
            async move {
                <Sqs as Backend>::connect(cfg)
                    .await
                    .expect("connect SNS/SQS")
            }
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

/// Issue a `ListQueues` against LocalStack until it succeeds. Testcontainers'
/// wait-strategy only confirms port 4566 is open; LocalStack's per-service
/// boot continues for a few seconds afterwards, and an SDK call against an
/// unready SQS endpoint returns errors that look like real bugs.
async fn wait_until_ready(endpoint: &str) {
    let aws_cfg = aws_config::from_env()
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .load()
        .await;
    let sqs = aws_sdk_sqs::Client::new(&aws_cfg);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if sqs.list_queues().send().await.is_ok() {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("LocalStack SQS did not become ready within 60s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}
