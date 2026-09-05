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

use aws_sdk_sqs::types::QueueAttributeName;
use shove::sns::{SnsConfig, SqsConsumer};
use shove::{Backend, ConsumerOptions, Sqs, Topic};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStack;

use harness::{DlqDrainFn, HarnessConfig, StressTestTopic, run_supervisor_scenarios};

/// SQS caps `ReceiveMessage` batches at 10.
const SQS_PREFETCH_CAP: u16 = 10;

/// Image tag started by `testcontainers_modules::localstack` (its pinned
/// default), recorded in the results provenance so a reader knows which
/// LocalStack produced the (non-representative) numbers.
const LOCALSTACK_VERSION: &str = "4.5";

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

    harness::spawn_ctrlc_watcher();
    let container = LocalStack::default()
        .with_env_var("LOCALSTACK_AUTH_TOKEN", auth_token)
        .start()
        .await
        .expect("failed to start LocalStack container");
    let port = container
        .get_host_port_ipv4(4566)
        .await
        .expect("failed to read LocalStack port");
    let _container = harness::ContainerGuard::new(container);
    let endpoint = format!("http://localhost:{port}");

    wait_until_ready(&endpoint).await;

    let purge_endpoint = endpoint.clone();
    let purge: harness::PurgeFn = Box::new(move |topology| {
        let endpoint = purge_endpoint.clone();
        Box::pin(async move {
            // `purge_queue` drains without deleting — avoids SQS's "wait 60 s
            // before recreating a queue with the same name" rule that breaks
            // the next scenario's topology declare.
            //
            // The physical names are derived from the topology handed in
            // (`src/backends/sns/topology.rs` naming): a sequenced topology
            // owns `.fifo` shard queues and a `.fifo` DLQ, an unsequenced one
            // the plain queue and DLQ.
            let mut names: Vec<String> = Vec::new();
            match topology.sequencing() {
                Some(seq) => {
                    for shard in 0..seq.routing_shards() {
                        names.push(format!("{}-seq-{shard}.fifo", topology.queue()));
                    }
                    if let Some(dlq) = topology.dlq() {
                        names.push(format!("{dlq}.fifo"));
                    }
                }
                None => {
                    names.push(topology.queue().to_string());
                    if let Some(dlq) = topology.dlq() {
                        names.push(dlq.to_string());
                    }
                }
            }

            let aws_cfg = aws_config::from_env()
                .region(aws_config::Region::new("us-east-1"))
                .endpoint_url(&endpoint)
                .load()
                .await;
            let sqs = aws_sdk_sqs::Client::new(&aws_cfg);
            for name in &names {
                let url = match sqs.get_queue_url().queue_name(name).send().await {
                    Ok(out) => match out.queue_url() {
                        Some(u) => u.to_string(),
                        None => continue,
                    },
                    Err(e) => {
                        let svc = e.into_service_error();
                        // Only a queue that does not exist yet is safely
                        // "nothing to purge" — any other lookup failure would
                        // silently skip a queue that still holds messages.
                        if svc.is_queue_does_not_exist() {
                            continue;
                        }
                        return Err(format!("lookup {name}: {svc}"));
                    }
                };
                match sqs.purge_queue().queue_url(&url).send().await {
                    // A purge is asynchronous — SQS may keep deleting for up
                    // to 60 s. Verify emptiness instead of trusting the
                    // accepted request. (On real AWS, messages published
                    // within that window may also be deleted; this harness
                    // only ever runs against LocalStack, whose purge is
                    // synchronous, so verified-empty is a settled state
                    // here.)
                    Ok(_) => await_queue_empty(&sqs, &url, name).await?,
                    Err(e) => {
                        let svc = e.into_service_error();
                        if !svc.is_purge_queue_in_progress() {
                            return Err(format!("purge {name}: {svc}"));
                        }
                        // One purge per queue per 60 s; a rate-limited purge
                        // proves nothing about messages published since the
                        // accepted one. Drain the remainder by hand.
                        drain_queue(&sqs, &url, name).await?;
                    }
                }
            }
            Ok(())
        })
    });

    // SQS dead-letters asynchronously: shove's reject path resets visibility
    // and the broker-side redrive policy moves a message only after
    // maxReceiveCount receives, so the fill phase must watch the DLQ itself
    // fill rather than trust its handler-invocation count.
    let depth_endpoint = endpoint.clone();
    let dlq_depth: harness::DlqDepthFn = Box::new(move || {
        let endpoint = depth_endpoint.clone();
        Box::pin(async move {
            let dlq = StressTestTopic::topology()
                .dlq()
                .ok_or_else(|| "stress topology has no DLQ".to_string())?;
            let aws_cfg = aws_config::from_env()
                .region(aws_config::Region::new("us-east-1"))
                .endpoint_url(&endpoint)
                .load()
                .await;
            let sqs = aws_sdk_sqs::Client::new(&aws_cfg);
            let url = sqs
                .get_queue_url()
                .queue_name(dlq)
                .send()
                .await
                .map_err(|e| format!("lookup {dlq}: {}", e.into_service_error()))?
                .queue_url()
                .ok_or_else(|| format!("{dlq} has no URL"))?
                .to_string();
            let attrs = sqs
                .get_queue_attributes()
                .queue_url(&url)
                .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
                .send()
                .await
                .map_err(|e| format!("attributes {dlq}: {}", e.into_service_error()))?;
            attrs
                .attributes()
                .and_then(|a| a.get(&QueueAttributeName::ApproximateNumberOfMessages))
                .and_then(|v| v.parse::<u64>().ok())
                .ok_or_else(|| format!("{dlq} reported no ApproximateNumberOfMessages"))
        })
    });

    let dlq_drain: DlqDrainFn<Sqs> = Box::new(|client, handler, _stop| {
        // This backend's `run_dlq` exits when the teardown closes the client;
        // the stop token is for backends without that path (see `DlqDrainFn`).
        Box::pin(async move {
            let consumer: SqsConsumer = <Sqs as Backend>::make_consumer(&client);
            consumer
                .run_dlq::<StressTestTopic, _>(handler, ())
                .await
                .map_err(|e| format!("run_dlq: {e}"))
        })
    });

    let hcfg = HarnessConfig::<Sqs>::new("sqs")
        .with_prefetch_cap(SQS_PREFETCH_CAP)
        .with_publish_chunk_size(SQS_PUBLISH_CHUNK)
        .with_purge(purge)
        .with_broker(
            "AWS SQS (LocalStack)",
            LOCALSTACK_VERSION,
            "docker localstack",
        )
        // The decisive flag: these numbers measure LocalStack, not AWS, so
        // they must never be published as an absolute SQS throughput claim.
        // Enforcing it here rather than in prose is the point.
        .not_representative()
        .with_dlq_drain(dlq_drain)
        .with_dlq_depth(dlq_depth);

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
}

/// Poll a queue until it reads empty across visible, in-flight, and delayed
/// counts — twice in a row, because each attribute is only approximate and a
/// single zero can be a short-poll artifact rather than a settled state.
async fn await_queue_empty(sqs: &aws_sdk_sqs::Client, url: &str, name: &str) -> Result<(), String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(65);
    let mut empty_reads = 0u32;
    loop {
        let attrs = sqs
            .get_queue_attributes()
            .queue_url(url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesDelayed)
            .send()
            .await
            .map_err(|e| format!("attributes {name}: {}", e.into_service_error()))?;
        let attrs = attrs
            .attributes()
            .ok_or_else(|| format!("{name} reported no attributes"))?;
        let mut depth = 0u64;
        for key in [
            QueueAttributeName::ApproximateNumberOfMessages,
            QueueAttributeName::ApproximateNumberOfMessagesNotVisible,
            QueueAttributeName::ApproximateNumberOfMessagesDelayed,
        ] {
            depth = depth.saturating_add(
                attrs
                    .get(&key)
                    .and_then(|v| v.parse::<u64>().ok())
                    .ok_or_else(|| format!("{name} reported no {key}"))?,
            );
        }
        if depth == 0 {
            empty_reads += 1;
            if empty_reads >= 2 {
                return Ok(());
            }
        } else {
            empty_reads = 0;
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "purge {name}: still holds {depth} messages after 65s"
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

/// Drain a queue by receive/delete — the fallback when `purge_queue` is
/// rate-limited (one purge per queue per 60 s) between back-to-back
/// scenarios. Long-polls so a short-poll's false empty cannot end the drain
/// early, then hands the settled-empty decision to [`await_queue_empty`].
async fn drain_queue(sqs: &aws_sdk_sqs::Client, url: &str, name: &str) -> Result<(), String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if std::time::Instant::now() >= deadline {
            return Err(format!("drain {name}: queue still not empty after 60s"));
        }
        let received = sqs
            .receive_message()
            .queue_url(url)
            .max_number_of_messages(10)
            .wait_time_seconds(1)
            .send()
            .await
            .map_err(|e| format!("drain {name}: receive: {}", e.into_service_error()))?;
        let messages = received.messages.unwrap_or_default();
        if messages.is_empty() {
            break;
        }
        for msg in &messages {
            if let Some(handle) = msg.receipt_handle() {
                sqs.delete_message()
                    .queue_url(url)
                    .receipt_handle(handle)
                    .send()
                    .await
                    .map_err(|e| format!("drain {name}: delete: {}", e.into_service_error()))?;
            }
        }
    }
    await_queue_empty(sqs, url, name).await
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
