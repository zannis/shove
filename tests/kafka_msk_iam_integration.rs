//! End-to-end MSK IAM auth test against LocalStack Pro's MSK service.
//!
//! Requires `LOCALSTACK_AUTH_TOKEN` to be set in the environment (a LocalStack
//! Pro license token). Without it the test exits 0 with a skip message. This is
//! the documented opt-in for CI environments that don't have the secret.
//!
//! Run locally:
//! ```
//! LOCALSTACK_AUTH_TOKEN=... dotenvx run -- cargo nextest run \
//!     --features kafka-msk-iam --test kafka_msk_iam_integration
//! ```
//!
//! # LocalStack MSK behaviour notes
//!
//! LocalStack Pro's MSK simulation creates a real Kafka container internally
//! (via `MSK_PROVIDER=msk_provided` mode). It:
//!
//! - Accepts `create_cluster_v2` with a minimal `ProvisionedRequest`.
//! - Typically reaches ACTIVE within 5-30 seconds.
//! - Returns bootstrap brokers via `get_bootstrap_brokers` in the
//!   `bootstrap_broker_string_sasl_iam` field using internal container hostnames
//!   that are not reachable from the test runner.
//!
//! **Bootstrap broker rewrite**: Because the broker addresses from the MSK API
//! response contain internal container hostnames, we replace the host portion
//! with the testcontainers-mapped host and port so rdkafka can actually connect.
//! LocalStack routes Kafka traffic through its edge port (4566).
//!
//! **TLS hostname verification**: We disable hostname verification
//! (`skip_hostname_verification: true`) because the LocalStack TLS certificate
//! is issued for `localhost`/internal hostnames that don't match the
//! testcontainers-mapped address.

#![cfg(feature = "kafka-msk-iam")]

use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_kafka::types::ClusterState;
use aws_sdk_kafka::types::{
    BrokerNodeGroupInfo, ClientAuthentication, Iam, ProvisionedRequest, Sasl,
};
use serde::{Deserialize, Serialize};
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::kafka::{KafkaClient, KafkaConfig, KafkaConsumer, KafkaSasl, KafkaTls};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topology::TopologyBuilder;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStackPro;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const EDGE_PORT: u16 = 4566;
const AWS_REGION: &str = "us-east-1";

// ---------------------------------------------------------------------------
// Skip guard
// ---------------------------------------------------------------------------

fn require_localstack_token() -> Option<String> {
    match std::env::var("LOCALSTACK_AUTH_TOKEN") {
        Ok(t) if !t.is_empty() => Some(t),
        _ => {
            eprintln!(
                "skipping kafka_msk_iam_integration: LOCALSTACK_AUTH_TOKEN not set. \
                 Set it to a LocalStack Pro auth token to run this test."
            );
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Test topic
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct MskTestPayload {
    id: String,
    body: String,
}

shove::define_topic!(
    MskTestTopic,
    MskTestPayload,
    TopologyBuilder::new("msk-iam-e2e").build()
);

// ---------------------------------------------------------------------------
// Waitable flag for single delivery
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct OnceReceived {
    flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
    payload: Arc<tokio::sync::Mutex<Option<MskTestPayload>>>,
}

impl OnceReceived {
    fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
            payload: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    async fn wait(&self, timeout: Duration) -> Option<MskTestPayload> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.flag.load(Ordering::Acquire) {
                return self.payload.lock().await.clone();
            }
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return None;
                }
            }
        }
    }
}

impl MessageHandler<MskTestTopic> for OnceReceived {
    type Context = ();
    async fn handle(&self, msg: MskTestPayload, _meta: MessageMetadata, _: &()) -> Outcome {
        *self.payload.lock().await = Some(msg);
        self.flag.store(true, Ordering::Release);
        self.notify.notify_waiters();
        Outcome::Ack
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build an MSK management client pointing at LocalStack's edge port.
///
/// Uses explicit `endpoint_url` on the aws-config builder rather than env vars
/// so it doesn't interfere with the MSK IAM signer's credential lookup.
async fn msk_management_client(endpoint: &str) -> aws_sdk_kafka::Client {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(AWS_REGION))
        .endpoint_url(endpoint)
        .load()
        .await;
    aws_sdk_kafka::Client::new(&config)
}

/// Create a minimal single-broker MSK cluster with IAM auth enabled.
///
/// Returns the cluster ARN from the API response.
async fn create_msk_cluster(msk: &aws_sdk_kafka::Client, cluster_name: &str) -> String {
    // LocalStack accepts a minimal ProvisionedRequest. The broker count and
    // instance type are effectively ignored by the simulator, but we must
    // supply the required `broker_node_group_info` field.
    let broker_info = BrokerNodeGroupInfo::builder()
        .instance_type("kafka.m5.large")
        // LocalStack ignores subnets; supply a placeholder to satisfy validation.
        .client_subnets("subnet-00000000")
        .build();

    let iam_auth = Iam::builder().enabled(true).build();
    let sasl_auth = Sasl::builder().iam(iam_auth).build();
    let client_auth = ClientAuthentication::builder().sasl(sasl_auth).build();

    let provisioned = ProvisionedRequest::builder()
        .broker_node_group_info(broker_info)
        .client_authentication(client_auth)
        .kafka_version("3.5.1")
        .number_of_broker_nodes(1)
        .build();

    let resp = msk
        .create_cluster_v2()
        .cluster_name(cluster_name)
        .provisioned(provisioned)
        .send()
        .await
        .expect("create_cluster_v2 should succeed against LocalStack");

    resp.cluster_arn()
        .expect("create_cluster_v2 response must contain cluster_arn")
        .to_owned()
}

/// Poll describe_cluster_v2 until the cluster is ACTIVE.
///
/// Panics if the cluster doesn't reach ACTIVE within `max_wait`.
async fn wait_for_active(msk: &aws_sdk_kafka::Client, cluster_arn: &str, max_wait: Duration) {
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        let resp = msk
            .describe_cluster_v2()
            .cluster_arn(cluster_arn)
            .send()
            .await
            .expect("describe_cluster_v2 should succeed");

        let state = resp.cluster_info().and_then(|c| c.state()).cloned();

        match state {
            Some(ClusterState::Active) => return,
            Some(ClusterState::Failed) => {
                panic!("LocalStack MSK cluster entered FAILED state for ARN {cluster_arn}")
            }
            other => {
                let label = other.as_ref().map(|s| s.as_str()).unwrap_or("<unknown>");
                if tokio::time::Instant::now() >= deadline {
                    panic!(
                        "LocalStack MSK cluster did not reach ACTIVE within {:?}; \
                         last state: {label}",
                        max_wait,
                    );
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

/// Retrieve bootstrap brokers and rewrite internal hostnames to the
/// testcontainers-mapped edge host:port.
///
/// LocalStack returns broker addresses using its internal container hostname
/// (e.g. `172.17.0.2:4511`). Those are not reachable from the test runner.
/// LocalStack routes all Kafka traffic through its single edge port (4566), so
/// we replace every `<host>:<port>` pair in the returned string with
/// `<edge_host>:<edge_port>`.
fn rewrite_bootstrap_brokers(raw: &str, edge_host: &str, edge_port: u16) -> String {
    // Each broker is "<host>:<port>". We keep only the first broker address
    // because LocalStack uses a single Kafka container regardless of the
    // requested broker count. Replacing the host+port of all entries with the
    // edge endpoint is equivalent and simpler.
    let replacement = format!("{edge_host}:{edge_port}");
    raw.split(',')
        .map(|_addr| replacement.as_str())
        // LocalStack reports one broker; dedup avoids sending the same address N times.
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(",")
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn msk_iam_end_to_end_against_localstack_pro() {
    let Some(token) = require_localstack_token() else {
        return;
    };

    // ── Spin up LocalStack Pro ──────────────────────────────────────────────
    let container = LocalStackPro::new(&token)
        .with_env_var("SERVICES", "kafka,sts,iam")
        .with_env_var("DEBUG", "0")
        .start()
        .await
        .expect("start LocalStack Pro container");

    let edge_host = container
        .get_host()
        .await
        .expect("container host")
        .to_string();
    let edge_port = container
        .get_host_port_ipv4(EDGE_PORT)
        .await
        .expect("edge port");
    let endpoint = format!("http://{edge_host}:{edge_port}");

    // Point the MSK IAM SASL signer at LocalStack via env vars.
    // The signer calls STS to fetch identity; LocalStack stubs that endpoint.
    //
    // SAFETY: nextest runs each test in its own process, so mutating env vars
    // here is not a data race.
    unsafe {
        std::env::set_var("AWS_ENDPOINT_URL", &endpoint);
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_REGION", AWS_REGION);
        std::env::set_var("AWS_DEFAULT_REGION", AWS_REGION);
    }

    // ── Create an MSK cluster via aws-sdk-kafka pointed at LocalStack ───────
    let msk = msk_management_client(&endpoint).await;
    let cluster_name = format!("shove-test-{}", uuid::Uuid::new_v4().simple());
    let cluster_arn = create_msk_cluster(&msk, &cluster_name).await;

    // ── Wait for ACTIVE state ────────────────────────────────────────────────
    wait_for_active(&msk, &cluster_arn, Duration::from_secs(90)).await;

    // ── Fetch bootstrap brokers ─────────────────────────────────────────────
    let brokers_resp = msk
        .get_bootstrap_brokers()
        .cluster_arn(&cluster_arn)
        .send()
        .await
        .expect("get_bootstrap_brokers should succeed");

    // `bootstrap_broker_string_sasl_iam` is the correct field for MSK IAM
    // OAUTHBEARER connections. LocalStack may return it in the plain
    // `bootstrap_broker_string` fallback if SASL/IAM is not tracked
    // separately; we try both in order.
    let raw_brokers = brokers_resp
        .bootstrap_broker_string_sasl_iam()
        .or_else(|| brokers_resp.bootstrap_broker_string_tls())
        .or_else(|| brokers_resp.bootstrap_broker_string())
        .expect(
            "LocalStack MSK did not return any bootstrap broker string; \
             cannot proceed with end-to-end test",
        );

    // Rewrite container-internal addresses to the testcontainers-exposed edge.
    // See module-level doc comment for explanation.
    let bootstrap = rewrite_bootstrap_brokers(raw_brokers, &edge_host, edge_port);

    // ── Build shove KafkaClient with MSK IAM SASL + TLS ────────────────────
    //
    // skip_hostname_verification: LocalStack's TLS cert is for internal
    // hostnames; we can't verify it against the testcontainers-mapped address.
    let cfg = KafkaConfig::new(&bootstrap)
        .with_tls(KafkaTls {
            skip_hostname_verification: true,
            ..KafkaTls::default()
        })
        .with_sasl(KafkaSasl::msk_iam(AWS_REGION));

    let client = KafkaClient::connect(&cfg)
        .await
        .expect("KafkaClient::connect with MSK IAM SASL should succeed");

    // ── Declare topology and produce one message ────────────────────────────
    let broker = Broker::<Kafka>::from_client(client.clone());
    broker
        .topology()
        .declare::<MskTestTopic>()
        .await
        .expect("declare MskTestTopic");

    let publisher = broker.publisher().await.expect("publisher");
    let sent = MskTestPayload {
        id: "msk-e2e-1".into(),
        body: "hello from LocalStack MSK IAM".into(),
    };
    publisher
        .publish::<MskTestTopic>(&sent)
        .await
        .expect("publish to MSK should succeed");

    // ── Consume and assert round-trip ───────────────────────────────────────
    let received = OnceReceived::new();
    let handler_clone = received.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<MskTestTopic, _>(
                handler_clone,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1),
            )
            .await
    });

    let got = received
        .wait(Duration::from_secs(60))
        .await
        .expect("should receive one message from LocalStack MSK within 60 s");

    assert_eq!(got, sent, "received payload must match sent payload");

    // ── Tear down ───────────────────────────────────────────────────────────
    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;

    // Keep the container alive until the end of the test so Docker doesn't
    // reclaim its network before rdkafka's background threads finish.
    drop(container);
}
