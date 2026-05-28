#![cfg(feature = "aws-sns-sqs")]

//! Integration tests for `Broker<Sqs>::ping`.

use std::time::Duration;

use shove::sns::{SnsClient, SnsConfig};
use shove::{Broker, ShoveError, Sqs};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStack;

struct TestBroker {
    #[allow(dead_code)]
    container: testcontainers::ContainerAsync<LocalStack>,
    sns_client: SnsClient,
}

impl TestBroker {
    async fn start() -> Self {
        // SAFETY: tests run single-threaded for env var manipulation.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
            std::env::set_var("AWS_REGION", "us-east-1");
        }
        let auth_token = std::env::var("LOCALSTACK_AUTH_TOKEN")
            .expect("LOCALSTACK_AUTH_TOKEN must be set (load via dotenvx)");

        let container = LocalStack::default()
            .with_env_var("LOCALSTACK_AUTH_TOKEN", auth_token)
            .start()
            .await
            .expect("start LocalStack container");
        let port = container
            .get_host_port_ipv4(4566)
            .await
            .expect("get LocalStack port");
        let endpoint_url = format!("http://localhost:{port}");

        let sns_config = SnsConfig {
            region: Some("us-east-1".into()),
            endpoint_url: Some(endpoint_url),
        };
        let sns_client = SnsClient::new(&sns_config).await.expect("create SnsClient");

        Self {
            container,
            sns_client,
        }
    }

    fn broker(&self) -> Broker<Sqs> {
        Broker::<Sqs>::from_client(self.sns_client.clone())
    }
}

#[tokio::test]
async fn ping_succeeds_against_running_localstack() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.ping().await.expect("ping should succeed");
}

#[tokio::test]
async fn ping_returns_connection_error_after_close() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.close().await;
    let err = broker.ping().await.expect_err("ping after close must fail");
    assert!(
        matches!(err, ShoveError::Connection(_)),
        "expected ShoveError::Connection, got {err:?}"
    );
}

#[tokio::test]
async fn ping_with_timeout_honors_deadline() {
    // Point at 127.0.0.1:1 (refused).
    let sns_config = SnsConfig {
        region: Some("us-east-1".into()),
        endpoint_url: Some("http://127.0.0.1:1".into()),
    };
    // SAFETY: tests run single-threaded for env var manipulation.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_REGION", "us-east-1");
    }
    let sns_client = SnsClient::new(&sns_config)
        .await
        .expect("SnsClient::new builds the config; no real handshake yet");
    let broker = Broker::<Sqs>::from_client(sns_client);

    let start = std::time::Instant::now();
    let err = broker
        .ping_with_timeout(Duration::from_millis(500))
        .await
        .expect_err("ping against unreachable endpoint must fail");
    let elapsed = start.elapsed();

    assert!(
        matches!(err, ShoveError::Connection(_)),
        "expected ShoveError::Connection, got {err:?}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "ping_with_timeout(500ms) returned in {elapsed:?}, expected < 3s"
    );
}
