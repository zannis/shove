//! Integration test for `RabbitMqClient` reconnect after a broker disconnect.
//!
//! Stops the broker's Erlang application via `rabbitmqctl stop_app`, restarts
//! it with `start_app`, and asserts that `client.create_channel()` recovers
//! within a bounded time. Without the connection-swap fix this test will fail
//! — the dead `Arc<Connection>` keeps returning `InvalidConnectionState`.
//!
//! Run with: `cargo test -q --features rabbitmq --test rabbitmq_reconnect`

use std::time::{Duration, Instant};

use shove::rabbitmq::{RabbitMqClient, RabbitMqConfig};
use testcontainers::core::ExecCommand;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;

const RECOVERY_BUDGET: Duration = Duration::from_secs(30);

#[tokio::test]
async fn client_recovers_after_broker_app_restart() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let container = RabbitMq::default()
        .start()
        .await
        .expect("failed to start RabbitMQ container");
    let host = container.get_host().await.expect("failed to get host");
    let port = container
        .get_host_port_ipv4(5672)
        .await
        .expect("failed to get amqp port");
    let uri = format!("amqp://guest:guest@{host}:{port}/%2f");

    let client = RabbitMqClient::connect(&RabbitMqConfig::new(uri.clone()))
        .await
        .expect("initial connect failed");

    // Sanity: a channel can be created on the live broker.
    let _ch = client
        .create_channel()
        .await
        .expect("baseline create_channel failed");
    assert!(client.is_connected(), "client should report connected");

    // Stop the RabbitMQ Erlang application. The container stays up but the
    // AMQP listener goes away and existing TCP connections drop.
    let mut stop = container
        .exec(ExecCommand::new(["rabbitmqctl", "stop_app"]))
        .await
        .expect("stop_app exec failed");
    let _ = stop.stdout_to_vec().await;

    // Give lapin a moment to notice the connection went away (heartbeat /
    // socket-close detection).
    tokio::time::sleep(Duration::from_millis(500)).await;

    // While the broker app is stopped, create_channel must error — but it must
    // be a recoverable error, not a panic or a deadlock.
    let down_result = client.create_channel().await;
    assert!(
        down_result.is_err(),
        "create_channel should fail while broker app is stopped"
    );

    // Restart the broker app on the same port.
    let mut start = container
        .exec(ExecCommand::new(["rabbitmqctl", "start_app"]))
        .await
        .expect("start_app exec failed");
    let _ = start.stdout_to_vec().await;

    // Poll until create_channel succeeds. With the fix, the FIRST call after
    // the broker is back should trigger reconnect-and-retry-once internally;
    // we still poll because the broker app may take a moment to bind the
    // listener after `start_app` returns.
    let begin = Instant::now();
    let mut attempts = 0u32;
    let recovered = loop {
        attempts += 1;
        match client.create_channel().await {
            Ok(_) => break true,
            Err(e) => {
                if begin.elapsed() > RECOVERY_BUDGET {
                    eprintln!(
                        "create_channel still failing after {:?} ({attempts} attempts): {e}",
                        begin.elapsed()
                    );
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    };
    let elapsed = begin.elapsed();

    assert!(
        recovered,
        "client did not recover within {RECOVERY_BUDGET:?} ({attempts} attempts)"
    );
    eprintln!("recovered in {elapsed:?} after {attempts} attempts");
}
