//! Integration test for the Kafka admin client's OAUTHBEARER token priming.
//!
//! # What this proves
//!
//! rdkafka services the OAUTHBEARER token-refresh event by polling the client's
//! main queue. The admin client never polls it (its poll thread drains only a
//! dedicated result queue), so under MSK IAM the admin client never acquired a
//! token and topic creation failed authentication. `shove` fixes this by
//! generating the token and setting it directly via
//! `rd_kafka_oauthbearer_set_token` when the admin client is created.
//!
//! This test drives that exact priming path
//! (`shove::kafka::prime_admin_oauth_token_for_test`, which wraps the same
//! private helper the MSK admin path uses) against a real Apache Kafka broker
//! configured for SASL_PLAINTEXT/OAUTHBEARER with the stock unsecured validator,
//! then creates a topic **without ever polling the admin client** — the exact
//! production scenario. Success can only mean the directly-set token
//! authenticated the connection.
//!
//! MSK IAM's real presigned-URL tokens can't be validated without AWS, so we
//! substitute an unsecured JWT the stock `OAuthBearerUnsecuredValidatorCallbackHandler`
//! accepts. The token path under test (generate on a blocking thread → FFI
//! `set_token`) is identical; only the token contents differ.
//!
//! # Run
//!
//! ```
//! cargo nextest run --features kafka-msk-iam,test-support \
//!     --test kafka_oauthbearer_admin_integration --no-capture
//! ```
#![cfg(all(feature = "kafka-msk-iam", feature = "test-support"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::config::ClientConfig;
use shove::kafka::prime_admin_oauth_token_for_test;
use testcontainers::core::{ContainerPort, ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const KAFKA_PORT: u16 = 9092;

// ---------------------------------------------------------------------------
// Unsecured JWT + counting OAUTHBEARER context
// ---------------------------------------------------------------------------

/// base64url without padding (RFC 7515 style), hand-rolled to avoid a dep.
fn b64url(input: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(T[((n >> 18) & 63) as usize] as char);
        out.push(T[((n >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(T[((n >> 6) & 63) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(T[(n & 63) as usize] as char);
        }
    }
    out
}

/// Build an unsecured OAUTHBEARER token `b64url(header).b64url(payload).`
/// (trailing dot, empty signature). Returns `(token, absolute_expiry_epoch_ms)`
/// — `lifetime_ms` is an absolute epoch timestamp, matching `MskIamContext`.
fn unsecured_jwt(principal: &str) -> (String, i64) {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let iat = now.as_secs() as i64;
    let exp = iat + 3600;
    let header = br#"{"alg":"none"}"#;
    let payload = format!(r#"{{"sub":"{principal}","iat":{iat},"exp":{exp}}}"#);
    let token = format!("{}.{}.", b64url(header), b64url(payload.as_bytes()));
    (token, exp * 1000)
}

/// OAUTHBEARER context that mints an unsecured JWT and counts how many times
/// `generate_oauth_token` is invoked. The counter is what proves `shove`'s
/// priming path actually ran (rather than the broker accepting for some other
/// reason).
#[derive(Clone)]
struct CountingOauthContext {
    principal: String,
    generated: Arc<AtomicUsize>,
}

impl ClientContext for CountingOauthContext {
    // Intentionally left at the default `false`: the admin client is never
    // polled, so librdkafka's own refresh path can't run — the only token this
    // client ever sees is the one shove sets directly.
    fn generate_oauth_token(
        &self,
        _cfg: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        self.generated.fetch_add(1, Ordering::SeqCst);
        let (token, lifetime_ms) = unsecured_jwt(&self.principal);
        Ok(OAuthToken {
            token,
            principal_name: self.principal.clone(),
            lifetime_ms,
        })
    }
}

// ---------------------------------------------------------------------------
// OAUTHBEARER broker harness
// ---------------------------------------------------------------------------

/// Start a single-node KRaft Apache Kafka broker with SASL_PLAINTEXT/OAUTHBEARER
/// on a client listener validated by the stock unsecured validator. Returns the
/// running container and the host bootstrap address.
///
/// The advertised host port is unknown at creation time, so we use the same
/// trick as the testcontainers kafka module: the entrypoint waits for a start
/// script that we write from the host once the mapped port is known.
async fn start_oauthbearer_kafka() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    // The unsecured login module supplies the module identity; the validator
    // handler (below) checks the token's `sub`.
    let listener_jaas = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule \
         required unsecuredLoginStringClaim_sub=\"admin\";";
    let validator = "org.apache.kafka.common.security.oauthbearer.internals.unsecured.\
         OAuthBearerUnsecuredValidatorCallbackHandler";
    let start_script = "/tmp/tc_start.sh";

    // NOTE: listener names must NOT contain underscores — the apache/kafka image
    // maps env vars to properties by a blind `_`->`.` swap, which mangles a name
    // like `SASL_HOST`. `CLIENT`/`BROKER`/`CONTROLLER` are safe.
    let image = GenericImage::new("apache/kafka", "3.8.0")
        .with_exposed_port(ContainerPort::Tcp(KAFKA_PORT))
        .with_entrypoint("bash")
        .with_cmd(vec![
            "-c".to_string(),
            format!(
                "while [ ! -f {s} ]; do sleep 0.1; done; chmod 755 {s} && {s}",
                s = start_script
            ),
        ])
        .with_env_var("CLUSTER_ID", "5L6g3nShT-eMCtK--X86sw")
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9094")
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
        .with_env_var(
            "KAFKA_LISTENERS",
            format!(
                "CLIENT://0.0.0.0:{KAFKA_PORT},BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094"
            ),
        )
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CLIENT:SASL_PLAINTEXT,BROKER:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
        )
        .with_env_var("KAFKA_SASL_ENABLED_MECHANISMS", "OAUTHBEARER")
        .with_env_var("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "OAUTHBEARER")
        .with_env_var(
            "KAFKA_LISTENER_NAME_CLIENT_OAUTHBEARER_SASL_SERVER_CALLBACK_HANDLER_CLASS",
            validator,
        )
        .with_env_var(
            "KAFKA_LISTENER_NAME_CLIENT_OAUTHBEARER_SASL_JAAS_CONFIG",
            listener_jaas,
        )
        .with_env_var(
            "KAFKA_LISTENER_NAME_BROKER_OAUTHBEARER_SASL_SERVER_CALLBACK_HANDLER_CLASS",
            validator,
        )
        .with_env_var(
            "KAFKA_LISTENER_NAME_BROKER_OAUTHBEARER_SASL_JAAS_CONFIG",
            listener_jaas,
        )
        .with_env_var("KAFKA_OPTS", "-Djava.security.auth.login.config=/dev/null")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");

    let container = image
        .start()
        .await
        .expect("failed to start Kafka container");
    let host_port = container
        .get_host_port_ipv4(ContainerPort::Tcp(KAFKA_PORT))
        .await
        .expect("failed to get mapped Kafka port");

    // Now that the mapped port is known, write the start script so the broker
    // advertises the CLIENT listener at 127.0.0.1:<mapped>. The exec's ready
    // condition is the real broker-startup gate.
    let script = format!(
        "#!/usr/bin/env bash\n\
         export KAFKA_ADVERTISED_LISTENERS='CLIENT://127.0.0.1:{host_port},BROKER://localhost:9093'\n\
         exec /etc/kafka/docker/run\n"
    );
    let write_cmd = ExecCommand::new(vec![
        "bash".to_string(),
        "-c".to_string(),
        format!("cat > {start_script} <<'EOF'\n{script}EOF"),
    ])
    .with_container_ready_conditions(vec![WaitFor::message_on_stdout("Kafka Server started")]);
    container
        .exec(write_cmd)
        .await
        .expect("failed to write start script");

    (container, format!("127.0.0.1:{host_port}"))
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

/// Priming the admin client's OAUTHBEARER token via shove's helper lets it
/// authenticate and create a topic **without any polling** — the scenario that
/// was broken for MSK IAM before the fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_authenticates_via_primed_oauthbearer_token() {
    let (_container, bootstrap) = start_oauthbearer_kafka().await;

    let generated = Arc::new(AtomicUsize::new(0));
    let ctx = CountingOauthContext {
        principal: "admin".to_string(),
        generated: Arc::clone(&generated),
    };

    // Build the admin client exactly as the MSK path does: SASL_PLAINTEXT +
    // OAUTHBEARER, with a context that provides no token on its own (the admin
    // client is never polled, so librdkafka can't run its refresh path).
    let admin: AdminClient<CountingOauthContext> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanism", "OAUTHBEARER")
        .create_with_context(ctx.clone())
        .expect("failed to create admin client");

    // The fix under test: generate the token and set it directly on the client.
    prime_admin_oauth_token_for_test(&admin, ctx)
        .await
        .expect("priming the admin OAUTHBEARER token failed");

    // Prove shove's priming path actually ran (not the broker accepting for some
    // unrelated reason).
    assert_eq!(
        generated.load(Ordering::SeqCst),
        1,
        "prime_admin_oauth_token_for_test must invoke generate_oauth_token exactly once"
    );

    // Create a topic. This is the operation that failed authentication before
    // the fix. We never poll the admin client — success means the directly-set
    // token authenticated the connection.
    let new_topic = NewTopic::new("oauth-admin-primed-topic", 1, TopicReplication::Fixed(1));
    let results = admin
        .create_topics(&[new_topic], &AdminOptions::new())
        .await
        .expect("create_topics RPC failed (admin did not authenticate via the primed token)");

    assert_eq!(results.len(), 1, "expected one topic result");
    match &results[0] {
        Ok(name) => assert_eq!(name, "oauth-admin-primed-topic"),
        Err((topic, code)) => panic!("topic {topic} creation failed under OAUTHBEARER: {code:?}"),
    }
}
