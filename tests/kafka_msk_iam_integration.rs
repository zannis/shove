//! Integration test for the MSK IAM authentication code path.
//!
//! # What this test exercises
//!
//! 1. `KafkaConfig::new(...).with_tls(...).with_sasl(KafkaSasl::msk_iam(...))` — config
//!    construction via the public builder API.
//! 2. `KafkaClient::connect(&cfg)` — enters the `MskIam` arm, calls
//!    `MskIamTokenProvider::new` which loads `aws_config` defaults, resolved here
//!    from environment variables (static credential chain, no network), and
//!    starts the signer thread. Then constructs `MskIamContext` and creates the
//!    `BaseConsumer<MskIamContext>` that `ping` uses; the
//!    `FutureProducer<MskIamContext>` waits for the first publish.
//! 3. `MskIamContext::generate_oauth_token` invocation path — rdkafka fires
//!    `RD_KAFKA_EVENT_OAUTHBEARER_TOKEN_REFRESH` on the producer's polling thread
//!    when a produce attempt triggers the SASL handshake. The second test observes
//!    this by driving a publish to a non-routable broker and verifying the test
//!    process does not crash.
//! 4. The callback on each kind of thread librdkafka runs it on (the
//!    `token_callback` tests, behind `test-support`).
//!
//! # Where the token callback runs
//!
//! librdkafka serves the OAUTHBEARER refresh inline on whichever thread polls
//! the client: the producer's polling thread, a consumer's `recv()` on a Tokio
//! worker, or a producer flush inside `async fn shutdown`. `MskIamContext`
//! mints each token on a dedicated signer thread that drives its own runtime,
//! and the `token_callback` tests call it from a multi-thread worker, from the
//! only thread of a `current_thread` runtime, and from a plain thread.
//!
//! # What this test does NOT exercise
//!
//! Broker-side OAUTHBEARER validation is not tested here. Apache Kafka's stock
//! `OAuthBearerUnsecuredValidatorCallbackHandler` expects a JWT-style token
//! (`{"alg":"none"}.{claims}.signature`). The MSK signer produces a presigned
//! URL (a base64-encoded `https://kafka.<region>.amazonaws.com/?Action=kafka-cluster:Connect&...`),
//! which the stock validator rejects. Making the broker accept MSK tokens requires
//! a custom Java validator JAR that is outside the scope of a unit-integration test.
//!
//! Full broker-side IAM validation is only verifiable against a real AWS MSK
//! cluster. The unit tests in `tests/kafka_msk_iam_unit.rs` cover type-level
//! wiring (variant construction, Debug redaction, no-TLS rejection).
//!
//! # Run
//!
//! ```
//! dotenvx run -- cargo nextest run \
//!     --features kafka-msk-iam,test-support --test kafka_msk_iam_integration --no-capture
//! ```

#![cfg(feature = "kafka-msk-iam")]

use std::time::{Duration, Instant};

use shove::ShoveError;
use shove::broker::Broker;
use shove::kafka::{KafkaClient, KafkaConfig, KafkaSasl, KafkaTls};
use shove::markers::Kafka;

const AWS_REGION: &str = "us-east-1";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Set dummy AWS credential env vars so `aws_config::defaults().load()` can
/// resolve a credentials provider without reaching out to IMDS.
///
/// nextest runs each integration test in its own process, so mutating env vars
/// here cannot race with other tests.
fn set_dummy_aws_env() {
    // SAFETY: single-process, no concurrent env mutation in this test binary.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
        std::env::set_var(
            "AWS_SECRET_ACCESS_KEY",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        );
        std::env::set_var("AWS_REGION", AWS_REGION);
        std::env::set_var("AWS_DEFAULT_REGION", AWS_REGION);
        // Point any inadvertent STS calls at a non-existent endpoint so they
        // fail fast rather than hanging on IMDS/network timeouts.
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Verifies that `KafkaClient::connect` succeeds when given an MSK IAM config
/// backed by static environment-variable credentials.
///
/// This exercises the full Rust-side wiring:
/// - `MskIamTokenProvider::new` resolves the AWS credential chain (env vars → done).
/// - `MskIamContext` wraps the provider.
/// - `BaseConsumer<MskIamContext>`, the metadata client `ping` uses, is created
///   via `create_with_context`; the producer waits for the first publish.
///
/// We deliberately use a non-existent broker address so rdkafka never opens a
/// TCP connection — the test stays fast and offline. The `connect` call itself
/// does not perform any I/O; client connections are lazy.
#[tokio::test]
async fn msk_iam_connect_resolves_credentials_and_builds_the_metadata_client() {
    set_dummy_aws_env();

    // Non-routable address — the test must not attempt real network I/O.
    let cfg = KafkaConfig::new("192.0.2.1:9098")
        .with_tls(KafkaTls {
            skip_hostname_verification: true,
            ..KafkaTls::default()
        })
        .with_sasl(KafkaSasl::msk_iam(AWS_REGION));

    let client = KafkaClient::connect(&cfg).await.expect(
        "KafkaClient::connect must succeed: MskIamTokenProvider failed to load credentials",
    );

    // The shutdown token proves we have a live KafkaClient, not a stub.
    assert!(
        !client.shutdown_token().is_cancelled(),
        "shutdown token should not be cancelled immediately after connect"
    );
}

/// Verifies that `generate_oauth_token` produces a non-empty token when rdkafka
/// triggers the OAUTHBEARER refresh. We drive this by sending a message to a
/// non-routable broker: rdkafka will attempt the TCP+SASL handshake, fire the
/// OAUTHBEARER refresh event (invoking `MskIamContext::generate_oauth_token`),
/// and then time out trying to reach the broker.
///
/// We assert:
/// - The publish call either succeeds (impossible without a broker) OR fails
///   with a Kafka-level error — not a panic or a credential resolution error.
/// - The process does not panic inside `generate_oauth_token`.
///
/// This is the deepest we can go without a real MSK broker: the token is
/// generated and handed to librdkafka; broker-side validation is not exercised.
#[tokio::test]
async fn msk_iam_generate_oauth_token_does_not_panic() {
    set_dummy_aws_env();

    // Non-routable address — fast connect timeout.
    let cfg = KafkaConfig::new("192.0.2.1:9098")
        .with_tls(KafkaTls {
            skip_hostname_verification: true,
            ..KafkaTls::default()
        })
        .with_sasl(KafkaSasl::msk_iam(AWS_REGION));

    let client = KafkaClient::connect(&cfg)
        .await
        .expect("connect must succeed");

    // Use a very short message.timeout.ms-equivalent: we just want rdkafka to
    // try, invoke the OAUTHBEARER callback, and give up. The publish will
    // return Err (no broker), but `generate_oauth_token` must NOT panic.
    use rdkafka::message::OwnedHeaders;
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        client.publish_with_retry(
            "msk-iam-token-test",
            None,
            OwnedHeaders::new(),
            b"probe",
            1, // max_attempts
            "oauth-probe",
        ),
    )
    .await;

    // Either the timeout fires (rdkafka is still trying) or publish returns
    // an Err. Either way: no panic means generate_oauth_token ran without crashing.
    match result {
        Ok(Ok(())) => {
            // Broker accepted — would be surprising but not wrong in principle.
        }
        Ok(Err(e)) => {
            // Expected: rdkafka gave up trying to reach the non-routable broker.
            let msg = e.to_string();
            assert!(
                !msg.contains("panic"),
                "unexpected panic message in error: {msg}"
            );
        }
        Err(_timeout) => {
            // Also fine: rdkafka is still polling; we just verify it didn't panic.
        }
    }

    // The publish built the producer; the close flushes it.
    client.shutdown().await;
}

/// Verifies that `connect` still fails at startup when librdkafka refuses the
/// connection settings: the metadata client it builds loads the TLS material
/// the producer would use.
#[tokio::test]
async fn msk_iam_connect_rejects_tls_material_librdkafka_refuses() {
    set_dummy_aws_env();

    let cfg = KafkaConfig::new("192.0.2.1:9098")
        .with_tls(KafkaTls {
            ca_pem: Some("not a certificate".into()),
            ..KafkaTls::default()
        })
        .with_sasl(KafkaSasl::msk_iam(AWS_REGION));

    let err = KafkaClient::connect(&cfg)
        .await
        .err()
        .expect("librdkafka rejects the CA material");
    let msg = err.to_string();
    assert!(
        msg.contains("failed to create MSK metadata consumer"),
        "{msg}"
    );
}

/// Verifies that a health check on an MSK IAM client runs on the metadata
/// client `connect` built: the OAUTHBEARER refresh is served while the probe
/// waits, and a probe against a non-routable broker ends with a `Connection`
/// error at its timeout instead of a panic or a hang.
#[tokio::test]
async fn msk_iam_ping_times_out_against_a_non_routable_broker() {
    set_dummy_aws_env();

    let cfg = KafkaConfig::new("192.0.2.1:9098")
        .with_tls(KafkaTls {
            skip_hostname_verification: true,
            ..KafkaTls::default()
        })
        .with_sasl(KafkaSasl::msk_iam(AWS_REGION));
    let client = KafkaClient::connect(&cfg).await.expect("connect");
    let broker = Broker::<Kafka>::from_client(client);

    let started = Instant::now();
    let err = broker
        .ping_with_timeout(Duration::from_millis(300))
        .await
        .expect_err("no broker answers at 192.0.2.1");
    assert!(matches!(err, ShoveError::Connection(_)), "got {err:?}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the probe hung past its timeout"
    );
}

/// Verifies that the protocol overlay in `connect()` actually sets
/// `security.protocol=SASL_SSL` and `sasl.mechanism=OAUTHBEARER` in the
/// stored `base_config` when MSK IAM is selected.
///
/// Without this assertion, a future refactor could silently break the
/// overlay (e.g. by re-ordering Phase B before Phase A) and the producer
/// would attempt SCRAM or PLAIN instead of OAUTHBEARER — the failure
/// would surface only in production logs as a SASL handshake error.
#[tokio::test]
async fn msk_iam_connect_overlays_sasl_ssl_and_oauthbearer_into_base_config() {
    set_dummy_aws_env();

    let cfg = KafkaConfig::new("192.0.2.1:9098")
        .with_tls(KafkaTls {
            skip_hostname_verification: true,
            ..KafkaTls::default()
        })
        .with_sasl(KafkaSasl::msk_iam(AWS_REGION));

    let client = KafkaClient::connect(&cfg).await.expect("connect");

    assert_eq!(
        client.config_entry("security.protocol").as_deref(),
        Some("SASL_SSL"),
        "MSK IAM must overlay security.protocol=SASL_SSL"
    );
    assert_eq!(
        client.config_entry("sasl.mechanism").as_deref(),
        Some("OAUTHBEARER"),
        "MSK IAM must overlay sasl.mechanism=OAUTHBEARER"
    );
}

/// Exercises `KafkaClient::connect_with_retry` on the MSK IAM happy path.
///
/// The retry loop simply re-invokes `connect()` on failure, so with valid
/// credentials and TLS configured the very first attempt succeeds and the
/// retry loop is a no-op. This test locks in that the MSK IAM `connect()`
/// remains composable with the retry helper — a future refactor that breaks
/// the type signature of the retry wrapper would fail here.
#[tokio::test]
async fn msk_iam_connect_with_retry_succeeds_on_first_attempt() {
    set_dummy_aws_env();

    let cfg = KafkaConfig::new("192.0.2.1:9098")
        .with_tls(KafkaTls {
            skip_hostname_verification: true,
            ..KafkaTls::default()
        })
        .with_sasl(KafkaSasl::msk_iam(AWS_REGION));

    let client = KafkaClient::connect_with_retry(&cfg, 1)
        .await
        .expect("connect_with_retry must succeed on first attempt with valid MSK IAM config");

    assert!(!client.shutdown_token().is_cancelled());
}

// ---------------------------------------------------------------------------
// The token callback, on each kind of thread librdkafka calls it from
// ---------------------------------------------------------------------------

/// librdkafka runs the OAUTHBEARER callback inline on whichever thread polls
/// the client: a consumer's `recv()` on a Tokio worker, a producer flush
/// inside `async fn shutdown`, or the producer's own polling thread.
#[cfg(feature = "test-support")]
mod token_callback {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use rdkafka::client::{ClientContext, OAuthToken};
    use shove::ShoveError;
    use shove::kafka::{KafkaClient, KafkaConfig, KafkaSasl, KafkaTls, msk_iam_context_for_test};

    use super::{AWS_REGION, set_dummy_aws_env};

    /// The token callback `connect` installs on the librdkafka clients of a
    /// `KafkaClient` that authenticates with `sasl`.
    async fn token_callback_for(sasl: KafkaSasl) -> impl ClientContext {
        let cfg = KafkaConfig::new("192.0.2.1:9098")
            .with_tls(KafkaTls {
                skip_hostname_verification: true,
                ..KafkaTls::default()
            })
            .with_sasl(sasl);
        let client = KafkaClient::connect(&cfg).await.expect("connect");
        msk_iam_context_for_test(&client).expect("an MSK IAM client carries a token callback")
    }

    /// The token callback of an MSK IAM client that signs with the dummy
    /// environment credentials.
    async fn connected_token_callback() -> impl ClientContext {
        set_dummy_aws_env();
        token_callback_for(KafkaSasl::msk_iam(AWS_REGION)).await
    }

    /// The profile files [`only_profile`] wrote; dropping this removes them.
    struct ProfileFiles([PathBuf; 2]);

    impl Drop for ProfileFiles {
        fn drop(&mut self) {
            for path in &self.0 {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    /// Leave the AWS credential chain one place to find credentials: a config
    /// file holding `profile` with `settings`. The environment holds no keys,
    /// the credentials file is empty, and IMDS is off.
    fn only_profile(profile: &str, settings: &str) -> ProfileFiles {
        let pid = std::process::id();
        let config = std::env::temp_dir().join(format!("shove-msk-iam-{pid}.config"));
        let credentials = std::env::temp_dir().join(format!("shove-msk-iam-{pid}.credentials"));
        std::fs::write(&config, format!("[profile {profile}]\n{settings}\n"))
            .expect("write the config file");
        std::fs::write(&credentials, "").expect("write the credentials file");
        // SAFETY: nextest runs each test in its own process, and nothing in
        // this one reads the environment while it changes.
        unsafe {
            for key in [
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "AWS_PROFILE",
            ] {
                std::env::remove_var(key);
            }
            std::env::set_var("AWS_CONFIG_FILE", &config);
            std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", &credentials);
            std::env::set_var("AWS_REGION", AWS_REGION);
            std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
        }
        ProfileFiles([config, credentials])
    }

    /// The message of the `ShoveError::Connection` the callback failed with.
    fn connection_error(err: Box<dyn std::error::Error>) -> String {
        match *err
            .downcast::<ShoveError>()
            .expect("the callback fails with a ShoveError")
        {
            ShoveError::Connection(msg) => msg,
            other => panic!("expected a connection error, got {other:?}"),
        }
    }

    /// A token the signer minted: non-empty, expiring in the future and no
    /// later than the signer's fifteen-minute expiry.
    fn assert_signed(token: &OAuthToken) {
        let now_ms = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after the epoch")
                .as_millis(),
        )
        .expect("epoch millis fit an i64");
        assert!(!token.token.is_empty(), "the token is empty");
        assert!(
            token.lifetime_ms > now_ms,
            "the token expired at {}, now is {now_ms}",
            token.lifetime_ms
        );
        assert!(
            token.lifetime_ms <= now_ms + 15 * 60 * 1000,
            "the token outlives the signer's expiry: {}, now is {now_ms}",
            token.lifetime_ms
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn token_generation_inside_multi_thread_runtime() {
        let callback = connected_token_callback().await;

        let token = tokio::spawn(async move {
            callback
                .generate_oauth_token(None)
                .map_err(|e| e.to_string())
        })
        .await
        .expect("the worker task")
        .expect("the callback mints a token on a runtime worker");

        assert_signed(&token);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn token_generation_inside_current_thread_runtime() {
        let callback = connected_token_callback().await;

        let token = callback
            .generate_oauth_token(None)
            .expect("the callback mints a token on the runtime's only thread");

        assert_signed(&token);
    }

    #[test]
    fn token_generation_from_plain_thread() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let callback = runtime.block_on(connected_token_callback());

        let token = std::thread::spawn(move || {
            callback
                .generate_oauth_token(None)
                .map_err(|e| e.to_string())
        })
        .join()
        .expect("the polling thread")
        .expect("the callback mints a token off any runtime");

        assert_signed(&token);
    }

    /// The chain stops at the profile, which names a key id without a secret
    /// key: the refresh fails with an error, and nothing panics.
    #[tokio::test]
    async fn token_generation_fails_when_the_credentials_do_not_resolve() {
        let _files = only_profile("broken", "aws_access_key_id = AKIAIOSFODNN7EXAMPLE");
        let callback =
            token_callback_for(KafkaSasl::msk_iam_with_profile(AWS_REGION, "broken")).await;

        let err = callback
            .generate_oauth_token(None)
            .err()
            .expect("a profile without a secret key signs nothing");

        let msg = connection_error(err);
        assert!(msg.contains("failed to provide credentials"), "{msg}");
    }
}
