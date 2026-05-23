#![cfg(feature = "kafka-msk-iam")]

use shove::kafka::KafkaSasl;

#[test]
fn msk_iam_constructor_sets_region_only() {
    let sasl = KafkaSasl::msk_iam("eu-west-2");
    match sasl {
        KafkaSasl::MskIam { region, profile } => {
            assert_eq!(region, "eu-west-2");
            assert!(profile.is_none());
        }
        _ => panic!("expected MskIam variant"),
    }
}

#[test]
fn msk_iam_constructor_with_profile() {
    let sasl = KafkaSasl::msk_iam_with_profile("us-east-1", "prod");
    match sasl {
        KafkaSasl::MskIam { region, profile } => {
            assert_eq!(region, "us-east-1");
            assert_eq!(profile.as_deref(), Some("prod"));
        }
        _ => panic!("expected MskIam variant"),
    }
}

#[test]
fn msk_iam_debug_shows_region_and_profile_not_secrets() {
    let sasl = KafkaSasl::msk_iam_with_profile("eu-west-2", "my-profile");
    let rendered = format!("{sasl:?}");
    assert!(rendered.contains("eu-west-2"));
    assert!(rendered.contains("my-profile"));
    assert!(!rendered.contains("password"));
}

#[tokio::test]
async fn msk_iam_without_tls_is_rejected_at_connect() {
    use shove::kafka::{KafkaClient, KafkaConfig};

    // No TLS configured; MSK IAM must refuse with a Topology error before
    // any network I/O so misconfigurations fail fast and visibly.
    let cfg =
        KafkaConfig::new("broker.example.invalid:9098").with_sasl(KafkaSasl::msk_iam("eu-west-2"));

    let result = KafkaClient::connect(&cfg).await;
    assert!(result.is_err(), "connect must reject MSK IAM without TLS");
    let err = result.err().unwrap();

    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("tls"),
        "error message should mention TLS, got: {msg}"
    );
}
