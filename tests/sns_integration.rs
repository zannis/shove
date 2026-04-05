//! Integration tests for the SNS publisher backend.
//!
//! Each test spins up a fresh LocalStack container via testcontainers, runs
//! the test, and drops the container on completion (automatic cleanup).
//!
//! Run with: `cargo test --features sns --test sns_integration`

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use shove::sns::*;
use shove::*;

use testcontainers::GenericImage;
use testcontainers::runners::AsyncRunner;

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

struct TestBroker {
    #[allow(dead_code)]
    container: testcontainers::ContainerAsync<GenericImage>,
    endpoint_url: String,
}

impl TestBroker {
    async fn start() -> Self {
        // Set dummy credentials for LocalStack/floci (LocalStack-compatible).
        // SAFETY: tests run single-threaded (--test-threads=1) so mutating the
        // environment does not cause data races.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
            std::env::set_var("AWS_REGION", "us-east-1");
        }

        let container = GenericImage::new("hectorvent/floci", "latest")
            .with_exposed_port(4566u16.into())
            .start()
            .await
            .expect("failed to start floci container");

        let port = container
            .get_host_port_ipv4(4566)
            .await
            .expect("failed to get floci port");

        let endpoint_url = format!("http://localhost:{port}");

        // Give floci a moment to initialize
        tokio::time::sleep(Duration::from_secs(2)).await;

        Self {
            container,
            endpoint_url,
        }
    }

    fn sns_config(&self) -> SnsConfig {
        SnsConfig {
            region: Some("us-east-1".into()),
            endpoint_url: Some(self.endpoint_url.clone()),
        }
    }

    /// Create an SQS client for test verification.
    async fn sqs_client(&self) -> aws_sdk_sqs::Client {
        let aws_config = aws_config::from_env()
            .region(aws_config::Region::new("us-east-1"))
            .endpoint_url(&self.endpoint_url)
            .load()
            .await;
        aws_sdk_sqs::Client::new(&aws_config)
    }

    /// Create a raw SNS client for test setup (subscribe operations).
    /// SnsClient::inner() is pub(crate), so we build our own here.
    async fn raw_sns_client(&self) -> aws_sdk_sns::Client {
        let aws_config = aws_config::from_env()
            .region(aws_config::Region::new("us-east-1"))
            .endpoint_url(&self.endpoint_url)
            .load()
            .await;
        aws_sdk_sns::Client::new(&aws_config)
    }

    /// Helper: create an SQS queue and subscribe it to an SNS topic.
    /// Returns the SQS queue URL.
    async fn subscribe_sqs_to_sns(
        &self,
        raw_sns: &aws_sdk_sns::Client,
        sqs_client: &aws_sdk_sqs::Client,
        topic_arn: &str,
        queue_name: &str,
    ) -> String {
        // Create SQS queue
        let mut create_req = sqs_client.create_queue().queue_name(queue_name);

        if queue_name.ends_with(".fifo") {
            create_req = create_req
                .attributes(aws_sdk_sqs::types::QueueAttributeName::FifoQueue, "true")
                .attributes(
                    aws_sdk_sqs::types::QueueAttributeName::ContentBasedDeduplication,
                    "true",
                );
        }

        let sqs_result = create_req.send().await.expect("failed to create SQS queue");
        let queue_url = sqs_result.queue_url().expect("no queue URL").to_string();

        // Get queue ARN
        let attrs_result = sqs_client
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
            .send()
            .await
            .expect("failed to get queue attributes");

        let queue_arn = attrs_result
            .attributes()
            .expect("no attributes map")
            .get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn)
            .expect("no queue ARN")
            .to_string();

        // Set SQS queue policy to allow SNS to send messages
        let policy = format!(
            r#"{{"Statement":[{{"Effect":"Allow","Principal":"*","Action":"sqs:SendMessage","Resource":"{queue_arn}","Condition":{{"ArnEquals":{{"aws:SourceArn":"{topic_arn}"}}}}}}]}}"#
        );
        sqs_client
            .set_queue_attributes()
            .queue_url(&queue_url)
            .attributes(aws_sdk_sqs::types::QueueAttributeName::Policy, policy)
            .send()
            .await
            .expect("failed to set queue policy");

        // Subscribe SQS to SNS
        raw_sns
            .subscribe()
            .topic_arn(topic_arn)
            .protocol("sqs")
            .endpoint(&queue_arn)
            .send()
            .await
            .expect("failed to subscribe SQS to SNS");

        queue_url
    }

    /// Helper: receive messages from SQS queue with a timeout.
    async fn receive_messages(
        &self,
        sqs_client: &aws_sdk_sqs::Client,
        queue_url: &str,
        expected_count: usize,
        timeout: Duration,
    ) -> Vec<aws_sdk_sqs::types::Message> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut all_messages = Vec::new();

        while all_messages.len() < expected_count && tokio::time::Instant::now() < deadline {
            let result = sqs_client
                .receive_message()
                .queue_url(queue_url)
                .max_number_of_messages(10)
                .wait_time_seconds(1)
                .message_attribute_names("All")
                .send()
                .await
                .expect("failed to receive SQS messages");

            let msgs = result.messages();
            all_messages.extend(msgs.iter().cloned());
        }

        all_messages
    }
}

// ---------------------------------------------------------------------------
// Test topics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
struct SimpleMessage {
    id: String,
    content: String,
}

define_topic!(
    SimpleWork,
    SimpleMessage,
    TopologyBuilder::new("simple-work").build()
);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
struct OrderMessage {
    order_id: String,
    amount: u64,
}

define_sequenced_topic!(
    OrderTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new("orders")
        .sequenced(SequenceFailure::FailAll)
        .routing_shards(2)
        .hold_queue(Duration::from_secs(5))
        .dlq()
        .build()
);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn publish_single_message() {
    let broker = TestBroker::start().await;
    let client = SnsClient::new(&broker.sns_config())
        .await
        .expect("failed to create SNS client");

    let registry = Arc::new(TopicRegistry::new());
    let declarer = SnsTopologyDeclarer::new(client.clone(), registry.clone());
    declare_topic::<SimpleWork>(&declarer)
        .await
        .expect("failed to declare topology");

    let publisher = SnsPublisher::new(client.clone(), registry.clone());

    let msg = SimpleMessage {
        id: "msg-1".into(),
        content: "hello".into(),
    };

    // Subscribe an SQS queue for verification
    let sqs_client = broker.sqs_client().await;
    let raw_sns = broker.raw_sns_client().await;
    let topic_arn = registry.get("simple-work").await.expect("ARN should exist");
    let queue_url = broker
        .subscribe_sqs_to_sns(&raw_sns, &sqs_client, &topic_arn, "simple-work-verify")
        .await;

    publisher
        .publish::<SimpleWork>(&msg)
        .await
        .expect("publish should succeed");

    let received = broker
        .receive_messages(&sqs_client, &queue_url, 1, Duration::from_secs(10))
        .await;

    assert_eq!(
        received.len(),
        1,
        "expected 1 message, got {}",
        received.len()
    );
}

#[tokio::test]
async fn publish_with_headers() {
    let broker = TestBroker::start().await;
    let client = SnsClient::new(&broker.sns_config())
        .await
        .expect("failed to create SNS client");

    let registry = Arc::new(TopicRegistry::new());
    let declarer = SnsTopologyDeclarer::new(client.clone(), registry.clone());
    declare_topic::<SimpleWork>(&declarer)
        .await
        .expect("failed to declare topology");

    let publisher = SnsPublisher::new(client.clone(), registry.clone());

    let sqs_client = broker.sqs_client().await;
    let raw_sns = broker.raw_sns_client().await;
    let topic_arn = registry.get("simple-work").await.expect("ARN should exist");
    let queue_url = broker
        .subscribe_sqs_to_sns(&raw_sns, &sqs_client, &topic_arn, "headers-verify")
        .await;

    let mut headers = HashMap::new();
    headers.insert("x-trace-id".to_string(), "trace-123".to_string());

    let msg = SimpleMessage {
        id: "msg-h".into(),
        content: "with headers".into(),
    };

    publisher
        .publish_with_headers::<SimpleWork>(&msg, headers)
        .await
        .expect("publish with headers should succeed");

    let received = broker
        .receive_messages(&sqs_client, &queue_url, 1, Duration::from_secs(10))
        .await;

    assert_eq!(received.len(), 1);
}

#[tokio::test]
async fn publish_batch_under_limit() {
    let broker = TestBroker::start().await;
    let client = SnsClient::new(&broker.sns_config())
        .await
        .expect("failed to create SNS client");

    let registry = Arc::new(TopicRegistry::new());
    let declarer = SnsTopologyDeclarer::new(client.clone(), registry.clone());
    declare_topic::<SimpleWork>(&declarer)
        .await
        .expect("failed to declare topology");

    let publisher = SnsPublisher::new(client.clone(), registry.clone());

    let sqs_client = broker.sqs_client().await;
    let raw_sns = broker.raw_sns_client().await;
    let topic_arn = registry.get("simple-work").await.expect("ARN should exist");
    let queue_url = broker
        .subscribe_sqs_to_sns(&raw_sns, &sqs_client, &topic_arn, "batch-under-verify")
        .await;

    let messages: Vec<SimpleMessage> = (0..5)
        .map(|i| SimpleMessage {
            id: format!("batch-{i}"),
            content: format!("message {i}"),
        })
        .collect();

    publisher
        .publish_batch::<SimpleWork>(&messages)
        .await
        .expect("batch publish should succeed");

    let received = broker
        .receive_messages(&sqs_client, &queue_url, 5, Duration::from_secs(10))
        .await;

    assert_eq!(
        received.len(),
        5,
        "expected 5 messages, got {}",
        received.len()
    );
}

#[tokio::test]
async fn publish_batch_over_limit_chunks() {
    let broker = TestBroker::start().await;
    let client = SnsClient::new(&broker.sns_config())
        .await
        .expect("failed to create SNS client");

    let registry = Arc::new(TopicRegistry::new());
    let declarer = SnsTopologyDeclarer::new(client.clone(), registry.clone());
    declare_topic::<SimpleWork>(&declarer)
        .await
        .expect("failed to declare topology");

    let publisher = SnsPublisher::new(client.clone(), registry.clone());

    let sqs_client = broker.sqs_client().await;
    let raw_sns = broker.raw_sns_client().await;
    let topic_arn = registry.get("simple-work").await.expect("ARN should exist");
    let queue_url = broker
        .subscribe_sqs_to_sns(&raw_sns, &sqs_client, &topic_arn, "batch-over-verify")
        .await;

    let messages: Vec<SimpleMessage> = (0..23)
        .map(|i| SimpleMessage {
            id: format!("big-{i}"),
            content: format!("message {i}"),
        })
        .collect();

    publisher
        .publish_batch::<SimpleWork>(&messages)
        .await
        .expect("batch publish over limit should succeed");

    let received = broker
        .receive_messages(&sqs_client, &queue_url, 23, Duration::from_secs(15))
        .await;

    assert_eq!(
        received.len(),
        23,
        "expected 23 messages, got {}",
        received.len()
    );
}

#[tokio::test]
async fn publish_sequenced_sets_group_id() {
    let broker = TestBroker::start().await;
    let client = SnsClient::new(&broker.sns_config())
        .await
        .expect("failed to create SNS client");

    let registry = Arc::new(TopicRegistry::new());
    let declarer = SnsTopologyDeclarer::new(client.clone(), registry.clone());
    declare_topic::<OrderTopic>(&declarer)
        .await
        .expect("failed to declare FIFO topology");

    let publisher = SnsPublisher::new(client.clone(), registry.clone());

    let sqs_client = broker.sqs_client().await;
    let raw_sns = broker.raw_sns_client().await;
    let topic_arn = registry.get("orders").await.expect("ARN should exist");
    let queue_url = broker
        .subscribe_sqs_to_sns(&raw_sns, &sqs_client, &topic_arn, "orders-verify.fifo")
        .await;

    let msg = OrderMessage {
        order_id: "ORD-001".into(),
        amount: 100,
    };

    publisher
        .publish::<OrderTopic>(&msg)
        .await
        .expect("sequenced publish should succeed");

    let received = broker
        .receive_messages(&sqs_client, &queue_url, 1, Duration::from_secs(10))
        .await;

    assert_eq!(received.len(), 1, "expected 1 sequenced message");
}

#[tokio::test]
async fn topology_declares_standard_topic() {
    let broker = TestBroker::start().await;
    let client = SnsClient::new(&broker.sns_config())
        .await
        .expect("failed to create SNS client");

    let registry = Arc::new(TopicRegistry::new());
    let declarer = SnsTopologyDeclarer::new(client.clone(), registry.clone());

    declare_topic::<SimpleWork>(&declarer)
        .await
        .expect("standard topology declaration should succeed");

    let arn = registry.get("simple-work").await;
    assert!(arn.is_some(), "ARN should be registered after declaration");
    assert!(
        arn.as_ref().unwrap().contains("simple-work"),
        "ARN should contain the topic name"
    );
}

#[tokio::test]
async fn topology_declares_fifo_topic() {
    let broker = TestBroker::start().await;
    let client = SnsClient::new(&broker.sns_config())
        .await
        .expect("failed to create SNS client");

    let registry = Arc::new(TopicRegistry::new());
    let declarer = SnsTopologyDeclarer::new(client.clone(), registry.clone());

    declare_topic::<OrderTopic>(&declarer)
        .await
        .expect("FIFO topology declaration should succeed");

    let arn = registry.get("orders").await;
    assert!(arn.is_some(), "ARN should be registered after declaration");
    assert!(
        arn.as_ref().unwrap().contains("orders.fifo"),
        "FIFO ARN should contain .fifo suffix"
    );
}

#[tokio::test]
async fn topology_idempotent_declaration() {
    let broker = TestBroker::start().await;
    let client = SnsClient::new(&broker.sns_config())
        .await
        .expect("failed to create SNS client");

    let registry = Arc::new(TopicRegistry::new());
    let declarer = SnsTopologyDeclarer::new(client.clone(), registry.clone());

    // Declare twice — should not error
    declare_topic::<SimpleWork>(&declarer)
        .await
        .expect("first declaration");
    declare_topic::<SimpleWork>(&declarer)
        .await
        .expect("second declaration should be idempotent");
}

#[tokio::test]
async fn publish_to_undeclared_topic_fails() {
    let broker = TestBroker::start().await;
    let client = SnsClient::new(&broker.sns_config())
        .await
        .expect("failed to create SNS client");

    let registry = Arc::new(TopicRegistry::new());
    // Do NOT declare topology
    let publisher = SnsPublisher::new(client, registry);

    let msg = SimpleMessage {
        id: "msg-fail".into(),
        content: "should fail".into(),
    };

    let result = publisher.publish::<SimpleWork>(&msg).await;
    assert!(
        result.is_err(),
        "publishing to undeclared topic should fail"
    );

    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("no SNS topic ARN registered"),
        "error should mention missing ARN, got: {err}"
    );
}
