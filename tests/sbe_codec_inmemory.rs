#![cfg(all(feature = "sbe", feature = "inmemory"))]

//! End-to-end SBE codec tests over the in-memory broker: a topic carrying
//! `SbeFrame<T>` publishes and consumes without re-encoding, and the handler
//! reads fields flyweight-style from the received frame. The hand-rolled
//! encoder/decoder below stands in for `sbe-tool`-generated code.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use shove::broker::Broker;
use shove::inmemory::InMemoryConfig;
use shove::markers::InMemory;
use shove::{
    ConsumerOptions, MessageHandler, MessageMetadata, Outcome, SbeCodec, SbeFrame, SbeHeader,
    SbeMessage, Topic, TopologyBuilder,
};

// ---------------------------------------------------------------------------
// Stand-in for sbe-tool output: <order> = price:u64, quantity:u64
// ---------------------------------------------------------------------------

struct Order;
impl SbeMessage for Order {
    const SCHEMA_ID: u16 = 42;
    const TEMPLATE_ID: u16 = 7;
}

const ORDER_BLOCK_LENGTH: u16 = 16;
const ORDER_SCHEMA_VERSION: u16 = 1;

fn encode_order(price: u64, quantity: u64) -> SbeFrame<Order> {
    let header = SbeHeader {
        block_length: ORDER_BLOCK_LENGTH,
        template_id: Order::TEMPLATE_ID,
        schema_id: Order::SCHEMA_ID,
        version: ORDER_SCHEMA_VERSION,
    };
    let mut frame = header.to_bytes(Order::BYTE_ORDER).to_vec();
    frame.extend_from_slice(&price.to_le_bytes());
    frame.extend_from_slice(&quantity.to_le_bytes());
    SbeFrame::new(frame).expect("frame carries Order ids")
}

fn decode_order(frame: &SbeFrame<Order>) -> (u64, u64) {
    let field = |offset: usize| {
        frame
            .body()
            .get(offset..offset + 8)
            .and_then(|b| b.try_into().ok())
            .map(u64::from_le_bytes)
            .expect("body holds two u64 fields")
    };
    (field(0), field(8))
}

// ---------------------------------------------------------------------------
// Topic
// ---------------------------------------------------------------------------

struct SbeOrdersTopic;
impl Topic for SbeOrdersTopic {
    type Message = SbeFrame<Order>;
    type Codec = SbeCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("sbe-orders-int").dlq().build())
    }
}

// The documented macro form: a generic message type with an explicit codec.
shove::define_topic!(
    SbeMacroTopic,
    SbeFrame<Order>,
    TopologyBuilder::new("sbe-macro-int").build(),
    codec = SbeCodec
);

#[test]
fn define_topic_accepts_sbe_frame_message_and_codec() {
    assert_eq!(
        <<SbeMacroTopic as Topic>::Codec as shove::Codec<SbeFrame<Order>>>::NAME,
        "sbe"
    );
    assert_eq!(SbeMacroTopic::topology().queue(), "sbe-macro-int");
}

async fn poll_until<F: Fn() -> bool>(cond: F, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    cond()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn end_to_end_publish_consume_flyweight_decode() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker.topology().declare::<SbeOrdersTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    for i in 1..=3u64 {
        publisher
            .publish::<SbeOrdersTopic>(&encode_order(i * 100, i))
            .await
            .unwrap();
    }

    let seen: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));

    #[derive(Clone)]
    struct H(Arc<Mutex<Vec<(u64, u64)>>>);
    impl MessageHandler<SbeOrdersTopic> for H {
        type Context = ();
        async fn handle(&self, msg: SbeFrame<Order>, _: MessageMetadata, _: &()) -> Outcome {
            assert_eq!(msg.header().version, ORDER_SCHEMA_VERSION);
            assert_eq!(msg.header().block_length, ORDER_BLOCK_LENGTH);
            self.0.lock().await.push(decode_order(&msg));
            Outcome::Ack
        }
    }

    let mut supervisor = broker.consumer_supervisor();
    let opts = ConsumerOptions::<InMemory>::new().with_shutdown(CancellationToken::new());
    supervisor
        .register::<SbeOrdersTopic, _>(H(seen.clone()), opts)
        .unwrap();

    let token = supervisor.cancellation_token();
    let seen_probe = seen.clone();
    let t = token.clone();
    tokio::spawn(async move {
        poll_until(
            move || seen_probe.try_lock().map(|v| v.len() == 3).unwrap_or(false),
            Duration::from_secs(2),
        )
        .await;
        t.cancel();
    });

    let outcome = supervisor
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(5))
        .await;
    assert!(outcome.is_clean());

    let mut orders = seen.lock().await.clone();
    orders.sort_unstable();
    assert_eq!(orders, vec![(100, 1), (200, 2), (300, 3)]);
}

#[tokio::test]
async fn frame_buffer_is_shared_from_publish_to_handler() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker.topology().declare::<SbeOrdersTopic>().await.unwrap();

    let frame = encode_order(250_000, 12);
    let published_ptr = frame.as_bytes().as_ptr() as usize;

    broker
        .publisher()
        .await
        .unwrap()
        .publish::<SbeOrdersTopic>(&frame)
        .await
        .unwrap();

    let received_ptr = Arc::new(AtomicUsize::new(0));

    #[derive(Clone)]
    struct H(Arc<AtomicUsize>);
    impl MessageHandler<SbeOrdersTopic> for H {
        type Context = ();
        async fn handle(&self, msg: SbeFrame<Order>, _: MessageMetadata, _: &()) -> Outcome {
            self.0
                .store(msg.as_bytes().as_ptr() as usize, Ordering::SeqCst);
            Outcome::Ack
        }
    }

    let mut supervisor = broker.consumer_supervisor();
    let opts = ConsumerOptions::<InMemory>::new().with_shutdown(CancellationToken::new());
    supervisor
        .register::<SbeOrdersTopic, _>(H(received_ptr.clone()), opts)
        .unwrap();

    let token = supervisor.cancellation_token();
    let ptr_probe = received_ptr.clone();
    let t = token.clone();
    tokio::spawn(async move {
        poll_until(
            move || ptr_probe.load(Ordering::SeqCst) != 0,
            Duration::from_secs(2),
        )
        .await;
        t.cancel();
    });

    let outcome = supervisor
        .run_until_timeout(token.cancelled_owned(), Duration::from_secs(5))
        .await;
    assert!(outcome.is_clean());

    assert_ne!(received_ptr.load(Ordering::SeqCst), 0, "message not seen");
    assert_eq!(
        received_ptr.load(Ordering::SeqCst),
        published_ptr,
        "in-memory delivery must hand the handler the same buffer that was published"
    );
}
