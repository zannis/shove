//! Sequenced (per-key FIFO) delivery with `SequenceFailure::FailAll`.
//!
//! Note: per-key FIFO consumption (`run_fifo`) isn't yet surfaced on the
//! generic `Broker<B>` / `ConsumerSupervisor<B>` / `ConsumerGroup<B>`
//! wrappers — this example therefore keeps using the backend-specific
//! `InMemoryConsumer::run_fifo` directly.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use shove::inmemory::{
    InMemoryBroker, InMemoryConsumer, InMemoryPublisher, InMemoryTopologyDeclarer,
};
use shove::{
    ConsumerOptions, InMemory, MessageHandler, MessageMetadata, Outcome, SequenceFailure,
    SequencedTopic, Topic, TopologyBuilder,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerEntry {
    account: String,
    seq: u64,
    amount: i64,
}

struct LedgerTopic;
impl Topic for LedgerTopic {
    type Message = LedgerEntry;
    fn topology() -> &'static shove::QueueTopology {
        static T: std::sync::OnceLock<shove::QueueTopology> = std::sync::OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("ledger")
                .sequenced(SequenceFailure::FailAll)
                .routing_shards(4)
                .hold_queue(Duration::from_millis(50))
                .dlq()
                .build()
        })
    }
    const SEQUENCE_KEY_FN: Option<fn(&Self::Message) -> String> = Some(Self::sequence_key);
}
impl SequencedTopic for LedgerTopic {
    fn sequence_key(msg: &LedgerEntry) -> String {
        msg.account.clone()
    }
}

#[derive(Clone)]
struct Handler {
    acked: Arc<AtomicUsize>,
}
impl MessageHandler<LedgerTopic> for Handler {
    type Context = ();
    async fn handle(&self, msg: LedgerEntry, _: MessageMetadata, _: &()) -> Outcome {
        println!(
            "applied entry account={} seq={} amount={:+}",
            msg.account, msg.seq, msg.amount
        );
        self.acked.fetch_add(1, Ordering::Relaxed);
        Outcome::Ack
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let broker = InMemoryBroker::new();
    let declarer = InMemoryTopologyDeclarer::new(broker.clone());
    declarer.declare(LedgerTopic::topology()).await.unwrap();

    let acked = Arc::new(AtomicUsize::new(0));
    let handler = Handler {
        acked: acked.clone(),
    };

    let shutdown = CancellationToken::new();
    let consumer = InMemoryConsumer::new(broker.clone());
    let shutdown_for_task = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<InMemory>::new()
            .with_shutdown(shutdown_for_task)
            .with_prefetch_count(1);
        consumer.run_fifo::<LedgerTopic, _>(handler, (), opts).await
    });

    let publisher = InMemoryPublisher::new(broker.clone());
    for seq in 0..5 {
        for account in ["alice", "bob", "carol"] {
            publisher
                .publish::<LedgerTopic>(&LedgerEntry {
                    account: account.into(),
                    seq,
                    amount: 100,
                })
                .await
                .unwrap();
        }
    }

    while acked.load(Ordering::Relaxed) < 15 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    shutdown.cancel();
    let _ = consume_handle.await;
}
