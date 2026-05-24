//! FIFO (sequenced) Redis Streams publish/consume example.
//!
//! Demonstrates per-account ordering using `SequencedTopic` + `register_fifo`.
//! Messages for the same account key are always delivered to the same shard and
//! processed in strict publish order, regardless of how many consumer tasks run.
//!
//! Run with:
//!   docker run --rm -p 6379:6379 redis:7-alpine
//!   cargo run --example redis_sequenced --features redis-streams

use std::sync::OnceLock;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use shove::consumer_group::ConsumerGroupConfig;
use shove::redis::{RedisConfig, RedisConsumerGroupConfig, RedisMode};
use shove::{
    Broker, JsonCodec, MessageHandler, MessageMetadata, Outcome, Redis, SequenceFailure,
    SequencedTopic, Topic, TopologyBuilder,
};

// ---------------------------------------------------------------------------
// Message type
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerEntry {
    account_id: String,
    amount_cents: i64,
    seq: u64,
}

// ---------------------------------------------------------------------------
// Topic definition
// ---------------------------------------------------------------------------

struct Ledger;

impl Topic for Ledger {
    type Message = LedgerEntry;
    type Codec = JsonCodec;

    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("ledger")
                .sequenced(SequenceFailure::Skip)
                .routing_shards(4)
                .build()
        })
    }

    const SEQUENCE_KEY_FN: Option<fn(&Self::Message) -> String> = Some(Ledger::sequence_key);
}

impl SequencedTopic for Ledger {
    fn sequence_key(msg: &LedgerEntry) -> String {
        msg.account_id.clone()
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

struct Handler;

impl MessageHandler<Ledger> for Handler {
    type Context = ();

    async fn handle(&self, msg: LedgerEntry, _: MessageMetadata, _: &()) -> Outcome {
        println!(
            "account={} seq={:03} amount={:+}",
            msg.account_id, msg.seq, msg.amount_cents
        );
        Outcome::Ack
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), shove::ShoveError> {
    tracing_subscriber::fmt::init();

    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());

    let broker = Broker::<Redis>::new(RedisConfig {
        mode: RedisMode::Standalone { url },
        group: None,
    })
    .await?;

    broker.topology().declare::<Ledger>().await?;

    // Publish interleaved entries for three accounts.
    let publisher = broker.publisher().await?;
    let entries = [
        ("acct-001", 1000, 0),
        ("acct-002", 2000, 0),
        ("acct-001", -500, 1),
        ("acct-003", 750, 0),
        ("acct-002", -200, 1),
        ("acct-001", 300, 2),
        ("acct-003", -100, 1),
    ];

    for (account_id, amount_cents, seq) in entries {
        publisher
            .publish::<Ledger>(&LedgerEntry {
                account_id: account_id.into(),
                amount_cents,
                seq,
            })
            .await?;
        println!("published account={account_id} seq={seq:03}");
    }

    // Consume with FIFO (one task per shard — entries for the same account
    // are always processed in the order they were published).
    let mut group = broker.consumer_group();
    group
        .register_fifo::<Ledger, _>(
            ConsumerGroupConfig::new(RedisConsumerGroupConfig::default()),
            || Handler,
        )
        .await?;

    println!("consuming — press Ctrl-C to stop");
    let outcome = group
        .run_until_timeout(
            async {
                tokio::signal::ctrl_c().await.ok();
            },
            Duration::from_secs(5),
        )
        .await;

    std::process::exit(outcome.exit_code());
}
