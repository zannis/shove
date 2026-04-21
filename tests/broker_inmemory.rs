//! Smoke test for `Broker<InMemory>`.
#![cfg(feature = "inmemory")]

use std::time::Duration;

use shove::broker::Broker;
use shove::inmemory::InMemoryConfig;
use shove::markers::InMemory;

#[tokio::test]
async fn broker_connects_and_closes() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker.close().await;
}

#[tokio::test]
async fn broker_publisher_publishes() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    // Just test that we can create a publisher without error.
    let _publisher = broker.publisher().await.unwrap();
    broker.close().await;
}

#[tokio::test]
async fn broker_topology_declares() {
    use shove::topic::Topic;
    use shove::topology::{QueueTopology, TopologyBuilder};

    struct SmokeTopic;
    impl Topic for SmokeTopic {
        type Message = String;
        fn topology() -> &'static QueueTopology {
            static T: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            T.get_or_init(|| TopologyBuilder::new("smoke-test").build())
        }
    }

    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker.topology().declare::<SmokeTopic>().await.unwrap();
    broker.close().await;
}

#[tokio::test]
async fn broker_consumer_supervisor_runs_empty() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    let supervisor = broker.consumer_supervisor();
    // No handlers registered -- run_until_timeout should return immediately when signal fires.
    let token = supervisor.cancellation_token();
    token.cancel(); // fire immediately
    let signal = token.clone().cancelled_owned();
    let outcome = supervisor
        .run_until_timeout(signal, Duration::from_secs(1))
        .await;
    assert!(outcome.is_clean());
    broker.close().await;
}

#[tokio::test]
async fn broker_consumer_group_exists_for_inmemory() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    // InMemory implements HasCoordinatedGroups, so this compiles.
    let _group = broker.consumer_group();
    broker.close().await;
}

/// The drain-timeout branch of `ConsumerSupervisor::run_until_timeout`:
/// a handler that refuses to finish forces the supervisor to abort, yielding
/// `timed_out: true`. Covers the timeout arm of the generic supervisor.
#[tokio::test]
async fn supervisor_drain_timeout_surfaces_in_outcome() {
    use shove::consumer::ConsumerOptions;
    use shove::handler::MessageHandler;
    use shove::metadata::MessageMetadata;
    use shove::outcome::Outcome;
    use shove::topic::Topic;
    use shove::topology::{QueueTopology, TopologyBuilder};

    struct SlowTopic;
    impl Topic for SlowTopic {
        type Message = String;
        fn topology() -> &'static QueueTopology {
            static T: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            T.get_or_init(|| TopologyBuilder::new("drain-timeout").build())
        }
    }

    struct StuckHandler;
    impl MessageHandler<SlowTopic> for StuckHandler {
        type Context = ();
        async fn handle(&self, _: String, _: MessageMetadata, _: &()) -> Outcome {
            // Park forever — on cancel the consumer loop still exits, but if
            // a handler is mid-flight the drain arm waits for it.
            std::future::pending::<()>().await;
            Outcome::Ack
        }
    }

    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker.topology().declare::<SlowTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<SlowTopic>(&"hang".to_string())
        .await
        .unwrap();

    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<SlowTopic, _>(StuckHandler, ConsumerOptions::new())
        .unwrap();
    let token = supervisor.cancellation_token();

    // Let the stuck handler pick up the message before we trigger shutdown.
    tokio::time::sleep(Duration::from_millis(100)).await;
    token.cancel();

    let signal = std::future::pending::<()>();
    let outcome = supervisor
        .run_until_timeout(signal, Duration::from_millis(200))
        .await;
    assert!(outcome.timed_out, "expected drain timeout, got {outcome:?}");
    assert_eq!(outcome.exit_code(), 3);

    broker.close().await;
}

/// Regression test: a handler whose `Context` is a non-unit `Arc<AppState>`
/// must compile through both `ConsumerSupervisor::register` and
/// `ConsumerGroup::register` once the `with_context` fluent API has been
/// applied, and the handler's `handle` call must receive the stored context.
#[tokio::test]
async fn non_unit_context_plumbs_through_supervisor_and_group() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use shove::consumer::ConsumerOptions;
    use shove::consumer_group::ConsumerGroupConfig;
    use shove::handler::MessageHandler;
    use shove::inmemory::InMemoryConsumerGroupConfig;
    use shove::metadata::MessageMetadata;
    use shove::outcome::Outcome;
    use shove::topic::Topic;
    use shove::topology::{QueueTopology, TopologyBuilder};

    #[derive(Clone)]
    struct AppState {
        counter: Arc<AtomicU32>,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct Msg {
        n: u32,
    }

    struct SupTopic;
    impl Topic for SupTopic {
        type Message = Msg;
        fn topology() -> &'static QueueTopology {
            static T: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            T.get_or_init(|| TopologyBuilder::new("ctx-supervisor").build())
        }
    }

    struct GroupTopic;
    impl Topic for GroupTopic {
        type Message = Msg;
        fn topology() -> &'static QueueTopology {
            static T: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            T.get_or_init(|| TopologyBuilder::new("ctx-group").build())
        }
    }

    struct CountingHandler;
    impl MessageHandler<SupTopic> for CountingHandler {
        type Context = AppState;
        async fn handle(&self, msg: Msg, _: MessageMetadata, ctx: &AppState) -> Outcome {
            ctx.counter.fetch_add(msg.n, Ordering::Relaxed);
            Outcome::Ack
        }
    }
    impl MessageHandler<GroupTopic> for CountingHandler {
        type Context = AppState;
        async fn handle(&self, msg: Msg, _: MessageMetadata, ctx: &AppState) -> Outcome {
            ctx.counter.fetch_add(msg.n, Ordering::Relaxed);
            Outcome::Ack
        }
    }

    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .unwrap();
    broker.topology().declare::<SupTopic>().await.unwrap();
    broker.topology().declare::<GroupTopic>().await.unwrap();

    let state = AppState {
        counter: Arc::new(AtomicU32::new(0)),
    };

    // ── ConsumerSupervisor<InMemory, AppState> ──
    let mut supervisor = broker.consumer_supervisor().with_context(state.clone());
    supervisor
        .register::<SupTopic, _>(CountingHandler, ConsumerOptions::new())
        .unwrap();

    // ── ConsumerGroup<InMemory, AppState> ──
    let mut group = broker.consumer_group().with_context(state.clone());
    group
        .register::<GroupTopic, _>(
            ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::new(1..=1)),
            || CountingHandler,
        )
        .await
        .unwrap();

    // Publish one message to each topic.
    let publisher = broker.publisher().await.unwrap();
    publisher.publish::<SupTopic>(&Msg { n: 7 }).await.unwrap();
    publisher
        .publish::<GroupTopic>(&Msg { n: 13 })
        .await
        .unwrap();

    // The group's `run_until_timeout` calls `start_all()`, so we must let it
    // run to start the group consumers. Drive shutdown from a signal that
    // waits for both counters to be visited, then cancels both harnesses.
    let sup_token = supervisor.cancellation_token();
    let group_token = group.cancellation_token();
    let counter_for_signal = state.counter.clone();
    let signal = async move {
        for _ in 0..200 {
            if counter_for_signal.load(Ordering::Relaxed) >= 20 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        sup_token.cancel();
        group_token.cancel();
    };

    // Supervisor: consumers were already spawned by `register()`. Use a
    // never-ready signal so the drain waits for the shared tokens above.
    let (sup_outcome, group_outcome) = tokio::join!(
        supervisor.run_until_timeout(std::future::pending::<()>(), Duration::from_secs(3)),
        group.run_until_timeout(signal, Duration::from_secs(3)),
    );

    assert!(sup_outcome.is_clean(), "supervisor: {sup_outcome:?}");
    assert!(group_outcome.is_clean(), "group: {group_outcome:?}");

    // Both handlers saw the injected AppState: supervisor added 7, group added 13.
    assert_eq!(state.counter.load(Ordering::Relaxed), 20);

    broker.close().await;
}
