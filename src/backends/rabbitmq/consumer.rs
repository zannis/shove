use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_lite::StreamExt;
use lapin::message::Delivery;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions, QueueBindOptions,
    QueueDeclareOptions,
};
use lapin::types::{FieldTable, ShortString};
use lapin::{Channel, Error as LapinError};
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::backend::ConsumerOptionsInner as ConsumerOptions;
use crate::backend::batch_consumer::{
    BatchConsumerOptionsInner, BatchSettlement, PREALLOC_CAP, batch_redelivery_backoff,
    invoke_batch_handler, next_redelivery_delay, settle_batch_outcome,
};
use crate::backends::rabbitmq::client::RabbitMqClient;
use crate::backends::rabbitmq::headers::{
    extract_dead_metadata, extract_message_metadata, get_retry_count,
};
use crate::backends::rabbitmq::publisher::ChannelPublisher;
use crate::backends::rabbitmq::router;
use crate::consumer::validate_message_size;
use crate::consumer_supervisor::{SupervisorOutcome, drive_fifo_until_timeout};
use crate::error::{Result, ShoveError};
use crate::handler::{BatchMessageHandler, MessageHandler};
use crate::metadata::MessageMetadata;
use crate::metrics;
use crate::outcome::Outcome;
use crate::retry::Backoff;
use crate::routing::{
    drain_timeout_outcome, handler_timeout_outcome, hold_index, retries_exhausted,
    shutdown_drain_timeout,
};
use crate::topic::{NotSequenced, SequencedTopic, Topic};
use crate::topology::{HoldQueue, SequenceFailure};
use crate::{QueueTopology, RabbitMq};

use super::map_lapin_error;

/// A delivery whose payload was moved into a shared, reference-counted
/// buffer at receipt. Decode (`Codec::decode_owned`) and hold-queue
/// republish both borrow the same allocation, so the payload is never
/// copied after the client hands it over. The inner `delivery` keeps the
/// acker, headers, and routing key; its `data` is left empty.
struct ReceivedDelivery {
    delivery: Delivery,
    payload: Bytes,
}

impl ReceivedDelivery {
    fn new(mut delivery: Delivery) -> Self {
        let payload = Bytes::from(std::mem::take(&mut delivery.data));
        Self { delivery, payload }
    }
}

/// Opens a channel with QoS and starts consuming from `queue`.
///
/// When `exactly_once` is `true` (requires the `rabbitmq-transactional` feature)
/// the channel is put into AMQP transaction mode (`tx_select`). Otherwise a
/// confirm-mode channel is created.
async fn open_consumer(
    client: &RabbitMqClient,
    queue: &str,
    prefetch_count: u16,
    exactly_once: bool,
) -> Result<(Channel, lapin::Consumer)> {
    #[cfg(feature = "rabbitmq-transactional")]
    let channel = if exactly_once {
        client.create_tx_channel().await?
    } else {
        client.create_confirm_channel().await?
    };
    #[cfg(not(feature = "rabbitmq-transactional"))]
    let channel = {
        let _ = exactly_once;
        client.create_confirm_channel().await?
    };
    channel
        .basic_qos(prefetch_count, BasicQosOptions::default())
        .await
        .map_err(|e| map_lapin_error("failed to set QoS", e))?;
    let consumer = channel
        .basic_consume(
            ShortString::from(queue),
            ShortString::from(""),
            BasicConsumeOptions {
                no_ack: false,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| map_lapin_error(&format!("failed to start consumer on {queue}"), e))?;
    Ok((channel, consumer))
}

/// How a concurrent consumer attaches to the broker.
///
/// The two variants share the whole delivery loop — concurrency, handler
/// timeouts, deserialization rejects, the shutdown drain — and differ in
/// exactly three places: how the stream is opened, whether the retry-budget
/// gate runs before the handler, and where a non-`Ack` outcome goes. That is
/// why this is a parameter rather than a second loop: the parts that are the
/// same are the parts that drift apart when they are copied.
#[derive(Clone, Copy)]
enum Attachment<'a> {
    /// The topology's shared, declared queue. The full retry chain applies:
    /// budget gate, hold queues, DLQ.
    Shared(&'a str),
    /// This process's own exclusive, auto-delete queue bound to `exchange`.
    /// There is no retry chain to apply — `.broadcast()` rejects a DLQ, hold
    /// queues and sequencing at build time.
    Broadcast { exchange: &'a str },
}

impl Attachment<'_> {
    fn is_broadcast(&self) -> bool {
        matches!(self, Self::Broadcast { .. })
    }
}

/// Open a consumer for `attachment`, returning the channel, the delivery
/// stream, and the name of the queue actually being consumed — which for a
/// broadcast subscription is the server-generated one, known only now.
async fn open_attached_consumer(
    client: &RabbitMqClient,
    attachment: Attachment<'_>,
    prefetch_count: u16,
    exactly_once: bool,
) -> Result<(Channel, lapin::Consumer, String)> {
    match attachment {
        Attachment::Shared(queue) => {
            let (channel, stream) =
                open_consumer(client, queue, prefetch_count, exactly_once).await?;
            Ok((channel, stream, queue.to_string()))
        }
        Attachment::Broadcast { exchange } => {
            open_broadcast_consumer(client, exchange, prefetch_count).await
        }
    }
}

/// Declare this process's own ephemeral subscription and start consuming it.
///
/// The queue is server-named, `exclusive`, `auto_delete` and non-durable:
/// nothing else can bind to it or consume from it, and the broker deletes it as
/// soon as this consumer goes away. That covers the abort path as well as the
/// drain path — lapin cancels the consumer and closes the channel from their
/// `Drop` impls, so a task that is aborted mid-run still leaves the broker with
/// nothing (AC5). "Nothing survives" is a property of how the queue is
/// declared, not of the teardown code being reached.
///
/// Never transactional. A broadcast subscription publishes nothing, so there is
/// no publish/ack pair for an AMQP transaction to make atomic.
async fn open_broadcast_consumer(
    client: &RabbitMqClient,
    exchange: &str,
    prefetch_count: u16,
) -> Result<(Channel, lapin::Consumer, String)> {
    let channel = client.create_confirm_channel().await?;
    channel
        .basic_qos(prefetch_count, BasicQosOptions::default())
        .await
        .map_err(|e| map_lapin_error("failed to set QoS", e))?;
    // Declared here as well as by the topology declarer so a subscriber-only
    // process does not depend on someone else having declared the topology
    // first — `queue_bind` to a missing exchange is a 404 that kills the
    // channel.
    super::topology::declare_broadcast_exchange(&channel, exchange).await?;

    let queue = channel
        .queue_declare(
            ShortString::from(""),
            QueueDeclareOptions {
                durable: false,
                exclusive: true,
                auto_delete: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| {
            map_lapin_error(
                &format!("failed to declare broadcast queue on '{exchange}'"),
                e,
            )
        })?;
    let name = queue.name().as_str().to_string();

    channel
        .queue_bind(
            ShortString::from(name.as_str()),
            ShortString::from(exchange),
            ShortString::from(""),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(|e| {
            map_lapin_error(
                &format!("failed to bind broadcast queue '{name}' to '{exchange}'"),
                e,
            )
        })?;

    let stream = channel
        .basic_consume(
            ShortString::from(name.as_str()),
            ShortString::from(""),
            BasicConsumeOptions {
                no_ack: false,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| {
            map_lapin_error(
                &format!("failed to start broadcast consumer on '{name}'"),
                e,
            )
        })?;

    info!(exchange, queue = %name, "broadcast subscription established");
    Ok((channel, stream, name))
}

/// Unwrap a delivery from the consumer stream.
fn unwrap_delivery(
    item: Option<std::result::Result<Delivery, LapinError>>,
    queue: &str,
) -> Result<Delivery> {
    match item {
        Some(Ok(d)) => Ok(d),
        Some(Err(e)) => {
            metrics::record_backend_error(
                metrics::BackendLabel::RabbitMq,
                metrics::BackendErrorKind::Consume,
            );
            Err(map_lapin_error(
                &format!("consumer stream error on {queue}"),
                e,
            ))
        }
        None => {
            metrics::record_backend_error(
                metrics::BackendLabel::RabbitMq,
                metrics::BackendErrorKind::Consume,
            );
            Err(ShoveError::Connection(format!(
                "consumer stream closed for {queue}"
            )))
        }
    }
}

/// Run `f` in a reconnect loop, retrying on transient errors until shutdown.
async fn run_with_reconnect<F, Fut>(
    shutdown: &CancellationToken,
    queue: &str,
    max_reconnect_attempts: Option<u32>,
    mut f: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut backoff = Backoff::default();
    let mut attempts = 0u32;
    loop {
        match f().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if !e.is_retryable() {
                    return Err(e);
                }
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                attempts += 1;
                if let Some(max) = max_reconnect_attempts
                    && attempts >= max
                {
                    tracing::error!(
                        queue,
                        attempts,
                        error = %e,
                        "max reconnect attempts reached, giving up"
                    );
                    return Err(ShoveError::Connection(format!(
                        "consumer on '{queue}' exhausted {max} reconnect attempt(s): {e}"
                    )));
                }
                let delay = backoff.next().expect("backoff is infinite");
                warn!(
                    queue,
                    attempt = attempts,
                    ?max_reconnect_attempts,
                    "consumer error, reconnecting in {delay:?}: {e}"
                );
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = shutdown.cancelled() => return Ok(()),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// KeyState — per-key state machine for concurrent-sequenced consumers
// ---------------------------------------------------------------------------

/// Tracks the processing state of a single sequence key within a
/// concurrent-sequenced shard consumer.
enum KeyState {
    /// A handler is currently running for this key.
    InFlight {
        received: Box<ReceivedDelivery>,
        outcome_rx: oneshot::Receiver<Outcome>,
    },
    /// The handler returned Retry/Defer and the message has been routed to a
    /// hold queue. The key is blocked until the retry comes back.
    /// The `Instant` records when the key entered this state, used to enforce
    /// `hold_queue_timeout` eviction.
    AwaitingRetry(Instant),
}

// ---------------------------------------------------------------------------
// RabbitMqConsumer
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct RabbitMqConsumer {
    client: RabbitMqClient,
}

impl RabbitMqConsumer {
    pub fn new(client: RabbitMqClient) -> Self {
        Self { client }
    }

    /// Runs the concurrent-sequenced consumer loop with reconnect handling.
    /// Processes multiple keys concurrently within a single shard, using local
    /// buffering for messages that arrive while their key is busy.
    #[allow(clippy::too_many_arguments)]
    async fn run_internal_concurrent_sequenced<T, H>(
        &self,
        handler: Arc<H>,
        ctx: Arc<H::Context>,
        queue: &str,
        topology: &'static QueueTopology,
        options: ConsumerOptions,
        on_failure: SequenceFailure,
        shard_hold_queues: Vec<HoldQueue>,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let mut poisoned_keys = HashSet::new();
        let mut pending_deliveries: HashMap<String, VecDeque<ReceivedDelivery>> = HashMap::new();
        let mut backoff = Backoff::default();
        let mut attempts = 0u32;
        loop {
            match self
                .consume_loop_concurrent_sequenced::<T, H>(
                    handler.clone(),
                    ctx.clone(),
                    queue,
                    topology,
                    &options,
                    on_failure,
                    &mut poisoned_keys,
                    &shard_hold_queues,
                    &mut pending_deliveries,
                )
                .await
            {
                Ok(publisher) => {
                    // Graceful shutdown — nack-requeue all pending buffered deliveries.
                    nack_requeue_all_pending(&mut pending_deliveries, Some(&publisher)).await;
                    return Ok(());
                }
                Err(e) => {
                    if options.shutdown.is_cancelled() {
                        // Channel may be in a bad state; just clear.
                        // Unacked messages return to the queue when the channel closes.
                        pending_deliveries.clear();
                        return Ok(());
                    }
                    // On reconnect, the channel is dead — we cannot ack/nack.
                    // Clear pending; the broker will redeliver after reconnect.
                    pending_deliveries.clear();
                    attempts += 1;
                    if let Some(max) = options.max_reconnect_attempts
                        && attempts >= max
                    {
                        tracing::error!(
                            queue,
                            attempts,
                            error = %e,
                            "max reconnect attempts reached, giving up"
                        );
                        return Err(ShoveError::Connection(format!(
                            "consumer on '{queue}' exhausted {max} reconnect attempt(s): {e}"
                        )));
                    }
                    let delay = backoff.next().expect("backoff is infinite");
                    warn!(
                        queue,
                        attempt = attempts,
                        max_reconnect_attempts = ?options.max_reconnect_attempts,
                        "consumer error, reconnecting in {delay:?}: {e}"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = options.shutdown.cancelled() => return Ok(()),
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn consume_loop_concurrent_sequenced<T, H>(
        &self,
        handler: Arc<H>,
        ctx: Arc<H::Context>,
        queue: &str,
        topology: &'static QueueTopology,
        options: &ConsumerOptions,
        on_failure: SequenceFailure,
        poisoned_keys: &mut HashSet<String>,
        shard_hold_queues: &[HoldQueue],
        pending_deliveries: &mut HashMap<String, VecDeque<ReceivedDelivery>>,
    ) -> Result<ChannelPublisher>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let prefetch = options.prefetch_count;
        #[cfg(feature = "rabbitmq-transactional")]
        let exactly_once = options.exactly_once;
        #[cfg(not(feature = "rabbitmq-transactional"))]
        let exactly_once = false;
        let (channel, mut stream) =
            open_consumer(&self.client, queue, prefetch, exactly_once).await?;
        #[cfg(feature = "rabbitmq-transactional")]
        let publisher = if exactly_once {
            ChannelPublisher::new_tx(channel)
        } else {
            ChannelPublisher::new(channel)
        };
        #[cfg(not(feature = "rabbitmq-transactional"))]
        let publisher = ChannelPublisher::new(channel);
        // Channel for handlers to signal completion by sending their sequence key.
        let (completed_tx, mut completed_rx) = mpsc::unbounded_channel::<String>();
        let topic: Arc<str> = Arc::from(T::topology().queue());
        let group: Option<Arc<str>> = options.consumer_group.clone();

        let mut key_states: HashMap<String, KeyState> = HashMap::new();
        let mut in_flight_count: usize = 0;

        // Interval for evicting keys that have been stuck in AwaitingRetry too long.
        // When no timeout is configured we use a very long period so the arm never
        // fires meaningfully (the `if options.hold_queue_timeout.is_some()` guard
        // in the select! makes it inactive, but Interval still needs a valid duration).
        // Poll at half the timeout so a key evicted just after a tick is caught
        // within 1.5× the configured maximum, not 2×.
        let eviction_period = options
            .hold_queue_timeout
            .map(|t| t / 2)
            .unwrap_or(Duration::from_secs(86400));
        let mut eviction_ticker = tokio::time::interval(eviction_period);
        eviction_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        eviction_ticker.tick().await; // consume the immediate first tick

        info!("concurrent-sequenced consumer started on sub-queue {queue} (prefetch={prefetch})");

        loop {
            // ── Drain completed handlers ──
            // Only process keys that have signalled completion via the channel.
            while let Ok(key) = completed_rx.try_recv() {
                let Some(state) = key_states.remove(&key) else {
                    continue;
                };
                let KeyState::InFlight {
                    received,
                    mut outcome_rx,
                } = state
                else {
                    // AwaitingRetry — shouldn't happen, but put it back.
                    key_states.insert(key, KeyState::AwaitingRetry(Instant::now()));
                    continue;
                };

                let outcome = match outcome_rx.try_recv() {
                    Ok(o) => o,
                    Err(TryRecvError::Closed) => {
                        warn!(queue, sequence_key = %key, "handler task panicked, retrying");
                        Outcome::Retry
                    }
                    Err(TryRecvError::Empty) => {
                        // Notified but not ready yet (shouldn't happen in practice).
                        key_states.insert(
                            key,
                            KeyState::InFlight {
                                received,
                                outcome_rx,
                            },
                        );
                        continue;
                    }
                };

                let delivery = &received.delivery;
                let retry_count = get_retry_count(delivery);
                debug!(queue, sequence_key = %key, ?outcome, "message handled (concurrent-sequenced)");

                match outcome {
                    Outcome::Ack | Outcome::Reject => {
                        // Terminal outcomes — process, then drain pending.
                        if matches!(outcome, Outcome::Ack) {
                            router::route_ack(delivery, &publisher).await?;
                        } else {
                            if on_failure == SequenceFailure::FailAll {
                                info!(
                                    sequence_key = %key,
                                    queue = %queue,
                                    "poisoning sequence key (FailAll)"
                                );
                                poisoned_keys.insert(key.clone());
                            }
                            router::route_reject(
                                delivery,
                                topology,
                                &publisher,
                                group.as_deref(),
                                metrics::FailReason::Rejected,
                            )
                            .await?;
                        }
                        in_flight_count -= 1;

                        // Drain pending deliveries for this key.
                        self.drain_pending_for_key::<T, H>(
                            &key,
                            &handler,
                            &ctx,
                            options,
                            on_failure,
                            poisoned_keys,
                            &completed_tx,
                            &mut key_states,
                            &mut in_flight_count,
                            pending_deliveries,
                            queue,
                            topology,
                            &publisher,
                            &topic,
                            &group,
                        )
                        .await;
                    }
                    Outcome::Retry | Outcome::Defer => {
                        // Non-terminal — route to hold queue, key enters AwaitingRetry.
                        if matches!(outcome, Outcome::Retry) {
                            route_shard_retry(
                                &received,
                                shard_hold_queues,
                                &publisher,
                                retry_count,
                                queue,
                            )
                            .await;
                        } else {
                            // Defer
                            if shard_hold_queues.is_empty() {
                                warn!(
                                    queue,
                                    "deferring message but no shard hold queues configured — requeuing with no delay"
                                );
                            }
                            if !shard_hold_queues.is_empty() {
                                let hold_queue = &shard_hold_queues[0];
                                let headers = router::clone_headers(delivery);
                                match publisher
                                    .publish_to_queue(hold_queue.name(), &received.payload, headers)
                                    .await
                                {
                                    Ok(()) => {
                                        if let Err(e) =
                                            delivery.ack(BasicAckOptions::default()).await
                                        {
                                            error!(
                                                "failed to ack delivery after deferring to shard hold queue: {e}"
                                            );
                                            publisher.rollback_if_tx().await;
                                            router::nack_requeue(delivery, &publisher).await.ok();
                                        } else if let Err(e) = publisher.commit_if_tx().await {
                                            error!("tx_commit failed for shard defer: {e}");
                                        } else {
                                            debug!(
                                                "deferring message to shard hold queue {}",
                                                hold_queue.name()
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            "failed to publish to shard hold queue {} for defer, requeuing: {e}",
                                            hold_queue.name()
                                        );
                                        router::nack_requeue(delivery, &publisher).await.ok();
                                    }
                                }
                            } else {
                                router::nack_requeue(delivery, &publisher).await.ok();
                            }
                        }
                        in_flight_count -= 1;
                        key_states.insert(key, KeyState::AwaitingRetry(Instant::now()));
                    }
                }
            }

            options
                .processing
                .store(in_flight_count > 0, Ordering::Relaxed);

            let can_accept = in_flight_count < prefetch as usize;

            tokio::select! {
                biased;

                _ = options.shutdown.cancelled() => {
                    debug!(
                        "shutdown signal, draining {} in-flight messages on {queue}",
                        in_flight_count
                    );
                    // Wait for all in-flight handlers to complete.
                    // When a `handler_timeout` is set the handler is already
                    // bounded by it and resolves its own timeout; this wait is
                    // only a backstop so shutdown cannot hang on a channel that
                    // never delivers. Hence the grace, and hence resolving to
                    // the *configured* timeout outcome rather than assuming
                    // Retry. With deadlines disabled the handler is still
                    // running when this fires, so `drain_timeout_outcome` keeps
                    // the backstop at Retry — see its docs.
                    let drain_timeout = shutdown_drain_timeout(options.handler_timeout);
                    for (key, state) in key_states.drain() {
                        if let KeyState::InFlight { received, outcome_rx } = state {
                            let outcome = tokio::time::timeout(drain_timeout, outcome_rx)
                                .await
                                .unwrap_or_else(|_| {
                                    let resolved = drain_timeout_outcome(
                                        options.handler_timeout,
                                        options.handler_timeout_outcome.clone(),
                                    );
                                    warn!(queue, sequence_key = %key, outcome = ?resolved, "handler outcome did not arrive within the shutdown drain");
                                    Ok(resolved)
                                })
                                // A closed channel means the handler task
                                // panicked or was aborted: no outcome exists,
                                // so redeliver.
                                .unwrap_or(Outcome::Retry);
                            let delivery = &received.delivery;
                            let retry_count = get_retry_count(delivery);
                            debug!(
                                queue,
                                sequence_key = %key,
                                ?outcome,
                                "draining in-flight message on shutdown"
                            );
                            match outcome {
                                Outcome::Ack => {
                                    router::route_ack(delivery, &publisher).await.ok();
                                }
                                Outcome::Retry => {
                                    route_shard_retry(
                                        &received,
                                        shard_hold_queues,
                                        &publisher,
                                        retry_count,
                                        queue,
                                    )
                                    .await;
                                }
                                Outcome::Reject => {
                                    router::route_reject(
                                        delivery,
                                        topology,
                                        &publisher,
                                        group.as_deref(),
                                        metrics::FailReason::Rejected,
                                    )
                                    .await
                                    .ok();
                                }
                                Outcome::Defer => {
                                    if shard_hold_queues.is_empty() {
                                        warn!(
                                            queue,
                                            "deferring message on shutdown but no shard hold queues configured — requeuing with no delay"
                                        );
                                    }
                                    if !shard_hold_queues.is_empty() {
                                        let hold_queue = &shard_hold_queues[0];
                                        let headers = router::clone_headers(delivery);
                                        match publisher
                                            .publish_to_queue(
                                                hold_queue.name(),
                                                &received.payload,
                                                headers,
                                            )
                                            .await
                                        {
                                            Ok(()) => {
                                                if let Err(e) =
                                                    delivery.ack(BasicAckOptions::default()).await
                                                {
                                                    error!("failed to ack delivery after defer on shutdown: {e}");
                                                    publisher.rollback_if_tx().await;
                                                    router::nack_requeue(delivery, &publisher).await.ok();
                                                } else if let Err(e) = publisher.commit_if_tx().await {
                                                    error!("tx_commit failed for defer on shutdown: {e}");
                                                }
                                            }
                                            Err(e) => {
                                                warn!("failed to defer on shutdown: {e}");
                                                router::nack_requeue(delivery, &publisher).await.ok();
                                            }
                                        }
                                    } else {
                                        router::nack_requeue(delivery, &publisher).await.ok();
                                    }
                                }
                            }
                        }
                        // AwaitingRetry keys: nothing to do — the message is
                        // already in a hold queue and will be redelivered.
                    }
                    // Pending deliveries are nack-requeued by the caller.
                    return Ok(publisher);
                }

                Some(key) = completed_rx.recv() => {
                    // Re-inject the key so the drain loop at the top picks it up.
                    // This is safe because the channel is unbounded.
                    let _ = completed_tx.send(key);
                }

                _ = eviction_ticker.tick(), if options.hold_queue_timeout.is_some() => {
                    let timeout = options.hold_queue_timeout.expect("guard checked is_some");
                    let now = Instant::now();
                    let timed_out: Vec<String> = key_states
                        .iter()
                        .filter_map(|(k, v)| {
                            if let KeyState::AwaitingRetry(entered_at) = v {
                                if now.duration_since(*entered_at) >= timeout {
                                    Some(k.clone())
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                        .collect();
                    for key in timed_out {
                        warn!(
                            queue,
                            sequence_key = %key,
                            ?timeout,
                            "sequence key stuck in AwaitingRetry, dead-lettering pending messages"
                        );
                        key_states.remove(&key);
                        if let Some(pending) = pending_deliveries.remove(&key) {
                            for pd in pending {
                                // Not a cascade: no already-counted failure
                                // accounts for these, so `route_reject` counts
                                // one failure per dead-lettered message. See
                                // `metrics::FailReason::SequenceTimeout`.
                                router::route_reject(
                                    &pd.delivery,
                                    topology,
                                    &publisher,
                                    group.as_deref(),
                                    metrics::FailReason::SequenceTimeout,
                                )
                                .await?;
                            }
                        }
                    }
                }

                item = stream.next(), if can_accept => {
                    let received = ReceivedDelivery::new(unwrap_delivery(item, queue)?);
                    let delivery = &received.delivery;
                    let seq_key = delivery.routing_key.to_string();
                    let retry_count = get_retry_count(delivery);

                    // ── FailAll: skip poisoned keys ──
                    if on_failure == SequenceFailure::FailAll
                        && poisoned_keys.contains(&seq_key)
                    {
                        warn!(
                            sequence_key = %seq_key,
                            queue = %queue,
                            "message with poisoned sequence key, sending to DLQ"
                        );
                        // Cascade: intentionally not counted — see `metrics::FailReason`.
                        router::route_reject_cascade(delivery, topology, &publisher, group.as_deref(), metrics::FailReason::Rejected).await?;
                        continue;
                    }

                    // ── Max retries check ──
                    if retries_exhausted(retry_count, options.max_retries) {
                        warn!(
                            queue = %queue,
                            retry_count,
                            max_retries = options.max_retries,
                            "message exceeded max retries, sending to DLQ"
                        );
                        if on_failure == SequenceFailure::FailAll {
                            info!(
                                sequence_key = %seq_key,
                                queue = %queue,
                                "poisoning sequence key (FailAll)"
                            );
                            poisoned_keys.insert(seq_key.clone());
                            // Also reject all pending deliveries for this key.
                            // Cascade: intentionally not counted — see `metrics::FailReason`.
                            if let Some(pending) = pending_deliveries.remove(&seq_key) {
                                for pd in pending {
                                    router::route_reject_cascade(&pd.delivery, topology, &publisher, group.as_deref(), metrics::FailReason::MaxRetriesExceeded)
                                        .await?;
                                }
                            }
                        }
                        router::route_reject(delivery, topology, &publisher, group.as_deref(), metrics::FailReason::MaxRetriesExceeded).await?;
                        continue;
                    }

                    // ── Check if key is busy ──
                    match key_states.get(&seq_key) {
                        Some(KeyState::InFlight { .. }) => {
                            // Handler running — buffer locally if within limit.
                            if let Some(limit) = options.max_pending_per_key {
                                let current_len = pending_deliveries
                                    .get(&seq_key)
                                    .map_or(0, |q| q.len());
                                if current_len >= limit {
                                    warn!(
                                        sequence_key = %seq_key,
                                        queue = %queue,
                                        limit,
                                        "per-key pending buffer full, rejecting to DLQ"
                                    );
                                    router::route_reject(delivery, topology, &publisher, group.as_deref(), metrics::FailReason::PendingFull).await?;
                                    continue;
                                }
                            }
                            debug!(
                                sequence_key = %seq_key,
                                queue = %queue,
                                "key in-flight, buffering delivery locally"
                            );
                            pending_deliveries
                                .entry(seq_key)
                                .or_insert_with(|| VecDeque::with_capacity(4))
                                .push_back(received);
                            continue;
                        }
                        Some(KeyState::AwaitingRetry(_)) => {
                            if retry_count > 0 || delivery.redelivered {
                                // This is the returning retry (or a nack+requeue
                                // redelivery when no hold queue is configured) —
                                // clear AwaitingRetry and fall through to spawn a
                                // handler below.
                                debug!(
                                    sequence_key = %seq_key,
                                    queue = %queue,
                                    retry_count,
                                    "returning retry clears AwaitingRetry"
                                );
                                key_states.remove(&seq_key);
                            } else {
                                // New message while awaiting retry — buffer if within limit.
                                if let Some(limit) = options.max_pending_per_key {
                                    let current_len = pending_deliveries
                                        .get(&seq_key)
                                        .map_or(0, |q| q.len());
                                    if current_len >= limit {
                                        warn!(
                                            sequence_key = %seq_key,
                                            queue = %queue,
                                            limit,
                                            "per-key pending buffer full, rejecting to DLQ"
                                        );
                                        router::route_reject(delivery, topology, &publisher, group.as_deref(), metrics::FailReason::PendingFull).await?;
                                        continue;
                                    }
                                }
                                debug!(
                                    sequence_key = %seq_key,
                                    queue = %queue,
                                    "key awaiting retry, buffering new delivery locally"
                                );
                                pending_deliveries
                                    .entry(seq_key)
                                    .or_default()
                                    .push_back(received);
                                continue;
                            }
                        }
                        None => {}
                    }

                    // ── Spawn handler for this key ──
                    let metadata = extract_message_metadata(&received.delivery);
                    match try_deserialize_or_reject::<T>(
                        &received,
                        &metadata,
                        queue,
                        topology,
                        &publisher,
                        options,
                        &topic,
                        group.as_deref(),
                    )
                    .await
                    {
                        None => {
                            // Reject undeserializable messages immediately.
                            if on_failure == SequenceFailure::FailAll {
                                poisoned_keys.insert(seq_key.clone());
                            }
                        }
                        Some(message) => {
                            let rx = spawn_handler_keyed::<T, H>(
                                &handler,
                                &ctx,
                                message,
                                metadata,
                                options.handler_timeout,
                                options.handler_timeout_outcome.clone(),
                                &completed_tx,
                                seq_key.clone(),
                                topic.clone(),
                                group.clone(),
                            );

                            key_states.insert(
                                seq_key,
                                KeyState::InFlight {
                                    received: Box::new(received),
                                    outcome_rx: rx,
                                },
                            );
                            in_flight_count += 1;
                            options.processing.store(true, Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    }

    /// Pop the next pending delivery for `key` and spawn a handler for it.
    /// Called after a terminal outcome (Ack/Reject) to drain buffered messages.
    #[allow(clippy::too_many_arguments)]
    async fn drain_pending_for_key<T, H>(
        &self,
        key: &str,
        handler: &Arc<H>,
        ctx: &Arc<H::Context>,
        options: &ConsumerOptions,
        on_failure: SequenceFailure,
        poisoned_keys: &mut HashSet<String>,
        completed_tx: &mpsc::UnboundedSender<String>,
        key_states: &mut HashMap<String, KeyState>,
        in_flight_count: &mut usize,
        pending_deliveries: &mut HashMap<String, VecDeque<ReceivedDelivery>>,
        queue: &str,
        topology: &'static QueueTopology,
        publisher: &ChannelPublisher,
        topic: &Arc<str>,
        group: &Option<Arc<str>>,
    ) where
        T: Topic,
        H: MessageHandler<T>,
    {
        // If the key is poisoned, reject all pending deliveries for it.
        // Cascade: intentionally not counted — see `metrics::FailReason`.
        if on_failure == SequenceFailure::FailAll && poisoned_keys.contains(key) {
            if let Some(pending) = pending_deliveries.remove(key) {
                for pd in pending {
                    router::route_reject_cascade(
                        &pd.delivery,
                        topology,
                        publisher,
                        group.as_deref(),
                        metrics::FailReason::Rejected,
                    )
                    .await
                    .ok();
                }
            }
            return;
        }

        let Some(pending) = pending_deliveries.get_mut(key) else {
            return;
        };

        // Pop the next delivery and try to spawn it.
        while let Some(received) = pending.pop_front() {
            let retry_count = get_retry_count(&received.delivery);

            // Max retries check on buffered delivery.
            if retries_exhausted(retry_count, options.max_retries) {
                warn!(
                    queue = %queue,
                    sequence_key = %key,
                    retry_count,
                    "buffered message exceeded max retries, sending to DLQ"
                );
                if on_failure == SequenceFailure::FailAll {
                    poisoned_keys.insert(key.to_string());
                    // Reject remaining pending for this key too.
                    router::route_reject(
                        &received.delivery,
                        topology,
                        publisher,
                        group.as_deref(),
                        metrics::FailReason::MaxRetriesExceeded,
                    )
                    .await
                    .ok();
                    // Cascade: intentionally not counted — see `metrics::FailReason`.
                    while let Some(pd) = pending.pop_front() {
                        router::route_reject_cascade(
                            &pd.delivery,
                            topology,
                            publisher,
                            group.as_deref(),
                            metrics::FailReason::MaxRetriesExceeded,
                        )
                        .await
                        .ok();
                    }
                    pending_deliveries.remove(key);
                    return;
                }
                router::route_reject(
                    &received.delivery,
                    topology,
                    publisher,
                    group.as_deref(),
                    metrics::FailReason::MaxRetriesExceeded,
                )
                .await
                .ok();
                continue;
            }

            let metadata = extract_message_metadata(&received.delivery);
            match try_deserialize_or_reject::<T>(
                &received,
                &metadata,
                queue,
                topology,
                publisher,
                options,
                topic,
                group.as_deref(),
            )
            .await
            {
                None => {
                    // Extra FailAll poisoning on deserialization failure. The
                    // failure itself is already counted inside
                    // `try_deserialize_or_reject`.
                    // Cascade: intentionally not counted — see `metrics::FailReason`.
                    if on_failure == SequenceFailure::FailAll {
                        poisoned_keys.insert(key.to_string());
                        while let Some(pd) = pending.pop_front() {
                            router::route_reject_cascade(
                                &pd.delivery,
                                topology,
                                publisher,
                                group.as_deref(),
                                metrics::FailReason::Deserialize,
                            )
                            .await
                            .ok();
                        }
                        pending_deliveries.remove(key);
                        return;
                    }
                    continue;
                }
                Some(message) => {
                    let rx = spawn_handler_keyed::<T, H>(
                        handler,
                        ctx,
                        message,
                        metadata,
                        options.handler_timeout,
                        options.handler_timeout_outcome.clone(),
                        completed_tx,
                        key.to_string(),
                        topic.clone(),
                        group.clone(),
                    );

                    key_states.insert(
                        key.to_string(),
                        KeyState::InFlight {
                            received: Box::new(received),
                            outcome_rx: rx,
                        },
                    );
                    *in_flight_count += 1;

                    // Clean up empty deque.
                    if pending.is_empty() {
                        pending_deliveries.remove(key);
                    }
                    return;
                }
            }
        }

        // All pending drained without spawning (all rejected).
        pending_deliveries.remove(key);
    }

    async fn run_internal_concurrent<T, H>(
        &self,
        handler: Arc<H>,
        ctx: Arc<H::Context>,
        attachment: Attachment<'_>,
        topology: &'static QueueTopology,
        options: ConsumerOptions,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let shutdown = options.shutdown.clone();
        // The reconnect label is the topic, not the queue: a broadcast
        // reconnect declares a *different* server-named queue each time, so
        // logging that name would make consecutive attempts look unrelated.
        run_with_reconnect(
            &shutdown,
            topology.queue(),
            options.max_reconnect_attempts,
            || {
                self.consume_loop_concurrent::<T, H>(
                    handler.clone(),
                    ctx.clone(),
                    attachment,
                    topology,
                    &options,
                )
            },
        )
        .await
    }

    async fn consume_loop_concurrent<T, H>(
        &self,
        handler: Arc<H>,
        ctx: Arc<H::Context>,
        attachment: Attachment<'_>,
        topology: &'static QueueTopology,
        options: &ConsumerOptions,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        #[cfg(feature = "rabbitmq-transactional")]
        let exactly_once = options.exactly_once;
        #[cfg(not(feature = "rabbitmq-transactional"))]
        let exactly_once = false;
        let (channel, mut stream, queue_name) = open_attached_consumer(
            &self.client,
            attachment,
            options.prefetch_count,
            exactly_once,
        )
        .await?;
        let queue = queue_name.as_str();
        #[cfg(feature = "rabbitmq-transactional")]
        let publisher = if exactly_once {
            ChannelPublisher::new_tx(channel)
        } else {
            ChannelPublisher::new(channel)
        };
        #[cfg(not(feature = "rabbitmq-transactional"))]
        let publisher = ChannelPublisher::new(channel);
        let notify = Arc::new(Notify::new());
        let max_in_flight = options.prefetch_count as usize;
        let topic: Arc<str> = Arc::from(T::topology().queue());
        let group: Option<Arc<str>> = options.consumer_group.clone();

        struct PendingMessage {
            received: ReceivedDelivery,
            outcome_rx: oneshot::Receiver<Outcome>,
        }

        let mut in_flight: VecDeque<PendingMessage> = VecDeque::with_capacity(max_in_flight);

        info!("concurrent consumer started on queue {queue} (max {max_in_flight} in-flight)");

        loop {
            // Drain completed messages from the front, preserving delivery order.
            while let Some(front) = in_flight.front_mut() {
                match front.outcome_rx.try_recv() {
                    Ok(outcome) => {
                        let msg = in_flight.pop_front().unwrap();
                        let retry_count = get_retry_count(&msg.received.delivery);
                        debug!(queue, ?outcome, "message handled (concurrent)");
                        route_outcome_for(
                            attachment,
                            &msg.received,
                            outcome,
                            topology,
                            &publisher,
                            retry_count,
                            group.as_deref(),
                        )
                        .await?;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Closed) => {
                        // Handler task panicked — treat as retry.
                        let msg = in_flight.pop_front().unwrap();
                        let retry_count = get_retry_count(&msg.received.delivery);
                        warn!(queue, "handler task panicked, retrying message");
                        route_outcome_for(
                            attachment,
                            &msg.received,
                            Outcome::Retry,
                            topology,
                            &publisher,
                            retry_count,
                            group.as_deref(),
                        )
                        .await?;
                    }
                }
            }

            options
                .processing
                .store(!in_flight.is_empty(), Ordering::Release);

            let can_accept = in_flight.len() < max_in_flight;

            tokio::select! {
                biased;

                _ = options.shutdown.cancelled() => {
                    debug!(
                        "shutdown signal, draining {} in-flight messages on {queue}",
                        in_flight.len()
                    );
                    // Same backstop as the sharded consumer's drain: the handler
                    // is already bounded by `handler_timeout` and resolves its
                    // own timeout, so this only stops shutdown hanging on a
                    // channel that never delivers. With deadlines disabled the
                    // handler is still running when it fires, so
                    // `drain_timeout_outcome` keeps it at Retry.
                    let drain_timeout = shutdown_drain_timeout(options.handler_timeout);
                    for pending in in_flight {
                        let outcome = tokio::time::timeout(drain_timeout, pending.outcome_rx)
                            .await
                            .unwrap_or_else(|_| {
                                let resolved = drain_timeout_outcome(
                                    options.handler_timeout,
                                    options.handler_timeout_outcome.clone(),
                                );
                                warn!(queue, outcome = ?resolved, "handler outcome did not arrive within the shutdown drain");
                                Ok(resolved)
                            })
                            // A closed channel means the handler task panicked
                            // or was aborted: no outcome exists, so redeliver.
                            .unwrap_or(Outcome::Retry);
                        let retry_count = get_retry_count(&pending.received.delivery);
                        route_outcome_for(
                            attachment,
                            &pending.received,
                            outcome,
                            topology,
                            &publisher,
                            retry_count,
                            group.as_deref(),
                        )
                        .await
                        .ok();
                    }
                    return Ok(());
                }

                _ = notify.notified() => {
                    // A handler completed — the drain at the top of the loop
                    // will process it on the next iteration.
                }

                item = stream.next(), if can_accept => {
                    let received = ReceivedDelivery::new(unwrap_delivery(item, queue)?);
                    let retry_count = get_retry_count(&received.delivery);

                    // The budget gate belongs to the retry chain, and a
                    // broadcast subscription has none. `BroadcastSubscriber`
                    // pins `max_retries` to 0, and `retries_exhausted(0, 0)` is
                    // true — so running this gate would dead-letter every
                    // message before its handler ever saw it. There is nothing
                    // to exhaust either: no path republishes a broadcast
                    // delivery with an incremented count, so `retry_count`
                    // stays 0 for the life of the subscription. Skipped
                    // outright rather than tuned.
                    if !attachment.is_broadcast()
                        && retries_exhausted(retry_count, options.max_retries)
                    {
                        warn!(
                            "message on {queue} exceeded max retries ({}/{}), sending to DLQ",
                            retry_count, options.max_retries
                        );
                        router::route_reject(&received.delivery, topology, &publisher, group.as_deref(), metrics::FailReason::MaxRetriesExceeded).await?;
                        continue;
                    }

                    let metadata = extract_message_metadata(&received.delivery);

                    if let Some(message) = try_deserialize_or_reject::<T>(
                        &received,
                        &metadata,
                        queue,
                        topology,
                        &publisher,
                        options,
                        &topic,
                        group.as_deref(),
                    )
                    .await
                    {
                        let rx = spawn_handler::<T, H>(
                            &handler,
                            &ctx,
                            message,
                            metadata,
                            options.handler_timeout,
                            options.handler_timeout_outcome.clone(),
                            &notify,
                            topic.clone(),
                            group.clone(),
                        );

                        in_flight.push_back(PendingMessage {
                            received,
                            outcome_rx: rx,
                        });
                        options.processing.store(true, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}

/// Consume a DLQ, deserializing each message inline and calling `handler.handle_dead`.
/// Always acks after handling (or on deserialization failure).
async fn consume_dlq_loop<T, H>(
    client: &RabbitMqClient,
    handler: &H,
    ctx: &H::Context,
    dlq: &str,
    options: &ConsumerOptions,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    // DLQ consumer never uses exactly-once mode (always acks, no hold-queue routing).
    let (_channel, mut stream) = open_consumer(client, dlq, options.prefetch_count, false).await?;

    info!("DLQ consumer started on queue {dlq}");

    // Hoisted out of the loop the way the main consumer hoists its own labels.
    let topic = T::topology().queue();

    loop {
        tokio::select! {
            _ = options.shutdown.cancelled() => {
                debug!("shutdown signal received, stopping DLQ consumer on {dlq}");
                return Ok(());
            }
            item = stream.next() => {
                let received = ReceivedDelivery::new(unwrap_delivery(item, dlq)?);
                let delivery = &received.delivery;

                let metadata = extract_dead_metadata(delivery);

                // Before the size gate, exactly as `try_deserialize_or_reject`
                // places it on the main loop. Labelled with the SOURCE topic
                // rather than the DLQ queue name, and with whatever group the
                // options carry (`run_dlq` builds defaults, so none): Redis
                // already drains its DLQ through `run_stream_loop`, which
                // labels every metric `topology.queue()` whichever stream it
                // reads, so a DLQ name here would make `topic` mean two
                // different things depending on the backend and would stop a
                // per-topic size profile summing across the main and DLQ paths.
                metrics::record_message_size(
                    topic,
                    options.consumer_group.as_deref(),
                    received.payload.len(),
                );

                if let Err(e) = options.validate_payload_message_size(received.payload.len()) {
                    warn!(
                        error = %e,
                        delivery_id = %metadata.message.delivery_id,
                        "oversized DLQ message — discarding"
                    );
                } else {
                    match <T::Codec as crate::Codec<T::Message>>::decode_owned(received.payload.clone()) {
                        Err(err) => {
                            error!(
                                error = %err,
                                delivery_id = %metadata.message.delivery_id,
                                "Failed to deserialize message from dead letter queue — discarding"
                            );
                        }
                        Ok(message) => {
                            handler.handle_dead(message, metadata, ctx).await;
                        }
                    }
                }

                // Always ack DLQ messages.
                if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                    error!("failed to ack DLQ delivery: {e}");
                }
            }
        }
    }
}

/// Route a delivery based on its outcome. Returns Err if a tx_commit fails and
/// the consumer loop should reconnect.
async fn route_outcome(
    received: &ReceivedDelivery,
    outcome: Outcome,
    topology: &'static QueueTopology,
    publisher: &ChannelPublisher,
    retry_count: u32,
    // Consumer-group label propagated to the terminal metric that
    // `router::route_reject` records — matches Kafka/NATS/Redis `route_outcome`.
    group: Option<&str>,
) -> Result<()> {
    let delivery = &received.delivery;
    match outcome {
        Outcome::Ack => router::route_ack(delivery, publisher).await,
        Outcome::Retry => {
            router::route_retry(
                delivery,
                &received.payload,
                topology,
                publisher,
                retry_count,
            )
            .await
        }
        Outcome::Reject => {
            router::route_reject(
                delivery,
                topology,
                publisher,
                group,
                metrics::FailReason::Rejected,
            )
            .await
        }
        Outcome::Defer => {
            router::route_defer(delivery, &received.payload, topology, publisher).await
        }
    }
}

/// Route `outcome` according to how this consumer is attached.
///
/// The split exists because broadcast has no retry chain to route into, not
/// because the two want different mechanics: both end at the same `router`
/// primitives, and both record the same terminal metric.
#[allow(clippy::too_many_arguments)]
async fn route_outcome_for(
    attachment: Attachment<'_>,
    received: &ReceivedDelivery,
    outcome: Outcome,
    topology: &'static QueueTopology,
    publisher: &ChannelPublisher,
    retry_count: u32,
    group: Option<&str>,
) -> Result<()> {
    match attachment {
        Attachment::Shared(_) => {
            route_outcome(received, outcome, topology, publisher, retry_count, group).await
        }
        Attachment::Broadcast { .. } => {
            route_broadcast_outcome(received, outcome, topology, publisher, group).await
        }
    }
}

/// Terminal routing for a broadcast subscription.
///
/// `.broadcast()` rejects a DLQ and hold queues at build time, so there is
/// nowhere for a failed message to go: `Retry` and `Reject` both discard, with
/// the warning and the `messages_discarded_total` increment that
/// `router::route_reject` already produces for a no-DLQ topology. The reasons
/// stay distinct — `max_retries_exceeded` for `Retry`, `rejected` for `Reject`
/// — matching what `decide_retry` yields on the InMemory path at
/// `max_retries = 0`, so one dashboard reads both backends.
///
/// `Defer` nack-requeues. That is redelivery to *this subscriber only*, which
/// is the contract rather than an approximation of it: the queue is exclusive,
/// so it has exactly one consumer and a requeued message can reach no other
/// instance's copy of the fan-out.
async fn route_broadcast_outcome(
    received: &ReceivedDelivery,
    outcome: Outcome,
    topology: &'static QueueTopology,
    publisher: &ChannelPublisher,
    group: Option<&str>,
) -> Result<()> {
    let delivery = &received.delivery;
    match outcome {
        Outcome::Ack => router::route_ack(delivery, publisher).await,
        Outcome::Retry => {
            warn!(
                queue = topology.queue(),
                "handler returned Retry on a broadcast subscription, which has no retry \
                 chain — discarding"
            );
            router::route_reject(
                delivery,
                topology,
                publisher,
                group,
                metrics::FailReason::MaxRetriesExceeded,
            )
            .await
        }
        Outcome::Reject => {
            router::route_reject(
                delivery,
                topology,
                publisher,
                group,
                metrics::FailReason::Rejected,
            )
            .await
        }
        Outcome::Defer => router::nack_requeue(delivery, publisher).await,
    }
}

/// Route a retry for a sequenced shard via per-shard hold queues.
async fn route_shard_retry(
    received: &ReceivedDelivery,
    shard_hold_queues: &[HoldQueue],
    publisher: &ChannelPublisher,
    retry_count: u32,
    queue: &str,
) {
    let delivery = &received.delivery;
    if !shard_hold_queues.is_empty() {
        let new_retry_count = retry_count + 1;
        let index = hold_index(retry_count, shard_hold_queues.len());
        let hold_queue = &shard_hold_queues[index];
        let headers = router::clone_headers_with_retry(delivery, new_retry_count);

        match publisher
            .publish_to_queue(hold_queue.name(), &received.payload, headers)
            .await
        {
            Ok(()) => {
                if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                    error!("failed to ack delivery after publishing to shard hold queue: {e}");
                    publisher.rollback_if_tx().await;
                    router::nack_requeue(delivery, publisher).await.ok();
                    return;
                }
                if let Err(e) = publisher.commit_if_tx().await {
                    error!("tx_commit failed for shard retry (attempt {new_retry_count}): {e}");
                    return;
                }
                debug!(
                    "retrying message via shard hold queue {} (attempt {})",
                    hold_queue.name(),
                    new_retry_count
                );
            }
            Err(e) => {
                warn!(
                    "failed to publish to shard hold queue {}, requeuing: {e}",
                    hold_queue.name()
                );
                router::nack_requeue(delivery, publisher).await.ok();
            }
        }
    } else {
        warn!(
            queue,
            retry_count,
            "retrying sequenced message but no shard hold queues configured — requeuing with no delay"
        );
        router::nack_requeue(delivery, publisher).await.ok();
    }
}

/// Nack-requeue all locally buffered deliveries (used on graceful shutdown).
///
/// In tx mode the channel is still open; we nack each delivery and commit to
/// make it immediately visible in the queue. In non-tx mode a plain nack is
/// sufficient (confirms are per-publish, not per-nack).
async fn nack_requeue_all_pending(
    pending_deliveries: &mut HashMap<String, VecDeque<ReceivedDelivery>>,
    publisher: Option<&ChannelPublisher>,
) {
    for (key, deliveries) in pending_deliveries.drain() {
        for received in deliveries {
            debug!(
                sequence_key = %key,
                "nack-requeuing buffered delivery on shutdown"
            );
            if let Err(e) = received
                .delivery
                .nack(BasicNackOptions {
                    requeue: true,
                    ..BasicNackOptions::default()
                })
                .await
            {
                error!("failed to nack-requeue buffered delivery: {e}");
            }
            if let Some(pub_) = publisher
                && let Err(e) = pub_.commit_if_tx().await
            {
                error!("tx_commit failed after nack-requeue on shutdown: {e}");
            }
        }
    }
}

/// Run the handler future with an optional timeout, emitting inflight/consumed/duration metrics.
/// Returns `Outcome::Retry` if the timeout is exceeded or the handler panics.
///
/// The future is run inside a child `tokio::spawn` so a panic inside the
/// user's handler is caught here (as `JoinError::is_panic`) and surfaced as
/// `Outcome::Retry` with metrics recorded — without this, the spawned task
/// aborts before the metric calls and panicked handlers disappear from the
/// consumed/latency series even though the caller still requeues them.
async fn invoke_handler<F>(
    fut: F,
    timeout: Option<Duration>,
    timeout_outcome: Option<Outcome>,
    topic: &str,
    group: Option<&str>,
) -> Outcome
where
    F: Future<Output = Outcome> + Send + 'static,
{
    let _inflight = metrics::InflightGuard::from_refs(topic, group);
    let start = std::time::Instant::now();
    let mut join = tokio::spawn(fut);
    let outcome = match timeout {
        Some(duration) => match tokio::time::timeout(duration, &mut join).await {
            Ok(Ok(o)) => o,
            Ok(Err(e)) => {
                warn!(error = %e, "handler task panicked, retrying message");
                Outcome::Retry
            }
            Err(_) => {
                join.abort();
                let resolved = handler_timeout_outcome(timeout_outcome);
                warn!(outcome = ?resolved, "handler exceeded timeout ({duration:?})");
                metrics::record_failed(topic, group, metrics::FailReason::Timeout);
                resolved
            }
        },
        None => match join.await {
            Ok(o) => o,
            Err(e) => {
                warn!(error = %e, "handler task panicked, retrying message");
                Outcome::Retry
            }
        },
    };
    let elapsed = start.elapsed().as_secs_f64();
    metrics::record_consumed(topic, group, &outcome);
    metrics::record_processing_duration(topic, group, &outcome, elapsed);
    outcome
}

/// Spawns a handler task for a deserialized message.
/// Returns the oneshot receiver that will resolve with the handler's outcome.
#[allow(clippy::too_many_arguments)]
fn spawn_handler<T, H>(
    handler: &Arc<H>,
    ctx: &Arc<H::Context>,
    message: T::Message,
    metadata: MessageMetadata,
    timeout: Option<Duration>,
    timeout_outcome: Option<Outcome>,
    notify: &Arc<Notify>,
    topic: Arc<str>,
    group: Option<Arc<str>>,
) -> oneshot::Receiver<Outcome>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let (tx, rx) = oneshot::channel();
    let h = handler.clone();
    let c = ctx.clone();
    let n = notify.clone();
    tokio::spawn(async move {
        let outcome = invoke_handler(
            async move { h.handle(message, metadata, c.as_ref()).await },
            timeout,
            timeout_outcome,
            &topic,
            group.as_deref(),
        )
        .await;
        let _ = tx.send(outcome);
        n.notify_one();
    });
    rx
}

/// Spawns a handler task for a sequenced message, signalling completion via an
/// mpsc channel with the sequence key. This avoids O(N) polling of all in-flight
/// keys to find which one completed.
#[allow(clippy::too_many_arguments)]
fn spawn_handler_keyed<T, H>(
    handler: &Arc<H>,
    ctx: &Arc<H::Context>,
    message: T::Message,
    metadata: MessageMetadata,
    timeout: Option<Duration>,
    timeout_outcome: Option<Outcome>,
    completed_tx: &mpsc::UnboundedSender<String>,
    key: String,
    topic: Arc<str>,
    group: Option<Arc<str>>,
) -> oneshot::Receiver<Outcome>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let (tx, rx) = oneshot::channel();
    let h = handler.clone();
    let c = ctx.clone();
    let completed = completed_tx.clone();
    tokio::spawn(async move {
        let outcome = invoke_handler(
            async move { h.handle(message, metadata, c.as_ref()).await },
            timeout,
            timeout_outcome,
            &topic,
            group.as_deref(),
        )
        .await;
        let _ = tx.send(outcome);
        let _ = completed.send(key);
    });
    rx
}

/// Attempts to deserialize a delivery's payload. On failure, logs the error
/// and rejects the delivery (nack without requeue).
/// Returns `Some(message)` on success, `None` if rejected.
#[allow(clippy::too_many_arguments)]
async fn try_deserialize_or_reject<T: Topic>(
    received: &ReceivedDelivery,
    metadata: &MessageMetadata,
    queue: &str,
    topology: &'static QueueTopology,
    publisher: &ChannelPublisher,
    options: &ConsumerOptions,
    topic: &str,
    group: Option<&str>,
) -> Option<T::Message> {
    metrics::record_message_size(topic, group, received.payload.len());

    if let Err(e) = options.validate_payload_message_size(received.payload.len()) {
        warn!(
            error = %e,
            delivery_id = %metadata.delivery_id,
            queue,
            "rejecting oversized message"
        );
        router::route_reject(
            &received.delivery,
            topology,
            publisher,
            group,
            metrics::FailReason::Oversize,
        )
        .await
        .ok();
        return None;
    }
    match <T::Codec as crate::Codec<T::Message>>::decode_owned(received.payload.clone()) {
        Ok(message) => Some(message),
        Err(err) => {
            error!(
                error = %err,
                delivery_id = %metadata.delivery_id,
                queue = %queue,
                "failed to deserialize message"
            );
            router::route_reject(
                &received.delivery,
                topology,
                publisher,
                group,
                metrics::FailReason::Deserialize,
            )
            .await
            .ok();
            None
        }
    }
}

impl RabbitMqConsumer {
    pub async fn run<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: crate::ConsumerOptions<RabbitMq>,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        self.run_with_inner::<T, H>(handler, ctx, options.into_inner())
            .await
    }

    pub(crate) async fn run_with_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let topology = T::topology();
        let consumer = RabbitMqConsumer::new(self.client.clone());
        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        consumer
            .run_internal_concurrent::<T, H>(
                handler,
                ctx,
                Attachment::Shared(topology.queue()),
                topology,
                options,
            )
            .await
    }

    /// Run this process's own ephemeral subscription to `T` until shutdown.
    ///
    /// Reached only through
    /// [`BroadcastSubscriber`](crate::broadcast::BroadcastSubscriber), which
    /// already refuses a non-broadcast topology; the guard here is the
    /// backend-side half of that check, so a direct caller cannot attach a
    /// fanout subscription to a topic whose publishes go to the shared queue
    /// and then wonder why nothing arrives.
    ///
    /// A reconnect declares a *new* ephemeral queue. Messages published while
    /// the connection was down reach no queue and are gone — deliver-new
    /// applied to the reconnect window, and the same best-effort contract the
    /// subscription has everywhere else.
    pub(crate) async fn run_broadcast_with_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let topology = T::topology();
        if !topology.broadcast() {
            return Err(ShoveError::Topology(format!(
                "topic '{}' is not a broadcast topology; a fanout subscription to it would \
                 receive nothing, because publishes go to the shared queue",
                topology.queue()
            )));
        }
        let exchange = super::topology::broadcast_exchange(topology.queue());
        let consumer = RabbitMqConsumer::new(self.client.clone());
        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        consumer
            .run_internal_concurrent::<T, H>(
                handler,
                ctx,
                Attachment::Broadcast {
                    exchange: &exchange,
                },
                topology,
                options,
            )
            .await
    }

    pub async fn run_fifo<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: crate::ConsumerOptions<RabbitMq>,
    ) -> Result<()>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        self.run_fifo_with_inner::<T, H>(handler, ctx, options.into_inner())
            .await
    }

    pub(crate) async fn run_fifo_with_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
    ) -> Result<()>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        let handles = self.spawn_fifo_shards::<T, H>(handler, ctx, options)?;
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => error!("sequenced consumer sub-task failed: {e}"),
                Err(e) => error!("sequenced consumer task panicked: {e}"),
            }
        }
        Ok(())
    }

    /// Spawn one task per routing shard and return the join handles.
    ///
    /// The `pub(crate)` visibility is required for Phase 2 of this work
    /// (Task 15) which calls it from the consumer-group module.
    pub(crate) fn spawn_fifo_shards<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
    ) -> Result<Vec<tokio::task::JoinHandle<Result<()>>>>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        let topology = T::topology();
        let seq = topology.sequencing().ok_or_else(|| {
            ShoveError::Topology("run_fifo called on topic without sequencing config".into())
        })?;

        let on_failure = seq.on_failure();
        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let prefetch = options.prefetch_count;
        let client = self.client.clone();
        let mut handles = Vec::with_capacity(seq.routing_shards() as usize);

        for i in 0..seq.routing_shards() {
            let sub_queue = format!("{}-seq-{i}", topology.queue());
            let shard_hold_queues = topology.shard_hold_queue_names(i);
            let h = handler.clone();
            let c = ctx.clone();
            let inner_client = client.clone();
            // Cloned, not rebuilt from defaults: a hand-copied allowlist silently
            // drops every knob added to `ConsumerOptionsInner` after it was
            // written, which is how the shards lost `handler_timeout_outcome`
            // and the `consumer_group` metrics label.
            let mut opts = options.clone();
            opts.prefetch_count = prefetch;
            // Per shard: each tracks its own in-flight count and stores
            // `in_flight_count > 0`, so one shared flag would let an idle shard
            // clear a busy shard's mark. The consequence, unchanged from before
            // this clone: a caller's own `processing_handle()` stays flat on
            // this path. Inert for scaling, because a FIFO group is pinned to
            // one consumer and `scale_down` refuses at `min_consumers`.
            // Aggregating it honestly needs a busy-shard counter, not a bool.
            opts.processing = Arc::new(AtomicBool::new(false));
            handles.push(tokio::spawn(async move {
                let consumer = RabbitMqConsumer::new(inner_client);
                consumer
                    .run_internal_concurrent_sequenced::<T, H>(
                        h,
                        c,
                        &sub_queue,
                        topology,
                        opts,
                        on_failure,
                        shard_hold_queues,
                    )
                    .await
            }));
        }

        Ok(handles)
    }

    /// Drain a FIFO consumer with a timeout, mirroring
    /// [`ConsumerSupervisor::run_until_timeout`] for sequenced topics.
    ///
    /// Spawns one task per `routing_shards` (same as [`Self::run_fifo`]). Races
    /// `signal` against shards exiting on their own. When `signal` resolves,
    /// cancels `options.shutdown` and waits up to `drain_timeout` for shards
    /// to finish; surviving shards are aborted and counted as panics.
    pub async fn run_fifo_until_timeout<T, H, S>(
        &self,
        handler: H,
        ctx: H::Context,
        options: crate::ConsumerOptions<RabbitMq>,
        signal: S,
        drain_timeout: Duration,
    ) -> SupervisorOutcome
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
        S: Future<Output = ()> + Send + 'static,
    {
        self.run_fifo_until_timeout_with_inner::<T, H, S>(
            handler,
            ctx,
            options.into_inner(),
            signal,
            drain_timeout,
        )
        .await
    }

    pub(crate) async fn run_fifo_until_timeout_with_inner<T, H, S>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
        signal: S,
        drain_timeout: Duration,
    ) -> SupervisorOutcome
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
        S: Future<Output = ()> + Send + 'static,
    {
        let shutdown = options.shutdown.clone();
        let handles = match self.spawn_fifo_shards::<T, H>(handler, ctx, options) {
            Ok(h) => h,
            Err(e) => {
                error!(error = %e, "run_fifo_until_timeout: shard spawn failed");
                return SupervisorOutcome {
                    errors: 1,
                    panics: 0,
                    timed_out: false,
                };
            }
        };
        drive_fifo_until_timeout(handles, shutdown, signal, drain_timeout).await
    }

    pub async fn run_dlq<T, H>(&self, handler: H, ctx: H::Context) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let topology = T::topology();
        let dlq = topology
            .dlq()
            .ok_or_else(|| ShoveError::Topology("run_dlq called on topic without DLQ".into()))?;
        let shutdown = self.client.shutdown_token();
        let options = ConsumerOptions::defaults_with_shutdown(shutdown);

        run_with_reconnect(
            &options.shutdown,
            dlq,
            options.max_reconnect_attempts,
            || consume_dlq_loop::<T, H>(&self.client, &handler, &ctx, dlq, &options),
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Batch consumption
// ---------------------------------------------------------------------------

/// Fallback for the reconnect backoff's impossible-`None` — `Backoff`'s
/// iterator never ends, so this only exists to keep the reconnect arm free of
/// panics; it mirrors `Backoff::default()`'s ceiling.
const RECONNECT_BACKOFF_FALLBACK: Duration = Duration::from_secs(30);

/// The flush threshold (and channel prefetch) actually used for a configured
/// `max_batch_size`: at least 1, at most `u16::MAX` — AMQP's `basic.qos`
/// prefetch window is a `u16`, and a batch is held **unacked** inside that
/// window, so a threshold above it could never fill and every flush would
/// stall until `max_batch_age`. This effective cap is a documented divergence
/// from Kafka/InMemory, which honour any configured size (they clamp only the
/// pre-allocation; that clamp applies here too, separately, in
/// [`RabbitMqBatch::new`]).
fn effective_batch_size(configured: usize) -> usize {
    configured.max(1).min(u16::MAX as usize)
}

/// One in-flight batch: decoded messages index-parallel with their delivery
/// tags, plus parked pre-handler drops as `(tag, reason)` pairs. No payload
/// retention (unlike Kafka's `retain_raw`): a RabbitMQ dead-letter is a
/// broker-side DLX move on nack, never a republish, so the broker holds the
/// bytes until the tag settles.
///
/// Every tag belongs to the one channel the enclosing [`consume_loop_batch`]
/// invocation opened, and the batch lives and dies with that invocation — a
/// batch structurally cannot span channels, which is the AMQP settling
/// constraint (ack/nack must go to the delivering channel) enforced by shape
/// rather than by bookkeeping.
struct RabbitMqBatch<T: Topic> {
    messages: Vec<(T::Message, MessageMetadata)>,
    /// Index-parallel with `messages`, ascending: tags are assigned in
    /// delivery order and ingest follows the stream.
    handled_tags: Vec<u64>,
    /// Pre-handler drops, `(delivery_tag, reason)`, ascending and interleaved
    /// with `handled_tags` in tag order. Counted by [`Self::flush_len`] so an
    /// all-poison window still trips the size trigger instead of growing for
    /// the whole `max_batch_age`.
    parked: Vec<(u64, metrics::FailReason)>,
    /// Pre-allocation installed by [`Self::take_messages`] —
    /// `effective_max_batch_size` clamped to [`PREALLOC_CAP`], never the raw
    /// value, for the same `Vec::with_capacity`-overflow reason both other
    /// backends clamp it.
    cap: usize,
}

impl<T: Topic> RabbitMqBatch<T> {
    fn new(effective_max_batch_size: usize) -> Self {
        let prealloc = effective_max_batch_size.min(PREALLOC_CAP);
        Self {
            messages: Vec::with_capacity(prealloc),
            handled_tags: Vec::with_capacity(prealloc),
            parked: Vec::new(),
            cap: prealloc,
        }
    }

    /// Messages plus every pre-handler drop — the quantity the size trigger
    /// bounds, and (unlike on Kafka/InMemory, where a no-DLQ drop is
    /// destroyed early) exactly the number of unacked deliveries this batch
    /// holds against the channel's prefetch window.
    fn flush_len(&self) -> usize {
        self.messages.len() + self.parked.len()
    }

    /// Counts parked drops too: an all-parked partial window at shutdown must
    /// still flush (dead-letter), not be abandoned to redeliver and re-count
    /// its failures on the next start.
    fn is_empty(&self) -> bool {
        self.flush_len() == 0
    }

    fn push(&mut self, message: T::Message, metadata: MessageMetadata, tag: u64) {
        self.messages.push((message, metadata));
        self.handled_tags.push(tag);
    }

    fn park(&mut self, tag: u64, reason: metrics::FailReason) {
        self.parked.push((tag, reason));
    }

    fn highest_handled_tag(&self) -> Option<u64> {
        self.handled_tags.last().copied()
    }

    /// Highest tag across handled and parked — both vectors are ascending, so
    /// this is the max of their last elements.
    fn highest_tag(&self) -> Option<u64> {
        self.handled_tags
            .last()
            .copied()
            .max(self.parked.last().map(|&(tag, _)| tag))
    }

    /// Take the handled messages for the handler call, refilling with a fresh
    /// pre-sized `Vec` rather than `mem::take`'s zero-capacity default — the
    /// taken `Vec` is genuinely moved into the handler.
    fn take_messages(&mut self) -> Vec<(T::Message, MessageMetadata)> {
        std::mem::replace(&mut self.messages, Vec::with_capacity(self.cap))
    }

    fn clear(&mut self) {
        self.messages.clear();
        self.handled_tags.clear();
        self.parked.clear();
    }
}

/// Fields the batch flush needs that do not change across flushes, mirroring
/// Kafka's `BatchFlushCtx` / InMemory's `InMemoryFlushCtx`.
struct RabbitMqFlushCtx<'a> {
    channel: &'a Channel,
    topology: &'static QueueTopology,
    topic: &'a str,
    group: Option<&'a str>,
    /// Per-consumer token (`options.shutdown`): races the redelivery backoff
    /// sleep so a wedged handler cannot add the escalated delay to a stop.
    shutdown: &'a CancellationToken,
    /// Client-wide token: same race, plus the loop's own abandon arm.
    client_shutdown: &'a CancellationToken,
    handler_timeout: Option<Duration>,
    handler_timeout_outcome: Option<Outcome>,
}

/// Prepare one freshly-delivered message and either push it into the batch or
/// park its tag for a dead-letter nack at the flush. The failure is counted
/// here (per message, at ingest); the *discard* is settled at the flush by
/// whether the broker accepted the nack — the `record_terminal` split, same
/// as Kafka's batch ingest.
fn ingest_batch_delivery<T: Topic>(
    batch: &mut RabbitMqBatch<T>,
    received: &ReceivedDelivery,
    max_message_size: Option<usize>,
    topic: &str,
    group: Option<&str>,
) {
    let tag = received.delivery.delivery_tag;
    metrics::record_message_size(topic, group, received.payload.len());

    if let Err(e) = validate_message_size(received.payload.len(), max_message_size) {
        warn!(error = %e, topic, "oversized message, dropped before the batch handler");
        metrics::record_failed(topic, group, metrics::FailReason::Oversize);
        batch.park(tag, metrics::FailReason::Oversize);
        return;
    }
    match <T::Codec as crate::Codec<T::Message>>::decode_owned(received.payload.clone()) {
        Ok(message) => {
            let metadata = extract_message_metadata(&received.delivery);
            batch.push(message, metadata, tag);
        }
        Err(e) => {
            warn!(error = %e, topic, "failed to deserialize message, dropped before the batch handler");
            metrics::record_failed(topic, group, metrics::FailReason::Deserialize);
            batch.park(tag, metrics::FailReason::Deserialize);
        }
    }
}

/// Hand the buffered batch to the handler and settle the single returned
/// [`Outcome`] via the shared [`settle_batch_outcome`] classifier — the same
/// three-way split Kafka and InMemory use, with RabbitMQ's mechanics in each
/// arm:
///
/// - `Commit`: parked pre-handler drops are individually nacked to the DLX
///   first (their tags interleave below the handled ones), then one
///   `basic_ack(multiple: true)` on the highest handled tag retires the whole
///   batch — the single-frame settle that is this backend's payoff.
/// - `DeadLetter`: one `basic_nack(multiple: true, requeue: false)` on the
///   batch's highest tag dead-letters everything broker-side.
/// - `Redeliver`: one `basic_nack(multiple: true, requeue: true)` — issued
///   **before** the backoff sleep, so aborting the task mid-backoff cannot
///   destroy a batch that exists only in this loop; the broker flags the
///   redeliveries `redelivered`. The sleep paces this consumer only and is
///   cut short by either shutdown token.
///
/// A window whose every message was dropped pre-handler never reaches the
/// handler at all: it is dead-lettered outright, because routing it through a
/// handler that answers `Retry` would redeliver the same poison forever — the
/// no-forward-progress failure Kafka's empty-batch commit arm exists to
/// prevent.
///
/// Every settle error propagates (see the router's batch-settling error
/// contract): after a failed frame the channel's outstanding-tag set is
/// unknown, so the only sound recovery is to abandon this channel — the
/// reconnect loop opens a fresh one and the broker requeues everything
/// unsettled.
async fn flush_rabbitmq_batch<T, H>(
    flush: &RabbitMqFlushCtx<'_>,
    handler: &H,
    ctx: &H::Context,
    batch: &mut RabbitMqBatch<T>,
    redelivery_backoff: &mut Backoff,
) -> Result<()>
where
    T: Topic,
    H: BatchMessageHandler<T>,
{
    if batch.flush_len() == 0 {
        return Ok(());
    }
    let batch_size = batch.messages.len();

    if batch_size == 0 {
        // All-poison window: nothing to hand the handler. One multi-nack
        // dead-letters every parked drop and the window retires.
        if let Some(highest) = batch.highest_tag() {
            router::reject_batch_multiple(
                flush.channel,
                flush.topology,
                flush.group,
                0,
                &batch.parked,
                highest,
            )
            .await?;
        }
        // Terminal progress resets the redelivery backoff — the InMemory
        // convention (`finish_terminal_flush`); Kafka's all-dropped arm
        // deliberately differs (no reset). Chosen, not drifted: a queue
        // alternating poison windows with retried ones re-earns escalation.
        *redelivery_backoff = batch_redelivery_backoff();
        batch.clear();
        return Ok(());
    }

    let messages = batch.take_messages();
    let outcome = invoke_batch_handler(
        || handler.handle_batch(messages, ctx),
        flush.handler_timeout,
        flush.handler_timeout_outcome.clone(),
        flush.topic,
        flush.group,
        batch_size as u64,
    )
    .await;

    match settle_batch_outcome(&outcome) {
        BatchSettlement::Commit => {
            router::settle_parked_batch(flush.channel, flush.topology, flush.group, &batch.parked)
                .await?;
            if let Some(highest_handled) = batch.highest_handled_tag() {
                router::ack_batch_multiple(flush.channel, highest_handled).await?;
            }
            *redelivery_backoff = batch_redelivery_backoff();
            debug!(queue = flush.topology.queue(), batch_size, "batch acked");
            batch.clear();
        }
        BatchSettlement::DeadLetter => {
            if let Some(highest) = batch.highest_tag() {
                router::reject_batch_multiple(
                    flush.channel,
                    flush.topology,
                    flush.group,
                    batch_size,
                    &batch.parked,
                    highest,
                )
                .await?;
            }
            *redelivery_backoff = batch_redelivery_backoff();
            batch.clear();
        }
        BatchSettlement::Redeliver => {
            let delay = next_redelivery_delay(redelivery_backoff);
            warn!(
                queue = flush.topology.queue(),
                batch_size,
                ?outcome,
                delay_ms = delay.as_millis() as u64,
                "batch handler returned a non-Ack outcome, redelivering the whole batch"
            );
            if let Some(highest) = batch.highest_tag() {
                router::redeliver_batch_multiple(flush.channel, highest).await?;
            }
            batch.clear();
            // The nack above already returned the batch to the broker, so the
            // sleep only paces THIS consumer's next window; shutdown (either
            // token) cuts the delay rather than the batch.
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = flush.shutdown.cancelled() => {}
                () = flush.client_shutdown.cancelled() => {}
            }
        }
    }
    Ok(())
}

impl RabbitMqConsumer {
    /// [`BatchConsumerImpl::run_batch`](crate::backend::BatchConsumerImpl)
    /// for RabbitMQ. AMQP has no batch receive, so this is a genuine
    /// accumulator over one consumer stream: take deliveries up to the flush
    /// threshold or until `max_batch_age` elapses, then flush; the payoff is
    /// settle-side, where `multiple: true` frames retire the whole batch in
    /// one round trip (see [`flush_rabbitmq_batch`]).
    ///
    /// # Prefetch and the effective batch size
    ///
    /// Buffered deliveries are held **unacked**, and AMQP's prefetch window
    /// (`basic.qos`) is a `u16` — a batch above the prefetch count can never
    /// fill, stalling every flush until `max_batch_age`. So the channel's
    /// prefetch is set to the flush threshold, and both are
    /// `min(max_batch_size, u16::MAX)`: above 65 535 the configured size is
    /// clamped (with a warning), a documented divergence from Kafka/InMemory,
    /// which honour any size. The pre-allocation is separately clamped to
    /// [`PREALLOC_CAP`], as on every backend.
    ///
    /// # Sequencing guard
    ///
    /// No `validate_batch_topic` call here: this is reachable only through
    /// the generic [`BatchConsumer::run`](crate::batch_consumer::BatchConsumer::run)
    /// wrapper, which already ran the guard — the same shape as InMemory.
    /// RabbitMQ deliberately adds no public inherent `run_batch` (Kafka's
    /// exists for pre-generic compatibility and re-runs the guard itself).
    ///
    /// # Channel lifecycle and reconnects
    ///
    /// Each inner-loop invocation opens one confirm-mode channel (never
    /// transactional — a batch consumer publishes nothing, so there is no
    /// publish/ack pair for a transaction to make atomic) and its batch is
    /// local to that invocation. On any retryable error — a dead stream, a
    /// failed settle frame — the invocation returns and the reconnect loop
    /// opens a fresh channel; the broker requeues everything unsettled when
    /// the old channel drops. The loop observes **both** the per-consumer
    /// token and the client token: after `client.shutdown()` channel creation
    /// fails retryably forever, and only the client token distinguishes that
    /// from a broker outage worth redialing.
    pub(crate) async fn run_batch_with_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: BatchConsumerOptionsInner,
    ) -> Result<()>
    where
        T: NotSequenced,
        H: BatchMessageHandler<T>,
    {
        let topology = T::topology();
        let queue = topology.queue();
        let configured = options.max_batch_size.max(1);
        let effective_max = effective_batch_size(options.max_batch_size);
        if configured > effective_max {
            warn!(
                queue,
                configured,
                effective_max,
                "max_batch_size exceeds AMQP's u16 prefetch window; clamping the flush threshold"
            );
        }
        let prefetch = effective_max as u16;
        let shutdown = options.shutdown.clone();
        let client_shutdown = self.client.shutdown_token();
        let group = options.consumer_group.clone();
        let topic: Arc<str> = Arc::from(queue);

        info!(
            queue,
            max_batch_size = effective_max,
            max_batch_age = ?options.max_batch_age,
            prefetch,
            "RabbitMQ batch consumer started"
        );

        let mut backoff = Backoff::default();
        let mut attempts = 0u32;
        loop {
            match self
                .consume_loop_batch::<T, H>(
                    &handler,
                    &ctx,
                    queue,
                    topology,
                    &options,
                    effective_max,
                    prefetch,
                    &shutdown,
                    &client_shutdown,
                    &topic,
                    group.as_deref(),
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    // Token check FIRST: `client.shutdown()` cancels its
                    // token, waits a short grace, then closes the connection,
                    // so mid-shutdown errors are expected — counting them
                    // against `max_reconnect_attempts` (or letting an
                    // unrecoverable teardown error surface as fatal) would
                    // misreport a graceful close.
                    if shutdown.is_cancelled() || client_shutdown.is_cancelled() {
                        return Ok(());
                    }
                    // A non-retryable error (misdeclared queue, auth) never
                    // heals by redialing.
                    if !e.is_retryable() {
                        return Err(e);
                    }
                    attempts += 1;
                    if let Some(max) = options.max_reconnect_attempts
                        && attempts >= max
                    {
                        error!(
                            queue,
                            attempts,
                            error = %e,
                            "max reconnect attempts reached, giving up"
                        );
                        return Err(ShoveError::Connection(format!(
                            "batch consumer on '{queue}' exhausted {max} reconnect attempt(s): {e}"
                        )));
                    }
                    let delay = backoff.next().unwrap_or(RECONNECT_BACKOFF_FALLBACK);
                    warn!(
                        queue,
                        attempt = attempts,
                        max_reconnect_attempts = ?options.max_reconnect_attempts,
                        "batch consumer error, reconnecting in {delay:?}: {e}"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = shutdown.cancelled() => return Ok(()),
                        _ = client_shutdown.cancelled() => return Ok(()),
                    }
                }
            }
        }
    }

    /// One channel's worth of the batch loop: open, accumulate, flush, until
    /// a shutdown token or an error ends the channel. See
    /// [`Self::run_batch_with_inner`] for the lifecycle contract.
    #[allow(clippy::too_many_arguments)]
    async fn consume_loop_batch<T, H>(
        &self,
        handler: &H,
        ctx: &H::Context,
        queue: &str,
        topology: &'static QueueTopology,
        options: &BatchConsumerOptionsInner,
        effective_max: usize,
        prefetch: u16,
        shutdown: &CancellationToken,
        client_shutdown: &CancellationToken,
        topic: &Arc<str>,
        group: Option<&str>,
    ) -> Result<()>
    where
        T: NotSequenced,
        H: BatchMessageHandler<T>,
    {
        let (channel, mut stream) = open_consumer(&self.client, queue, prefetch, false).await?;

        let flush_ctx = RabbitMqFlushCtx {
            channel: &channel,
            topology,
            topic: topic.as_ref(),
            group,
            shutdown,
            client_shutdown,
            handler_timeout: options.handler_timeout,
            handler_timeout_outcome: options.handler_timeout_outcome.clone(),
        };

        let mut batch: RabbitMqBatch<T> = RabbitMqBatch::new(effective_max);
        let mut deadline: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;
        let mut redelivery_backoff = batch_redelivery_backoff();

        loop {
            if batch.flush_len() >= effective_max {
                flush_rabbitmq_batch(
                    &flush_ctx,
                    handler,
                    ctx,
                    &mut batch,
                    &mut redelivery_backoff,
                )
                .await?;
                deadline = None;
                continue;
            }

            let sleep_until_deadline = async {
                match deadline.as_mut() {
                    Some(d) => d.await,
                    None => std::future::pending::<()>().await,
                }
            };

            tokio::select! {
                biased;

                () = shutdown.cancelled() => {
                    // The connection stays up on a per-consumer cancel, so
                    // the partial batch can still be flushed and settled.
                    if !batch.is_empty() {
                        flush_rabbitmq_batch(&flush_ctx, handler, ctx, &mut batch, &mut redelivery_backoff).await?;
                    }
                    info!(queue, "shutdown signal received, RabbitMQ batch consumer stopped");
                    return Ok(());
                }
                () = client_shutdown.cancelled() => {
                    // `client.shutdown()` closes the connection a short grace
                    // after cancelling this token — flushing here would run
                    // the handler precisely when its settle is doomed,
                    // duplicating side effects on redelivery. Abandon
                    // instead: nothing is acked, the closing connection
                    // requeues the whole window, and no handler ran twice.
                    info!(queue, "client shutdown, RabbitMQ batch consumer stopped");
                    return Ok(());
                }
                () = sleep_until_deadline => {
                    flush_rabbitmq_batch(&flush_ctx, handler, ctx, &mut batch, &mut redelivery_backoff).await?;
                    deadline = None;
                }
                item = stream.next() => {
                    let received = ReceivedDelivery::new(unwrap_delivery(item, queue)?);
                    // Armed on ANY ingest — parked included — so an
                    // all-poison window still flushes by age.
                    if deadline.is_none() {
                        deadline = Some(Box::pin(tokio::time::sleep(options.max_batch_age)));
                    }
                    ingest_batch_delivery::<T>(
                        &mut batch,
                        &received,
                        options.max_message_size,
                        topic.as_ref(),
                        group,
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::topology::TopologyBuilder;

    #[tokio::test]
    async fn invoke_handler_returns_outcome_without_timeout() {
        let outcome = invoke_handler(async { Outcome::Ack }, None, None, "test-topic", None).await;
        assert!(matches!(outcome, Outcome::Ack));
    }

    #[tokio::test]
    async fn invoke_handler_returns_outcome_within_timeout() {
        let timeout = Some(Duration::from_secs(1));
        let outcome =
            invoke_handler(async { Outcome::Reject }, timeout, None, "test-topic", None).await;
        assert!(matches!(outcome, Outcome::Reject));
    }

    #[tokio::test]
    async fn invoke_handler_returns_retry_on_timeout() {
        let timeout = Some(Duration::from_millis(10));
        let outcome = invoke_handler(
            async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Outcome::Ack
            },
            timeout,
            None,
            "test-topic",
            None,
        )
        .await;
        assert!(matches!(outcome, Outcome::Retry));
    }

    #[tokio::test]
    async fn invoke_handler_returns_retry_on_panic() {
        // The handler panics inside the spawned task. Without panic catching,
        // the task aborts before `record_consumed`/`record_processing_duration`
        // run and the caller sees a dropped channel; we surface `Retry` so
        // metrics are recorded for the requeued message.
        let outcome = invoke_handler::<std::pin::Pin<Box<dyn Future<Output = Outcome> + Send>>>(
            Box::pin(async { panic!("boom") }),
            None,
            None,
            "test-topic",
            None,
        )
        .await;
        assert!(matches!(outcome, Outcome::Retry));
    }

    #[test]
    fn unwrap_delivery_returns_delivery_on_some_ok() {
        use lapin::types::ShortString;
        let delivery = Delivery::mock(
            1,
            ShortString::from(""),
            ShortString::from(""),
            false,
            vec![],
        );
        let result = unwrap_delivery(Some(Ok(delivery)), "test-queue");
        assert!(result.is_ok());
    }

    #[test]
    fn unwrap_delivery_returns_connection_error_on_some_err() {
        let lapin_err = LapinError::from(lapin::ErrorKind::InvalidChannelState(
            lapin::ChannelState::Closed,
            "test",
        ));
        let result = unwrap_delivery(Some(Err(lapin_err)), "test-queue");
        match result {
            Err(ShoveError::Connection(msg)) => {
                assert!(msg.contains("consumer stream error on test-queue"));
            }
            other => panic!("expected ShoveError::Connection, got {other:?}"),
        }
    }

    #[test]
    fn unwrap_delivery_returns_connection_error_on_none() {
        let result = unwrap_delivery(None, "test-queue");
        match result {
            Err(ShoveError::Connection(msg)) => {
                assert!(msg.contains("consumer stream closed for test-queue"));
            }
            other => panic!("expected ShoveError::Connection, got {other:?}"),
        }
    }

    #[test]
    fn key_state_in_flight_holds_delivery_and_receiver() {
        use lapin::types::ShortString;
        let delivery = Delivery::mock(
            1,
            ShortString::from("key-a"),
            ShortString::from(""),
            false,
            vec![1, 2, 3],
        );
        let (_tx, rx) = oneshot::channel::<Outcome>();
        let state = KeyState::InFlight {
            received: Box::new(ReceivedDelivery::new(delivery)),
            outcome_rx: rx,
        };
        let KeyState::InFlight { received, .. } = state else {
            panic!("expected InFlight");
        };
        assert_eq!(&received.payload[..], &[1, 2, 3]);
        assert!(received.delivery.data.is_empty());
    }

    #[test]
    fn key_state_awaiting_retry_is_distinct() {
        let state = KeyState::AwaitingRetry(Instant::now());
        assert!(matches!(state, KeyState::AwaitingRetry(_)));
        assert!(!matches!(state, KeyState::InFlight { .. }));
    }

    #[tokio::test]
    async fn nack_requeue_all_pending_handles_empty_map() {
        let mut pending: HashMap<String, VecDeque<ReceivedDelivery>> = HashMap::new();
        nack_requeue_all_pending(&mut pending, None).await;
        assert!(pending.is_empty());
    }

    /// [`effective_batch_size`]: the wire-imposed clamp. A threshold above
    /// the u16 prefetch window could never fill (the batch is held unacked
    /// inside it), stalling every flush until `max_batch_age` — the hang the
    /// clamp exists to prevent.
    #[test]
    fn effective_batch_size_clamps_to_the_prefetch_window() {
        assert_eq!(effective_batch_size(0), 1);
        assert_eq!(effective_batch_size(1), 1);
        assert_eq!(effective_batch_size(500), 500);
        assert_eq!(effective_batch_size(u16::MAX as usize), u16::MAX as usize);
        assert_eq!(
            effective_batch_size(u16::MAX as usize + 1),
            u16::MAX as usize
        );
        assert_eq!(effective_batch_size(usize::MAX), u16::MAX as usize);
    }

    /// [`RabbitMqBatch`]'s accounting: parked pre-handler drops count toward
    /// the flush threshold (an all-poison window must trip the size trigger),
    /// and the settle targets are the highest tags of each set.
    #[test]
    fn batch_buffer_counts_parked_and_tracks_highest_tags() {
        struct BufTopic;
        impl Topic for BufTopic {
            type Message = ();
            type Codec = crate::JsonCodec;
            fn topology() -> &'static crate::QueueTopology {
                static TOPOLOGY: std::sync::OnceLock<crate::QueueTopology> =
                    std::sync::OnceLock::new();
                TOPOLOGY
                    .get_or_init(|| TopologyBuilder::new("rmq-batch-buf").build())
            }
        }

        let mut batch: RabbitMqBatch<BufTopic> = RabbitMqBatch::new(8);
        assert!(batch.is_empty());
        assert_eq!(batch.highest_tag(), None);

        batch.push((), MessageMetadata::builder().build(), 1);
        batch.park(2, metrics::FailReason::Oversize);
        batch.push((), MessageMetadata::builder().build(), 3);
        batch.park(4, metrics::FailReason::Deserialize);

        assert_eq!(
            batch.flush_len(),
            4,
            "parked drops count toward the size trigger"
        );
        assert!(!batch.is_empty());
        assert_eq!(batch.highest_handled_tag(), Some(3));
        assert_eq!(
            batch.highest_tag(),
            Some(4),
            "a parked tag can be the batch's highest"
        );

        let messages = batch.take_messages();
        assert_eq!(messages.len(), 2);
        assert_eq!(
            batch.parked.len(),
            2,
            "take_messages leaves parked for the settle"
        );

        batch.clear();
        assert!(batch.is_empty());
        assert_eq!(batch.highest_tag(), None);
    }

    /// `with_max_batch_size(usize::MAX)` passes the public `> 0` assert; the
    /// buffer must clamp its pre-allocation rather than aborting inside
    /// `Vec::with_capacity` — the same [`PREALLOC_CAP`] trade both other
    /// backends make.
    #[test]
    fn batch_buffer_clamps_the_preallocation_not_the_batch_size() {
        struct BufTopic;
        impl Topic for BufTopic {
            type Message = ();
            type Codec = crate::JsonCodec;
            fn topology() -> &'static crate::QueueTopology {
                static TOPOLOGY: std::sync::OnceLock<crate::QueueTopology> =
                    std::sync::OnceLock::new();
                TOPOLOGY.get_or_init(|| {
                    TopologyBuilder::new("rmq-batch-buf-prealloc").build()
                })
            }
        }

        let batch: RabbitMqBatch<BufTopic> = RabbitMqBatch::new(usize::MAX);
        assert_eq!(batch.cap, PREALLOC_CAP);
        assert_eq!(batch.messages.capacity(), PREALLOC_CAP);
    }
}
