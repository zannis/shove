use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_lite::StreamExt;
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions};
use lapin::types::{FieldTable, ShortString};
use lapin::{Channel, Error as LapinError};
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::QueueTopology;
use crate::backends::rabbitmq::client::RabbitMqClient;
use crate::backends::rabbitmq::headers::{
    extract_dead_metadata, extract_message_metadata, get_retry_count,
};
use crate::backends::rabbitmq::publisher::ChannelPublisher;
use crate::backends::rabbitmq::router;
use crate::backend::ConsumerOptionsInner as ConsumerOptions;
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::metadata::MessageMetadata;
use crate::outcome::Outcome;
use crate::retry::Backoff;
use crate::topic::{SequencedTopic, Topic};
use crate::topology::{HoldQueue, SequenceFailure};

use super::map_lapin_error;

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

/// Unwrap a delivery from the consumer stream.
fn unwrap_delivery(
    item: Option<std::result::Result<Delivery, LapinError>>,
    queue: &str,
) -> Result<Delivery> {
    match item {
        Some(Ok(d)) => Ok(d),
        Some(Err(e)) => Err(map_lapin_error(
            &format!("consumer stream error on {queue}"),
            e,
        )),
        None => Err(ShoveError::Connection(format!(
            "consumer stream closed for {queue}"
        ))),
    }
}

/// Run `f` in a reconnect loop, retrying on transient errors until shutdown.
async fn run_with_reconnect<F, Fut>(
    shutdown: &CancellationToken,
    queue: &str,
    mut f: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut backoff = Backoff::default();
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
                let delay = backoff.next().expect("backoff is infinite");
                warn!("consumer error on {queue}: {e}. Reconnecting in {delay:?}");
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
        delivery: Box<Delivery>,
        outcome_rx: oneshot::Receiver<Outcome>,
    },
    /// The handler returned Retry/Defer and the message has been routed to a
    /// hold queue. The key is blocked until the retry comes back.
    AwaitingRetry,
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
    async fn run_internal_concurrent_sequenced<T, H>(
        &self,
        handler: Arc<H>,
        queue: &str,
        topology: &'static QueueTopology,
        options: ConsumerOptions,
        on_failure: SequenceFailure,
        shard_hold_queues: Vec<HoldQueue>,
    ) -> Result<()>
    where
        T: Topic,
        T::Message: for<'de> serde::Deserialize<'de>,
        H: MessageHandler<T, Context = ()>,
    {
        let mut poisoned_keys = HashSet::new();
        let mut pending_deliveries: HashMap<String, VecDeque<Delivery>> = HashMap::new();
        let mut backoff = Backoff::default();
        loop {
            match self
                .consume_loop_concurrent_sequenced::<T, H>(
                    handler.clone(),
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
                    let delay = backoff.next().expect("backoff is infinite");
                    warn!("consumer error on {queue}: {e}. Reconnecting in {delay:?}");
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
        queue: &str,
        topology: &'static QueueTopology,
        options: &ConsumerOptions,
        on_failure: SequenceFailure,
        poisoned_keys: &mut HashSet<String>,
        shard_hold_queues: &[HoldQueue],
        pending_deliveries: &mut HashMap<String, VecDeque<Delivery>>,
    ) -> Result<ChannelPublisher>
    where
        T: Topic,
        T::Message: for<'de> serde::Deserialize<'de>,
        H: MessageHandler<T, Context = ()>,
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

        let mut key_states: HashMap<String, KeyState> = HashMap::new();
        let mut in_flight_count: usize = 0;

        info!("concurrent-sequenced consumer started on sub-queue {queue} (prefetch={prefetch})");

        loop {
            // ── Drain completed handlers ──
            // Only process keys that have signalled completion via the channel.
            while let Ok(key) = completed_rx.try_recv() {
                let Some(state) = key_states.remove(&key) else {
                    continue;
                };
                let KeyState::InFlight {
                    delivery,
                    mut outcome_rx,
                } = state
                else {
                    // AwaitingRetry — shouldn't happen, but put it back.
                    key_states.insert(key, state);
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
                                delivery,
                                outcome_rx,
                            },
                        );
                        continue;
                    }
                };

                let retry_count = get_retry_count(&delivery);
                debug!(queue, sequence_key = %key, ?outcome, "message handled (concurrent-sequenced)");

                match outcome {
                    Outcome::Ack | Outcome::Reject => {
                        // Terminal outcomes — process, then drain pending.
                        if matches!(outcome, Outcome::Ack) {
                            router::route_ack(&delivery, &publisher).await;
                        } else {
                            if on_failure == SequenceFailure::FailAll {
                                info!(
                                    sequence_key = %key,
                                    queue = %queue,
                                    "poisoning sequence key (FailAll)"
                                );
                                poisoned_keys.insert(key.clone());
                            }
                            router::route_reject(&delivery, topology, &publisher).await;
                        }
                        in_flight_count -= 1;

                        // Drain pending deliveries for this key.
                        self.drain_pending_for_key::<T, H>(
                            &key,
                            &handler,
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
                        )
                        .await;
                    }
                    Outcome::Retry | Outcome::Defer => {
                        // Non-terminal — route to hold queue, key enters AwaitingRetry.
                        if matches!(outcome, Outcome::Retry) {
                            route_shard_retry(
                                &delivery,
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
                                let headers = router::clone_headers(&delivery);
                                match publisher
                                    .publish_to_queue(hold_queue.name(), &delivery.data, headers)
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
                                            router::nack_requeue(&delivery, &publisher).await;
                                        } else {
                                            if let Err(e) = publisher.commit_if_tx().await {
                                                error!("tx_commit failed for shard defer: {e}");
                                            } else {
                                                debug!(
                                                    "deferring message to shard hold queue {}",
                                                    hold_queue.name()
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            "failed to publish to shard hold queue {} for defer, requeuing: {e}",
                                            hold_queue.name()
                                        );
                                        router::nack_requeue(&delivery, &publisher).await;
                                    }
                                }
                            } else {
                                router::nack_requeue(&delivery, &publisher).await;
                            }
                        }
                        in_flight_count -= 1;
                        key_states.insert(key, KeyState::AwaitingRetry);
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
                    for (key, state) in key_states.drain() {
                        if let KeyState::InFlight { delivery, outcome_rx } = state {
                            let outcome = outcome_rx.await.unwrap_or(Outcome::Retry);
                            let retry_count = get_retry_count(&delivery);
                            debug!(
                                queue,
                                sequence_key = %key,
                                ?outcome,
                                "draining in-flight message on shutdown"
                            );
                            match outcome {
                                Outcome::Ack => router::route_ack(&delivery, &publisher).await,
                                Outcome::Retry => {
                                    route_shard_retry(
                                        &delivery,
                                        shard_hold_queues,
                                        &publisher,
                                        retry_count,
                                        queue,
                                    )
                                    .await;
                                }
                                Outcome::Reject => {
                                    router::route_reject(&delivery, topology, &publisher).await;
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
                                        let headers = router::clone_headers(&delivery);
                                        match publisher
                                            .publish_to_queue(
                                                hold_queue.name(),
                                                &delivery.data,
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
                                                    router::nack_requeue(&delivery, &publisher).await;
                                                } else if let Err(e) = publisher.commit_if_tx().await {
                                                    error!("tx_commit failed for defer on shutdown: {e}");
                                                }
                                            }
                                            Err(e) => {
                                                warn!("failed to defer on shutdown: {e}");
                                                router::nack_requeue(&delivery, &publisher).await;
                                            }
                                        }
                                    } else {
                                        router::nack_requeue(&delivery, &publisher).await;
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

                item = stream.next(), if can_accept => {
                    let delivery = unwrap_delivery(item, queue)?;
                    let seq_key = delivery.routing_key.to_string();
                    let retry_count = get_retry_count(&delivery);

                    // ── FailAll: skip poisoned keys ──
                    if on_failure == SequenceFailure::FailAll
                        && poisoned_keys.contains(&seq_key)
                    {
                        warn!(
                            sequence_key = %seq_key,
                            queue = %queue,
                            "message with poisoned sequence key, sending to DLQ"
                        );
                        router::route_reject(&delivery, topology, &publisher).await;
                        continue;
                    }

                    // ── Max retries check ──
                    if retry_count >= options.max_retries {
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
                            if let Some(pending) = pending_deliveries.remove(&seq_key) {
                                for pd in pending {
                                    router::route_reject(&pd, topology, &publisher).await;
                                }
                            }
                        }
                        router::route_reject(&delivery, topology, &publisher).await;
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
                                    router::route_reject(&delivery, topology, &publisher).await;
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
                                .push_back(delivery);
                            continue;
                        }
                        Some(KeyState::AwaitingRetry) => {
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
                                        router::route_reject(&delivery, topology, &publisher).await;
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
                                    .push_back(delivery);
                                continue;
                            }
                        }
                        None => {}
                    }

                    // ── Spawn handler for this key ──
                    let metadata = extract_message_metadata(&delivery);
                    match try_deserialize_or_reject::<T>(&delivery, &metadata, queue, topology, &publisher, options).await {
                        None => {
                            // Reject undeserializable messages immediately.
                            if on_failure == SequenceFailure::FailAll {
                                poisoned_keys.insert(seq_key.clone());
                            }
                        }
                        Some(message) => {
                            let rx = spawn_handler_keyed::<T, H>(
                                &handler,
                                message,
                                metadata,
                                options.handler_timeout,
                                &completed_tx,
                                seq_key.clone(),
                            );

                            key_states.insert(
                                seq_key,
                                KeyState::InFlight {
                                    delivery: Box::new(delivery),
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
        options: &ConsumerOptions,
        on_failure: SequenceFailure,
        poisoned_keys: &mut HashSet<String>,
        completed_tx: &mpsc::UnboundedSender<String>,
        key_states: &mut HashMap<String, KeyState>,
        in_flight_count: &mut usize,
        pending_deliveries: &mut HashMap<String, VecDeque<Delivery>>,
        queue: &str,
        topology: &'static QueueTopology,
        publisher: &ChannelPublisher,
    ) where
        T: Topic,
        T::Message: for<'de> serde::Deserialize<'de>,
        H: MessageHandler<T, Context = ()>,
    {
        // If the key is poisoned, reject all pending deliveries for it.
        if on_failure == SequenceFailure::FailAll && poisoned_keys.contains(key) {
            if let Some(pending) = pending_deliveries.remove(key) {
                for pd in pending {
                    router::route_reject(&pd, topology, publisher).await;
                }
            }
            return;
        }

        let Some(pending) = pending_deliveries.get_mut(key) else {
            return;
        };

        // Pop the next delivery and try to spawn it.
        while let Some(delivery) = pending.pop_front() {
            let retry_count = get_retry_count(&delivery);

            // Max retries check on buffered delivery.
            if retry_count >= options.max_retries {
                warn!(
                    queue = %queue,
                    sequence_key = %key,
                    retry_count,
                    "buffered message exceeded max retries, sending to DLQ"
                );
                if on_failure == SequenceFailure::FailAll {
                    poisoned_keys.insert(key.to_string());
                    // Reject remaining pending for this key too.
                    router::route_reject(&delivery, topology, publisher).await;
                    while let Some(pd) = pending.pop_front() {
                        router::route_reject(&pd, topology, publisher).await;
                    }
                    pending_deliveries.remove(key);
                    return;
                }
                router::route_reject(&delivery, topology, publisher).await;
                continue;
            }

            let metadata = extract_message_metadata(&delivery);
            match try_deserialize_or_reject::<T>(
                &delivery, &metadata, queue, topology, publisher, options,
            )
            .await
            {
                None => {
                    // Extra FailAll poisoning on deserialization failure.
                    if on_failure == SequenceFailure::FailAll {
                        poisoned_keys.insert(key.to_string());
                        while let Some(pd) = pending.pop_front() {
                            router::route_reject(&pd, topology, publisher).await;
                        }
                        pending_deliveries.remove(key);
                        return;
                    }
                    continue;
                }
                Some(message) => {
                    let rx = spawn_handler_keyed::<T, H>(
                        handler,
                        message,
                        metadata,
                        options.handler_timeout,
                        completed_tx,
                        key.to_string(),
                    );

                    key_states.insert(
                        key.to_string(),
                        KeyState::InFlight {
                            delivery: Box::new(delivery),
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
        queue: &str,
        topology: &'static QueueTopology,
        options: ConsumerOptions,
    ) -> Result<()>
    where
        T: Topic,
        T::Message: for<'de> serde::Deserialize<'de>,
        H: MessageHandler<T, Context = ()>,
    {
        let shutdown = options.shutdown.clone();
        run_with_reconnect(&shutdown, queue, || {
            self.consume_loop_concurrent::<T, H>(handler.clone(), queue, topology, &options)
        })
        .await
    }

    async fn consume_loop_concurrent<T, H>(
        &self,
        handler: Arc<H>,
        queue: &str,
        topology: &'static QueueTopology,
        options: &ConsumerOptions,
    ) -> Result<()>
    where
        T: Topic,
        T::Message: for<'de> serde::Deserialize<'de>,
        H: MessageHandler<T, Context = ()>,
    {
        #[cfg(feature = "rabbitmq-transactional")]
        let exactly_once = options.exactly_once;
        #[cfg(not(feature = "rabbitmq-transactional"))]
        let exactly_once = false;
        let (channel, mut stream) =
            open_consumer(&self.client, queue, options.prefetch_count, exactly_once).await?;
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

        struct PendingMessage {
            delivery: Delivery,
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
                        let retry_count = get_retry_count(&msg.delivery);
                        debug!(queue, ?outcome, "message handled (concurrent)");
                        route_outcome(&msg.delivery, outcome, topology, &publisher, retry_count)
                            .await;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Closed) => {
                        // Handler task panicked — treat as retry.
                        let msg = in_flight.pop_front().unwrap();
                        let retry_count = get_retry_count(&msg.delivery);
                        warn!(queue, "handler task panicked, retrying message");
                        route_outcome(
                            &msg.delivery,
                            Outcome::Retry,
                            topology,
                            &publisher,
                            retry_count,
                        )
                        .await;
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
                    for pending in in_flight {
                        let outcome = pending.outcome_rx.await.unwrap_or(Outcome::Retry);
                        let retry_count = get_retry_count(&pending.delivery);
                        route_outcome(
                            &pending.delivery,
                            outcome,
                            topology,
                            &publisher,
                            retry_count,
                        )
                        .await;
                    }
                    return Ok(());
                }

                _ = notify.notified() => {
                    // A handler completed — the drain at the top of the loop
                    // will process it on the next iteration.
                }

                item = stream.next(), if can_accept => {
                    let delivery = unwrap_delivery(item, queue)?;
                    let retry_count = get_retry_count(&delivery);

                    if retry_count >= options.max_retries {
                        warn!(
                            "message on {queue} exceeded max retries ({}/{}), sending to DLQ",
                            retry_count, options.max_retries
                        );
                        router::route_reject(&delivery, topology, &publisher).await;
                        continue;
                    }

                    let metadata = extract_message_metadata(&delivery);

                    if let Some(message) = try_deserialize_or_reject::<T>(&delivery, &metadata, queue, topology, &publisher, options).await {
                        let rx = spawn_handler::<T, H>(
                            &handler,
                            message,
                            metadata,
                            options.handler_timeout,
                            &notify,
                        );

                        in_flight.push_back(PendingMessage {
                            delivery,
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
    dlq: &str,
    options: &ConsumerOptions,
) -> Result<()>
where
    T: Topic,
    T::Message: for<'de> serde::Deserialize<'de>,
    H: MessageHandler<T, Context = ()>,
{
    // DLQ consumer never uses exactly-once mode (always acks, no hold-queue routing).
    let (_channel, mut stream) = open_consumer(client, dlq, options.prefetch_count, false).await?;

    info!("DLQ consumer started on queue {dlq}");

    loop {
        tokio::select! {
            _ = options.shutdown.cancelled() => {
                debug!("shutdown signal received, stopping DLQ consumer on {dlq}");
                return Ok(());
            }
            item = stream.next() => {
                let delivery = unwrap_delivery(item, dlq)?;

                let metadata = extract_dead_metadata(&delivery);

                if let Err(e) = options.validate_payload_message_size(delivery.data.len()) {
                    warn!(
                        error = %e,
                        delivery_id = %metadata.message.delivery_id,
                        "oversized DLQ message — discarding"
                    );
                } else {
                    match serde_json::from_slice::<T::Message>(&delivery.data) {
                        Err(err) => {
                            error!(
                                error = %err,
                                delivery_id = %metadata.message.delivery_id,
                                "Failed to deserialize message from dead letter queue — discarding"
                            );
                        }
                        Ok(message) => {
                            handler.handle_dead(message, metadata, &()).await;
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

/// Route a delivery based on its outcome.
async fn route_outcome(
    delivery: &Delivery,
    outcome: Outcome,
    topology: &'static QueueTopology,
    publisher: &ChannelPublisher,
    retry_count: u32,
) {
    match outcome {
        Outcome::Ack => router::route_ack(delivery, publisher).await,
        Outcome::Retry => router::route_retry(delivery, topology, publisher, retry_count).await,
        Outcome::Reject => router::route_reject(delivery, topology, publisher).await,
        Outcome::Defer => router::route_defer(delivery, topology, publisher).await,
    }
}

/// Route a retry for a sequenced shard via per-shard hold queues.
async fn route_shard_retry(
    delivery: &Delivery,
    shard_hold_queues: &[HoldQueue],
    publisher: &ChannelPublisher,
    retry_count: u32,
    queue: &str,
) {
    if !shard_hold_queues.is_empty() {
        let new_retry_count = retry_count + 1;
        let index = (retry_count as usize).min(shard_hold_queues.len() - 1);
        let hold_queue = &shard_hold_queues[index];
        let headers = router::clone_headers_with_retry(delivery, new_retry_count);

        match publisher
            .publish_to_queue(hold_queue.name(), &delivery.data, headers)
            .await
        {
            Ok(()) => {
                if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                    error!("failed to ack delivery after publishing to shard hold queue: {e}");
                    publisher.rollback_if_tx().await;
                    router::nack_requeue(delivery, publisher).await;
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
                router::nack_requeue(delivery, publisher).await;
            }
        }
    } else {
        warn!(
            queue,
            retry_count,
            "retrying sequenced message but no shard hold queues configured — requeuing with no delay"
        );
        router::nack_requeue(delivery, publisher).await;
    }
}

/// Nack-requeue all locally buffered deliveries (used on graceful shutdown).
///
/// In tx mode the channel is still open; we nack each delivery and commit to
/// make it immediately visible in the queue. In non-tx mode a plain nack is
/// sufficient (confirms are per-publish, not per-nack).
async fn nack_requeue_all_pending(
    pending_deliveries: &mut HashMap<String, VecDeque<Delivery>>,
    publisher: Option<&ChannelPublisher>,
) {
    for (key, deliveries) in pending_deliveries.drain() {
        for delivery in deliveries {
            debug!(
                sequence_key = %key,
                "nack-requeuing buffered delivery on shutdown"
            );
            if let Err(e) = delivery
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

/// Run the handler future with an optional timeout.
/// Returns `Outcome::Retry` if the timeout is exceeded.
async fn invoke_handler(fut: impl Future<Output = Outcome>, timeout: Option<Duration>) -> Outcome {
    match timeout {
        Some(duration) => tokio::time::timeout(duration, fut)
            .await
            .unwrap_or_else(|_elapsed| {
                warn!("handler exceeded timeout ({duration:?}), retrying message");
                Outcome::Retry
            }),
        None => fut.await,
    }
}

/// Spawns a handler task for a deserialized message.
/// Returns the oneshot receiver that will resolve with the handler's outcome.
fn spawn_handler<T, H>(
    handler: &Arc<H>,
    message: T::Message,
    metadata: MessageMetadata,
    timeout: Option<Duration>,
    notify: &Arc<Notify>,
) -> oneshot::Receiver<Outcome>
where
    T: Topic,
    H: MessageHandler<T, Context = ()>,
{
    let (tx, rx) = oneshot::channel();
    let h = handler.clone();
    let n = notify.clone();
    tokio::spawn(async move {
        let outcome = invoke_handler(h.handle(message, metadata, &()), timeout).await;
        let _ = tx.send(outcome);
        n.notify_one();
    });
    rx
}

/// Spawns a handler task for a sequenced message, signalling completion via an
/// mpsc channel with the sequence key. This avoids O(N) polling of all in-flight
/// keys to find which one completed.
fn spawn_handler_keyed<T, H>(
    handler: &Arc<H>,
    message: T::Message,
    metadata: MessageMetadata,
    timeout: Option<Duration>,
    completed_tx: &mpsc::UnboundedSender<String>,
    key: String,
) -> oneshot::Receiver<Outcome>
where
    T: Topic,
    H: MessageHandler<T, Context = ()>,
{
    let (tx, rx) = oneshot::channel();
    let h = handler.clone();
    let ctx = completed_tx.clone();
    tokio::spawn(async move {
        let outcome = invoke_handler(h.handle(message, metadata, &()), timeout).await;
        let _ = tx.send(outcome);
        let _ = ctx.send(key);
    });
    rx
}

/// Attempts to deserialize a delivery's payload. On failure, logs the error
/// and rejects the delivery (nack without requeue).
/// Returns `Some(message)` on success, `None` if rejected.
async fn try_deserialize_or_reject<T: Topic>(
    delivery: &Delivery,
    metadata: &MessageMetadata,
    queue: &str,
    topology: &'static QueueTopology,
    publisher: &ChannelPublisher,
    options: &ConsumerOptions,
) -> Option<T::Message>
where
    T::Message: for<'de> serde::Deserialize<'de>,
{
    if let Err(e) = options.validate_payload_message_size(delivery.data.len()) {
        warn!(
            error = %e,
            delivery_id = %metadata.delivery_id,
            queue,
            "rejecting oversized message"
        );
        router::route_reject(delivery, topology, publisher).await;
        return None;
    }
    match serde_json::from_slice::<T::Message>(&delivery.data) {
        Ok(message) => Some(message),
        Err(err) => {
            error!(
                error = %err,
                delivery_id = %metadata.delivery_id,
                queue = %queue,
                "failed to deserialize message"
            );
            router::route_reject(delivery, topology, publisher).await;
            None
        }
    }
}

impl RabbitMqConsumer {
    pub async fn run<T: Topic>(
        &self,
        handler: impl MessageHandler<T, Context = ()>,
        options: crate::ConsumerOptions<crate::markers::RabbitMq>,
    ) -> Result<()> {
        self.run_with_inner::<T>(handler, options.into_inner()).await
    }

    pub(crate) async fn run_with_inner<T: Topic>(
        &self,
        handler: impl MessageHandler<T, Context = ()>,
        options: ConsumerOptions,
    ) -> Result<()> {
        let topology = T::topology();
        let consumer = RabbitMqConsumer::new(self.client.clone());
        let handler = Arc::new(handler);
        consumer
            .run_internal_concurrent::<T, _>(handler, topology.queue(), topology, options)
            .await
    }

    pub async fn run_fifo<T: SequencedTopic>(
        &self,
        handler: impl MessageHandler<T, Context = ()>,
        options: crate::ConsumerOptions<crate::markers::RabbitMq>,
    ) -> Result<()> {
        self.run_fifo_with_inner::<T>(handler, options.into_inner())
            .await
    }

    pub(crate) async fn run_fifo_with_inner<T: SequencedTopic>(
        &self,
        handler: impl MessageHandler<T, Context = ()>,
        options: ConsumerOptions,
    ) -> Result<()> {
        let topology = T::topology();
        let seq = topology.sequencing().ok_or_else(|| {
            ShoveError::Topology("run_fifo called on topic without sequencing config".into())
        })?;

        let on_failure = seq.on_failure();
        let handler = Arc::new(handler);
        let shutdown = options.shutdown.clone();
        let prefetch = options.prefetch_count;
        let client = self.client.clone();
        let mut handles = Vec::with_capacity(seq.routing_shards() as usize);

        for i in 0..seq.routing_shards() {
            let sub_queue = format!("{}-seq-{i}", topology.queue());
            let shard_hold_queues = topology.shard_hold_queue_names(i);
            let h = handler.clone();
            let inner_client = client.clone();
            let mut opts = ConsumerOptions::defaults_with_shutdown(shutdown.clone());
            opts.max_retries = options.max_retries;
            opts.prefetch_count = prefetch;
            opts.handler_timeout = options.handler_timeout;
            opts.max_pending_per_key = options.max_pending_per_key;
            opts.max_message_size = options.max_message_size;
            handles.push(tokio::spawn(async move {
                let consumer = RabbitMqConsumer::new(inner_client);
                consumer
                    .run_internal_concurrent_sequenced::<T, _>(
                        h,
                        &sub_queue,
                        topology,
                        opts,
                        on_failure,
                        shard_hold_queues,
                    )
                    .await
            }));
        }

        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => error!("sequenced consumer sub-task failed: {e}"),
                Err(e) => error!("sequenced consumer task panicked: {e}"),
            }
        }

        Ok(())
    }

    pub async fn run_dlq<T: Topic>(
        &self,
        handler: impl MessageHandler<T, Context = ()>,
    ) -> Result<()> {
        let topology = T::topology();
        let dlq = topology
            .dlq()
            .ok_or_else(|| ShoveError::Topology("run_dlq called on topic without DLQ".into()))?;
        let shutdown = self.client.shutdown_token();
        let options = ConsumerOptions::defaults_with_shutdown(shutdown);

        run_with_reconnect(&options.shutdown, dlq, || {
            consume_dlq_loop::<T, _>(&self.client, &handler, dlq, &options)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invoke_handler_returns_outcome_without_timeout() {
        let outcome = invoke_handler(async { Outcome::Ack }, None).await;
        assert!(matches!(outcome, Outcome::Ack));
    }

    #[tokio::test]
    async fn invoke_handler_returns_outcome_within_timeout() {
        let timeout = Some(Duration::from_secs(1));
        let outcome = invoke_handler(async { Outcome::Reject }, timeout).await;
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
            vec![],
        );
        let (_tx, rx) = oneshot::channel::<Outcome>();
        let state = KeyState::InFlight {
            delivery: Box::new(delivery),
            outcome_rx: rx,
        };
        assert!(matches!(state, KeyState::InFlight { .. }));
    }

    #[test]
    fn key_state_awaiting_retry_is_distinct() {
        let state = KeyState::AwaitingRetry;
        assert!(matches!(state, KeyState::AwaitingRetry));
        assert!(!matches!(state, KeyState::InFlight { .. }));
    }

    #[tokio::test]
    async fn nack_requeue_all_pending_handles_empty_map() {
        let mut pending: HashMap<String, VecDeque<Delivery>> = HashMap::new();
        nack_requeue_all_pending(&mut pending, None).await;
        assert!(pending.is_empty());
    }
}
