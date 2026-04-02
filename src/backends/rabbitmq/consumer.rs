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

use crate::backends::rabbitmq::client::RabbitMqClient;
use crate::backends::rabbitmq::headers::{
    extract_dead_metadata, extract_message_metadata, get_retry_count,
};
use crate::backends::rabbitmq::publisher::ChannelPublisher;
use crate::backends::rabbitmq::router;
use crate::consumer::{Consumer, ConsumerOptions};
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::metadata::MessageMetadata;
use crate::outcome::Outcome;
use crate::topic::{SequencedTopic, Topic};
use crate::topology::{HoldQueue, SequenceFailure};
use crate::{QueueTopology, RECONNECT_DELAY};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Opens a channel with QoS and starts consuming from `queue`.
async fn open_consumer(
    client: &RabbitMqClient,
    queue: &str,
    prefetch_count: u16,
) -> Result<(Channel, lapin::Consumer)> {
    let channel = client.create_confirm_channel().await?;
    channel
        .basic_qos(prefetch_count, BasicQosOptions::default())
        .await
        .map_err(|e| ShoveError::Connection(format!("failed to set QoS: {e}")))?;
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
        .map_err(|e| ShoveError::Connection(format!("failed to start consumer on {queue}: {e}")))?;
    Ok((channel, consumer))
}

/// Unwrap a delivery from the consumer stream.
fn unwrap_delivery(
    item: Option<std::result::Result<Delivery, LapinError>>,
    queue: &str,
) -> Result<Delivery> {
    match item {
        Some(Ok(d)) => Ok(d),
        Some(Err(e)) => Err(ShoveError::Connection(format!(
            "consumer stream error on {queue}: {e}"
        ))),
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
    loop {
        match f().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                warn!("consumer error on {queue}: {e}. Reconnecting in {RECONNECT_DELAY:?}");
                tokio::select! {
                    _ = tokio::time::sleep(RECONNECT_DELAY) => {}
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
        H: MessageHandler<T>,
    {
        let mut poisoned_keys = HashSet::new();
        let mut pending_deliveries: HashMap<String, VecDeque<Delivery>> = HashMap::new();
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
                Ok(()) => {
                    // Graceful shutdown — nack-requeue all pending buffered deliveries.
                    nack_requeue_all_pending(&mut pending_deliveries).await;
                    return Ok(());
                }
                Err(e) => {
                    if options.shutdown.is_cancelled() {
                        nack_requeue_all_pending(&mut pending_deliveries).await;
                        return Ok(());
                    }
                    // On reconnect, nack-requeue pending deliveries since the
                    // channel is dead and we cannot ack/nack them anymore.
                    // They will be redelivered by the broker after reconnect.
                    pending_deliveries.clear();
                    warn!("consumer error on {queue}: {e}. Reconnecting in {RECONNECT_DELAY:?}");
                    tokio::select! {
                        _ = tokio::time::sleep(RECONNECT_DELAY) => {}
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
        _topology: &'static QueueTopology,
        options: &ConsumerOptions,
        on_failure: SequenceFailure,
        poisoned_keys: &mut HashSet<String>,
        shard_hold_queues: &[HoldQueue],
        pending_deliveries: &mut HashMap<String, VecDeque<Delivery>>,
    ) -> Result<()>
    where
        T: Topic,
        T::Message: for<'de> serde::Deserialize<'de>,
        H: MessageHandler<T>,
    {
        let prefetch = options.prefetch_count;
        let (channel, mut stream) = open_consumer(&self.client, queue, prefetch).await?;
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
                            router::route_ack(&delivery).await;
                        } else {
                            if on_failure == SequenceFailure::FailAll {
                                info!(
                                    sequence_key = %key,
                                    queue = %queue,
                                    "poisoning sequence key (FailAll)"
                                );
                                poisoned_keys.insert(key.clone());
                            }
                            router::route_reject(&delivery).await;
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
                            )
                            .await;
                        } else {
                            // Defer
                            if !shard_hold_queues.is_empty() {
                                let hold_queue = &shard_hold_queues[0];
                                let headers = router::clone_headers(&delivery);
                                match publisher
                                    .publish_to_queue(hold_queue.name(), &delivery.data, headers)
                                    .await
                                {
                                    Ok(()) => {
                                        debug!(
                                            "deferring message to shard hold queue {}",
                                            hold_queue.name()
                                        );
                                        if let Err(e) =
                                            delivery.ack(BasicAckOptions::default()).await
                                        {
                                            error!(
                                                "failed to ack delivery after deferring to shard hold queue: {e}"
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            "failed to publish to shard hold queue {} for defer, requeuing: {e}",
                                            hold_queue.name()
                                        );
                                        router::nack_requeue(&delivery).await;
                                    }
                                }
                            } else {
                                router::nack_requeue(&delivery).await;
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
                                Outcome::Ack => router::route_ack(&delivery).await,
                                Outcome::Retry => {
                                    route_shard_retry(
                                        &delivery,
                                        shard_hold_queues,
                                        &publisher,
                                        retry_count,
                                    )
                                    .await;
                                }
                                Outcome::Reject => router::route_reject(&delivery).await,
                                Outcome::Defer => {
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
                                                }
                                            }
                                            Err(e) => {
                                                warn!("failed to defer on shutdown: {e}");
                                                router::nack_requeue(&delivery).await;
                                            }
                                        }
                                    } else {
                                        router::nack_requeue(&delivery).await;
                                    }
                                }
                            }
                        }
                        // AwaitingRetry keys: nothing to do — the message is
                        // already in a hold queue and will be redelivered.
                    }
                    // Pending deliveries are nack-requeued by the caller.
                    return Ok(());
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
                        router::route_reject(&delivery).await;
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
                                    router::route_reject(&pd).await;
                                }
                            }
                        }
                        router::route_reject(&delivery).await;
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
                                    router::route_reject(&delivery).await;
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
                            if retry_count > 0 {
                                // This is the returning retry — clear AwaitingRetry
                                // and fall through to spawn a handler below.
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
                                        router::route_reject(&delivery).await;
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
                    match try_deserialize_or_reject::<T>(&delivery, &metadata, queue).await {
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
    ) where
        T: Topic,
        T::Message: for<'de> serde::Deserialize<'de>,
        H: MessageHandler<T>,
    {
        // If the key is poisoned, reject all pending deliveries for it.
        if on_failure == SequenceFailure::FailAll && poisoned_keys.contains(key) {
            if let Some(pending) = pending_deliveries.remove(key) {
                for pd in pending {
                    router::route_reject(&pd).await;
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
                    router::route_reject(&delivery).await;
                    while let Some(pd) = pending.pop_front() {
                        router::route_reject(&pd).await;
                    }
                    pending_deliveries.remove(key);
                    return;
                }
                router::route_reject(&delivery).await;
                continue;
            }

            let metadata = extract_message_metadata(&delivery);
            match try_deserialize_or_reject::<T>(&delivery, &metadata, queue).await {
                None => {
                    // Extra FailAll poisoning on deserialization failure.
                    if on_failure == SequenceFailure::FailAll {
                        poisoned_keys.insert(key.to_string());
                        while let Some(pd) = pending.pop_front() {
                            router::route_reject(&pd).await;
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
        H: MessageHandler<T>,
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
        H: MessageHandler<T>,
    {
        let (channel, mut stream) =
            open_consumer(&self.client, queue, options.prefetch_count).await?;
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
                        router::route_reject(&delivery).await;
                        continue;
                    }

                    let metadata = extract_message_metadata(&delivery);

                    if let Some(message) = try_deserialize_or_reject::<T>(&delivery, &metadata, queue).await {
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
    H: MessageHandler<T>,
{
    let (_channel, mut stream) = open_consumer(client, dlq, options.prefetch_count).await?;

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

                match serde_json::from_slice::<T::Message>(&delivery.data) {
                    Err(err) => {
                        error!(
                            error = %err,
                            delivery_id = %metadata.message.delivery_id,
                            "Failed to deserialize message from dead letter queue — discarding"
                        );
                    }
                    Ok(message) => {
                        handler.handle_dead(message, metadata).await;
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
        Outcome::Ack => router::route_ack(delivery).await,
        Outcome::Retry => router::route_retry(delivery, topology, publisher, retry_count).await,
        Outcome::Reject => router::route_reject(delivery).await,
        Outcome::Defer => router::route_defer(delivery, topology, publisher).await,
    }
}

/// Route a retry for a sequenced shard via per-shard hold queues.
async fn route_shard_retry(
    delivery: &Delivery,
    shard_hold_queues: &[HoldQueue],
    publisher: &ChannelPublisher,
    retry_count: u32,
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
                debug!(
                    "retrying message via shard hold queue {} (attempt {})",
                    hold_queue.name(),
                    new_retry_count
                );
                if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                    error!("failed to ack delivery after publishing to shard hold queue: {e}");
                }
            }
            Err(e) => {
                warn!(
                    "failed to publish to shard hold queue {}, requeuing: {e}",
                    hold_queue.name()
                );
                router::nack_requeue(delivery).await;
            }
        }
    } else {
        router::nack_requeue(delivery).await;
    }
}

/// Nack-requeue all locally buffered deliveries (used on shutdown/reconnect).
async fn nack_requeue_all_pending(pending_deliveries: &mut HashMap<String, VecDeque<Delivery>>) {
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
    H: MessageHandler<T>,
{
    let (tx, rx) = oneshot::channel();
    let h = handler.clone();
    let n = notify.clone();
    tokio::spawn(async move {
        let outcome = invoke_handler(h.handle(message, metadata), timeout).await;
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
    H: MessageHandler<T>,
{
    let (tx, rx) = oneshot::channel();
    let h = handler.clone();
    let ctx = completed_tx.clone();
    tokio::spawn(async move {
        let outcome = invoke_handler(h.handle(message, metadata), timeout).await;
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
) -> Option<T::Message>
where
    T::Message: for<'de> serde::Deserialize<'de>,
{
    match serde_json::from_slice::<T::Message>(&delivery.data) {
        Ok(message) => Some(message),
        Err(err) => {
            error!(
                error = %err,
                delivery_id = %metadata.delivery_id,
                queue = %queue,
                "failed to deserialize message"
            );
            router::route_reject(delivery).await;
            None
        }
    }
}

impl Consumer for RabbitMqConsumer {
    fn run<T: Topic>(
        &self,
        handler: impl MessageHandler<T>,
        options: ConsumerOptions,
    ) -> impl Future<Output = Result<()>> + Send {
        let client = self.client.clone();
        async move {
            let topology = T::topology();
            let consumer = RabbitMqConsumer::new(client);
            let handler = Arc::new(handler);
            consumer
                .run_internal_concurrent::<T, _>(handler, topology.queue(), topology, options)
                .await
        }
    }

    fn run_fifo<T: SequencedTopic>(
        &self,
        handler: impl MessageHandler<T>,
        options: ConsumerOptions,
    ) -> impl Future<Output = Result<()>> + Send {
        let client = self.client.clone();
        async move {
            let topology = T::topology();
            let seq = topology.sequencing().ok_or_else(|| {
                ShoveError::Topology(
                    "run_fifo called on topic without sequencing config".into(),
                )
            })?;

            let on_failure = seq.on_failure();
            let handler = Arc::new(handler);
            let shutdown = options.shutdown.clone();
            let prefetch = options.prefetch_count;
            let mut handles = Vec::with_capacity(seq.routing_shards() as usize);

            for i in 0..seq.routing_shards() {
                let sub_queue = format!("{}-seq-{i}", topology.queue());
                let shard_hold_queues = topology.shard_hold_queue_names(i);
                let h = handler.clone();
                let inner_client = client.clone();
                let mut opts = ConsumerOptions::new(shutdown.clone())
                    .with_max_retries(options.max_retries)
                    .with_prefetch_count(prefetch);
                opts.max_pending_per_key = options.max_pending_per_key;
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
    }

    fn run_dlq<T: Topic>(
        &self,
        handler: impl MessageHandler<T>,
    ) -> impl Future<Output = Result<()>> + Send {
        let client = self.client.clone();
        async move {
            let topology = T::topology();
            let dlq = topology.dlq().ok_or_else(|| {
                ShoveError::Topology("run_dlq called on topic without DLQ".into())
            })?;
            let shutdown = client.shutdown_token();
            let options = ConsumerOptions::new(shutdown);

            run_with_reconnect(&options.shutdown, dlq, || {
                consume_dlq_loop::<T, _>(&client, &handler, dlq, &options)
            })
            .await
        }
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
        nack_requeue_all_pending(&mut pending).await;
        assert!(pending.is_empty());
    }
}
