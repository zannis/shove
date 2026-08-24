//! Redis broadcast — a plain `XREAD` from `$`, deliberately **not**
//! `XREADGROUP`.
//!
//! # Why no consumer group
//!
//! `XREADGROUP` is what makes the competing-consumer path work, and it is
//! exactly wrong here. A group gives every reader a shared cursor (so two
//! instances would *split* the fan-out rather than each receive it) and a PEL
//! per consumer name (so every pod restart leaves a stale consumer entry behind
//! for something to reap). The per-pod-UUID-group workaround the docs page
//! warns about is precisely that failure.
//!
//! A bare `XREAD` has neither. There is no `XGROUP`, so there is no PEL, no
//! consumer registry, and no `XACK` — which means the "nothing survives"
//! half of the [`BroadcastImpl`] contract is *structural* on Redis rather than
//! something teardown has to undo. There is nothing to tear down.
//!
//! # Deliver-new
//!
//! The first read passes the `$` special id, which resolves to "entries added
//! after this call" — an entry already in the stream is never returned. Every
//! later read passes the last id actually seen instead. That matters: `$`
//! re-resolves on each call, so reusing it would silently drop anything that
//! arrived between two reads.
//!
//! The cursor also survives a reconnect rather than resetting to `$`.
//! Deliver-new fixes where the subscription *starts*; it is not a licence to
//! skip an outage window. Whether messages published during the outage are
//! actually still there to resume onto is the operator's call, not this
//! module's — they have to have survived both Redis persistence and the
//! stream's `MAXLEN` bound. What the cursor guarantees is that this loop does
//! not skip past them of its own accord, which is more than NATS can offer:
//! its ephemeral consumer cannot resume where a previous one stopped, so a NATS
//! reconnect always restarts at `New`.
//!
//! # The stream is not trimmed by reading it
//!
//! `XREAD` never acknowledges and never trims, so nothing about a broadcast
//! subscriber makes the stream shrink. The stream must be `MAXLEN`-bounded by
//! whoever declares or publishes to it; this is the caveat the docs page
//! already states, and it is the one real asymmetry with NATS, whose `Interest`
//! retention drops delivered messages on its own.
//!
//! [`BroadcastImpl`]: crate::backend::BroadcastImpl

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::Mutex as TokioMutex;
use tokio_util::sync::CancellationToken;

use crate::backend::ConsumerOptionsInner;
use crate::backend::broadcast::{BROADCAST_DEFER_DELAY, BroadcastAction, settle_broadcast_outcome};
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::metadata::MessageMetadata;
use crate::metrics;
use crate::outcome::Outcome;
use crate::routing::handler_timeout_outcome;
use crate::topic::Topic;

use super::client::RedisClient;
use super::constants::{BLOCK_MS, PAYLOAD_FIELD, X_MESSAGE_ID};
use super::consumer::{
    RedisConsumer, parse_xreadgroup_reply, partition_entry_fields, run_with_reconnect,
};

/// The `$` special id: "only entries added after this call".
const DELIVER_NEW: &str = "$";

impl RedisConsumer {
    pub(crate) async fn run_broadcast_with_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        run_broadcast_impl::<T, H>(self.client_ref().clone(), handler, ctx, options).await
    }
}

async fn run_broadcast_impl<T, H>(
    client: RedisClient,
    handler: H,
    ctx: H::Context,
    options: ConsumerOptionsInner,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let topology = T::topology();
    let stream = topology.queue();
    if !topology.broadcast() {
        return Err(ShoveError::Topology(format!(
            "topic '{stream}' is not a broadcast topology; an ephemeral subscription to it \
             would receive nothing, because publishes go to the shared queue"
        )));
    }

    let shutdown = options.shutdown.clone();
    let handler = Arc::new(handler);
    let ctx = Arc::new(ctx);
    let topic: Arc<str> = Arc::from(stream);

    tracing::info!(stream, "Redis broadcast subscription starting");

    // Reconnects, like every other consumer in this crate. Resolving the
    // subscription with the error instead reads as the more honest option and
    // is not: `BroadcastSubscriber` only surfaces a task's error through
    // `SupervisorOutcome` when `run_until_timeout` returns, i.e. at shutdown,
    // so a subscriber killed by a broker blip would go on looking healthy while
    // receiving nothing for the rest of the process's life.
    //
    // The cursor is held *outside* the reconnect closure, which matters: Redis
    // retains stream entries, so resuming from the last id actually seen means
    // a reconnect replays what arrived during the outage rather than skipping
    // it. Deliver-new is a statement about where the subscription *starts*, not
    // a licence to lose everything after a blip — so unlike NATS (whose
    // ephemeral consumer cannot resume) Redis has no gap here at all, as long
    // as the entries are still inside the stream's MAXLEN bound.
    let cursor = Arc::new(TokioMutex::new(DELIVER_NEW.to_owned()));

    run_with_reconnect(&shutdown, stream, options.max_reconnect_attempts, || {
        let client = client.clone();
        let handler = Arc::clone(&handler);
        let ctx = Arc::clone(&ctx);
        let options = options.clone();
        let topic = Arc::clone(&topic);
        let shutdown = shutdown.clone();
        let cursor = Arc::clone(&cursor);
        async move {
            let mut conn = client.dedicated_conn().await?;

            loop {
                if shutdown.is_cancelled() {
                    return Ok(());
                }

                let last_id = cursor.lock().await.clone();

                // COUNT 1: broadcast is one delivery loop reading a signal
                // stream, and `Defer`'s in-loop redelivery needs the entry still
                // in hand, so there is no prefetch window to fill.
                let mut cmd = redis::cmd("XREAD");
                cmd.arg("COUNT")
                    .arg(1)
                    .arg("BLOCK")
                    .arg(BLOCK_MS)
                    .arg("STREAMS")
                    .arg(stream)
                    .arg(&last_id);

                let raw_reply: redis::Value = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return Ok(()),
                    result = conn.query(&mut cmd) => match result {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(
                                stream,
                                error = %e,
                                "XREAD failed on a broadcast subscription"
                            );
                            metrics::record_backend_error(
                                metrics::BackendLabel::Redis,
                                metrics::BackendErrorKind::Consume,
                            );
                            return Err(e);
                        }
                    }
                };

                // Same reply shape as XREADGROUP — an array of
                // [stream, [[id, [k, v, …]], …]]. A `BLOCK` timeout with nothing
                // new is `Nil`, which parses to empty.
                for (entry_id, fields_vec) in parse_xreadgroup_reply(raw_reply, 1) {
                    // Advance before handling, not after. The cursor has to move
                    // even when the entry is discarded or the handler defers, or
                    // the same entry is read forever.
                    *cursor.lock().await = entry_id.clone();

                    handle_entry::<T, H>(
                        &handler, &ctx, &options, &topic, &shutdown, entry_id, fields_vec,
                    )
                    .await;
                }
            }
        }
    })
    .await
}

/// Decode and deliver one stream entry, honouring `Defer` within this
/// subscription only.
///
/// Delivery is inline, so a handler that keeps returning `Defer` holds up the
/// entries behind it. That is a consequence of broadcast being one loop pinned
/// at concurrency 1, and it is documented on the concepts page rather than
/// worked around: there is no second consumer to hand the backlog to, and no
/// DLQ or hold queue to park the deferred entry in.
async fn handle_entry<T, H>(
    handler: &Arc<H>,
    ctx: &Arc<H::Context>,
    options: &ConsumerOptionsInner,
    topic: &Arc<str>,
    shutdown: &CancellationToken,
    entry_id: String,
    fields_vec: Vec<(String, String)>,
) where
    T: Topic,
    H: MessageHandler<T>,
{
    let (mut fields, user_headers) = partition_entry_fields(fields_vec);
    let user_headers = Arc::new(user_headers);

    // Every pre-handler rejection below discards rather than dead-letters:
    // `build()` rejects `.broadcast()` with `.dlq()`, so there is nowhere else
    // for a bad entry to go. With no group there is also no `XACK` to make —
    // moving the cursor past it is the whole of the disposal.
    let Some(payload_raw) = fields.remove(PAYLOAD_FIELD) else {
        tracing::warn!(
            topic = %topic,
            entry_id,
            "entry has no payload field on a broadcast subscription — discarding"
        );
        metrics::record_terminal(
            topic,
            options.consumer_group.as_deref(),
            metrics::FailReason::Malformed,
            false,
        )
        .confirm();
        return;
    };

    metrics::record_message_size(topic, options.consumer_group.as_deref(), payload_raw.len());

    if let Some(max) = options.max_message_size
        && payload_raw.len() > max
    {
        tracing::warn!(
            topic = %topic,
            entry_id,
            size = payload_raw.len(),
            limit = max,
            "oversized message on a broadcast subscription — discarding (no DLQ)"
        );
        metrics::record_terminal(
            topic,
            options.consumer_group.as_deref(),
            metrics::FailReason::Oversize,
            false,
        )
        .confirm();
        return;
    }

    let delivery_id = fields
        .get(X_MESSAGE_ID)
        .cloned()
        .unwrap_or_else(|| entry_id.clone());

    loop {
        // Decoded per attempt: `T::Message` has no `Clone` bound and the
        // handler takes it by value, so a `Defer` needs a fresh one.
        let msg: T::Message =
            match <T::Codec as crate::Codec<T::Message>>::decode(payload_raw.as_bytes()) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(
                        topic = %topic,
                        entry_id,
                        error = %e,
                        "undeserializable message on a broadcast subscription — discarding (no DLQ)"
                    );
                    metrics::record_terminal(
                        topic,
                        options.consumer_group.as_deref(),
                        metrics::FailReason::Deserialize,
                        false,
                    )
                    .confirm();
                    return;
                }
            };

        let meta = MessageMetadata {
            // A broadcast subscription has no retry chain to count against, and
            // no PEL to read a delivery count from.
            retry_count: 0,
            delivery_id: delivery_id.clone(),
            redelivered: false,
            delivery_count: None,
            headers: Arc::clone(&user_headers),
        };

        options.processing.store(true, Ordering::Release);
        let outcome = invoke_broadcast_handler::<T, H>(
            handler.clone(),
            ctx.clone(),
            msg,
            meta,
            options,
            topic,
        )
        .await;
        options.processing.store(false, Ordering::Release);

        match settle_broadcast_outcome(&outcome, topic, options.consumer_group.as_deref()) {
            BroadcastAction::Done => return,
            BroadcastAction::Redeliver => {
                tokio::select! {
                    _ = tokio::time::sleep(BROADCAST_DEFER_DELAY) => {}
                    // A deferred message has no DLQ and no durable home, so
                    // holding the drain open for its backoff buys nothing.
                    _ = shutdown.cancelled() => return,
                }
            }
        }
    }
}

/// The handler-invocation wrapper, minus the lease and retry machinery a
/// broadcast subscription has no use for — with no PEL there is no reaper to
/// race, so there is no lease to hold. Timeout handling, panic containment and
/// the inflight/consumed/duration metrics stay identical to the
/// competing-consumer path.
async fn invoke_broadcast_handler<T, H>(
    handler: Arc<H>,
    ctx: Arc<H::Context>,
    msg: T::Message,
    meta: MessageMetadata,
    options: &ConsumerOptionsInner,
    topic: &Arc<str>,
) -> Outcome
where
    T: Topic,
    H: MessageHandler<T>,
{
    let _inflight = metrics::InflightGuard::new(topic.clone(), options.consumer_group.clone());
    let start = std::time::Instant::now();
    let mut join = tokio::spawn(async move { handler.handle(msg, meta, ctx.as_ref()).await });

    let outcome = match options.handler_timeout {
        Some(duration) => match tokio::time::timeout(duration, &mut join).await {
            Ok(Ok(o)) => o,
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "broadcast handler panicked");
                Outcome::Retry
            }
            Err(_) => {
                join.abort();
                let resolved = handler_timeout_outcome(options.handler_timeout_outcome.clone());
                tracing::warn!(
                    outcome = ?resolved,
                    "broadcast handler timed out after {duration:?}"
                );
                metrics::record_failed(
                    topic,
                    options.consumer_group.as_deref(),
                    metrics::FailReason::Timeout,
                );
                resolved
            }
        },
        None => match join.await {
            Ok(o) => o,
            Err(e) => {
                tracing::warn!(error = %e, "broadcast handler panicked");
                Outcome::Retry
            }
        },
    };

    let elapsed = start.elapsed().as_secs_f64();
    metrics::record_consumed(topic, options.consumer_group.as_deref(), &outcome);
    metrics::record_processing_duration(
        topic,
        options.consumer_group.as_deref(),
        &outcome,
        elapsed,
    );
    outcome
}
