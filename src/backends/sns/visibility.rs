//! The visibility heartbeat for messages a consumer holds un-dispatched: a
//! sequenced key's pending buffer and an accumulating or running batch.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::time::Duration;

use aws_sdk_sqs::types::{Message, QueueAttributeName};

use crate::backends::sns::router::{
    MAX_BATCH_ENTRIES, SQS_MAX_VISIBILITY_TIMEOUT_SECS, VisibilityChange,
};
use crate::error::{Result, ShoveError};

/// Upper bound on one extension request, however long the queue's timeout is.
const MAX_CHUNK_BOUND: Duration = Duration::from_secs(5);

/// How held messages are kept hidden: every `period`, each is set to be hidden
/// for `extend_to_secs` from that moment, so a tick that lands late still
/// leaves half the queue's timeout of margin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Heartbeat {
    pub(crate) extend_to_secs: i32,
    pub(crate) period: Duration,
}

impl Heartbeat {
    /// `None` for a queue whose timeout is 0: nothing can be held on it.
    pub(crate) fn for_queue_timeout(queue_timeout_secs: i32) -> Option<Self> {
        if queue_timeout_secs <= 0 {
            return None;
        }
        let extend_to_secs = queue_timeout_secs.min(SQS_MAX_VISIBILITY_TIMEOUT_SECS);
        let period_ms = u64::from(extend_to_secs.unsigned_abs()).saturating_mul(500);
        Some(Self {
            extend_to_secs,
            period: Duration::from_millis(period_ms),
        })
    }

    /// How long one `ChangeMessageVisibilityBatch` request may take.
    pub(crate) fn chunk_bound(&self) -> Duration {
        (self.period / 4).min(MAX_CHUNK_BOUND)
    }

    /// How long one round may spend over all its requests. A handle extended
    /// at the start of a round is next reached within `period + budget`, which
    /// is 3/4 of the window it was extended for. A round that runs out of
    /// budget resumes at the first unsent chunk on the next tick; with `n`
    /// requests taking `latency` each, a round covers every handle when
    /// `n * latency <= period / 2`, so a few thousand held handles fit at the
    /// default timeout.
    pub(crate) fn round_budget(&self) -> Duration {
        self.period / 2
    }
}

/// What one extension round did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Round {
    pub(crate) stale: Vec<String>,
    /// Requests that outlasted their bound.
    pub(crate) timed_out: usize,
    /// The round ran out of budget before every chunk was sent.
    pub(crate) cut_short: bool,
}

/// Sends every 10-handle chunk of `handles` through `send`, starting at the
/// chunk `cursor` names so a round cut short by its budget resumes where it
/// stopped and no chunk is systematically skipped. Each request has its own
/// bound, so a slow one does not stop the chunks after it. A timed-out chunk
/// is where the next round starts.
pub(crate) async fn run_round<F, Fut>(
    handles: &[String],
    cursor: &mut usize,
    chunk_bound: Duration,
    budget: Duration,
    mut send: F,
) -> Round
where
    F: FnMut(Vec<String>) -> Fut,
    Fut: Future<Output = VisibilityChange>,
{
    let mut round = Round::default();
    let chunks: Vec<&[String]> = handles.chunks(MAX_BATCH_ENTRIES).collect();
    let count = chunks.len();
    if count == 0 {
        *cursor = 0;
        return round;
    }
    let start = *cursor % count;
    let deadline = tokio::time::Instant::now().checked_add(budget);
    let mut next_start = None;
    for step in 0..count {
        let index = start.saturating_add(step) % count;
        let remaining = match deadline {
            Some(d) => d.saturating_duration_since(tokio::time::Instant::now()),
            None => chunk_bound,
        };
        if remaining.is_zero() {
            round.cut_short = true;
            next_start.get_or_insert(index);
            break;
        }
        let Some(chunk) = chunks.get(index) else {
            continue;
        };
        match tokio::time::timeout(chunk_bound.min(remaining), send(chunk.to_vec())).await {
            Ok(change) => round.stale.extend(change.stale),
            Err(_) => {
                round.timed_out = round.timed_out.saturating_add(1);
                next_start.get_or_insert(index);
            }
        }
    }
    *cursor = next_start.unwrap_or(0);
    round
}

/// Reads the `VisibilityTimeout` out of a `GetQueueAttributes` answer.
pub(crate) fn parse_visibility_timeout(
    queue_url: &str,
    attributes: Option<&HashMap<QueueAttributeName, String>>,
) -> Result<i32> {
    attributes
        .and_then(|a| a.get(&QueueAttributeName::VisibilityTimeout))
        .and_then(|v| v.parse::<i32>().ok())
        .ok_or_else(|| {
            ShoveError::Topology(format!(
                "SQS GetQueueAttributes on {queue_url} returned no usable VisibilityTimeout"
            ))
        })
}

/// Every receipt handle in the pending buffers, keys in sorted order so a
/// round's cursor means the same position on the next tick.
pub(crate) fn held_handles(pending: &HashMap<String, VecDeque<Message>>) -> Vec<String> {
    let mut keys: Vec<&String> = pending.keys().collect();
    keys.sort();
    keys.into_iter()
        .filter_map(|k| pending.get(k))
        .flatten()
        .filter_map(|m| m.receipt_handle())
        .map(str::to_string)
        .collect()
}

/// Removes the entries whose handle is in `stale`, and every entry queued
/// behind one in its key: the stale message returns to the queue ahead of
/// them. Returns the handles of the entries removed behind a stale one, which
/// the caller releases.
pub(crate) fn drop_stale(
    pending: &mut HashMap<String, VecDeque<Message>>,
    stale: &[String],
) -> Vec<String> {
    let mut released = Vec::new();
    for buffer in pending.values_mut() {
        let first_stale = buffer.iter().position(|m| {
            m.receipt_handle()
                .is_some_and(|h| stale.iter().any(|s| s == h))
        });
        let Some(at) = first_stale else {
            continue;
        };
        released.extend(
            buffer
                .drain(at..)
                .skip(1)
                .filter_map(|m| m.receipt_handle().map(str::to_string)),
        );
    }
    pending.retain(|_, buffer| !buffer.is_empty());
    released
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(handle: &str) -> Message {
        Message::builder().receipt_handle(handle).build()
    }

    fn pending(entries: &[(&str, &[&str])]) -> HashMap<String, VecDeque<Message>> {
        entries
            .iter()
            .map(|(k, hs)| (k.to_string(), hs.iter().map(|h| msg(h)).collect()))
            .collect()
    }

    fn handles(p: &HashMap<String, VecDeque<Message>>, key: &str) -> Vec<String> {
        p.get(key)
            .map(|b| {
                b.iter()
                    .filter_map(|m| m.receipt_handle().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn extends_to_the_queue_timeout_every_half_of_it() {
        assert_eq!(
            Heartbeat::for_queue_timeout(30),
            Some(Heartbeat {
                extend_to_secs: 30,
                period: Duration::from_secs(15)
            })
        );
        assert_eq!(
            Heartbeat::for_queue_timeout(1),
            Some(Heartbeat {
                extend_to_secs: 1,
                period: Duration::from_millis(500)
            })
        );
    }

    #[test]
    fn a_queue_that_holds_nothing_has_no_heartbeat() {
        assert_eq!(Heartbeat::for_queue_timeout(0), None);
        assert_eq!(Heartbeat::for_queue_timeout(-1), None);
        assert_eq!(Heartbeat::for_queue_timeout(i32::MIN), None);
    }

    #[test]
    fn the_extension_is_capped_at_the_sqs_maximum() {
        let max = Heartbeat::for_queue_timeout(SQS_MAX_VISIBILITY_TIMEOUT_SECS);
        assert_eq!(
            max,
            Some(Heartbeat {
                extend_to_secs: SQS_MAX_VISIBILITY_TIMEOUT_SECS,
                period: Duration::from_secs(21600)
            })
        );
        assert_eq!(Heartbeat::for_queue_timeout(i32::MAX), max);
    }

    #[test]
    fn requests_and_rounds_are_bounded_inside_the_period() {
        let short = Heartbeat::for_queue_timeout(4);
        assert_eq!(
            short.map(|h| h.chunk_bound()),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            short.map(|h| h.round_budget()),
            Some(Duration::from_secs(1))
        );
        let default = Heartbeat::for_queue_timeout(30);
        assert_eq!(
            default.map(|h| h.chunk_bound()),
            Some(Duration::from_millis(3750))
        );
        assert_eq!(
            default.map(|h| h.round_budget()),
            Some(Duration::from_millis(7500))
        );
        let long = Heartbeat::for_queue_timeout(SQS_MAX_VISIBILITY_TIMEOUT_SECS);
        assert_eq!(long.map(|h| h.chunk_bound()), Some(Duration::from_secs(5)));
    }

    #[test]
    fn visibility_timeout_is_read_from_the_attributes() {
        let attrs = HashMap::from([(QueueAttributeName::VisibilityTimeout, "45".to_string())]);
        assert_eq!(parse_visibility_timeout("q", Some(&attrs)).ok(), Some(45));
    }

    #[test]
    fn a_missing_or_unreadable_timeout_is_an_error() {
        assert!(parse_visibility_timeout("q", None).is_err());
        assert!(parse_visibility_timeout("q", Some(&HashMap::new())).is_err());
        let bad = HashMap::from([(QueueAttributeName::VisibilityTimeout, "soon".to_string())]);
        assert!(parse_visibility_timeout("q", Some(&bad)).is_err());
    }

    #[test]
    fn every_buffered_handle_is_held_and_handleless_messages_are_skipped() {
        let mut p = pending(&[("a", &["a1", "a2"]), ("b", &["b1"])]);
        p.entry("c".into())
            .or_default()
            .push_back(Message::builder().build());
        assert_eq!(held_handles(&p), ["a1", "a2", "b1"]);
        assert!(held_handles(&HashMap::new()).is_empty());
    }

    #[test]
    fn a_stale_entry_takes_the_entries_behind_it_and_leaves_other_keys() {
        let mut p = pending(&[("a", &["a1", "a2", "a3"]), ("b", &["b1", "b2"])]);
        let released = drop_stale(&mut p, &["a2".to_string(), "b2".to_string()]);
        assert_eq!(handles(&p, "a"), ["a1"]);
        assert_eq!(handles(&p, "b"), ["b1"]);
        let mut released = released;
        released.sort();
        assert_eq!(released, ["a3"]);
    }

    #[test]
    fn a_stale_head_empties_its_key() {
        let mut p = pending(&[("a", &["a1", "a2"]), ("b", &["b1"])]);
        let released = drop_stale(&mut p, &["a1".to_string()]);
        assert_eq!(released, ["a2"]);
        assert!(!p.contains_key("a"));
        assert_eq!(handles(&p, "b"), ["b1"]);
    }

    #[test]
    fn no_stale_handles_changes_nothing() {
        let mut p = pending(&[("a", &["a1", "a2"])]);
        assert!(drop_stale(&mut p, &[]).is_empty());
        assert_eq!(handles(&p, "a"), ["a1", "a2"]);
    }

    fn numbered(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("h{i}")).collect()
    }

    /// A sender that records each chunk's first handle and takes `delay_of`
    /// that chunk's index to answer.
    fn recording_sender(
        seen: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        delay_of: impl Fn(usize) -> Duration + Copy + 'static,
    ) -> impl FnMut(Vec<String>) -> std::pin::Pin<Box<dyn Future<Output = VisibilityChange>>> {
        move |chunk: Vec<String>| {
            let seen = seen.clone();
            Box::pin(async move {
                let first = chunk.first().cloned().unwrap_or_default();
                let index = first.trim_start_matches('h').parse::<usize>().unwrap_or(0) / 10;
                if let Ok(mut s) = seen.lock() {
                    s.push(first);
                }
                tokio::time::sleep(delay_of(index)).await;
                VisibilityChange::default()
            })
        }
    }

    #[tokio::test]
    async fn a_round_sends_every_chunk_of_a_large_set() {
        let seen = Default::default();
        let mut cursor = 0;
        let round = run_round(
            &numbered(95),
            &mut cursor,
            Duration::from_millis(200),
            Duration::from_secs(5),
            recording_sender(std::sync::Arc::clone(&seen), |_| Duration::ZERO),
        )
        .await;
        assert_eq!(seen.lock().map(|s| s.len()).ok(), Some(10));
        assert!(!round.cut_short);
        assert_eq!(round.timed_out, 0);
        assert_eq!(cursor, 0);
    }

    #[tokio::test]
    async fn a_slow_chunk_does_not_stop_the_chunks_after_it() {
        let seen: std::sync::Arc<std::sync::Mutex<Vec<String>>> = Default::default();
        let mut cursor = 0;
        let round = run_round(
            &numbered(30),
            &mut cursor,
            Duration::from_millis(50),
            Duration::from_secs(5),
            recording_sender(std::sync::Arc::clone(&seen), |i| {
                if i == 1 {
                    Duration::from_secs(30)
                } else {
                    Duration::ZERO
                }
            }),
        )
        .await;
        assert_eq!(
            seen.lock().map(|s| s.clone()).ok(),
            Some(numbered(30).into_iter().step_by(10).collect())
        );
        assert_eq!(round.timed_out, 1);
        assert_eq!(
            cursor, 1,
            "the next round starts at the chunk that timed out"
        );
    }

    #[tokio::test]
    async fn a_round_cut_short_resumes_where_it_stopped() {
        let seen: std::sync::Arc<std::sync::Mutex<Vec<String>>> = Default::default();
        let mut cursor = 0;
        let all = numbered(50);
        let mut covered = std::collections::HashSet::new();
        for _ in 0..3 {
            let round = run_round(
                &all,
                &mut cursor,
                Duration::from_millis(100),
                Duration::from_millis(70),
                recording_sender(std::sync::Arc::clone(&seen), |_| Duration::from_millis(30)),
            )
            .await;
            assert!(round.cut_short || cursor == 0);
            if let Ok(mut s) = seen.lock() {
                covered.extend(s.drain(..));
            }
        }
        assert_eq!(covered.len(), 5, "every chunk was sent within three rounds");
    }

    #[tokio::test]
    async fn stale_handles_from_every_chunk_are_collected() {
        let mut cursor = 0;
        let round = run_round(
            &numbered(25),
            &mut cursor,
            Duration::from_millis(200),
            Duration::from_secs(5),
            |chunk: Vec<String>| async move {
                VisibilityChange {
                    stale: chunk.into_iter().take(1).collect(),
                    errors: 0,
                }
            },
        )
        .await;
        assert_eq!(round.stale, ["h0", "h10", "h20"]);
    }
}
