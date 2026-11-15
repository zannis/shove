//! The visibility heartbeat for messages a consumer holds un-dispatched: a
//! sequenced key's pending buffer and an accumulating or running batch.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use aws_sdk_sqs::types::{Message, QueueAttributeName};

use crate::backends::sns::router::SQS_MAX_VISIBILITY_TIMEOUT_SECS;
use crate::error::{Result, ShoveError};

/// Upper bound on one extension round, however long the queue's timeout is.
const MAX_EXTEND_BOUND: Duration = Duration::from_secs(5);

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

    /// How long one extension round may take before it is abandoned until the
    /// next tick.
    pub(crate) fn bound(&self) -> Duration {
        self.period.min(MAX_EXTEND_BOUND)
    }
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

/// Every receipt handle in the pending buffers.
pub(crate) fn held_handles(pending: &HashMap<String, VecDeque<Message>>) -> Vec<String> {
    pending
        .values()
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
    fn a_round_is_bounded_by_the_period_and_by_five_seconds() {
        let short = Heartbeat::for_queue_timeout(4);
        assert_eq!(short.map(|h| h.bound()), Some(Duration::from_secs(2)));
        let default = Heartbeat::for_queue_timeout(30);
        assert_eq!(default.map(|h| h.bound()), Some(Duration::from_secs(5)));
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
        let mut held = held_handles(&p);
        held.sort();
        assert_eq!(held, ["a1", "a2", "b1"]);
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
}
