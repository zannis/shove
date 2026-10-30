use std::time::Duration;

use crate::batch::BatchFailure;

/// Errors that can occur during pub/sub operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ShoveError {
    /// Failed to serialize or deserialize a message.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Connection-level failure (channel closed, timeout, network error).
    #[error("connection error: {0}")]
    Connection(String),

    /// Topology declaration or validation failed.
    #[error("topology error: {0}")]
    Topology(String),

    /// Input validation failed (e.g. message too large, reserved header).
    #[error("validation error: {0}")]
    Validation(String),

    /// A `Codec` failed to encode or decode a payload.
    ///
    /// `codec` is the codec's stable `NAME`. `source` is the codec-specific
    /// error (e.g. `prost::EncodeError`, `prost::DecodeError`).
    #[error("codec error in {codec}: {source}")]
    Codec {
        codec: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// An error from an external SDK or backend that doesn't map to a known category.
    /// Treated as non-retryable so it surfaces immediately to the operator.
    #[error("unknown backend error: {0}")]
    Unknown(String),

    /// A [`Publisher::publish_batch`] call partially succeeded: the backend
    /// confirmed at least one record and did not confirm at least one other.
    ///
    /// The payload names the indices that still need re-publishing, so a
    /// caller can retry only those instead of re-producing the whole batch.
    /// A batch that fails as a *whole* does not use this variant — it returns
    /// the same bare error it always has.
    ///
    /// Boxed to keep `ShoveError` small: it is returned by value from every
    /// fallible call in the crate.
    ///
    /// [`Publisher::publish_batch`]: crate::publisher::Publisher::publish_batch
    #[error("batch publish: {0}")]
    PartialBatch(Box<BatchFailure>),

    /// An offset commit a stopping consumer could not get confirmed.
    ///
    /// The Kafka receive loop returns this from its shutdown arm when the
    /// synchronous commit it issues after the handler drain returns an
    /// error, misses the shutdown deadline, or has no thread to run on.
    /// Under either commit policy, and on a sequenced consumer, it also
    /// returns this for the commit in flight when the stop landed, when that
    /// commit has no answer within its share of the deadline; no final
    /// commit follows it. The member ends with this error instead of a clean
    /// exit: a group run counts it under
    /// [`SupervisorOutcome::errors`](crate::SupervisorOutcome::errors), so
    /// [`exit_code`](crate::SupervisorOutcome::exit_code) is `1`, and the
    /// run otherwise ends as before. It is not retryable, so a consumer that
    /// reconnects on transient errors returns it instead.
    ///
    /// The next member of the group resumes each partition from the last
    /// position the broker accepted, which an earlier commit may have
    /// advanced past some of the records this commit covered; the
    /// records from that position on are redelivered, which is at-least-once
    /// delivery made visible. The error is not raised for a position the
    /// member never tried to commit: an acknowledged offset on a partition a
    /// rebalance revoked is dropped with the partition and redelivered by
    /// its new owner, silently, as before.
    ///
    /// A member the autoscaler retires on scale-down makes the same final
    /// commit, and a coordinator in the middle of the rebalance that
    /// retirement triggers can reject it with a rebalance in progress or a
    /// stale generation; that rejection counts under `errors` like any
    /// other, and the new owner redelivers from the last accepted position.
    ///
    /// Why the result is surfaced at all: Apache Kafka's Java consumer
    /// raises the result of an explicit `commitSync()` as an exception, and
    /// its `close()` commits only under auto-commit, which shove turns off;
    /// librdkafka's `rd_kafka_commit` returns the error code of a
    /// synchronous commit, while `rd_kafka_consumer_close` reports only the
    /// close itself. shove makes an explicit synchronous commit and returns
    /// its result, because a process that gates its restart on the exit
    /// code cannot read a log line.
    ///
    /// Boxed to keep `ShoveError` small, like [`PartialBatch`](Self::PartialBatch).
    #[error(transparent)]
    Commit(Box<FailedCommit>),
}

/// An offset commit at shutdown that was not confirmed, the final commit
/// or the commit in flight when the stop landed: what the commit carried
/// and why it was not confirmed. The payload of [`ShoveError::Commit`].
///
/// `#[non_exhaustive]`: read the fields, and match with `..`, so a field
/// can be added later.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "offset commit at shutdown on '{topic}' for {} was not confirmed: {kind}",
    format_offsets(offsets)
)]
#[non_exhaustive]
pub struct FailedCommit {
    /// The topic the member consumed.
    pub topic: String,
    /// The offsets the consumer tried to commit, per partition: one
    /// `(partition, offset)` pair per partition the member held, the
    /// offset exclusive as Kafka commits it. Never empty: a member with
    /// nothing to commit has no commit to fail.
    pub offsets: Vec<(i32, i64)>,
    /// How the commit failed: the error it returned, no answer within the
    /// deadline, or no thread to run it on.
    pub kind: CommitFailure,
}

/// Why an offset commit at shutdown was not confirmed; the `kind` of
/// [`ShoveError::Commit`].
///
/// `#[non_exhaustive]`: match with a wildcard arm.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CommitFailure {
    /// The commit returned an error, carried as text: the coordinator's
    /// answer, or a librdkafka local error, such as a timeout of its own or
    /// an unknown partition. An answer from the coordinator says this
    /// commit did not land. A local error raised after the request was
    /// sent, such as a timeout, does not prove that: librdkafka reports it
    /// after its own retries, and the broker may have accepted the commit
    /// by then. The next member resumes from whatever position the broker
    /// holds.
    ///
    /// The text is librdkafka's rendering of that error: the error code and
    /// its description, and nothing else. It never carries a record's
    /// payload, key or headers, nor a personal or account identifier taken
    /// from one, so it is safe to log and to return as is.
    #[error("rejected: {0}")]
    Rejected(String),
    /// The commit had no answer within the time it carries: what is left of
    /// the shutdown deadline for a final commit, all of it when no commit was
    /// in flight, its share of that deadline for a commit
    /// found in flight at the stop, under either commit policy and on a
    /// sequenced consumer, or the time waited when the commit thread ended
    /// without reporting. The result is unknown: the detached commit thread
    /// may still land it after the consumer has returned.
    #[error("no answer after waiting {0:?}; the result is unknown")]
    Deadline(Duration),
    /// No thread could be spawned to run the commit, so this commit was
    /// never made and the consumer's close moved off the runtime by itself.
    #[error("no thread could be spawned for the commit; this commit was never made")]
    NoThread,
}

/// `[p0@o0, p1@o1]`, the `offsets` of [`FailedCommit`] in its message.
fn format_offsets(offsets: &[(i32, i64)]) -> String {
    let pairs: Vec<String> = offsets
        .iter()
        .map(|(partition, offset)| format!("{partition}@{offset}"))
        .collect();
    format!("[{}]", pairs.join(", "))
}

impl ShoveError {
    /// Returns `true` for transient errors that may succeed on retry (connection
    /// failures). Non-transient errors (topology, serialization) are returned
    /// immediately so callers don't waste time retrying.
    ///
    /// A [`PartialBatch`](Self::PartialBatch) delegates to the backend error
    /// behind it: re-publishing the outstanding records is worth attempting
    /// exactly when that error was.
    pub fn is_retryable(&self) -> bool {
        match self {
            ShoveError::Connection(_) => true,
            ShoveError::PartialBatch(f) => f.source().is_retryable(),
            _ => false,
        }
    }
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, ShoveError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_serialization_error() {
        let json_err = serde_json::from_str::<String>("not json").unwrap_err();
        let err = ShoveError::Serialization(json_err);
        let msg = err.to_string();
        assert!(msg.starts_with("serialization error:"), "got: {msg}");
    }

    #[test]
    fn display_connection_error() {
        let err = ShoveError::Connection("channel closed".into());
        assert_eq!(err.to_string(), "connection error: channel closed");
    }

    #[test]
    fn display_topology_error() {
        let err = ShoveError::Topology("missing exchange".into());
        assert_eq!(err.to_string(), "topology error: missing exchange");
    }

    #[test]
    fn from_serde_json_error() {
        let json_err = serde_json::from_str::<String>("{}").unwrap_err();
        let err: ShoveError = json_err.into();
        assert!(matches!(err, ShoveError::Serialization(_)));
    }

    #[test]
    fn display_codec_error() {
        let inner: Box<dyn std::error::Error + Send + Sync> = "boom".into();
        let err = ShoveError::Codec {
            codec: "protobuf",
            source: inner,
        };
        let msg = err.to_string();
        assert!(msg.contains("codec error in protobuf"), "got: {msg}");
    }

    #[test]
    fn codec_error_is_not_retryable() {
        let inner: Box<dyn std::error::Error + Send + Sync> = "boom".into();
        let err = ShoveError::Codec {
            codec: "json",
            source: inner,
        };
        assert!(!err.is_retryable());
    }

    fn commit_error() -> ShoveError {
        ShoveError::Commit(Box::new(FailedCommit {
            topic: "orders".into(),
            offsets: vec![(0, 8), (3, 12)],
            kind: CommitFailure::Rejected("Broker: Group authorization failed".into()),
        }))
    }

    /// The payload is boxed so that the error every fallible call returns
    /// by value stays the size it was before the variant existed.
    #[test]
    fn the_commit_payload_is_boxed_so_the_error_stays_small() {
        assert!(
            std::mem::size_of::<ShoveError>() <= 40,
            "ShoveError is {} bytes; box a large payload",
            std::mem::size_of::<ShoveError>()
        );
    }

    #[test]
    fn display_commit_error_names_the_topic_the_offsets_and_the_kind() {
        assert_eq!(
            commit_error().to_string(),
            "offset commit at shutdown on 'orders' for [0@8, 3@12] was not confirmed: \
             rejected: Broker: Group authorization failed"
        );
        let deadline = ShoveError::Commit(Box::new(FailedCommit {
            topic: "orders".into(),
            offsets: vec![(0, 8)],
            kind: CommitFailure::Deadline(Duration::from_secs(20)),
        }));
        assert_eq!(
            deadline.to_string(),
            "offset commit at shutdown on 'orders' for [0@8] was not confirmed: no answer after \
             waiting 20s; the result is unknown"
        );
        assert_eq!(
            CommitFailure::NoThread.to_string(),
            "no thread could be spawned for the commit; this commit was never made"
        );
    }

    /// A failed final commit is never retried: a reconnect would rejoin the
    /// group with the position still uncommitted and read as a clean member.
    #[test]
    fn commit_error_is_not_retryable() {
        assert!(!commit_error().is_retryable());
    }

    /// `PartialBatch` has no retryability of its own — it inherits the
    /// backend error's. A connection blip is worth re-publishing for; a
    /// topology error is not, no matter how many records got through.
    #[test]
    fn partial_batch_delegates_retryability_to_its_source() {
        use crate::batch::BatchReport;

        let retryable = BatchReport::prefix(1, 3, ShoveError::Connection("channel closed".into()))
            .resolve(3)
            .result
            .unwrap_err();
        assert!(matches!(retryable, ShoveError::PartialBatch(_)));
        assert!(retryable.is_retryable());

        let permanent = BatchReport::prefix(1, 3, ShoveError::Topology("missing queue".into()))
            .resolve(3)
            .result
            .unwrap_err();
        assert!(matches!(permanent, ShoveError::PartialBatch(_)));
        assert!(!permanent.is_retryable());
    }
}
