//! Operational metrics emitted via the `metrics` facade.
//!
//! Consuming services install a recorder (e.g. `metrics-exporter-prometheus`)
//! and expose the scrape endpoint themselves; this crate never opens a port.
//!
//! See `docs/pages/guides/observability.mdx` for the full metric reference.

use std::sync::OnceLock;

/// Default name prefix applied to every metric. Override with [`set_prefix`].
const DEFAULT_PREFIX: &str = "shove";

static PREFIX: OnceLock<String> = OnceLock::new();
static NAMES: OnceLock<MetricNames> = OnceLock::new();

/// Override the prefix applied to every emitted metric name.
///
/// Call once at startup, **before** any metric emission and before installing
/// the recorder. The prefix is materialised into the [`MetricNames`] cache the
/// first time any helper emits, so calling `set_prefix` after that point
/// silently has no effect — and rather than mask the misconfiguration, this
/// function panics in that case. Calling twice in a row also panics.
///
/// The prefix must match Prometheus' metric-name grammar
/// (`[a-zA-Z_][a-zA-Z0-9_]*`); names are formatted as `{prefix}_<suffix>` so
/// hyphens or other special characters in the prefix produce invalid metric
/// names that exporters will reject.
///
/// # Panics
///
/// - If `set_prefix` has already been called.
/// - If any metric has already been emitted (the cache is locked).
pub fn set_prefix(prefix: impl Into<String>) {
    assert!(
        NAMES.get().is_none(),
        "shove::metrics::set_prefix called after metric emission already initialized \
         the name cache; call set_prefix at startup before any broker/publisher work",
    );
    PREFIX
        .set(prefix.into())
        .expect("shove::metrics::set_prefix called twice; the prefix is set-once at startup");
}

fn prefix() -> &'static str {
    // PREFIX is a static OnceLock<String>, so the returned &str is 'static.
    PREFIX.get().map(String::as_str).unwrap_or(DEFAULT_PREFIX)
}

#[allow(dead_code)] // Fields are read by emission helpers behind the `metrics` feature.
pub(crate) struct MetricNames {
    pub messages_consumed_total: String,
    pub messages_failed_total: String,
    pub messages_published_total: String,
    pub message_processing_duration_seconds: String,
    pub message_publish_duration_seconds: String,
    pub message_size_bytes: String,
    pub messages_inflight: String,
    pub autoscaler_decisions_total: String,
    pub autoscaler_messages_ready: String,
    pub autoscaler_messages_in_flight: String,
    pub autoscaler_active_consumers: String,
    pub backend_errors_total: String,
}

#[allow(dead_code)] // used by emission helpers when the `metrics` feature is on
pub(crate) fn names() -> &'static MetricNames {
    NAMES.get_or_init(|| {
        let p = prefix();
        MetricNames {
            messages_consumed_total: format!("{p}_messages_consumed_total"),
            messages_failed_total: format!("{p}_messages_failed_total"),
            messages_published_total: format!("{p}_messages_published_total"),
            message_processing_duration_seconds: format!("{p}_message_processing_duration_seconds"),
            message_publish_duration_seconds: format!("{p}_message_publish_duration_seconds"),
            message_size_bytes: format!("{p}_message_size_bytes"),
            messages_inflight: format!("{p}_messages_inflight"),
            autoscaler_decisions_total: format!("{p}_autoscaler_decisions_total"),
            autoscaler_messages_ready: format!("{p}_autoscaler_messages_ready"),
            autoscaler_messages_in_flight: format!("{p}_autoscaler_messages_in_flight"),
            autoscaler_active_consumers: format!("{p}_autoscaler_active_consumers"),
            backend_errors_total: format!("{p}_backend_errors_total"),
        }
    })
}

use crate::outcome::Outcome;

/// Stable string label for an [`Outcome`].
#[allow(dead_code)]
pub(crate) fn outcome_label(o: &Outcome) -> &'static str {
    match o {
        Outcome::Ack => "ack",
        Outcome::Retry => "retry",
        Outcome::Reject => "reject",
        Outcome::Defer => "defer",
    }
}

/// Reason categories for messages that failed in the consumer pipeline.
///
/// Includes both pre-handler failures (oversize, deserialize, pending_full)
/// and post-handler terminal outcomes (timeout, max_retries_exceeded,
/// rejected) so operators can alert on every code path that retires a
/// message without an Ack.
///
/// # Sequenced consumers: count independent failures, not cascades
///
/// On a sequenced topic declared with [`SequenceFailure::FailAll`], one
/// message failing poisons its sequence key: every delivery already buffered
/// for that key, plus every later delivery that arrives while the key stays
/// poisoned, is dead-lettered without ever reaching the handler.
///
/// Only the message that *actually* failed is counted. The cascade is not.
///
/// The reason is that a cascade discard is not an independent failure — it is
/// collateral of one already-counted failure. Counting it would scale
/// `shove_messages_failed_total` by the queue depth behind the poisoned key,
/// so a single bad message could register as hundreds of failures and make the
/// counter useless for exactly the alerting it exists to support. The
/// cascade's size is an ordering-policy consequence, observable through the
/// `warn!`/`info!` logs at each poisoning site, not a failure count.
///
/// The rule applies to every backend that implements poisoned-key semantics
/// (in-memory, RabbitMQ, SQS). Each cascade site is marked with a
/// `// Cascade: intentionally not counted` comment pointing back here, so the
/// backends do not drift apart the next time one of them is touched.
///
/// [`SequenceFailure::FailAll`]: crate::topology::SequenceFailure
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub(crate) enum FailReason {
    Oversize,
    Deserialize,
    PendingFull,
    Timeout,
    MaxRetriesExceeded,
    Rejected,
    SchemaFrame,
    SchemaValidation,
}

#[allow(dead_code)]
impl FailReason {
    pub(crate) fn as_label(self) -> &'static str {
        match self {
            FailReason::Oversize => "oversize",
            FailReason::Deserialize => "deserialize",
            FailReason::PendingFull => "pending_full",
            FailReason::Timeout => "timeout",
            FailReason::MaxRetriesExceeded => "max_retries_exceeded",
            FailReason::Rejected => "rejected",
            FailReason::SchemaFrame => "schema_frame",
            FailReason::SchemaValidation => "schema_validation",
        }
    }

    /// Classify a schema-registry DLQ death-reason into a metric reason.
    ///
    /// Frame-level failures (`schema_frame_invalid`, `schema_unsupported_codec`)
    /// map to [`FailReason::SchemaFrame`]; subject-resolve and validation
    /// failures (and any unrecognised reason) map to
    /// [`FailReason::SchemaValidation`].
    #[cfg(feature = "kafka-schema-registry")]
    pub(crate) fn for_schema_reason(reason: &str) -> FailReason {
        match reason {
            "schema_frame_invalid" | "schema_unsupported_codec" => FailReason::SchemaFrame,
            _ => FailReason::SchemaValidation,
        }
    }
}

/// Backend identifier label values used in `backend_errors_total`.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub(crate) enum BackendLabel {
    InMemory,
    RabbitMq,
    Kafka,
    Nats,
    Redis,
    SnsSqs,
}

#[allow(dead_code)]
impl BackendLabel {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            BackendLabel::InMemory => "inmemory",
            BackendLabel::RabbitMq => "rabbitmq",
            BackendLabel::Kafka => "kafka",
            BackendLabel::Nats => "nats",
            BackendLabel::Redis => "redis",
            BackendLabel::SnsSqs => "sns_sqs",
        }
    }
}

/// Backend error category for `backend_errors_total{kind}`.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub(crate) enum BackendErrorKind {
    Connection,
    Publish,
    Consume,
    Topology,
    Ack,
}

#[allow(dead_code)]
impl BackendErrorKind {
    pub(crate) fn as_label(self) -> &'static str {
        match self {
            BackendErrorKind::Connection => "connection",
            BackendErrorKind::Publish => "publish",
            BackendErrorKind::Consume => "consume",
            BackendErrorKind::Topology => "topology",
            BackendErrorKind::Ack => "ack",
        }
    }
}

const DEFAULT_GROUP: &str = "default";

#[allow(dead_code)]
pub(crate) fn group_label(group: Option<&str>) -> &str {
    group.unwrap_or(DEFAULT_GROUP)
}

// ---------------------------------------------------------------------------
// Emission helpers — `#[cfg(feature = "metrics")]` real bodies; no-op stubs
// when the feature is off.
// ---------------------------------------------------------------------------

#[cfg(feature = "metrics")]
pub(crate) fn record_consumed(topic: &str, group: Option<&str>, outcome: &Outcome) {
    ::metrics::counter!(
        names().messages_consumed_total.as_str(),
        "topic" => topic.to_string(),
        "consumer_group" => group_label(group).to_string(),
        "outcome" => outcome_label(outcome),
    )
    .increment(1);
}

#[cfg(not(feature = "metrics"))]
#[allow(dead_code)] // Callers gated behind backend features.
pub(crate) fn record_consumed(_: &str, _: Option<&str>, _: &Outcome) {}

/// `record_consumed` for a whole batch at once, so a batch consumer's
/// `messages_consumed_total` counts *messages* (comparable with the
/// single-message consumers) rather than flushes.
#[cfg(feature = "metrics")]
pub(crate) fn record_consumed_n(topic: &str, group: Option<&str>, outcome: &Outcome, count: u64) {
    if count == 0 {
        return;
    }
    ::metrics::counter!(
        names().messages_consumed_total.as_str(),
        "topic" => topic.to_string(),
        "consumer_group" => group_label(group).to_string(),
        "outcome" => outcome_label(outcome),
    )
    .increment(count);
}

#[cfg(not(feature = "metrics"))]
#[allow(dead_code)] // Callers gated behind backend features.
pub(crate) fn record_consumed_n(_: &str, _: Option<&str>, _: &Outcome, _: u64) {}

#[cfg(feature = "metrics")]
pub(crate) fn record_failed(topic: &str, group: Option<&str>, reason: FailReason) {
    ::metrics::counter!(
        names().messages_failed_total.as_str(),
        "topic" => topic.to_string(),
        "consumer_group" => group_label(group).to_string(),
        "reason" => reason.as_label(),
    )
    .increment(1);
}

#[cfg(not(feature = "metrics"))]
#[allow(dead_code)] // Callers gated behind backend features.
pub(crate) fn record_failed(_: &str, _: Option<&str>, _: FailReason) {}

/// `record_failed` for a whole batch at once, so a batch consumer's
/// `messages_failed_total` counts *messages* (comparable with the
/// single-message consumers) rather than flushes.
#[cfg(feature = "metrics")]
#[allow(dead_code)] // Callers gated behind backend features.
pub(crate) fn record_failed_n(topic: &str, group: Option<&str>, reason: FailReason, count: u64) {
    if count == 0 {
        return;
    }
    ::metrics::counter!(
        names().messages_failed_total.as_str(),
        "topic" => topic.to_string(),
        "consumer_group" => group_label(group).to_string(),
        "reason" => reason.as_label(),
    )
    .increment(count);
}

#[cfg(not(feature = "metrics"))]
#[allow(dead_code)] // Callers gated behind backend features.
pub(crate) fn record_failed_n(_: &str, _: Option<&str>, _: FailReason, _: u64) {}

#[cfg(feature = "metrics")]
pub(crate) fn record_published(topic: &str, ok: bool) {
    record_published_n(topic, ok, 1);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn record_published(_: &str, _: bool) {}

#[cfg(feature = "metrics")]
pub(crate) fn record_published_n(topic: &str, ok: bool, count: u64) {
    if count == 0 {
        return;
    }
    let outcome = if ok { "success" } else { "error" };
    ::metrics::counter!(
        names().messages_published_total.as_str(),
        "topic" => topic.to_string(),
        "outcome" => outcome,
    )
    .increment(count);
}

#[cfg(not(feature = "metrics"))]
#[allow(dead_code)] // Callers gated behind backend features.
pub(crate) fn record_published_n(_: &str, _: bool, _: u64) {}

#[cfg(feature = "metrics")]
pub(crate) fn record_processing_duration(
    topic: &str,
    group: Option<&str>,
    outcome: &Outcome,
    elapsed_secs: f64,
) {
    ::metrics::histogram!(
        names().message_processing_duration_seconds.as_str(),
        "topic" => topic.to_string(),
        "consumer_group" => group_label(group).to_string(),
        "outcome" => outcome_label(outcome),
    )
    .record(elapsed_secs);
}

#[cfg(not(feature = "metrics"))]
#[allow(dead_code)] // Callers gated behind backend features.
pub(crate) fn record_processing_duration(_: &str, _: Option<&str>, _: &Outcome, _: f64) {}

#[cfg(feature = "metrics")]
pub(crate) fn record_publish_duration(topic: &str, ok: bool, elapsed_secs: f64) {
    let outcome = if ok { "success" } else { "error" };
    ::metrics::histogram!(
        names().message_publish_duration_seconds.as_str(),
        "topic" => topic.to_string(),
        "outcome" => outcome,
    )
    .record(elapsed_secs);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn record_publish_duration(_: &str, _: bool, _: f64) {}

#[cfg(feature = "metrics")]
pub(crate) fn record_message_size(topic: &str, group: Option<&str>, bytes: usize) {
    ::metrics::histogram!(
        names().message_size_bytes.as_str(),
        "topic" => topic.to_string(),
        "consumer_group" => group_label(group).to_string(),
    )
    .record(bytes as f64);
}

#[cfg(not(feature = "metrics"))]
#[allow(dead_code)] // Callers gated behind backend features.
pub(crate) fn record_message_size(_: &str, _: Option<&str>, _: usize) {}

#[cfg(feature = "metrics")]
pub(crate) fn inc_inflight(topic: &str, group: Option<&str>, count: u64) {
    ::metrics::gauge!(
        names().messages_inflight.as_str(),
        "topic" => topic.to_string(),
        "consumer_group" => group_label(group).to_string(),
    )
    .increment(count as f64);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn inc_inflight(_: &str, _: Option<&str>, _: u64) {}

#[cfg(feature = "metrics")]
pub(crate) fn dec_inflight(topic: &str, group: Option<&str>, count: u64) {
    ::metrics::gauge!(
        names().messages_inflight.as_str(),
        "topic" => topic.to_string(),
        "consumer_group" => group_label(group).to_string(),
    )
    .decrement(count as f64);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn dec_inflight(_: &str, _: Option<&str>, _: u64) {}

/// RAII handle that increments the inflight gauge on construction and
/// decrements it on drop. Use this instead of paired `inc_inflight` /
/// `dec_inflight` calls so the decrement runs even on panic, early
/// return, or `?`-shortcircuit.
#[allow(dead_code)]
pub(crate) struct InflightGuard {
    topic: std::sync::Arc<str>,
    group: Option<std::sync::Arc<str>>,
    count: u64,
}

#[allow(dead_code)]
impl InflightGuard {
    pub(crate) fn new(topic: std::sync::Arc<str>, group: Option<std::sync::Arc<str>>) -> Self {
        Self::with_count(topic, group, 1)
    }

    /// Batch variant: the gauge counts *messages* in flight, so a batch of
    /// `count` messages handed to a handler moves it by `count`, not by one.
    /// Keeps `messages_inflight` comparable between the single-message and
    /// batch consumers.
    pub(crate) fn with_count(
        topic: std::sync::Arc<str>,
        group: Option<std::sync::Arc<str>>,
        count: u64,
    ) -> Self {
        inc_inflight(&topic, group.as_deref(), count);
        Self {
            topic,
            group,
            count,
        }
    }

    /// Convenience constructor for borrowed inputs.
    pub(crate) fn from_refs(topic: &str, group: Option<&str>) -> Self {
        Self::new(std::sync::Arc::from(topic), group.map(std::sync::Arc::from))
    }

    /// Convenience constructor for borrowed inputs — see [`Self::with_count`].
    pub(crate) fn from_refs_n(topic: &str, group: Option<&str>, count: u64) -> Self {
        Self::with_count(
            std::sync::Arc::from(topic),
            group.map(std::sync::Arc::from),
            count,
        )
    }

    pub(crate) fn topic(&self) -> &str {
        &self.topic
    }

    pub(crate) fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        dec_inflight(&self.topic, self.group.as_deref(), self.count);
    }
}

#[cfg(feature = "metrics")]
pub(crate) fn record_autoscaler_decision(group: &str, direction: &'static str) {
    ::metrics::counter!(
        names().autoscaler_decisions_total.as_str(),
        "consumer_group" => group.to_string(),
        "direction" => direction,
    )
    .increment(1);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn record_autoscaler_decision(_: &str, _: &'static str) {}

#[cfg(feature = "metrics")]
pub(crate) fn record_autoscaler_backlog(
    group: &str,
    messages_ready: u64,
    messages_in_flight: u64,
    active_consumers: u16,
) {
    ::metrics::gauge!(
        names().autoscaler_messages_ready.as_str(),
        "consumer_group" => group.to_string(),
    )
    .set(messages_ready as f64);
    ::metrics::gauge!(
        names().autoscaler_messages_in_flight.as_str(),
        "consumer_group" => group.to_string(),
    )
    .set(messages_in_flight as f64);
    ::metrics::gauge!(
        names().autoscaler_active_consumers.as_str(),
        "consumer_group" => group.to_string(),
    )
    .set(active_consumers as f64);
}

#[cfg(not(feature = "metrics"))]
#[allow(dead_code)] // Callers gated behind backend features.
pub(crate) fn record_autoscaler_backlog(_: &str, _: u64, _: u64, _: u16) {}

#[cfg(feature = "metrics")]
pub(crate) fn record_backend_error(backend: BackendLabel, kind: BackendErrorKind) {
    ::metrics::counter!(
        names().backend_errors_total.as_str(),
        "backend" => backend.as_str(),
        "kind" => kind.as_label(),
    )
    .increment(1);
}

#[cfg(not(feature = "metrics"))]
#[allow(dead_code)] // Callers gated behind backend features.
pub(crate) fn record_backend_error(_: BackendLabel, _: BackendErrorKind) {}

#[cfg(test)]
mod tests {
    use super::*;

    // PREFIX is process-wide and set-once. We don't test the override path
    // here because it would race with other tests — covered separately by
    // a dedicated integration-style test that runs in its own process.
    #[test]
    fn default_prefix_is_shove() {
        assert_eq!(prefix(), "shove");
    }

    #[test]
    fn names_use_default_prefix() {
        let n = names();
        assert_eq!(
            n.messages_consumed_total.as_str(),
            "shove_messages_consumed_total"
        );
        assert_eq!(
            n.messages_failed_total.as_str(),
            "shove_messages_failed_total"
        );
        assert_eq!(
            n.messages_published_total.as_str(),
            "shove_messages_published_total"
        );
        assert_eq!(
            n.message_processing_duration_seconds.as_str(),
            "shove_message_processing_duration_seconds"
        );
        assert_eq!(
            n.message_publish_duration_seconds.as_str(),
            "shove_message_publish_duration_seconds"
        );
        assert_eq!(n.message_size_bytes.as_str(), "shove_message_size_bytes");
        assert_eq!(n.messages_inflight.as_str(), "shove_messages_inflight");
        assert_eq!(
            n.autoscaler_decisions_total.as_str(),
            "shove_autoscaler_decisions_total"
        );
        assert_eq!(
            n.backend_errors_total.as_str(),
            "shove_backend_errors_total"
        );
    }

    #[test]
    fn names_are_cached_static_pointers() {
        let a = names();
        let b = names();
        // Same allocation reused on every call — verifies the OnceLock cache.
        assert!(std::ptr::eq(a, b));
    }

    #[test]
    fn backend_label_as_str_all_variants() {
        assert_eq!(BackendLabel::InMemory.as_str(), "inmemory");
        assert_eq!(BackendLabel::RabbitMq.as_str(), "rabbitmq");
        assert_eq!(BackendLabel::Kafka.as_str(), "kafka");
        assert_eq!(BackendLabel::Nats.as_str(), "nats");
        assert_eq!(BackendLabel::Redis.as_str(), "redis");
        assert_eq!(BackendLabel::SnsSqs.as_str(), "sns_sqs");
    }

    #[test]
    fn backend_error_kind_as_str_all_variants() {
        assert_eq!(BackendErrorKind::Connection.as_label(), "connection");
        assert_eq!(BackendErrorKind::Publish.as_label(), "publish");
        assert_eq!(BackendErrorKind::Consume.as_label(), "consume");
        assert_eq!(BackendErrorKind::Topology.as_label(), "topology");
        assert_eq!(BackendErrorKind::Ack.as_label(), "ack");
    }

    #[test]
    fn autoscaler_backlog_gauge_names_use_default_prefix() {
        let n = names();
        assert_eq!(
            n.autoscaler_messages_ready.as_str(),
            "shove_autoscaler_messages_ready"
        );
        assert_eq!(
            n.autoscaler_messages_in_flight.as_str(),
            "shove_autoscaler_messages_in_flight"
        );
        assert_eq!(
            n.autoscaler_active_consumers.as_str(),
            "shove_autoscaler_active_consumers"
        );
    }

    #[cfg(feature = "kafka-schema-registry")]
    #[test]
    fn for_schema_reason_routes_frame_and_validation() {
        assert_eq!(
            FailReason::for_schema_reason("schema_frame_invalid").as_label(),
            "schema_frame"
        );
        assert_eq!(
            FailReason::for_schema_reason("schema_unsupported_codec").as_label(),
            "schema_frame"
        );
        assert_eq!(
            FailReason::for_schema_reason("schema_resolve_failed").as_label(),
            "schema_validation"
        );
        assert_eq!(
            FailReason::for_schema_reason("schema_validation_failed").as_label(),
            "schema_validation"
        );
        // Any unknown reason falls back to schema_validation.
        assert_eq!(
            FailReason::for_schema_reason("unknown_reason").as_label(),
            "schema_validation"
        );
    }
}
