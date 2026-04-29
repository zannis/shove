//! Operational metrics emitted via the `metrics` facade.
//!
//! Consuming services install a recorder (e.g. `metrics-exporter-prometheus`)
//! and expose the scrape endpoint themselves; this crate never opens a port.
//!
//! See `docs/pages/guides/observability.mdx` for the full metric reference.

use std::sync::OnceLock;

/// Default name prefix applied to every metric. Override with [`set_prefix`].
const DEFAULT_PREFIX: &str = "shove";

static PREFIX: OnceLock<&'static str> = OnceLock::new();
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
/// The provided string is leaked so we can hand `&'static str` names to the
/// `metrics` macros without per-emit allocation.
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
    let leaked: &'static str = Box::leak(prefix.into().into_boxed_str());
    PREFIX.set(leaked).expect(
        "shove::metrics::set_prefix called twice; the prefix is set-once at startup",
    );
}

fn prefix() -> &'static str {
    PREFIX.get().copied().unwrap_or(DEFAULT_PREFIX)
}

pub(crate) struct MetricNames {
    pub messages_consumed_total: &'static str,
    pub messages_failed_total: &'static str,
    pub messages_published_total: &'static str,
    pub message_processing_duration_seconds: &'static str,
    pub message_publish_duration_seconds: &'static str,
    pub message_size_bytes: &'static str,
    pub messages_inflight: &'static str,
    pub autoscaler_decisions_total: &'static str,
    pub backend_errors_total: &'static str,
}

#[allow(dead_code)] // used by emission helpers when the `metrics` feature is on
pub(crate) fn names() -> &'static MetricNames {
    NAMES.get_or_init(|| {
        let p = prefix();
        let leak = |s: String| -> &'static str { Box::leak(s.into_boxed_str()) };
        MetricNames {
            messages_consumed_total: leak(format!("{p}_messages_consumed_total")),
            messages_failed_total: leak(format!("{p}_messages_failed_total")),
            messages_published_total: leak(format!("{p}_messages_published_total")),
            message_processing_duration_seconds: leak(format!(
                "{p}_message_processing_duration_seconds"
            )),
            message_publish_duration_seconds: leak(format!(
                "{p}_message_publish_duration_seconds"
            )),
            message_size_bytes: leak(format!("{p}_message_size_bytes")),
            messages_inflight: leak(format!("{p}_messages_inflight")),
            autoscaler_decisions_total: leak(format!("{p}_autoscaler_decisions_total")),
            backend_errors_total: leak(format!("{p}_backend_errors_total")),
        }
    })
}

use crate::outcome::Outcome;

/// Stable string label for an [`Outcome`]. Stays low-cardinality (4 values).
#[allow(dead_code)]
pub(crate) fn outcome_label(o: &Outcome) -> &'static str {
    match o {
        Outcome::Ack => "ack",
        Outcome::Retry => "retry",
        Outcome::Reject => "reject",
        Outcome::Defer => "defer",
    }
}

/// Reason categories for messages that failed before reaching the handler.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub(crate) enum FailReason {
    Oversize,
    Deserialize,
    PendingFull,
    Timeout,
}

#[allow(dead_code)]
impl FailReason {
    pub(crate) fn as_label(self) -> &'static str {
        match self {
            FailReason::Oversize => "oversize",
            FailReason::Deserialize => "deserialize",
            FailReason::PendingFull => "pending_full",
            FailReason::Timeout => "timeout",
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
        names().messages_consumed_total,
        "topic" => topic.to_string(),
        "consumer_group" => group_label(group).to_string(),
        "outcome" => outcome_label(outcome),
    )
    .increment(1);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn record_consumed(_: &str, _: Option<&str>, _: &Outcome) {}

#[cfg(feature = "metrics")]
pub(crate) fn record_failed(topic: &str, group: Option<&str>, reason: FailReason) {
    ::metrics::counter!(
        names().messages_failed_total,
        "topic" => topic.to_string(),
        "consumer_group" => group_label(group).to_string(),
        "reason" => reason.as_label(),
    )
    .increment(1);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn record_failed(_: &str, _: Option<&str>, _: FailReason) {}

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
        names().messages_published_total,
        "topic" => topic.to_string(),
        "outcome" => outcome,
    )
    .increment(count);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn record_published_n(_: &str, _: bool, _: u64) {}

#[cfg(feature = "metrics")]
pub(crate) fn record_processing_duration(
    topic: &str,
    group: Option<&str>,
    outcome: &Outcome,
    elapsed_secs: f64,
) {
    ::metrics::histogram!(
        names().message_processing_duration_seconds,
        "topic" => topic.to_string(),
        "consumer_group" => group_label(group).to_string(),
        "outcome" => outcome_label(outcome),
    )
    .record(elapsed_secs);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn record_processing_duration(_: &str, _: Option<&str>, _: &Outcome, _: f64) {}

#[cfg(feature = "metrics")]
pub(crate) fn record_publish_duration(topic: &str, ok: bool, elapsed_secs: f64) {
    let outcome = if ok { "success" } else { "error" };
    ::metrics::histogram!(
        names().message_publish_duration_seconds,
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
        names().message_size_bytes,
        "topic" => topic.to_string(),
        "consumer_group" => group_label(group).to_string(),
    )
    .record(bytes as f64);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn record_message_size(_: &str, _: Option<&str>, _: usize) {}

#[cfg(feature = "metrics")]
pub(crate) fn inc_inflight(topic: &str, group: Option<&str>) {
    ::metrics::gauge!(
        names().messages_inflight,
        "topic" => topic.to_string(),
        "consumer_group" => group_label(group).to_string(),
    )
    .increment(1.0);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn inc_inflight(_: &str, _: Option<&str>) {}

#[cfg(feature = "metrics")]
pub(crate) fn dec_inflight(topic: &str, group: Option<&str>) {
    ::metrics::gauge!(
        names().messages_inflight,
        "topic" => topic.to_string(),
        "consumer_group" => group_label(group).to_string(),
    )
    .decrement(1.0);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn dec_inflight(_: &str, _: Option<&str>) {}

/// RAII handle that increments the inflight gauge on construction and
/// decrements it on drop. Use this instead of paired `inc_inflight` /
/// `dec_inflight` calls so the decrement runs even on panic, early
/// return, or `?`-shortcircuit.
#[allow(dead_code)]
pub(crate) struct InflightGuard {
    topic: std::sync::Arc<str>,
    group: Option<std::sync::Arc<str>>,
}

#[allow(dead_code)]
impl InflightGuard {
    pub(crate) fn new(topic: std::sync::Arc<str>, group: Option<std::sync::Arc<str>>) -> Self {
        inc_inflight(&topic, group.as_deref());
        Self { topic, group }
    }

    /// Convenience constructor for borrowed inputs.
    pub(crate) fn from_refs(topic: &str, group: Option<&str>) -> Self {
        Self::new(
            std::sync::Arc::from(topic),
            group.map(std::sync::Arc::from),
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
        dec_inflight(&self.topic, self.group.as_deref());
    }
}

#[cfg(feature = "metrics")]
pub(crate) fn record_autoscaler_decision(group: &str, direction: &'static str) {
    ::metrics::counter!(
        names().autoscaler_decisions_total,
        "consumer_group" => group.to_string(),
        "direction" => direction,
    )
    .increment(1);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn record_autoscaler_decision(_: &str, _: &'static str) {}

#[cfg(feature = "metrics")]
pub(crate) fn record_backend_error(backend: BackendLabel, kind: BackendErrorKind) {
    ::metrics::counter!(
        names().backend_errors_total,
        "backend" => backend.as_str(),
        "kind" => kind.as_label(),
    )
    .increment(1);
}

#[cfg(not(feature = "metrics"))]
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
        assert_eq!(n.messages_consumed_total, "shove_messages_consumed_total");
        assert_eq!(n.messages_failed_total, "shove_messages_failed_total");
        assert_eq!(n.messages_published_total, "shove_messages_published_total");
        assert_eq!(
            n.message_processing_duration_seconds,
            "shove_message_processing_duration_seconds"
        );
        assert_eq!(
            n.message_publish_duration_seconds,
            "shove_message_publish_duration_seconds"
        );
        assert_eq!(n.message_size_bytes, "shove_message_size_bytes");
        assert_eq!(n.messages_inflight, "shove_messages_inflight");
        assert_eq!(
            n.autoscaler_decisions_total,
            "shove_autoscaler_decisions_total"
        );
        assert_eq!(n.backend_errors_total, "shove_backend_errors_total");
    }

    #[test]
    fn names_are_cached_static_pointers() {
        let a = names();
        let b = names();
        // Same allocation reused on every call — verifies the OnceLock cache.
        assert!(std::ptr::eq(a, b));
    }
}