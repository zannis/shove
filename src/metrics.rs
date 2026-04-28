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

/// Override the prefix applied to every emitted metric name. Call once,
/// before any metric emission and before installing the recorder.
///
/// Subsequent calls are silently ignored (the prefix is set-once, matching
/// `metrics::set_global_recorder`). Defaults to `"shove"` if never called.
///
/// The provided string is leaked so we can hand `&'static str` names to
/// the `metrics` macros without per-emit allocation.
pub fn set_prefix(prefix: impl Into<String>) {
    let leaked: &'static str = Box::leak(prefix.into().into_boxed_str());
    let _ = PREFIX.set(leaked);
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
    pub consumer_workers: &'static str,
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
            consumer_workers: leak(format!("{p}_consumer_workers")),
            autoscaler_decisions_total: leak(format!("{p}_autoscaler_decisions_total")),
            backend_errors_total: leak(format!("{p}_backend_errors_total")),
        }
    })
}

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
        assert_eq!(n.consumer_workers, "shove_consumer_workers");
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