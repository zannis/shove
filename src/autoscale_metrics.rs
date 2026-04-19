//! Honest, partial-data metrics struct for autoscaler inputs. See DESIGN_V2.md §9.1.

use std::time::Duration;

/// Metrics snapshot for one queue / topic. All fields are `Option<T>`:
/// each backend fills what it can and leaves the rest `None`. Scaling
/// policies handle the `Option` shape explicitly instead of reading a
/// field that silently reports zero on a backend that can't compute it.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct AutoscaleMetrics {
    /// Messages waiting to be delivered. Kafka: consumer lag; SQS:
    /// `ApproximateNumberOfMessages`; RabbitMQ: queue depth.
    pub backlog: Option<u64>,
    /// Messages currently in flight (delivered but not yet acked).
    pub inflight: Option<u64>,
    /// Throughput (messages per second) over the most recent window.
    pub throughput_per_sec: Option<f64>,
    /// Observed per-message processing latency.
    pub processing_latency: Option<Duration>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_all_none() {
        let m = AutoscaleMetrics::default();
        assert!(m.backlog.is_none());
        assert!(m.inflight.is_none());
        assert!(m.throughput_per_sec.is_none());
        assert!(m.processing_latency.is_none());
    }
}
