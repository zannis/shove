//! The fetch queue settings of a Kafka consumer: the two thresholds
//! librdkafka keeps a partition's local queue under, and the backoff it waits
//! out before it fetches a partition that reached one.

use std::ops::RangeInclusive;

use rdkafka::ClientConfig;

use super::constants::FETCH_QUEUE_BACKOFF_MS;
use crate::error::{Result, ShoveError};

/// librdkafka's range for `queued.min.messages`.
pub(crate) const QUEUED_MIN_MESSAGES_RANGE: RangeInclusive<u32> = 1..=10_000_000;

/// librdkafka's range for `queued.max.messages.kbytes`.
pub(crate) const QUEUED_MAX_MESSAGES_KBYTES_RANGE: RangeInclusive<u32> = 1..=2_097_151;

/// librdkafka's range for `fetch.queue.backoff.ms`.
pub(crate) const FETCH_QUEUE_BACKOFF_MS_RANGE: RangeInclusive<u32> = 0..=300_000;

/// The fetch queue settings a consumer's librdkafka client is created with.
///
/// `None` leaves a setting to its default: librdkafka's for the two
/// thresholds, `FETCH_QUEUE_BACKOFF_MS` for the backoff. Set on
/// `ConsumerOptions::<Kafka>` and `KafkaConsumerGroupConfig`, and checked
/// against librdkafka's ranges where a consumer starts or a group registers,
/// by [`validate`](Self::validate), and again where the config is built, by
/// [`apply`](Self::apply).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct FetchQueueTuning {
    pub(crate) queued_min_messages: Option<u32>,
    pub(crate) queued_max_messages_kbytes: Option<u32>,
    pub(crate) fetch_queue_backoff_ms: Option<u32>,
}

impl FetchQueueTuning {
    /// Refuses a setting outside librdkafka's range with
    /// `ShoveError::Validation`, naming `queue`, the setter and the range.
    /// Unset settings pass.
    pub(crate) fn validate(&self, queue: &str) -> Result<()> {
        within(
            queue,
            "with_queued_min_messages",
            "queued.min.messages",
            QUEUED_MIN_MESSAGES_RANGE,
            self.queued_min_messages,
            "librdkafka's default",
        )?;
        within(
            queue,
            "with_queued_max_messages_kbytes",
            "queued.max.messages.kbytes",
            QUEUED_MAX_MESSAGES_KBYTES_RANGE,
            self.queued_max_messages_kbytes,
            "librdkafka's default",
        )?;
        within(
            queue,
            "with_fetch_queue_backoff_ms",
            "fetch.queue.backoff.ms",
            FETCH_QUEUE_BACKOFF_MS_RANGE,
            self.fetch_queue_backoff_ms,
            &format!("the default of {FETCH_QUEUE_BACKOFF_MS} ms"),
        )
    }

    /// Validates, then sets the settings on `config`: the backoff always,
    /// the thresholds only when set. A refused tuning sets nothing.
    pub(crate) fn apply(&self, config: &mut ClientConfig, queue: &str) -> Result<()> {
        self.validate(queue)?;
        config.set(
            "fetch.queue.backoff.ms",
            self.fetch_queue_backoff_ms
                .unwrap_or(FETCH_QUEUE_BACKOFF_MS)
                .to_string(),
        );
        if let Some(messages) = self.queued_min_messages {
            config.set("queued.min.messages", messages.to_string());
        }
        if let Some(kbytes) = self.queued_max_messages_kbytes {
            config.set("queued.max.messages.kbytes", kbytes.to_string());
        }
        Ok(())
    }
}

/// `Ok` for a value that is unset or inside `range`.
fn within(
    queue: &str,
    setter: &str,
    property: &str,
    range: RangeInclusive<u32>,
    value: Option<u32>,
    unset: &str,
) -> Result<()> {
    match value {
        Some(value) if !range.contains(&value) => Err(ShoveError::Validation(format!(
            "topic '{queue}': `{setter}` takes a value in {}..={}, the range librdkafka admits \
             for `{property}`, and this consumer has {value}. Set a value in that range, or \
             leave the setting unset for {unset}.",
            range.start(),
            range.end()
        ))),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use rdkafka::consumer::BaseConsumer;

    use super::*;
    use crate::error::ShoveError;

    fn applied(tuning: FetchQueueTuning) -> ClientConfig {
        let mut config = ClientConfig::new();
        tuning
            .apply(&mut config, "orders")
            .expect("an in-range tuning applies");
        config
    }

    #[test]
    fn unset_tuning_sets_the_default_backoff_and_leaves_the_thresholds_to_librdkafka() {
        let config = applied(FetchQueueTuning::default());

        assert_eq!(config.get("fetch.queue.backoff.ms"), Some("25"));
        assert_eq!(config.get("queued.min.messages"), None);
        assert_eq!(config.get("queued.max.messages.kbytes"), None);
    }

    #[test]
    fn every_setting_replaces_librdkafkas_value_as_given() {
        let config = applied(FetchQueueTuning {
            queued_min_messages: Some(5_000),
            queued_max_messages_kbytes: Some(2_048),
            fetch_queue_backoff_ms: Some(250),
        });

        assert_eq!(config.get("queued.min.messages"), Some("5000"));
        assert_eq!(config.get("queued.max.messages.kbytes"), Some("2048"));
        assert_eq!(config.get("fetch.queue.backoff.ms"), Some("250"));
    }

    #[test]
    fn a_threshold_alone_keeps_the_default_backoff() {
        let config = applied(FetchQueueTuning {
            queued_min_messages: Some(5_000),
            ..FetchQueueTuning::default()
        });

        assert_eq!(config.get("queued.min.messages"), Some("5000"));
        assert_eq!(config.get("queued.max.messages.kbytes"), None);
        assert_eq!(config.get("fetch.queue.backoff.ms"), Some("25"));
    }

    #[test]
    fn a_backoff_of_zero_is_admitted_and_not_read_as_unset() {
        let config = applied(FetchQueueTuning {
            fetch_queue_backoff_ms: Some(0),
            ..FetchQueueTuning::default()
        });

        assert_eq!(config.get("fetch.queue.backoff.ms"), Some("0"));
    }

    #[test]
    fn the_default_backoff_is_inside_librdkafkas_range() {
        assert!(FETCH_QUEUE_BACKOFF_MS_RANGE.contains(&FETCH_QUEUE_BACKOFF_MS));
    }

    #[test]
    fn every_value_outside_the_range_is_refused_naming_the_queue_the_setter_and_the_range() {
        let outside = [
            (
                FetchQueueTuning {
                    queued_min_messages: Some(0),
                    ..FetchQueueTuning::default()
                },
                "with_queued_min_messages",
                "queued.min.messages",
                "1..=10000000",
            ),
            (
                FetchQueueTuning {
                    queued_min_messages: Some(10_000_001),
                    ..FetchQueueTuning::default()
                },
                "with_queued_min_messages",
                "queued.min.messages",
                "1..=10000000",
            ),
            (
                FetchQueueTuning {
                    queued_max_messages_kbytes: Some(0),
                    ..FetchQueueTuning::default()
                },
                "with_queued_max_messages_kbytes",
                "queued.max.messages.kbytes",
                "1..=2097151",
            ),
            (
                FetchQueueTuning {
                    queued_max_messages_kbytes: Some(2_097_152),
                    ..FetchQueueTuning::default()
                },
                "with_queued_max_messages_kbytes",
                "queued.max.messages.kbytes",
                "1..=2097151",
            ),
            (
                FetchQueueTuning {
                    fetch_queue_backoff_ms: Some(300_001),
                    ..FetchQueueTuning::default()
                },
                "with_fetch_queue_backoff_ms",
                "fetch.queue.backoff.ms",
                "0..=300000",
            ),
        ];

        for (tuning, setter, property, range) in outside {
            let mut config = ClientConfig::new();
            for refused in [
                tuning.validate("orders"),
                tuning.apply(&mut config, "orders"),
            ] {
                let Err(ShoveError::Validation(msg)) = refused else {
                    panic!("{tuning:?} must be refused with ShoveError::Validation: {refused:?}");
                };
                assert!(
                    msg.contains("orders")
                        && msg.contains(setter)
                        && msg.contains(property)
                        && msg.contains(range),
                    "the message names the topic, the setter, the property and the range: {msg}"
                );
            }
            assert_eq!(
                config.get("fetch.queue.backoff.ms"),
                None,
                "a refused tuning sets nothing on the config"
            );
        }
    }

    #[test]
    fn the_edges_of_every_range_are_admitted() {
        for tuning in [
            FetchQueueTuning {
                queued_min_messages: Some(1),
                queued_max_messages_kbytes: Some(1),
                fetch_queue_backoff_ms: Some(0),
            },
            FetchQueueTuning {
                queued_min_messages: Some(10_000_000),
                queued_max_messages_kbytes: Some(2_097_151),
                fetch_queue_backoff_ms: Some(300_000),
            },
        ] {
            tuning
                .validate("orders")
                .expect("the edges of the ranges are admitted");
        }
    }

    /// The ranges are librdkafka's own: it takes each edge and refuses the
    /// value past it, at the property level, before any client is created.
    #[test]
    fn the_ranges_are_the_ones_librdkafka_enforces() {
        for (property, range) in [
            ("queued.min.messages", QUEUED_MIN_MESSAGES_RANGE),
            (
                "queued.max.messages.kbytes",
                QUEUED_MAX_MESSAGES_KBYTES_RANGE,
            ),
            ("fetch.queue.backoff.ms", FETCH_QUEUE_BACKOFF_MS_RANGE),
        ] {
            let create = |value: u32| {
                let mut config = ClientConfig::new();
                config.set(property, value.to_string());
                config.create_native_config()
            };
            for edge in [*range.start(), *range.end()] {
                create(edge).unwrap_or_else(|e| {
                    panic!("librdkafka refused {property}={edge}, an edge of our range: {e}")
                });
            }
            for past in [range.start().checked_sub(1), range.end().checked_add(1)]
                .into_iter()
                .flatten()
            {
                assert!(
                    create(past).is_err(),
                    "librdkafka took {property}={past}, past our range"
                );
            }
        }
    }

    /// Creating a consumer runs librdkafka's cross-property checks, which
    /// `queued.max.messages.kbytes` takes part in through `fetch.max.bytes`:
    /// both ends of every range survive them.
    #[test]
    fn a_consumer_is_created_at_both_ends_of_every_range() {
        for tuning in [
            FetchQueueTuning {
                queued_min_messages: Some(1),
                queued_max_messages_kbytes: Some(1),
                fetch_queue_backoff_ms: Some(0),
            },
            FetchQueueTuning {
                queued_min_messages: Some(10_000_000),
                queued_max_messages_kbytes: Some(2_097_151),
                fetch_queue_backoff_ms: Some(300_000),
            },
        ] {
            let mut config = applied(tuning);
            config.set("group.id", "fetch-queue-range-ends");
            config
                .create::<BaseConsumer>()
                .unwrap_or_else(|e| panic!("a consumer on {tuning:?} was refused: {e}"));
        }
    }
}
