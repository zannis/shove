use std::collections::HashMap;
use std::sync::Arc;

use lapin::message::Delivery;
use lapin::types::{AMQPValue, FieldArray, FieldTable, LongString};

use crate::metadata::{DeadMessageMetadata, MessageMetadata};

pub(crate) const RETRY_COUNT_KEY: &str = "x-retry-count";

/// Header key for the stable per-message deduplication ID.
///
/// Stamped by [`RabbitMqPublisher`] on every outgoing message and preserved
/// through all hold-queue hops. Handlers can use this value to detect the
/// duplicate delivery that arises when the publish-then-ack race fires (the
/// broker requeues the original while the hold-queue copy is also in flight).
///
/// See [`MessageMetadata::headers`] for access from a handler.
pub const MESSAGE_ID_KEY: &str = "x-message-id";

/// Header the broker fills in when it dead-letters a message; read back for
/// the dead-letter metadata.
const DEATH_KEY: &str = "x-death";

/// Header the previous version minted on a sequenced hold-queue copy. This
/// version neither writes nor reads it, and only hides it from handlers on a
/// copy parked before an upgrade.
pub(crate) const RETRY_TOKEN_KEY: &str = "shove-retry-token";

/// Header carrying the sequence key of a hold-queue copy the previous version
/// parked. Such a copy dead-letters back with the shard queue name as its
/// routing key, so this is the only place its key survives. Read only to
/// handle those copies; hidden from handlers.
pub(crate) const SEQUENCE_KEY_HEADER: &str = "shove-sequence-key";

/// Header names this backend writes or reads back as its own. A new one belongs
/// here: a test checks that `validate_headers` reserves every name in this list.
#[cfg(test)]
const INTERNAL_KEYS: &[&str] = &[
    RETRY_COUNT_KEY,
    MESSAGE_ID_KEY,
    DEATH_KEY,
    RETRY_TOKEN_KEY,
    SEQUENCE_KEY_HEADER,
];

fn long_string_to_owned(s: &LongString) -> String {
    // Fast path for valid UTF-8 (common case) avoids the extra allocation
    // that from_utf8_lossy().into_owned() always incurs.
    match std::str::from_utf8(s.as_bytes()) {
        Ok(valid) => valid.to_owned(),
        Err(_) => String::from_utf8_lossy(s.as_bytes()).into_owned(),
    }
}

pub(crate) fn get_retry_count(delivery: &Delivery) -> u32 {
    let headers: &FieldTable = match delivery.properties.headers().as_ref() {
        Some(h) => h,
        None => return 0,
    };

    match headers.inner().get(RETRY_COUNT_KEY) {
        Some(AMQPValue::LongUInt(v)) => *v,
        Some(AMQPValue::ShortUInt(v)) => u32::from(*v),
        Some(AMQPValue::ShortShortUInt(v)) => u32::from(*v),
        Some(AMQPValue::LongLongInt(v)) => u32::try_from(*v).unwrap_or(0),
        Some(AMQPValue::LongInt(v)) => u32::try_from(*v).unwrap_or(0),
        Some(AMQPValue::ShortInt(v)) => u32::try_from(*v).unwrap_or(0),
        Some(AMQPValue::ShortShortInt(v)) => u32::try_from(*v).unwrap_or(0),
        _ => 0,
    }
}

/// The sequence key of a delivery.
///
/// Only a copy that the previous version parked in a shard hold queue needs the
/// header: sequenced retries no longer publish one. Such a return is
/// dead-lettered through the default exchange (the
/// empty exchange name, see `hold_queue_args`) with the shard queue's own name
/// as routing key; only such a delivery takes its key from the key header.
/// The broker reports the exchange, so a producer cannot forge it through
/// headers. Any other delivery, including every one that arrives through the
/// topic's hash exchange, uses its routing key. A return whose header is
/// missing or not a string (parked by a version before the header existed)
/// also uses the routing key. A producer that publishes straight to the shard
/// queue through the default exchange can file a message under any key this
/// way; shard queues must not be writable by untrusted publishers.
pub(crate) fn delivery_sequence_key(delivery: &Delivery, shard_queue: &str) -> String {
    let is_return =
        delivery.exchange.as_str().is_empty() && delivery.routing_key.as_str() == shard_queue;
    let from_header = delivery
        .properties
        .headers()
        .as_ref()
        .filter(|_| is_return)
        .and_then(|t| match t.inner().get(SEQUENCE_KEY_HEADER) {
            Some(AMQPValue::LongString(s)) => Some(long_string_to_owned(s)),
            Some(AMQPValue::ShortString(s)) => Some(s.to_string()),
            _ => None,
        });
    from_header.unwrap_or_else(|| delivery.routing_key.to_string())
}

pub(crate) fn extract_message_metadata(delivery: &Delivery) -> MessageMetadata {
    let headers = extract_string_headers(delivery);
    MessageMetadata {
        retry_count: get_retry_count(delivery),
        delivery_id: delivery.delivery_tag.to_string(),
        redelivered: delivery.redelivered,
        // AMQP 0-9-1 carries a `redelivered` flag and nothing more — there is no
        // per-message delivery counter to report.
        delivery_count: None,
        // AMQP has no partitions and no log offset either.
        partition: None,
        offset: None,
        timestamp_ms: None,
        headers: Arc::new(headers),
    }
}

pub(crate) fn extract_string_headers(delivery: &Delivery) -> HashMap<String, String> {
    let Some(table) = delivery.properties.headers().as_ref() else {
        return HashMap::new();
    };

    let inner = table.inner();
    let mut headers = HashMap::with_capacity(inner.len());
    for (k, v) in inner.iter() {
        if k.as_str() == RETRY_TOKEN_KEY || k.as_str() == SEQUENCE_KEY_HEADER {
            continue;
        }
        let value = match v {
            AMQPValue::LongString(s) => long_string_to_owned(s),
            AMQPValue::ShortString(s) => s.to_string(),
            _ => continue,
        };
        headers.insert(k.to_string(), value);
    }
    headers
}

pub(crate) fn extract_dead_metadata(delivery: &Delivery) -> DeadMessageMetadata {
    let message = extract_message_metadata(delivery);

    let (reason, original_queue, death_count) = delivery
        .properties
        .headers()
        .as_ref()
        .and_then(|headers| headers.inner().get(DEATH_KEY))
        .and_then(|value| {
            if let AMQPValue::FieldArray(array) = value {
                extract_first_death_entry(array)
            } else {
                None
            }
        })
        .unwrap_or((None, None, 0));

    DeadMessageMetadata {
        message,
        reason,
        original_queue,
        death_count,
    }
}

fn extract_first_death_entry(array: &FieldArray) -> Option<(Option<String>, Option<String>, u32)> {
    let first = array.as_slice().first()?;

    let table = if let AMQPValue::FieldTable(t) = first {
        t
    } else {
        return None;
    };

    let inner = table.inner();

    let reason = inner.get("reason").and_then(|v| match v {
        AMQPValue::LongString(s) => Some(long_string_to_owned(s)),
        _ => None,
    });

    let original_queue = inner.get("queue").and_then(|v| match v {
        AMQPValue::LongString(s) => Some(long_string_to_owned(s)),
        _ => None,
    });

    let death_count = inner
        .get("count")
        .and_then(|v| match v {
            AMQPValue::LongLongInt(n) => u32::try_from(*n).ok(),
            AMQPValue::LongInt(n) => u32::try_from(*n).ok(),
            AMQPValue::ShortInt(n) => u32::try_from(*n).ok(),
            AMQPValue::ShortShortInt(n) => u32::try_from(*n).ok(),
            _ => None,
        })
        .unwrap_or(0);

    Some((reason, original_queue, death_count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use lapin::BasicProperties;
    use lapin::message::Delivery;
    use lapin::types::{AMQPValue, FieldArray, FieldTable, LongString, ShortString};

    use crate::publisher_internal::assert_internal_keys_reserved;

    #[test]
    fn every_internal_key_is_reserved_by_validate_headers() {
        assert_internal_keys_reserved("RabbitMQ", INTERNAL_KEYS, &[]);
    }

    fn make_delivery(headers: Option<FieldTable>, tag: u64, redelivered: bool) -> Delivery {
        let mut delivery = Delivery::mock(
            tag,
            ShortString::from(""),
            ShortString::from(""),
            redelivered,
            vec![],
        );
        if let Some(h) = headers {
            delivery.properties = BasicProperties::default().with_headers(h);
        }
        delivery
    }

    // ---- get_retry_count ----

    #[test]
    fn retry_count_no_headers() {
        let delivery = make_delivery(None, 1, false);
        assert_eq!(get_retry_count(&delivery), 0);
    }

    #[test]
    fn retry_count_headers_without_retry_key() {
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from("x-some-other-key"),
            AMQPValue::LongString(LongString::from("value")),
        );
        let delivery = make_delivery(Some(table), 1, false);
        assert_eq!(get_retry_count(&delivery), 0);
    }

    #[test]
    fn retry_count_long_long_int() {
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from(RETRY_COUNT_KEY),
            AMQPValue::LongLongInt(42),
        );
        let delivery = make_delivery(Some(table), 1, false);
        assert_eq!(get_retry_count(&delivery), 42);
    }

    #[test]
    fn retry_count_long_int() {
        let mut table = FieldTable::default();
        table.insert(ShortString::from(RETRY_COUNT_KEY), AMQPValue::LongInt(7));
        let delivery = make_delivery(Some(table), 1, false);
        assert_eq!(get_retry_count(&delivery), 7);
    }

    #[test]
    fn retry_count_short_int() {
        let mut table = FieldTable::default();
        table.insert(ShortString::from(RETRY_COUNT_KEY), AMQPValue::ShortInt(3));
        let delivery = make_delivery(Some(table), 1, false);
        assert_eq!(get_retry_count(&delivery), 3);
    }

    #[test]
    fn retry_count_short_short_int() {
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from(RETRY_COUNT_KEY),
            AMQPValue::ShortShortInt(5),
        );
        let delivery = make_delivery(Some(table), 1, false);
        assert_eq!(get_retry_count(&delivery), 5);
    }

    #[test]
    fn retry_count_negative_value_returns_zero() {
        let mut table = FieldTable::default();
        // Negative LongLongInt — u32::try_from fails, should return 0.
        table.insert(
            ShortString::from(RETRY_COUNT_KEY),
            AMQPValue::LongLongInt(-1),
        );
        let delivery = make_delivery(Some(table), 1, false);
        assert_eq!(get_retry_count(&delivery), 0);
    }

    /// Verifies that the type written by `clone_headers_with_retry` (LongUInt)
    /// is correctly read back by `get_retry_count`.
    #[test]
    fn retry_count_round_trips_through_written_type() {
        let retry_count: u32 = 5;
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from(RETRY_COUNT_KEY),
            AMQPValue::LongUInt(retry_count),
        );
        let delivery = make_delivery(Some(table), 1, false);
        assert_eq!(get_retry_count(&delivery), retry_count);
    }

    #[test]
    fn retry_count_non_integer_value_returns_zero() {
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from(RETRY_COUNT_KEY),
            AMQPValue::LongString(LongString::from("not-a-number")),
        );
        let delivery = make_delivery(Some(table), 1, false);
        assert_eq!(get_retry_count(&delivery), 0);
    }

    #[test]
    fn retry_count_short_uint() {
        let mut table = FieldTable::default();
        table.insert(ShortString::from(RETRY_COUNT_KEY), AMQPValue::ShortUInt(12));
        let delivery = make_delivery(Some(table), 1, false);
        assert_eq!(get_retry_count(&delivery), 12);
    }

    #[test]
    fn retry_count_short_short_uint() {
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from(RETRY_COUNT_KEY),
            AMQPValue::ShortShortUInt(7),
        );
        let delivery = make_delivery(Some(table), 1, false);
        assert_eq!(get_retry_count(&delivery), 7);
    }

    // ---- extract_string_headers ----

    fn delivery_with(exchange: &str, routing_key: &str, headers: Option<FieldTable>) -> Delivery {
        let mut delivery = make_delivery(headers, 1, false);
        delivery.exchange = ShortString::from(exchange);
        delivery.routing_key = ShortString::from(routing_key);
        delivery
    }

    fn key_table(value: AMQPValue, with_death: bool) -> FieldTable {
        let mut table = FieldTable::default();
        table.insert(ShortString::from(SEQUENCE_KEY_HEADER), value);
        if with_death {
            table.insert(
                ShortString::from(DEATH_KEY),
                AMQPValue::FieldArray(vec![AMQPValue::FieldTable(FieldTable::default())].into()),
            );
        }
        table
    }

    #[test]
    fn delivery_through_the_hash_exchange_ignores_a_forged_key_header() {
        // Even with the shard queue's name as routing key and a forged x-death.
        let forged = key_table(AMQPValue::LongString("J".into()), true);
        let delivery = delivery_with("t-seq-hash", "q-seq-0", Some(forged));
        assert_eq!(delivery_sequence_key(&delivery, "q-seq-0"), "q-seq-0");
        let forged = key_table(AMQPValue::LongString("J".into()), false);
        let delivery = delivery_with("t-seq-hash", "K", Some(forged));
        assert_eq!(delivery_sequence_key(&delivery, "q-seq-0"), "K");
    }

    #[test]
    fn default_exchange_delivery_for_another_queue_ignores_the_header() {
        let table = key_table(AMQPValue::LongString("J".into()), true);
        let delivery = delivery_with("", "K", Some(table));
        assert_eq!(delivery_sequence_key(&delivery, "q-seq-0"), "K");
    }

    #[test]
    fn returning_delivery_takes_its_key_from_the_header() {
        let table = key_table(AMQPValue::LongString("K".into()), true);
        let returning = delivery_with("", "q-seq-0", Some(table));
        assert_eq!(delivery_sequence_key(&returning, "q-seq-0"), "K");
    }

    #[test]
    fn returning_delivery_without_a_usable_header_uses_the_routing_key() {
        let missing = delivery_with("", "q-seq-0", None);
        assert_eq!(delivery_sequence_key(&missing, "q-seq-0"), "q-seq-0");
        let wrong = key_table(AMQPValue::LongUInt(7), true);
        let delivery = delivery_with("", "q-seq-0", Some(wrong));
        assert_eq!(delivery_sequence_key(&delivery, "q-seq-0"), "q-seq-0");
    }

    #[test]
    fn sequence_key_header_is_hidden_from_handlers() {
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from(SEQUENCE_KEY_HEADER),
            AMQPValue::LongString("K".into()),
        );
        let delivery = delivery_with("", "q", Some(table));
        assert!(!extract_string_headers(&delivery).contains_key(SEQUENCE_KEY_HEADER));
    }

    #[test]
    fn retry_token_is_hidden_from_handlers() {
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from(RETRY_TOKEN_KEY),
            AMQPValue::LongString("tok".into()),
        );
        table.insert(
            ShortString::from("x-trace-id"),
            AMQPValue::LongString("tid".into()),
        );
        let delivery = make_delivery(Some(table), 1, false);
        let headers = extract_string_headers(&delivery);
        assert!(!headers.contains_key(RETRY_TOKEN_KEY));
        assert!(headers.contains_key("x-trace-id"));
    }

    #[test]
    fn string_headers_no_headers() {
        let delivery = make_delivery(None, 1, false);
        let result = extract_string_headers(&delivery);
        assert!(result.is_empty());
    }

    #[test]
    fn string_headers_long_string() {
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from("x-trace-id"),
            AMQPValue::LongString(LongString::from("abc-123")),
        );
        let delivery = make_delivery(Some(table), 1, false);
        let result = extract_string_headers(&delivery);
        assert_eq!(result.get("x-trace-id"), Some(&"abc-123".to_string()));
    }

    #[test]
    fn string_headers_short_string() {
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from("x-source"),
            AMQPValue::ShortString(ShortString::from("svc-a")),
        );
        let delivery = make_delivery(Some(table), 1, false);
        let result = extract_string_headers(&delivery);
        assert_eq!(result.get("x-source"), Some(&"svc-a".to_string()));
    }

    #[test]
    fn string_headers_non_string_filtered_out() {
        let mut table = FieldTable::default();
        table.insert(ShortString::from("x-count"), AMQPValue::LongInt(99));
        let delivery = make_delivery(Some(table), 1, false);
        let result = extract_string_headers(&delivery);
        assert!(result.is_empty());
    }

    #[test]
    fn string_headers_mixed_only_strings_extracted() {
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from("x-trace-id"),
            AMQPValue::LongString(LongString::from("trace-xyz")),
        );
        table.insert(ShortString::from("x-count"), AMQPValue::LongInt(5));
        table.insert(
            ShortString::from("x-source"),
            AMQPValue::ShortString(ShortString::from("svc-b")),
        );
        let delivery = make_delivery(Some(table), 1, false);
        let result = extract_string_headers(&delivery);
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("x-trace-id"), Some(&"trace-xyz".to_string()));
        assert_eq!(result.get("x-source"), Some(&"svc-b".to_string()));
        assert!(!result.contains_key("x-count"));
    }

    // ---- extract_message_metadata ----

    #[test]
    fn message_metadata_basic_extraction() {
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from(RETRY_COUNT_KEY),
            AMQPValue::LongLongInt(3),
        );
        table.insert(
            ShortString::from("x-trace-id"),
            AMQPValue::LongString(LongString::from("tid-001")),
        );
        let delivery = make_delivery(Some(table), 42, true);
        let meta = extract_message_metadata(&delivery);

        assert_eq!(meta.retry_count, 3);
        assert_eq!(meta.delivery_id, "42");
        assert!(meta.redelivered);
        assert_eq!(
            meta.delivery_count, None,
            "AMQP 0-9-1 has no delivery counter — `redelivered` is all the \
             protocol carries, and reporting a fabricated count would be worse \
             than reporting `None`"
        );
        assert_eq!(meta.headers.get("x-trace-id"), Some(&"tid-001".to_string()));
    }

    #[test]
    fn message_metadata_no_headers() {
        let delivery = make_delivery(None, 7, false);
        let meta = extract_message_metadata(&delivery);

        assert_eq!(meta.retry_count, 0);
        assert_eq!(meta.delivery_id, "7");
        assert!(!meta.redelivered);
        assert!(meta.headers.is_empty());
    }

    // ---- extract_dead_metadata ----

    #[test]
    fn dead_metadata_no_x_death_header() {
        let delivery = make_delivery(None, 1, false);
        let meta = extract_dead_metadata(&delivery);

        assert_eq!(meta.reason, None);
        assert_eq!(meta.original_queue, None);
        assert_eq!(meta.death_count, 0);
    }

    #[test]
    fn dead_metadata_valid_x_death_entry() {
        let mut death_entry = FieldTable::default();
        death_entry.insert(
            ShortString::from("reason"),
            AMQPValue::LongString(LongString::from("rejected")),
        );
        death_entry.insert(
            ShortString::from("queue"),
            AMQPValue::LongString(LongString::from("my-queue")),
        );
        death_entry.insert(ShortString::from("count"), AMQPValue::LongLongInt(2));

        let mut array_vec = FieldArray::default();
        array_vec.push(AMQPValue::FieldTable(death_entry));

        let mut table = FieldTable::default();
        table.insert(
            ShortString::from("x-death"),
            AMQPValue::FieldArray(array_vec),
        );

        let delivery = make_delivery(Some(table), 1, false);
        let meta = extract_dead_metadata(&delivery);

        assert_eq!(meta.reason, Some("rejected".to_string()));
        assert_eq!(meta.original_queue, Some("my-queue".to_string()));
        assert_eq!(meta.death_count, 2);
    }

    #[test]
    fn dead_metadata_x_death_missing_optional_fields() {
        // Entry with no reason/queue/count — all should default gracefully.
        let death_entry = FieldTable::default();

        let mut array_vec = FieldArray::default();
        array_vec.push(AMQPValue::FieldTable(death_entry));

        let mut table = FieldTable::default();
        table.insert(
            ShortString::from("x-death"),
            AMQPValue::FieldArray(array_vec),
        );

        let delivery = make_delivery(Some(table), 1, false);
        let meta = extract_dead_metadata(&delivery);

        assert_eq!(meta.reason, None);
        assert_eq!(meta.original_queue, None);
        assert_eq!(meta.death_count, 0);
    }
}
