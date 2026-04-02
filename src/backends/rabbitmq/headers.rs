use std::collections::HashMap;

use lapin::message::Delivery;
use lapin::types::{AMQPValue, FieldArray, FieldTable, LongString};

use crate::metadata::{DeadMessageMetadata, MessageMetadata};

pub(crate) const RETRY_COUNT_KEY: &str = "x-retry-count";

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

pub(crate) fn extract_message_metadata(delivery: &Delivery) -> MessageMetadata {
    let headers = extract_string_headers(delivery);
    MessageMetadata {
        retry_count: get_retry_count(delivery),
        delivery_id: delivery.delivery_tag.to_string(),
        redelivered: delivery.redelivered,
        headers,
    }
}

pub(crate) fn extract_string_headers(delivery: &Delivery) -> HashMap<String, String> {
    let Some(table) = delivery.properties.headers().as_ref() else {
        return HashMap::new();
    };

    let inner = table.inner();
    let mut headers = HashMap::with_capacity(inner.len());
    for (k, v) in inner.iter() {
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
        .and_then(|headers| headers.inner().get("x-death"))
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

    // ---- extract_string_headers ----

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
