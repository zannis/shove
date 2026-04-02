use std::collections::HashMap;
use std::marker::PhantomData;

use lapin::message::Delivery;
use lapin::types::{AMQPValue, FieldArray, FieldTable};
use tracing::error;

use crate::handler::MessageHandler;
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::outcome::Outcome;
use crate::topic::Topic;

pub(crate) const RETRY_COUNT_KEY: &str = "x-retry-count";

pub(crate) trait RawHandler: Send + Sync + 'static {
    async fn handle(&self, delivery: &Delivery) -> Outcome;
}

pub(crate) struct TypedAdapter<H, T> {
    handler: H,
    _phantom: PhantomData<T>,
}

impl<H, T> TypedAdapter<H, T> {
    pub(crate) fn new(handler: H) -> Self {
        Self {
            handler,
            _phantom: PhantomData,
        }
    }
}

impl<H, T> RawHandler for TypedAdapter<H, T>
where
    H: MessageHandler<T> + Send + Sync + 'static,
    T: Topic + Send + Sync + 'static,
    T::Message: for<'de> serde::Deserialize<'de> + Send,
{
    async fn handle(&self, delivery: &Delivery) -> Outcome {
        let metadata = extract_message_metadata(delivery);

        match serde_json::from_slice::<T::Message>(&delivery.data) {
            Err(err) => {
                error!(
                    error = %err,
                    delivery_id = %metadata.delivery_id,
                    "Failed to deserialize message from main queue"
                );
                Outcome::Reject
            }
            Ok(message) => self.handler.handle(message, metadata).await,
        }
    }
}

pub(crate) struct DlqAdapter<H, T> {
    handler: H,
    _phantom: PhantomData<T>,
}

impl<H, T> DlqAdapter<H, T> {
    pub(crate) fn new(handler: H) -> Self {
        Self {
            handler,
            _phantom: PhantomData,
        }
    }
}

impl<H, T> RawHandler for DlqAdapter<H, T>
where
    H: MessageHandler<T> + Send + Sync + 'static,
    T: Topic + Send + Sync + 'static,
    T::Message: for<'de> serde::Deserialize<'de> + Send,
{
    async fn handle(&self, delivery: &Delivery) -> Outcome {
        let metadata = extract_dead_metadata(delivery);

        match serde_json::from_slice::<T::Message>(&delivery.data) {
            Err(err) => {
                error!(
                    error = %err,
                    delivery_id = %metadata.message.delivery_id,
                    "Failed to deserialize message from dead letter queue — discarding"
                );
                Outcome::Ack
            }
            Ok(message) => {
                self.handler.handle_dead(message, metadata).await;
                Outcome::Ack
            }
        }
    }
}

pub(crate) fn get_retry_count(delivery: &Delivery) -> u32 {
    let headers: &FieldTable = match delivery.properties.headers().as_ref() {
        Some(h) => h,
        None => return 0,
    };

    match headers.inner().get(RETRY_COUNT_KEY) {
        Some(AMQPValue::LongLongInt(v)) => u32::try_from(*v).unwrap_or(0),
        Some(AMQPValue::LongInt(v)) => u32::try_from(*v).unwrap_or(0),
        Some(AMQPValue::ShortInt(v)) => u32::try_from(*v).unwrap_or(0),
        Some(AMQPValue::ShortShortInt(v)) => u32::try_from(*v).unwrap_or(0),
        _ => 0,
    }
}

fn extract_message_metadata(delivery: &Delivery) -> MessageMetadata {
    let headers = extract_string_headers(delivery);
    MessageMetadata {
        retry_count: get_retry_count(delivery),
        delivery_id: delivery.delivery_tag.to_string(),
        redelivered: delivery.redelivered,
        headers,
    }
}

fn extract_string_headers(delivery: &Delivery) -> HashMap<String, String> {
    let Some(table) = delivery.properties.headers().as_ref() else {
        return HashMap::new();
    };

    table
        .inner()
        .iter()
        .filter_map(|(k, v)| {
            let key = k.to_string();
            let value = match v {
                AMQPValue::LongString(s) => {
                    Some(String::from_utf8_lossy(s.as_bytes()).into_owned())
                }
                AMQPValue::ShortString(s) => Some(s.to_string()),
                _ => None,
            };
            value.map(|v| (key, v))
        })
        .collect()
}

fn extract_dead_metadata(delivery: &Delivery) -> DeadMessageMetadata {
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

    let reason = inner.get("reason").and_then(|v| {
        if let AMQPValue::LongString(s) = v {
            Some(String::from_utf8_lossy(s.as_bytes()).into_owned())
        } else {
            None
        }
    });

    let original_queue = inner.get("queue").and_then(|v| {
        if let AMQPValue::LongString(s) = v {
            Some(String::from_utf8_lossy(s.as_bytes()).into_owned())
        } else {
            None
        }
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
