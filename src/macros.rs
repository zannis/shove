/// Define a topic with static topology.
///
/// Creates a unit struct and implements `Topic` with an internal `OnceLock`
/// so the topology is computed once and returned as `&'static QueueTopology`.
///
/// Accepts an optional visibility modifier (defaults to inherited). The codec
/// defaults to `JsonCodec`; pass `codec = MyCodec` as the final argument to
/// override.
///
/// ```ignore
/// define_topic!(pub OrderSettlement, SettlementEvent,
///     TopologyBuilder::new("order-settlement").dlq().build()
/// );
///
/// define_topic!(pub(crate) InternalTopic, InternalEvent,
///     TopologyBuilder::new("internal").build()
/// );
///
/// define_topic!(RawTopic, Vec<u8>,
///     TopologyBuilder::new("raw").build(),
///     codec = shove::RawBytesCodec
/// );
/// ```
#[macro_export]
macro_rules! define_topic {
    // Default form: codec inferred as JsonCodec.
    ($vis:vis $name:ident, $message:ty, $topology:expr) => {
        $crate::define_topic!(
            $vis $name, $message, $topology, codec = $crate::JsonCodec
        );
    };
    // Explicit codec form.
    ($vis:vis $name:ident, $message:ty, $topology:expr, codec = $codec:ty) => {
        $vis struct $name;
        impl $crate::Topic for $name {
            type Message = $message;
            type Codec = $codec;
            fn topology() -> &'static $crate::QueueTopology {
                static TOPOLOGY: std::sync::OnceLock<$crate::QueueTopology> =
                    std::sync::OnceLock::new();
                TOPOLOGY.get_or_init(|| $topology)
            }
        }
        // Unsequenced topics are batch-consumable. `define_sequenced_topic!`
        // deliberately omits this impl — see `NotSequenced`.
        impl $crate::NotSequenced for $name {}
    };
}

/// Define a sequenced topic with static topology.
///
/// Creates a unit struct, implements both `Topic` (with `SEQUENCE_KEY_FN`)
/// and `SequencedTopic`.
///
/// `$key_fn` must be a non-capturing closure or bare function pointer.
/// Capturing closures produce a compile error; the diagnostic includes the
/// name `SEQUENCE_KEY_FN_MUST_NOT_CAPTURE_VARIABLES` to make the restriction
/// self-evident.
///
/// The codec defaults to `JsonCodec`; pass `codec = MyCodec` as the final
/// argument to override.
///
/// ```ignore
/// define_sequenced_topic!(pub AccountLedger, LedgerEntry, |msg| msg.account_id.clone(),
///     TopologyBuilder::new("account-ledger")
///         .sequenced(SequenceFailure::FailAll)
///         .hold_queue(Duration::from_secs(5))
///         .dlq()
///         .build()
/// );
/// ```
#[macro_export]
macro_rules! define_sequenced_topic {
    // Default form: codec inferred as JsonCodec.
    ($vis:vis $name:ident, $message:ty, $key_fn:expr, $topology:expr) => {
        $crate::define_sequenced_topic!(
            $vis $name, $message, $key_fn, $topology, codec = $crate::JsonCodec
        );
    };
    // Explicit codec form.
    ($vis:vis $name:ident, $message:ty, $key_fn:expr, $topology:expr, codec = $codec:ty) => {
        $vis struct $name;
        impl $crate::Topic for $name {
            type Message = $message;
            type Codec = $codec;
            fn topology() -> &'static $crate::QueueTopology {
                static TOPOLOGY: std::sync::OnceLock<$crate::QueueTopology> =
                    std::sync::OnceLock::new();
                TOPOLOGY.get_or_init(|| $topology)
            }
            const SEQUENCE_KEY_FN: Option<fn(&$message) -> String> = Some(Self::sequence_key);
        }
        impl $crate::SequencedTopic for $name {
            fn sequence_key(message: &$message) -> String {
                // Using a named const makes the constraint self-evident in the
                // compiler error: if `$key_fn` captures variables, rustc will
                // print `SEQUENCE_KEY_FN_MUST_NOT_CAPTURE_VARIABLES` in the
                // "expected fn pointer" diagnostic, pointing directly at the rule.
                const SEQUENCE_KEY_FN_MUST_NOT_CAPTURE_VARIABLES:
                    fn(&$message) -> String = $key_fn;
                SEQUENCE_KEY_FN_MUST_NOT_CAPTURE_VARIABLES(message)
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::topology::{SequenceFailure, TopologyBuilder};
    use crate::{Codec, RawBytesCodec, SequencedTopic, Topic};

    // -- message types --

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
    struct OrderEvent {
        order_id: String,
        amount: u64,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
    struct LedgerEntry {
        account_id: String,
        delta: i64,
    }

    // -- define_topic! tests --

    define_topic!(
        MacroBasicTopic,
        OrderEvent,
        TopologyBuilder::new("macro-basic").build()
    );

    #[test]
    fn define_topic_queue_name() {
        assert_eq!(MacroBasicTopic::topology().queue(), "macro-basic");
    }

    #[test]
    fn define_topic_sequence_key_fn_is_none() {
        assert!(MacroBasicTopic::SEQUENCE_KEY_FN.is_none());
    }

    #[test]
    fn define_topic_no_dlq_by_default() {
        assert!(MacroBasicTopic::topology().dlq().is_none());
    }

    #[test]
    fn define_topic_message_roundtrips_serialization() {
        let event = OrderEvent {
            order_id: "ord-1".into(),
            amount: 100,
        };
        let json = serde_json::to_string(&event).expect("serialize");
        let decoded: OrderEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, event);
    }

    #[test]
    fn define_topic_default_codec_is_json() {
        assert_eq!(
            <<MacroBasicTopic as Topic>::Codec as Codec<<MacroBasicTopic as Topic>::Message>>::NAME,
            "json"
        );
    }

    define_topic!(
        MacroRawTopic,
        Vec<u8>,
        TopologyBuilder::new("macro-raw").build(),
        codec = RawBytesCodec
    );

    #[test]
    fn define_topic_explicit_codec_overrides_default() {
        assert_eq!(
            <<MacroRawTopic as Topic>::Codec as Codec<<MacroRawTopic as Topic>::Message>>::NAME,
            "raw"
        );
    }

    // -- define_topic! with DLQ and hold queues --

    define_topic!(
        MacroDlqTopic,
        OrderEvent,
        TopologyBuilder::new("macro-dlq")
            .dlq()
            .hold_queue(Duration::from_secs(30))
            .build()
    );

    #[test]
    fn define_topic_with_dlq() {
        assert_eq!(MacroDlqTopic::topology().dlq(), Some("macro-dlq-dlq"));
    }

    #[test]
    fn define_topic_with_hold_queue() {
        let hqs = MacroDlqTopic::topology().hold_queues();
        assert_eq!(hqs.len(), 1);
        assert_eq!(hqs[0].name(), "macro-dlq-hold-30s");
    }

    // -- define_sequenced_topic! tests --

    define_sequenced_topic!(
        MacroSeqTopic,
        LedgerEntry,
        |msg| msg.account_id.clone(),
        TopologyBuilder::new("macro-seq")
            .sequenced(SequenceFailure::FailAll)
            .hold_queue(Duration::from_secs(5))
            .dlq()
            .build()
    );

    #[test]
    fn define_sequenced_topic_sequence_key_fn_is_some() {
        assert!(MacroSeqTopic::SEQUENCE_KEY_FN.is_some());
    }

    #[test]
    fn define_sequenced_topic_key_fn_returns_expected_value() {
        let key_fn = MacroSeqTopic::SEQUENCE_KEY_FN.unwrap();
        let entry = LedgerEntry {
            account_id: "acc-42".into(),
            delta: -10,
        };
        assert_eq!(key_fn(&entry), "acc-42");
    }

    #[test]
    fn define_sequenced_topic_sequence_key_method() {
        let entry = LedgerEntry {
            account_id: "acc-99".into(),
            delta: 5,
        };
        assert_eq!(MacroSeqTopic::sequence_key(&entry), "acc-99");
    }

    #[test]
    fn define_sequenced_topic_has_sequencing_config() {
        let seq = MacroSeqTopic::topology()
            .sequencing()
            .expect("sequencing config should be present");
        assert_eq!(seq.on_failure(), SequenceFailure::FailAll);
        assert_eq!(seq.exchange(), "macro-seq-seq-hash");
    }

    #[test]
    fn define_sequenced_topic_has_dlq() {
        assert_eq!(MacroSeqTopic::topology().dlq(), Some("macro-seq-dlq"));
    }

    #[test]
    fn define_sequenced_topic_default_codec_is_json() {
        assert_eq!(
            <<MacroSeqTopic as Topic>::Codec as Codec<<MacroSeqTopic as Topic>::Message>>::NAME,
            "json"
        );
    }
}
