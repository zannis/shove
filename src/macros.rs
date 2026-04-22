/// Define a topic with static topology.
///
/// Creates a unit struct and implements `Topic` with an internal `OnceLock`
/// so the topology is computed once and returned as `&'static QueueTopology`.
///
/// Accepts an optional visibility modifier (defaults to inherited):
/// ```ignore
/// define_topic!(pub OrderSettlement, SettlementEvent,
///     TopologyBuilder::new("order-settlement").dlq().build()
/// );
///
/// define_topic!(pub(crate) InternalTopic, InternalEvent,
///     TopologyBuilder::new("internal").build()
/// );
/// ```
#[macro_export]
macro_rules! define_topic {
    ($vis:vis $name:ident, $message:ty, $topology:expr) => {
        $vis struct $name;
        impl $crate::Topic for $name {
            type Message = $message;
            fn topology() -> &'static $crate::QueueTopology {
                static TOPOLOGY: std::sync::OnceLock<$crate::QueueTopology> =
                    std::sync::OnceLock::new();
                TOPOLOGY.get_or_init(|| $topology)
            }
        }
    };
}

/// Define a sequenced topic with static topology.
///
/// Creates a unit struct, implements both `Topic` (with `SEQUENCE_KEY_FN`)
/// and `SequencedTopic`.
///
/// `$key_fn` must be a non-capturing closure or bare function pointer.
/// Capturing closures will produce a compile error because they cannot be
/// coerced to `fn(&Message) -> String`.
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
    ($vis:vis $name:ident, $message:ty, $key_fn:expr, $topology:expr) => {
        $vis struct $name;
        impl $crate::Topic for $name {
            type Message = $message;
            fn topology() -> &'static $crate::QueueTopology {
                static TOPOLOGY: std::sync::OnceLock<$crate::QueueTopology> =
                    std::sync::OnceLock::new();
                TOPOLOGY.get_or_init(|| $topology)
            }
            const SEQUENCE_KEY_FN: Option<fn(&$message) -> String> = Some(Self::sequence_key);
        }
        impl $crate::SequencedTopic for $name {
            fn sequence_key(message: &$message) -> String {
                let f: fn(&$message) -> String = $key_fn;
                f(message)
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::topology::{SequenceFailure, TopologyBuilder};
    use crate::{SequencedTopic, Topic};

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
}
