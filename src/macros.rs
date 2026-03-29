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
