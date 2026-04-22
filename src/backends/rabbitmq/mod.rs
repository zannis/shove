pub mod autoscaler;
pub mod backend;
pub mod client;
pub mod consumer;
pub mod consumer_group;
pub(crate) mod headers;
pub mod management;
pub mod publisher;
pub mod registry;
pub(crate) mod router;
pub mod topology;

use lapin::Error as LapinError;

use crate::error::ShoveError;

/// Maps a lapin error to the appropriate [`ShoveError`] variant using lapin's
/// own `can_be_recovered()` classification. Recoverable errors (IO, channel
/// state, heartbeat) become `Connection`; permanent errors (auth, protocol
/// version, parsing) become `Topology`.
pub(super) fn map_lapin_error(context: &str, e: LapinError) -> ShoveError {
    if e.can_be_recovered() {
        ShoveError::Connection(format!("{context}: {e}"))
    } else {
        ShoveError::Topology(format!("{context}: {e}"))
    }
}
