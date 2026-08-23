//! PEL leases for consumers that resolve their own handler timeouts.
//!
//! A Redis Streams consumer owns an entry by having it in its
//! per-consumer PEL. Ownership is not exclusive against a reaper: any
//! XAUTOCLAIM whose `min-idle-time` the entry has exceeded takes it away,
//! re-adds it to the stream, and XACKs the original. That is exactly the
//! crash recovery [`super::reaper`] exists to provide.
//!
//! ## Why a lease is needed
//!
//! Without `ConsumerOptions::with_handler_timeout_outcome` the two actors
//! never collide: at the deadline the consumer deliberately does *nothing*
//! and the reaper's reclaim is the redelivery mechanism.
//!
//! With the option set the consumer becomes an actor at that same instant —
//! it acks, holds, requeues, or dead-letters the entry. [`super::maintenance`]
//! keeps this process's own reaper out of the way by backing its threshold
//! off to twice the handler timeout, but that reconciliation is per process.
//! A second process consuming the same stream and group with the same 30 s
//! handler timeout and *no* outcome override still sweeps at 30 s, so it can
//! reclaim and re-add an entry at the very deadline its owner is resolving.
//! For a timeout resolved to [`Outcome::Ack`] that means the message the
//! owner asked to drop stays alive; for `Defer`/`Retry`/`Reject` it means the
//! owner's copy *and* the reaper's re-add both survive.
//!
//! [`Outcome::Ack`]: crate::Outcome::Ack
//!
//! ## What a lease does
//!
//! While such a handler runs, [`run_under_lease`] re-asserts ownership of the
//! entry every [`renew_interval`] — half the handler timeout. Each renewal is
//! a [`touch`], which resets the entry's idle clock to zero. A foreign reaper
//! needs the entry idle for at least its own threshold, so as long as that
//! threshold is not shorter than half our handler timeout it can never reach
//! it while we are working. At the deadline the idle clock reads at most half
//! the timeout, leaving the owner the other half to route its outcome.
//!
//! This does not remove the requirement to configure handler timeouts
//! consistently across every consumer of a stream and group — a process whose
//! timeout is under half of ours reclaims inside the renewal gap. It does
//! close the case the requirement could not: same timeout everywhere,
//! differing only in whether the outcome override is set.
//!
//! ## What a lease cannot do
//!
//! Losing the lease is still possible — the owner may be descheduled past a
//! renewal, or the renewal may fail against an unhealthy connection. [`touch`]
//! therefore doubles as the guard the owner runs immediately before routing a
//! timeout outcome: it reports whether we still hold the entry, atomically
//! with the renewal, so an owner that lost the race declines to route rather
//! than adding a second copy alongside the reaper's re-add.

use std::future::Future;
use std::time::Duration;

use super::client::RedisConnection;
use crate::error::{Result, ShoveError};

/// Floor on the renewal period.
///
/// `tokio::time::interval` panics on a zero period, so a floor is required
/// for sub-millisecond handler timeouts. Choosing it at 100 ms also stops
/// very short timeouts from turning into a renewal storm: a handler whose
/// whole deadline is under 200 ms finishes or times out well inside any
/// plausible reaper threshold, so it does not need renewing at all.
const MIN_RENEW_INTERVAL: Duration = Duration::from_millis(100);

/// The PEL entry a consumer is holding while its handler runs.
pub(super) struct Lease<'a> {
    pub(super) stream: &'a str,
    pub(super) group: &'a str,
    /// The XREADGROUP consumer name that owns the entry — the identity the
    /// PEL records, and the one [`touch`] checks against.
    pub(super) consumer: &'a str,
    pub(super) entry_id: &'a str,
}

/// Check that `consumer` still owns `entry_id` and, if so, reset its idle
/// clock — atomically, so a reaper cannot interleave between the two.
///
/// Returns `1` when the lease was held and renewed, `0` when it was not.
/// `XCLAIM ... JUSTID` resets idle time without incrementing the delivery
/// counter, so renewals do not inflate a message's apparent retry history.
///
/// One key, so this is routable on Redis Cluster.
const TOUCH_LEASE: &str = r"
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1, ARGV[3])
if #pending == 0 then
  return 0
end
redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[3], 0, ARGV[2], 'JUSTID')
return 1
";

/// How often to renew a lease for a handler with the given timeout.
///
/// Half the timeout, floored at [`MIN_RENEW_INTERVAL`]. Half means the idle
/// clock is never above `timeout / 2` while the handler runs, which keeps a
/// foreign reaper sweeping at the same `timeout` from ever reaching its
/// threshold, and leaves the owner `timeout / 2` of margin to route its
/// outcome once the deadline fires.
pub(super) fn renew_interval(handler_timeout: Duration) -> Duration {
    (handler_timeout / 2).max(MIN_RENEW_INTERVAL)
}

/// Re-assert ownership of the lease's entry and reset its idle clock.
///
/// `Ok(true)` means the lease is held; `Ok(false)` means a reaper claimed the
/// entry and now owns its redelivery.
pub(super) async fn touch(conn: &mut RedisConnection, lease: &Lease<'_>) -> Result<bool> {
    conn.query::<i64>(
        redis::cmd("EVAL")
            .arg(TOUCH_LEASE)
            .arg(1)
            .arg(lease.stream)
            .arg(lease.group)
            .arg(lease.entry_id)
            .arg(lease.consumer),
    )
    .await
    .map(|held| held == 1)
    .map_err(|e| ShoveError::Connection(format!("lease renewal failed: {e}")))
}

/// Await `fut` under `handler_timeout`, renewing `lease` for as long as it
/// runs.
///
/// With `lease` set to `None` this is exactly `tokio::time::timeout` — the
/// path taken by consumers that leave a timed-out entry in the PEL for the
/// reaper, where renewing would defeat the intended reclaim.
///
/// A failed renewal is logged and the handler is left to run: the lease may
/// still be held (the failure can be transient), and [`touch`] is run again
/// before the outcome is routed. Losing it only downgrades this consumer to
/// the reaper-reclaim behaviour it would have had without the override.
pub(super) async fn run_under_lease<F: Future>(
    conn: &mut RedisConnection,
    lease: Option<&Lease<'_>>,
    handler_timeout: Duration,
    fut: F,
) -> std::result::Result<F::Output, tokio::time::error::Elapsed> {
    let mut fut = std::pin::pin!(tokio::time::timeout(handler_timeout, fut));

    let Some(lease) = lease else {
        return fut.await;
    };

    let mut ticker = tokio::time::interval(renew_interval(handler_timeout));
    // `interval`'s first tick resolves immediately; the entry was just read,
    // so its idle clock is already at zero.
    ticker.tick().await;

    loop {
        tokio::select! {
            // A handler that finished wins over a renewal that came due in
            // the same poll — routing its outcome is what we were waiting for.
            biased;
            result = &mut fut => return result,
            _ = ticker.tick() => {
                match touch(conn, lease).await {
                    Ok(true) => {}
                    Ok(false) => tracing::warn!(
                        stream = lease.stream,
                        entry_id = lease.entry_id,
                        "lease lost while the handler was running — a reaper \
                         reclaimed the entry and now owns its redelivery",
                    ),
                    Err(e) => tracing::warn!(
                        stream = lease.stream,
                        entry_id = lease.entry_id,
                        error = %e,
                        "lease renewal failed — the entry may be reclaimed",
                    ),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renew_interval_is_half_the_timeout() {
        // Half keeps the idle clock under any reaper sweeping at the same
        // handler timeout, and leaves the other half for outcome routing.
        assert_eq!(
            renew_interval(Duration::from_secs(30)),
            Duration::from_secs(15)
        );
        assert_eq!(
            renew_interval(Duration::from_secs(45)),
            Duration::from_millis(22_500)
        );
    }

    #[test]
    fn renew_interval_is_floored() {
        // `tokio::time::interval` panics on a zero period, so the floor is
        // load-bearing for degenerate timeouts, not just a tuning choice.
        assert_eq!(renew_interval(Duration::ZERO), MIN_RENEW_INTERVAL);
        assert_eq!(renew_interval(Duration::from_millis(1)), MIN_RENEW_INTERVAL);
        assert_eq!(
            renew_interval(Duration::from_millis(199)),
            MIN_RENEW_INTERVAL
        );
        // Above 2x the floor the proportional rule takes over again.
        assert_eq!(
            renew_interval(Duration::from_millis(400)),
            Duration::from_millis(200)
        );
    }

    #[test]
    fn touch_script_reads_and_claims_one_key() {
        // Redis Cluster routes EVAL by declared keys; a second key here would
        // make every renewal CROSSSLOT-fail on a clustered deployment.
        assert_eq!(TOUCH_LEASE.matches("KEYS[").count(), 2);
        assert!(!TOUCH_LEASE.contains("KEYS[2]"));
        // Ownership must be checked before the reclaim, or renewal would
        // steal the entry back from a reaper that already re-added it.
        let check = TOUCH_LEASE.find("XPENDING").expect("ownership check");
        let claim = TOUCH_LEASE.find("XCLAIM").expect("idle reset");
        assert!(check < claim);
        // JUSTID keeps renewals out of the delivery counter.
        assert!(TOUCH_LEASE.contains("JUSTID"));
    }
}
