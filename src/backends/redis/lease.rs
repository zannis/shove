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
//! needs the entry idle for at least its own threshold, so it only reaches
//! that threshold if a renewal gap exceeds it.
//!
//! The condition, stated exactly: a foreign reaper is kept off the entry while
//! we work iff its idle threshold is **strictly greater** than our largest
//! successful-renewal gap. That gap is [`renew_interval`] *plus* whatever
//! scheduling and Redis latency the renewal actually incurs — it is not a
//! clean `T / 2`.
//!
//! Two consequences worth naming, because both are observable rather than
//! theoretical:
//!
//! - Handler timeouts must be configured **consistently** across every
//!   consumer of a stream and group. This was already required (see
//!   `ConsumerOptions::with_handler_timeout`); the lease does not relax it.
//!   With a consistent `T` the foreign threshold is `T` and the renewal gap is
//!   `T / 2`, so the margin is real. With `T = 60s` here and `T = 30s` there,
//!   it is not: the 30 s sweep and our 30 s renewal come due together and
//!   ordinary timer jitter decides the order.
//! - The [`MIN_RENEW_INTERVAL`] floor means the gap is `max(T / 2, 100ms)`, so
//!   below `T = 200ms` the floor — not the proportional rule — sets it, and
//!   the margin narrows from `T / 2` to `T - 100ms`. At `T <= 100ms` it is
//!   gone entirely: the first renewal is not due until the deadline has
//!   already passed, so such consumers get no lease protection at all.
//!
//! ## What a lease cannot do
//!
//! **It does not make the owner the only actor, and this backend does not
//! claim it does.** Losing the lease is always possible — the owner may be
//! descheduled past a renewal, or the renewal may fail against an unhealthy
//! connection. [`touch`] therefore doubles as a guard the owner runs
//! immediately before routing an outcome (see
//! `super::consumer::resolve_under_lease`): it reports whether we still hold
//! the entry, atomically with the renewal, so an owner that has already lost
//! the race declines to route rather than adding a second copy alongside the
//! reaper's re-add.
//!
//! That guard is check-then-act, and the two halves are *not* serialized
//! against each other. `touch` returning true is a statement about the past;
//! applying an outcome is a separate round trip — often two, an `XADD` to a
//! DLQ or hold queue followed by an `XACK`. An owner descheduled between the
//! check and the write can still be overtaken by a reaper, leaving a
//! duplicate.
//!
//! That gap is a **choice this backend makes, not a Redis limitation**. Two
//! things could close it and neither is ruled out:
//!
//! - `Ack` and a no-DLQ `Reject` touch only the source stream, and an
//!   immediate `Retry`/`Defer` with no hold queue does `XADD` + `XACK` on that
//!   same stream. All three are single-key, so the check and the action fit in
//!   one script on any deployment, cluster or not.
//! - DLQ and hold-queue routing does span keys, but Redis Cluster runs
//!   multi-key scripts fine when the keys share a hash slot, which is what
//!   hash tags are for. shove's derived `"{queue}-dlq"` and hold-queue names
//!   append to the queue name, so a queue already carrying a tag keeps it and
//!   the derived keys land in the same slot.
//!
//! What is genuinely not available is a formulation that works for *arbitrary*
//! user-supplied destination names on a clustered deployment, where the source
//! and destination can fall in different slots. Rather than serialize some
//! outcomes and not others, this backend currently takes the at-least-once
//! fallback uniformly for all of them. Folding the single-key cases into
//! `TOUCH_LEASE` is open work, not a dead end.
//!
//! So the honest guarantee is: the lease **narrows** the window in which a
//! foreign reaper and a resolving owner both act on one entry, from "every
//! handler that reaches its deadline" to "an owner that loses its lease and
//! does not notice in time". It does not close it. Redis Streams delivery is
//! at-least-once, and a reclaim-induced duplicate stays within that contract —
//! which is why the guard errs toward the duplicate whenever ownership cannot
//! be established, rather than toward dropping or stranding the entry.

use std::future::Future;
use std::time::Duration;

use super::client::RedisConnection;
use crate::error::{Result, ShoveError};
use crate::outcome::Outcome;

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
/// clock stays around `timeout / 2` while the handler runs, so a foreign
/// reaper sweeping at the same `timeout` has roughly that much margin before
/// it can reach its threshold, and the owner has roughly that much to route
/// its outcome once the deadline fires.
///
/// "Roughly" is load-bearing: the actual gap is this interval plus the latency
/// of the renewal itself, and below `2 * MIN_RENEW_INTERVAL` the floor takes
/// over from the proportional rule entirely. See the module docs for what that
/// does and does not buy.
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

/// Await a handler future, resolving a panic to [`Outcome::Retry`].
///
/// The Redis loops drive the handler inline rather than in a dedicated task,
/// so there is no `JoinError` to inspect; `catch_unwind` provides the same
/// isolation without the per-message task allocation. Used by both the timed
/// path ([`run_under_lease`]) and the untimed one.
pub(super) async fn catch_handler_panic<F>(fut: F) -> Outcome
where
    F: Future<Output = Outcome>,
{
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;

    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(outcome) => outcome,
        Err(_panic) => {
            tracing::warn!("handler panicked, retrying message");
            Outcome::Retry
        }
    }
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
///
/// A panicking handler resolves to [`Outcome::Retry`], matching every other
/// backend. Without the unwind boundary the panic would tear down the
/// consumer loop (or the per-message task) with the entry still in the PEL:
/// it would eventually redeliver via the reaper, but without burning retry
/// budget, and with a timeout outcome configured `maintenance` also backs
/// reclaim off. Resolving to `Retry` here keeps a panic on the same accounted
/// path as any other failure.
pub(super) async fn run_under_lease<F>(
    conn: &mut RedisConnection,
    lease: Option<&Lease<'_>>,
    handler_timeout: Duration,
    fut: F,
) -> std::result::Result<Outcome, tokio::time::error::Elapsed>
where
    F: Future<Output = Outcome>,
{
    let mut fut = std::pin::pin!(tokio::time::timeout(
        handler_timeout,
        catch_handler_panic(fut)
    ));

    let Some(lease) = lease else {
        return fut.await;
    };

    let mut ticker = tokio::time::interval(renew_interval(handler_timeout));
    // What matters is the gap since the last *successful* renewal, not
    // keeping to an absolute schedule. Under the default `Burst` behaviour a
    // renewal that took longer than the interval would be followed by a run
    // of immediate catch-up ticks, which renew an already-fresh idle clock.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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
        // Half keeps the idle clock under a reaper sweeping at the same
        // handler timeout, and leaves the other half for outcome routing.
        // Only against the *same* timeout: see `renew_interval_gives_no_margin_
        // against_a_shorter_foreign_timeout` for what a mismatch costs.
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

    /// The floor is not free: below `2 * MIN_RENEW_INTERVAL` it, not the
    /// proportional rule, sets the renewal gap, so the margin against a
    /// same-timeout sweep shrinks from `T / 2` to `T - 100ms` and reaches zero
    /// at the floor itself. Pinned so the module docs' claim about sub-100 ms
    /// timeouts cannot quietly stop being true.
    #[test]
    fn the_floor_erases_the_margin_for_sub_100ms_timeouts() {
        // At or under the floor the first renewal is not due until the
        // deadline has already passed — there is no lease at all.
        for t in [Duration::from_millis(40), MIN_RENEW_INTERVAL] {
            assert!(
                renew_interval(t) >= t,
                "renew_interval({t:?}) is not sooner than the deadline itself",
            );
        }
        // Between the floor and twice it, a margin exists but is the floor's
        // remainder rather than half the timeout.
        let t = Duration::from_millis(150);
        assert_eq!(renew_interval(t), MIN_RENEW_INTERVAL);
        assert_eq!(t - renew_interval(t), Duration::from_millis(50));
        // At exactly 2x the floor the two rules agree and the margin is back
        // to a clean half.
        let boundary = MIN_RENEW_INTERVAL * 2;
        assert_eq!(renew_interval(boundary), boundary / 2);
    }

    /// A foreign reaper is only kept off the entry when its threshold exceeds
    /// our renewal gap. Consistent handler timeouts are what make that true;
    /// this pins the counterexample the docs cite, so "configure them
    /// consistently" stays a stated requirement rather than a hope.
    #[test]
    fn renew_interval_gives_no_margin_against_a_shorter_foreign_timeout() {
        let ours = Duration::from_secs(60);
        let theirs = Duration::from_secs(30);
        assert_eq!(
            renew_interval(ours),
            theirs,
            "our renewal and their sweep come due together, so jitter decides \
             which of us acts on the entry first",
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
