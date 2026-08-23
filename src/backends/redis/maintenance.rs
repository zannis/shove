//! Process-wide registry of per-`(client, stream, group)` maintenance tasks.
//!
//! Every Redis consumer loop — registry-managed or direct — calls
//! [`acquire`] for the stream it consumes. The first acquisition for a
//! `(client, stream, group)` key spawns one reaper sidecar (XAUTOCLAIM crash
//! recovery + acked-entry trimming, see [`super::reaper`]); subsequent
//! acquisitions for the same key just join it. When the last
//! [`MaintenanceGuard`] drops, the sidecar is cancelled and the entry
//! removed. This keeps maintenance at exactly one task per key no matter how
//! many consumers a group scales to — the N-redundant-sweepers pathology the
//! reaper consolidation removed must not come back through this door.
//!
//! ## Mixed handler-timeout policies
//!
//! XAUTOCLAIM acts on the whole group's PEL, so the one sidecar's reclaim
//! policy applies to every consumer on the key. The registry therefore runs
//! the *conservative* effective policy across all live guards: if any guard
//! belongs to a no-timeout consumer, reclaim is disabled (trim-only);
//! otherwise the longest timeout wins. Either way an entry is never
//! reclaimed earlier than its owner's own deadline. When a guard joining or
//! leaving changes the effective policy, the sidecar is cancelled and
//! respawned with the new one.
//!
//! A consumer that resolves its own timeouts
//! (`ConsumerOptions::with_handler_timeout_outcome`) contributes a policy of
//! twice its handler timeout, so the reaper never races the owner at the
//! deadline — see [`reclaim_policy`].
//!
//! This reconciliation is per process. A separate process (or an
//! independently connected client) consuming the same stream and group runs
//! its own sidecar on its own policy, so handler timeouts should be
//! configured consistently across every consumer of a stream and group —
//! see `ConsumerOptions::with_handler_timeout`.
//!
//! Consistent timeouts alone would not save a consumer that resolves its own:
//! a second process with the same timeout and no override still sweeps at the
//! deadline its owner resolves on, and no policy visible here can stop it.
//! Those consumers therefore defend the entry directly, by holding a renewed
//! PEL lease on it — see [`super::lease`].
//!
//! The two mechanisms have different reach, and it matters not to conflate
//! them. The `2x` backoff below is a **single-process** guarantee: within one
//! process the registry sees every consumer on the key, so it can place the
//! sidecar's threshold beyond every owner's deadline. The lease only
//! *narrows* the same race across processes — narrowing is the whole claim.
//! It does not make the owner the sole actor, and the ownership check it
//! offers is not serialized against the writes that apply an outcome. See
//! [`super::lease`] for the exact bound and its failure modes; the short
//! version is that Redis Streams stays at-least-once here.
//!
//! The key includes the client identity so two clients pointed at different
//! Redis servers never share a maintenance task; two distinct clients on the
//! same server merely run duplicate sweeps, which XAUTOCLAIM and the
//! min-across-groups trim are both safe under.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::client::RedisClient;
use super::reaper::spawn_maintenance;
use crate::consumer::DEFAULT_HANDLER_TIMEOUT;

type Key = (usize, String, String);

/// One consumer's reclaim policy: its handler timeout, or `None` for
/// consumers that may hold in-flight work indefinitely.
type Policy = Option<Duration>;

/// Spawner stored per entry so a policy change can respawn the sidecar.
type Spawner = Arc<dyn Fn(CancellationToken, Policy) + Send + Sync>;

struct Entry {
    /// One element per live guard. The effective policy is derived from the
    /// whole set; `len()` doubles as the refcount.
    policies: Vec<Policy>,
    shutdown: CancellationToken,
    spawner: Spawner,
}

impl Entry {
    /// Conservative reclaim policy across all live guards: any no-timeout
    /// guard disables reclaim; otherwise the longest timeout wins.
    fn effective(&self) -> Policy {
        if self.policies.iter().any(Option::is_none) {
            None
        } else {
            self.policies.iter().flatten().max().copied()
        }
    }

    /// Cancel the running sidecar and start one with the current effective
    /// policy.
    fn respawn(&mut self) {
        self.shutdown.cancel();
        self.shutdown = CancellationToken::new();
        (self.spawner)(self.shutdown.clone(), self.effective());
    }
}

static REGISTRY: OnceLock<Mutex<HashMap<Key, Entry>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<Key, Entry>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock() -> std::sync::MutexGuard<'static, HashMap<Key, Entry>> {
    // The critical sections below never panic, but recover from poisoning
    // anyway — abandoning maintenance for the whole process over an
    // unrelated panic would silently stop crash recovery and trimming.
    registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Handle for one consumer's interest in a stream's maintenance. Dropping
/// the last guard for a key cancels the underlying sidecar; dropping a guard
/// whose policy was pinning the effective one respawns the sidecar with the
/// recomputed policy.
pub(super) struct MaintenanceGuard {
    key: Key,
    policy: Policy,
}

impl Drop for MaintenanceGuard {
    fn drop(&mut self) {
        let mut map = lock();
        if let Some(entry) = map.get_mut(&self.key) {
            let before = entry.effective();
            if let Some(pos) = entry.policies.iter().position(|p| *p == self.policy) {
                entry.policies.swap_remove(pos);
            }
            if entry.policies.is_empty() {
                entry.shutdown.cancel();
                map.remove(&self.key);
            } else if entry.effective() != before {
                entry.respawn();
            }
        }
    }
}

/// Derive the sidecar's sweep interval and XAUTOCLAIM idle threshold from a
/// consumer's handler timeout.
///
/// With a timeout, the idle threshold is the timeout itself (an entry must
/// have been pending at least one handler-timeout before reclaim) and the
/// sweep interval is that floored at 30 s — the same numbers the former
/// consumer-group reaper factory used. Without one
/// ([`ConsumerOptions::without_handler_timeout`]) there is no deadline after
/// which an in-flight entry can be presumed dead, so reclaim is disabled
/// (`None`) and the sidecar only trims, on the default 30 s cadence.
///
/// [`ConsumerOptions::without_handler_timeout`]: crate::ConsumerOptions::without_handler_timeout
fn sidecar_timing(handler_timeout: Option<Duration>) -> (Duration, Option<u64>) {
    match handler_timeout {
        Some(timeout) => {
            let min_idle_ms = timeout.as_millis() as u64;
            let interval = Duration::from_millis(min_idle_ms.max(30_000));
            (interval, Some(min_idle_ms))
        }
        None => (DEFAULT_HANDLER_TIMEOUT, None),
    }
}

/// This consumer's reclaim policy, given its handler timeout and whether it
/// resolves a timeout to an outcome itself.
///
/// With no `handler_timeout_outcome` override the consumer does nothing when a
/// handler times out — it deliberately leaves the entry in the PEL for the
/// reaper to redeliver. Reclaiming at exactly `handler_timeout` is then the
/// intended behaviour, and the reaper is the only actor on that entry.
///
/// With an override set the consumer becomes an actor at that same deadline:
/// it XACKs, XADDs to a hold queue, or dead-letters the entry the moment the
/// timeout fires. XACK is group-wide rather than owner-checked, so a reaper
/// sweeping with an idle threshold of exactly `handler_timeout` can claim and
/// redeliver the very entry its owner is concurrently resolving — leaving a
/// duplicate on the stream after a timeout-to-`Ack`, or two redeliveries after
/// a timeout-to-`Defer`. Backing the threshold off to twice the timeout keeps
/// *this process's* sidecar clear of *this process's* owners: a live owner
/// resolves first, and a genuinely dead one is still recovered, just one extra
/// timeout later.
///
/// The margin is proportional rather than a fixed constant so it scales with
/// whatever the caller considers "too slow", and so short timeouts don't
/// inherit a multi-second crash-recovery delay.
///
/// It buys nothing against a sidecar in another process, which seeds its own
/// threshold from its own consumers and may sweep at plain `handler_timeout`.
/// That case is the [`super::lease`]'s to narrow, and narrowing is all it can
/// do — see the module docs above.
fn reclaim_policy(handler_timeout: Option<Duration>, resolves_own_timeout: bool) -> Policy {
    match handler_timeout {
        Some(timeout) if resolves_own_timeout => Some(timeout.saturating_mul(2)),
        other => other,
    }
}

/// Ensure a maintenance sidecar runs for `(client, stream, group)` and
/// return a guard expressing this consumer's interest in it.
///
/// This consumer's reclaim policy is derived from `handler_timeout` and
/// `resolves_own_timeout` (see [`reclaim_policy`]); the sidecar runs with the
/// conservative effective policy across all live guards on the key (see the
/// module docs), with timing derived via [`sidecar_timing`].
pub(super) fn acquire(
    client: &RedisClient,
    stream: &str,
    handler_timeout: Option<Duration>,
    resolves_own_timeout: bool,
) -> MaintenanceGuard {
    let key = (
        client.instance_id(),
        stream.to_owned(),
        client.group().to_owned(),
    );
    let client = client.clone();
    let stream = stream.to_owned();
    let spawner: Spawner = Arc::new(move |shutdown, policy| {
        let (interval, min_idle_ms) = sidecar_timing(policy);
        spawn_maintenance(
            client.clone(),
            vec![stream.clone()],
            client.group().to_owned(),
            interval,
            min_idle_ms,
            shutdown,
        );
    });
    acquire_with(
        key,
        reclaim_policy(handler_timeout, resolves_own_timeout),
        spawner,
    )
}

/// Core refcount + effective-policy logic, generic over the spawner so the
/// dedup and respawn behaviour is unit-testable without a Redis connection.
fn acquire_with(key: Key, policy: Policy, spawner: Spawner) -> MaintenanceGuard {
    let mut map = lock();
    match map.get_mut(&key) {
        Some(entry) => {
            let before = entry.effective();
            entry.policies.push(policy);
            if entry.effective() != before {
                entry.respawn();
            }
        }
        None => {
            let shutdown = CancellationToken::new();
            spawner(shutdown.clone(), policy);
            map.insert(
                key.clone(),
                Entry {
                    policies: vec![policy],
                    shutdown,
                    spawner,
                },
            );
        }
    }
    MaintenanceGuard { key, policy }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn key(n: usize) -> Key {
        // Distinct high client-ids so tests can't collide with each other or
        // with real acquisitions in the shared process-wide registry.
        (usize::MAX - n, format!("stream-{n}"), format!("group-{n}"))
    }

    #[test]
    fn timing_with_timeout_floors_interval_and_sets_min_idle() {
        assert_eq!(
            sidecar_timing(Some(Duration::from_secs(5))),
            (Duration::from_secs(30), Some(5_000)),
            "short timeouts keep the 30s sweep floor but gate reclaim at the timeout"
        );
        assert_eq!(
            sidecar_timing(Some(Duration::from_secs(45))),
            (Duration::from_secs(45), Some(45_000)),
            "long timeouts stretch the sweep interval to match"
        );
    }

    #[test]
    fn timing_without_timeout_disables_reclaim() {
        // `without_handler_timeout()` is an explicit promise that handlers
        // may run indefinitely — maintenance must never reclaim their
        // in-flight entries on a made-up deadline. Trim still runs.
        assert_eq!(
            sidecar_timing(None),
            (Duration::from_secs(30), None),
            "no handler deadline means no XAUTOCLAIM deadline"
        );
    }

    #[test]
    fn reclaim_policy_unchanged_without_a_timeout_outcome() {
        // The consumer does nothing at the deadline, so the reaper is the only
        // actor and reclaiming exactly at the timeout is the intended design.
        assert_eq!(
            reclaim_policy(Some(Duration::from_secs(30)), false),
            Some(Duration::from_secs(30))
        );
        assert_eq!(reclaim_policy(None, false), None);
    }

    #[test]
    fn reclaim_policy_backs_off_when_the_consumer_resolves_its_own_timeout() {
        // With an override the owner acts at the deadline; the reaper must not
        // claim the same entry at the same instant.
        assert_eq!(
            reclaim_policy(Some(Duration::from_secs(30)), true),
            Some(Duration::from_secs(60))
        );
    }

    #[test]
    fn reclaim_policy_never_invents_a_deadline() {
        // `without_handler_timeout()` disables reclaim; a timeout outcome is
        // unreachable in that configuration and must not resurrect it.
        assert_eq!(reclaim_policy(None, true), None);
    }

    use std::sync::Mutex as StdMutex;

    type SpawnLog = Arc<StdMutex<Vec<(CancellationToken, Policy)>>>;

    /// Spawn recorder: collects each (token, policy) the registry spawns
    /// with, so tests can assert on respawns and effective policies.
    fn recorder() -> (SpawnLog, Spawner) {
        let log: SpawnLog = Arc::new(StdMutex::new(Vec::new()));
        let l = Arc::clone(&log);
        let spawner: Spawner = Arc::new(move |token, policy| {
            l.lock().unwrap().push((token, policy));
        });
        (log, spawner)
    }

    #[test]
    fn second_acquire_for_same_key_does_not_spawn() {
        let (log, spawner) = recorder();
        let g1 = acquire_with(key(1), Some(Duration::from_secs(30)), spawner.clone());
        let g2 = acquire_with(key(1), Some(Duration::from_secs(30)), spawner);
        assert_eq!(log.lock().unwrap().len(), 1, "one sidecar per key");
        drop(g1);
        drop(g2);
    }

    #[test]
    fn distinct_keys_spawn_independently() {
        let (log, spawner) = recorder();
        let g1 = acquire_with(key(2), Some(Duration::from_secs(30)), spawner.clone());
        let g2 = acquire_with(key(3), Some(Duration::from_secs(30)), spawner);
        assert_eq!(log.lock().unwrap().len(), 2);
        drop(g1);
        drop(g2);
    }

    #[test]
    fn last_guard_cancels_and_next_acquire_respawns() {
        let (log, spawner) = recorder();
        let g1 = acquire_with(key(4), Some(Duration::from_secs(30)), spawner.clone());
        let g2 = acquire_with(key(4), Some(Duration::from_secs(30)), spawner.clone());
        assert_eq!(log.lock().unwrap().len(), 1);

        drop(g1);
        assert!(
            !log.lock().unwrap()[0].0.is_cancelled(),
            "sidecar must survive while a guard remains"
        );
        drop(g2);
        assert!(
            log.lock().unwrap()[0].0.is_cancelled(),
            "dropping the last guard must cancel the sidecar"
        );

        let g3 = acquire_with(key(4), Some(Duration::from_secs(30)), spawner);
        assert_eq!(
            log.lock().unwrap().len(),
            2,
            "fresh acquire after teardown respawns"
        );
        drop(g3);
        assert!(log.lock().unwrap()[1].0.is_cancelled());
    }

    // -- Mixed timeout policies on one key --------------------------------
    //
    // XAUTOCLAIM acts on the whole group's PEL, so one sidecar's policy
    // applies to every consumer on the key. The registry must therefore run
    // the *conservative* effective policy across all live guards: any
    // no-timeout guard disables reclaim entirely; otherwise the longest
    // timeout wins — entries may never be reclaimed earlier than their
    // owner's own deadline.

    #[test]
    fn no_timeout_guard_downgrades_sidecar_to_trim_only() {
        let (log, spawner) = recorder();
        let g1 = acquire_with(key(5), Some(Duration::from_secs(30)), spawner.clone());
        assert_eq!(
            log.lock().unwrap().last().unwrap().1,
            Some(Duration::from_secs(30))
        );

        // A no-timeout consumer joins: the sidecar must be respawned with
        // reclaim disabled, or its in-flight work could be force-reclaimed.
        let g2 = acquire_with(key(5), None, spawner);
        {
            let entries = log.lock().unwrap();
            assert_eq!(entries.len(), 2, "policy change must respawn the sidecar");
            assert!(entries[0].0.is_cancelled(), "old sidecar must be cancelled");
            assert_eq!(entries[1].1, None, "effective policy must be trim-only");
        }

        // The no-timeout consumer leaves: reclaim may resume.
        drop(g2);
        {
            let entries = log.lock().unwrap();
            assert_eq!(entries.len(), 3);
            assert!(entries[1].0.is_cancelled());
            assert_eq!(entries[2].1, Some(Duration::from_secs(30)));
        }
        drop(g1);
        assert!(log.lock().unwrap()[2].0.is_cancelled());
    }

    #[test]
    fn longest_timeout_wins_among_timeout_guards() {
        let (log, spawner) = recorder();
        let g1 = acquire_with(key(6), Some(Duration::from_secs(30)), spawner.clone());
        let g2 = acquire_with(key(6), Some(Duration::from_secs(120)), spawner.clone());
        {
            let entries = log.lock().unwrap();
            assert_eq!(entries.len(), 2, "longer timeout must respawn the sidecar");
            assert_eq!(entries[1].1, Some(Duration::from_secs(120)));
        }

        // A shorter timeout joining must NOT lower the effective deadline.
        let g3 = acquire_with(key(6), Some(Duration::from_secs(5)), spawner);
        assert_eq!(
            log.lock().unwrap().len(),
            2,
            "shorter timeout must not respawn or lower the deadline"
        );

        drop(g2);
        {
            let entries = log.lock().unwrap();
            assert_eq!(entries.len(), 3, "dropping the 120s guard recomputes");
            assert_eq!(entries[2].1, Some(Duration::from_secs(30)));
        }
        drop(g1);
        drop(g3);
        assert!(log.lock().unwrap()[2].0.is_cancelled());
    }
}
