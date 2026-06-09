//! Process-wide registry of per-`(client, stream, group)` maintenance tasks.
//!
//! Every Redis consumer loop — registry-managed or direct — calls
//! [`acquire`] for the stream it consumes. The first acquisition for a
//! `(client, stream, group)` key spawns one reaper sidecar (XAUTOCLAIM crash
//! recovery + acked-entry trimming, see [`super::reaper`]); subsequent
//! acquisitions for the same key just bump a refcount. When the last
//! [`MaintenanceGuard`] drops, the sidecar is cancelled and the entry
//! removed. This keeps maintenance at exactly one task per key no matter how
//! many consumers a group scales to — the N-redundant-sweepers pathology the
//! reaper consolidation removed must not come back through this door.
//!
//! The key includes the client identity so two clients pointed at different
//! Redis servers never share a maintenance task; two distinct clients on the
//! same server merely run duplicate sweeps, which XAUTOCLAIM and the
//! min-across-groups trim are both safe under.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::client::RedisClient;
use super::reaper::spawn_reaper;
use crate::consumer::DEFAULT_HANDLER_TIMEOUT;

type Key = (usize, String, String);

struct Entry {
    refcount: usize,
    shutdown: CancellationToken,
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

/// Refcount handle for one consumer's interest in a stream's maintenance.
/// Dropping the last guard for a key cancels the underlying sidecar.
pub(super) struct MaintenanceGuard {
    key: Key,
}

impl Drop for MaintenanceGuard {
    fn drop(&mut self) {
        let mut map = lock();
        if let Some(entry) = map.get_mut(&self.key) {
            entry.refcount -= 1;
            if entry.refcount == 0 {
                entry.shutdown.cancel();
                map.remove(&self.key);
            }
        }
    }
}

/// Ensure a maintenance sidecar runs for `(client, stream, group)` and
/// return a guard expressing this consumer's interest in it.
///
/// `handler_timeout` seeds the sidecar timing exactly like the former
/// consumer-group reaper factory did: the XAUTOCLAIM idle threshold is the
/// resolved handler timeout, and the sweep interval is that floored at 30 s.
/// The first acquirer's timing wins for the lifetime of the entry.
pub(super) fn acquire(
    client: &RedisClient,
    stream: &str,
    handler_timeout: Option<Duration>,
) -> MaintenanceGuard {
    let timeout = handler_timeout.unwrap_or(DEFAULT_HANDLER_TIMEOUT);
    let min_idle_ms = timeout.as_millis() as u64;
    let interval = Duration::from_millis(min_idle_ms.max(30_000));
    let group = client.group().to_owned();
    let stream = stream.to_owned();
    acquire_with(
        (client.instance_id(), stream.clone(), group.clone()),
        move |shutdown| {
            spawn_reaper(
                client.clone(),
                vec![stream],
                group,
                interval,
                min_idle_ms,
                shutdown,
            );
        },
    )
}

/// Core refcount logic, generic over the spawner so the dedup behaviour is
/// unit-testable without a Redis connection.
fn acquire_with(key: Key, spawn: impl FnOnce(CancellationToken)) -> MaintenanceGuard {
    let mut map = lock();
    match map.get_mut(&key) {
        Some(entry) => entry.refcount += 1,
        None => {
            let shutdown = CancellationToken::new();
            spawn(shutdown.clone());
            map.insert(
                key.clone(),
                Entry {
                    refcount: 1,
                    shutdown,
                },
            );
        }
    }
    MaintenanceGuard { key }
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
    fn second_acquire_for_same_key_does_not_spawn() {
        let spawns = AtomicUsize::new(0);
        let g1 = acquire_with(key(1), |_| {
            spawns.fetch_add(1, Ordering::Relaxed);
        });
        let g2 = acquire_with(key(1), |_| {
            spawns.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(spawns.load(Ordering::Relaxed), 1, "one sidecar per key");
        drop(g1);
        drop(g2);
    }

    #[test]
    fn distinct_keys_spawn_independently() {
        let spawns = AtomicUsize::new(0);
        let g1 = acquire_with(key(2), |_| {
            spawns.fetch_add(1, Ordering::Relaxed);
        });
        let g2 = acquire_with(key(3), |_| {
            spawns.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(spawns.load(Ordering::Relaxed), 2);
        drop(g1);
        drop(g2);
    }

    #[test]
    fn last_guard_cancels_and_next_acquire_respawns() {
        let mut tokens = Vec::new();
        let g1 = acquire_with(key(4), |t| tokens.push(t));
        let g2 = acquire_with(key(4), |t| tokens.push(t));
        assert_eq!(tokens.len(), 1);

        drop(g1);
        assert!(
            !tokens[0].is_cancelled(),
            "sidecar must survive while a guard remains"
        );
        drop(g2);
        assert!(
            tokens[0].is_cancelled(),
            "dropping the last guard must cancel the sidecar"
        );

        let g3 = acquire_with(key(4), |t| tokens.push(t));
        assert_eq!(tokens.len(), 2, "fresh acquire after teardown respawns");
        drop(g3);
        assert!(tokens[1].is_cancelled());
    }
}
