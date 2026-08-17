use bytes::Bytes;
use lru::LruCache;
use rustc_hash::FxBuildHasher;
use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Caps how many entries a single `sweep` call removes. Scanning for
/// expired/marked entries is cheap even over a large cache (~4ms/1M
/// entries, measured), but each removal itself is not (~500ns/entry,
/// measured — `LruCache::pop` unlinks from both a hash map and a linked
/// list) — sweeping hundreds of thousands of entries in one call has been
/// measured to take 100ms+, which would stall every other command queued
/// behind it on the single-threaded cache actor for that long. Chunking
/// removals lets client commands interleave between `sweep` calls instead.
pub(crate) const SWEEP_BUDGET: usize = 2_000;

struct Entry {
    value: Bytes,
    expires_at: Option<Instant>,
}

pub struct Cache {
    entries: LruCache<Bytes, Entry, FxBuildHasher>,
    used_bytes: usize,
    max_memory_bytes: usize,
    /// Keys handed off to another node during an ADR-0008 migration this
    /// node was the source for, awaiting `sweep`'s next pass.
    migrated: HashSet<Bytes>,
    /// Expired/marked keys queued for removal by `sweep`, in
    /// `SWEEP_BUDGET`-sized bites. Refilled (by scanning `entries` for
    /// expired keys and draining `migrated`) only once this drains empty,
    /// so an in-progress sweep pass isn't rescanned from scratch every
    /// call.
    pending_removal: VecDeque<Bytes>,
}

impl Entry {
    fn is_expired_at(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }
}

impl Cache {
    pub fn new(max_memory_bytes: usize) -> Self {
        Self {
            entries: LruCache::unbounded_with_hasher(FxBuildHasher),
            used_bytes: 0,
            max_memory_bytes,
            migrated: HashSet::new(),
            pending_removal: VecDeque::new(),
        }
    }

    pub fn set(&mut self, key: Bytes, value: Bytes) {
        self.insert(key, value, None);
    }

    pub fn set_with_ttl(&mut self, key: Bytes, value: Bytes, ttl: Duration) {
        // The TTL comes straight off the wire with no upper bound, so a huge
        // value (up to `u64::MAX` seconds) would overflow `Instant + Duration`
        // and panic — taking down the single cache actor and, with it, every
        // client's cache operations. A TTL too far out to represent is treated
        // as "never expires" (no expiry), which is the closest honest meaning.
        let expires_at = Instant::now().checked_add(ttl);
        self.insert(key, value, expires_at);
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Bytes> {
        self.get_at(key, Instant::now())
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        self.delete_at(key, Instant::now())
    }

    fn insert(&mut self, key: Bytes, value: Bytes, expires_at: Option<Instant>) {
        // Entries stored long-term must not keep a shared receive-buffer
        // chunk (which may span an entire pipelined batch) alive just to
        // retain a few bytes of it, so re-copy into right-sized allocations
        // here, where the invariant is actually enforced for every caller.
        let key = Bytes::copy_from_slice(&key);
        let value = Bytes::copy_from_slice(&value);

        let key_len = key.len();
        let value_len = value.len();
        let entry = Entry { value, expires_at };

        // A fresh write is not the value a handoff transferred: a stale
        // `migrated` mark left over from an earlier value must not condemn
        // this one to the next sweep (it would silently delete it).
        self.migrated.remove(&key[..]);

        match self.entries.put(key, entry) {
            Some(replaced) => self.used_bytes = self.used_bytes - replaced.value.len() + value_len,
            None => self.used_bytes += key_len + value_len,
        }

        // Evict least-recently-used entries until the cache fits its memory
        // budget, but never evict the entry just inserted above: it is
        // always the most-recently-used one, so `pop_lru` would only reach
        // it once nothing else is left.
        while self.used_bytes > self.max_memory_bytes && self.entries.len() > 1 {
            let (evicted_key, evicted_entry) = self
                .entries
                .pop_lru()
                .expect("len() > 1 guarantees an entry to evict");

            self.used_bytes -= evicted_key.len() + evicted_entry.value.len();
            // The marked value is gone; a future entry under this key is a
            // different value and must not inherit the mark.
            self.migrated.remove(&evicted_key[..]);
        }
    }

    fn get_at(&mut self, key: &[u8], now: Instant) -> Option<Bytes> {
        let entry = self.entries.get(key)?;

        if entry.is_expired_at(now) {
            self.remove_entry(key);
            return None;
        }

        Some(entry.value.clone())
    }

    fn delete_at(&mut self, key: &[u8], now: Instant) -> bool {
        let expired = self
            .entries
            .peek(key)
            .is_some_and(|entry| entry.is_expired_at(now));

        if expired {
            self.remove_entry(key);
            return false;
        }

        self.remove_entry(key).is_some()
    }

    fn remove_entry(&mut self, key: &[u8]) -> Option<Entry> {
        let entry = self.entries.pop(key)?;
        self.used_bytes -= key.len() + entry.value.len();
        // The mark referred to this entry's value; whatever is stored
        // under the key later is a different value.
        self.migrated.remove(key);
        Some(entry)
    }

    /// A point-in-time snapshot of every non-expired entry, each with its
    /// remaining TTL (not the original one, so a transferred entry expires
    /// at the same wall-clock time it would have here). For ADR-0008's
    /// migration task, to compute which of these a newly joining node now
    /// owns. Uses `LruCache::iter`, not `get`, so listing entries doesn't
    /// itself perturb recency.
    pub fn entries(&self) -> Vec<(Bytes, Bytes, Option<Duration>)> {
        self.entries_at(Instant::now())
    }

    fn entries_at(&self, now: Instant) -> Vec<(Bytes, Bytes, Option<Duration>)> {
        self.entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired_at(now))
            .map(|(key, entry)| {
                let remaining_ttl = entry
                    .expires_at
                    .map(|expires_at| expires_at.saturating_duration_since(now));

                (key.clone(), entry.value.clone(), remaining_ttl)
            })
            .collect()
    }

    /// The current value and remaining TTL for one key, same shape as one
    /// `entries()` row, without disturbing recency (`LruCache::peek`, not
    /// `get`). For ADR-0008's migration task, to re-check a key's *live*
    /// value right before sending it, instead of trusting whatever
    /// `entries()`'s snapshot captured at the start of the handoff — a
    /// concurrent client write between the snapshot and this key's turn
    /// would otherwise ship a stale value to the joining node. `None` if
    /// the key isn't present or has expired.
    pub fn peek_entry(&self, key: &[u8]) -> Option<(Bytes, Bytes, Option<Duration>)> {
        self.peek_entry_at(key, Instant::now())
    }

    fn peek_entry_at(&self, key: &[u8], now: Instant) -> Option<(Bytes, Bytes, Option<Duration>)> {
        let entry = self.entries.peek(key)?;

        if entry.is_expired_at(now) {
            return None;
        }

        let remaining_ttl = entry
            .expires_at
            .map(|expires_at| expires_at.saturating_duration_since(now));

        Some((
            Bytes::copy_from_slice(key),
            entry.value.clone(),
            remaining_ttl,
        ))
    }

    /// Marks `key` as handed off during an ADR-0008 migration this node
    /// was the source for. A no-op if the key is already marked or no
    /// longer present; `sweep` reclaims marked entries later.
    pub fn mark_migrated(&mut self, key: &[u8]) {
        if self.entries.contains(key) {
            self.migrated.insert(Bytes::copy_from_slice(key));
        }
    }

    /// Reverses `mark_migrated`: this node is keeping `key` after all (its
    /// migration was cancelled), so it must not be swept. A no-op if `key`
    /// wasn't marked. Does not touch `pending_removal` — a key already
    /// queued there by an earlier `sweep` refill finishes being removed
    /// regardless (see `sweep_at`), since cancellation only runs while
    /// `migration_in_progress` keeps `sweep` paused, so nothing this node
    /// marks can have reached `pending_removal` yet.
    pub fn unmark_migrated(&mut self, key: &[u8]) {
        self.migrated.remove(key);
    }

    /// ADR-0008's active-deletion facility: reclaims entries marked by
    /// `mark_migrated`, and — since `get_at`/`delete_at` only expire a
    /// TTL'd entry lazily, on access — also proactively removes anything
    /// past its TTL, so an unread expired key doesn't sit in memory
    /// indefinitely. Removes at most `SWEEP_BUDGET` entries per call (the
    /// caller should call again if the backlog isn't drained yet — see
    /// `pending_removal`), so one call can't stall every other cache
    /// command behind it for as long as a full pass over a large cache
    /// would take. Returns how many entries were actually removed this
    /// call (a marked or expired key may already be gone, e.g. deleted by
    /// a client in the meantime) — `< SWEEP_BUDGET` means the backlog is
    /// now fully drained.
    pub fn sweep(&mut self) -> usize {
        self.sweep_at(Instant::now())
    }

    fn sweep_at(&mut self, now: Instant) -> usize {
        if self.pending_removal.is_empty() {
            self.pending_removal.extend(
                self.entries
                    .iter()
                    .filter(|(_, entry)| entry.is_expired_at(now))
                    .map(|(key, _)| key.clone()),
            );
            // Marks stay in `migrated` until the moment of removal (not
            // drained here): the queue is only a snapshot of candidates,
            // and a key rewritten after this point clears its mark, which
            // the removability re-check below must still observe.
            self.pending_removal.extend(self.migrated.iter().cloned());
        }

        let mut removed = 0;

        for _ in 0..SWEEP_BUDGET {
            let Some(key) = self.pending_removal.pop_front() else {
                break;
            };

            // Re-check at removal time: the snapshot above may be stale —
            // the key may have been rewritten (mark cleared, or no longer
            // expired) since it was queued, and a fresh value must never
            // be swept on the strength of an old candidate entry.
            let removable = self.migrated.contains(&key[..])
                || self
                    .entries
                    .peek(&key[..])
                    .is_some_and(|entry| entry.is_expired_at(now));

            if removable && self.remove_entry(&key).is_some() {
                removed += 1;
            }
        }

        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UNBOUNDED: usize = usize::MAX;

    #[test]
    fn gets_a_previously_set_value() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));

        assert_eq!(cache.get(b"name"), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn get_returns_none_for_missing_key() {
        let mut cache = Cache::new(UNBOUNDED);

        assert_eq!(cache.get(b"missing"), None);
    }

    #[test]
    fn delete_returns_true_for_existing_key() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));

        assert!(cache.delete(b"name"));
    }

    #[test]
    fn delete_returns_false_for_missing_key() {
        let mut cache = Cache::new(UNBOUNDED);

        assert!(!cache.delete(b"name"));
    }

    #[test]
    fn set_overwrites_existing_value() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));
        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Bob"));

        assert_eq!(cache.get(b"name"), Some(Bytes::from_static(b"Bob")));
    }

    #[test]
    fn deleted_value_can_no_longer_be_retrieved() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));
        cache.delete(b"name");

        assert_eq!(cache.get(b"name"), None);
    }

    #[test]
    fn gets_a_previously_set_value_with_ttl() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            Bytes::from_static(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        assert_eq!(cache.get(b"name"), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn set_with_an_overflowing_ttl_never_expires_instead_of_panicking() {
        // A TTL near `u64::MAX` seconds overflows `Instant + Duration`, which
        // panics — taking down the whole cache actor. Such a value is stored
        // with no expiry instead: it must not panic, and the entry stays
        // retrievable arbitrarily far into the future.
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            Bytes::from_static(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(u64::MAX),
        );

        let far_future = Instant::now() + Duration::from_secs(1_000_000_000);

        assert_eq!(
            cache.get_at(b"name", far_future),
            Some(Bytes::from_static(b"Alice"))
        );
    }

    #[test]
    fn delete_returns_true_before_expiration() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            Bytes::from_static(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        assert!(cache.delete(b"name"));
    }

    #[test]
    fn get_returns_value_before_expiration() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            Bytes::from_static(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        let future = Instant::now() + Duration::from_secs(4);

        assert_eq!(
            cache.get_at(b"name", future),
            Some(Bytes::from_static(b"Alice"))
        );
    }

    #[test]
    fn get_returns_none_after_expiration() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            Bytes::from_static(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        let future = Instant::now() + Duration::from_secs(6);

        assert_eq!(cache.get_at(b"name", future), None);
    }

    #[test]
    fn get_removes_expired_entry() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            Bytes::from_static(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        let future = Instant::now() + Duration::from_secs(6);

        cache.get_at(b"name", future);

        assert!(!cache.entries.contains(b"name".as_slice()));
    }

    #[test]
    fn delete_returns_false_after_expiration() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            Bytes::from_static(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        let future = Instant::now() + Duration::from_secs(6);

        assert!(!cache.delete_at(b"name", future));
    }

    #[test]
    fn evicts_least_recently_used_entry_when_over_memory_limit() {
        // Each entry costs 2 (key) + 4 (value) = 6 bytes; room for exactly two.
        let mut cache = Cache::new(12);

        cache.set(Bytes::from_static(b"k1"), Bytes::from_static(b"vvvv"));
        cache.set(Bytes::from_static(b"k2"), Bytes::from_static(b"vvvv"));
        cache.set(Bytes::from_static(b"k3"), Bytes::from_static(b"vvvv"));

        assert_eq!(cache.get(b"k1"), None);
        assert_eq!(cache.get(b"k2"), Some(Bytes::from_static(b"vvvv")));
        assert_eq!(cache.get(b"k3"), Some(Bytes::from_static(b"vvvv")));
    }

    #[test]
    fn get_protects_an_entry_from_eviction_by_marking_it_recently_used() {
        let mut cache = Cache::new(12);

        cache.set(Bytes::from_static(b"k1"), Bytes::from_static(b"vvvv"));
        cache.set(Bytes::from_static(b"k2"), Bytes::from_static(b"vvvv"));

        cache.get(b"k1");

        cache.set(Bytes::from_static(b"k3"), Bytes::from_static(b"vvvv"));

        assert_eq!(cache.get(b"k1"), Some(Bytes::from_static(b"vvvv")));
        assert_eq!(cache.get(b"k2"), None);
        assert_eq!(cache.get(b"k3"), Some(Bytes::from_static(b"vvvv")));
    }

    #[test]
    fn overwriting_a_key_does_not_double_count_memory_usage() {
        let mut cache = Cache::new(12);

        cache.set(Bytes::from_static(b"k1"), Bytes::from_static(b"vvvv"));
        cache.set(Bytes::from_static(b"k1"), Bytes::from_static(b"vvvv"));
        cache.set(Bytes::from_static(b"k2"), Bytes::from_static(b"vvvv"));

        assert_eq!(cache.get(b"k1"), Some(Bytes::from_static(b"vvvv")));
        assert_eq!(cache.get(b"k2"), Some(Bytes::from_static(b"vvvv")));
    }

    #[test]
    fn overwrite_accounts_for_a_shrinking_value_precisely() {
        let mut cache = Cache::new(10);

        cache.set(Bytes::from_static(b"a"), Bytes::from_static(b"XXX")); // size 4, used 4
        cache.set(Bytes::from_static(b"b"), Bytes::from_static(b"XXX")); // size 4, used 8
        cache.set(Bytes::from_static(b"a"), Bytes::from_static(b"Z")); // shrinks to size 2, used 6
        cache.set(Bytes::from_static(b"c"), Bytes::from_static(b"WWWWW")); // size 6, used 12 > 10: evicts LRU "b"

        assert_eq!(cache.get(b"b"), None);
        assert_eq!(cache.get(b"a"), Some(Bytes::from_static(b"Z")));
        assert_eq!(cache.get(b"c"), Some(Bytes::from_static(b"WWWWW")));
    }

    #[test]
    fn eviction_loop_accounts_for_freed_bytes_precisely() {
        let mut cache = Cache::new(7);

        cache.set(Bytes::from_static(b"a"), Bytes::from_static(b"X")); // size 2, used 2
        cache.set(Bytes::from_static(b"b"), Bytes::from_static(b"X")); // size 2, used 4
        cache.set(Bytes::from_static(b"c"), Bytes::from_static(b"X")); // size 2, used 6
        cache.set(Bytes::from_static(b"d"), Bytes::from_static(b"WWW")); // size 4, used 10 > 7: evicts "a" then "b"

        assert_eq!(cache.get(b"a"), None);
        assert_eq!(cache.get(b"b"), None);
        assert_eq!(cache.get(b"c"), Some(Bytes::from_static(b"X")));
        assert_eq!(cache.get(b"d"), Some(Bytes::from_static(b"WWW")));
    }

    #[test]
    fn delete_frees_the_deleted_entrys_exact_byte_count() {
        let mut cache = Cache::new(10);

        cache.set(Bytes::from_static(b"a"), Bytes::from_static(b"XXX")); // size 4
        cache.set(Bytes::from_static(b"b"), Bytes::from_static(b"XXX")); // size 4, used 8
        cache.delete(b"a"); // used 4
        cache.set(Bytes::from_static(b"c"), Bytes::from_static(b"XXX")); // size 4, used 8, fits

        assert_eq!(cache.get(b"b"), Some(Bytes::from_static(b"XXX")));
        assert_eq!(cache.get(b"c"), Some(Bytes::from_static(b"XXX")));
    }

    #[test]
    fn delete_does_not_under_report_freed_bytes() {
        let mut cache = Cache::new(6);

        cache.set(Bytes::from_static(b"a"), Bytes::from_static(b"XXX")); // size 4
        cache.set(Bytes::from_static(b"b"), Bytes::from_static(b"X")); // size 2, used 6
        cache.delete(b"a"); // used 2
        cache.set(Bytes::from_static(b"c"), Bytes::from_static(b"WWWW")); // size 5, used 7 > 6: evicts "b"

        assert_eq!(cache.get(b"b"), None);
        assert_eq!(cache.get(b"c"), Some(Bytes::from_static(b"WWWW")));
    }

    #[test]
    fn a_single_entry_larger_than_the_limit_is_kept_and_not_evicted() {
        let mut cache = Cache::new(4);

        cache.set(Bytes::from_static(b"k1"), Bytes::from_static(b"vvvv"));

        assert_eq!(cache.get(b"k1"), Some(Bytes::from_static(b"vvvv")));
    }

    #[test]
    fn delete_frees_memory_for_subsequent_inserts() {
        let mut cache = Cache::new(6);

        cache.set(Bytes::from_static(b"k1"), Bytes::from_static(b"vvvv"));
        cache.delete(b"k1");
        cache.set(Bytes::from_static(b"k2"), Bytes::from_static(b"vvvv"));

        assert_eq!(cache.get(b"k1"), None);
        assert_eq!(cache.get(b"k2"), Some(Bytes::from_static(b"vvvv")));
    }

    #[test]
    fn entries_includes_every_stored_key_with_its_value() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        cache.set(Bytes::from_static(b"b"), Bytes::from_static(b"2"));

        let mut entries = cache.entries();
        entries.sort_by(|(a, ..), (b, ..)| a.cmp(b));

        assert_eq!(
            entries,
            vec![
                (Bytes::from_static(b"a"), Bytes::from_static(b"1"), None),
                (Bytes::from_static(b"b"), Bytes::from_static(b"2"), None),
            ]
        );
    }

    #[test]
    fn entries_reports_the_remaining_ttl_not_the_original_one() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            Bytes::from_static(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(10),
        );

        let later = Instant::now() + Duration::from_secs(4);
        let entries = cache.entries_at(later);

        assert_eq!(entries.len(), 1);
        let (_, _, remaining_ttl) = &entries[0];
        // 10s TTL minus the 4s that "elapsed" leaves ~6s, not the original 10s.
        assert!(remaining_ttl.unwrap() <= Duration::from_secs(6));
        assert!(remaining_ttl.unwrap() > Duration::from_secs(5));
    }

    #[test]
    fn entries_excludes_expired_keys() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            Bytes::from_static(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        let future = Instant::now() + Duration::from_secs(6);

        assert_eq!(cache.entries_at(future), Vec::new());
    }

    #[test]
    fn entries_does_not_disturb_lru_order() {
        let mut cache = Cache::new(8);

        cache.set(Bytes::from_static(b"a"), Bytes::from_static(b"XX")); // used 3
        cache.set(Bytes::from_static(b"b"), Bytes::from_static(b"XX")); // used 6

        // If listing entries touched recency the same way `get` does, "a"
        // would become most-recently-used here and survive the eviction
        // below instead of "b".
        let _ = cache.entries();

        cache.set(Bytes::from_static(b"c"), Bytes::from_static(b"XXX")); // evicts "a" (still LRU)

        assert_eq!(cache.get(b"a"), None);
        assert_eq!(cache.get(b"b"), Some(Bytes::from_static(b"XX")));
    }

    #[test]
    fn peek_entry_returns_the_current_value_and_remaining_ttl() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            Bytes::from_static(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        let (key, value, ttl) = cache.peek_entry(b"name").unwrap();

        assert_eq!(key, Bytes::from_static(b"name"));
        assert_eq!(value, Bytes::from_static(b"Alice"));
        assert!(ttl.unwrap() <= Duration::from_secs(5));
    }

    #[test]
    fn peek_entry_is_none_for_a_missing_key() {
        let cache = Cache::new(UNBOUNDED);

        assert_eq!(cache.peek_entry(b"missing"), None);
    }

    #[test]
    fn peek_entry_is_none_for_an_expired_key() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            Bytes::from_static(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        let future = Instant::now() + Duration::from_secs(6);

        assert_eq!(cache.peek_entry_at(b"name", future), None);
    }

    #[test]
    fn peek_entry_does_not_disturb_lru_order() {
        let mut cache = Cache::new(8);

        cache.set(Bytes::from_static(b"a"), Bytes::from_static(b"XX"));
        cache.set(Bytes::from_static(b"b"), Bytes::from_static(b"XX"));

        let _ = cache.peek_entry(b"a");

        cache.set(Bytes::from_static(b"c"), Bytes::from_static(b"XXX")); // evicts "a" (still LRU)

        assert_eq!(cache.get(b"a"), None);
        assert_eq!(cache.get(b"b"), Some(Bytes::from_static(b"XX")));
    }

    #[test]
    fn mark_migrated_is_a_no_op_for_a_missing_key() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.mark_migrated(b"missing");

        assert_eq!(cache.sweep(), 0);
    }

    #[test]
    fn mark_migrated_does_not_remove_or_change_the_entry() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));
        cache.mark_migrated(b"name");

        assert_eq!(cache.get(b"name"), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn sweep_does_not_remove_a_value_rewritten_after_its_mark() {
        // Regression for issue #2: a mark refers to the value that was
        // handed off, not to the key forever — deleting the marked value
        // and writing a fresh one must not condemn the fresh one.
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));
        cache.mark_migrated(b"name");
        cache.delete(b"name");
        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Bob"));

        assert_eq!(cache.sweep(), 0);
        assert_eq!(cache.get(b"name"), Some(Bytes::from_static(b"Bob")));
    }

    #[test]
    fn overwriting_a_marked_key_clears_the_mark() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));
        cache.mark_migrated(b"name");
        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Bob"));

        assert_eq!(cache.sweep(), 0);
        assert_eq!(cache.get(b"name"), Some(Bytes::from_static(b"Bob")));
    }

    #[test]
    fn eviction_clears_the_mark_for_the_evicted_key() {
        // Room for roughly one small entry at a time, so the second set
        // evicts the first.
        let mut cache = Cache::new(16);

        cache.set(Bytes::from_static(b"aaaa"), Bytes::from_static(b"11111111"));
        cache.mark_migrated(b"aaaa");
        cache.set(Bytes::from_static(b"bbbb"), Bytes::from_static(b"22222222")); // evicts "aaaa"
        cache.set(Bytes::from_static(b"aaaa"), Bytes::from_static(b"33333333")); // fresh value

        cache.sweep();
        assert_eq!(cache.get(b"aaaa"), Some(Bytes::from_static(b"33333333")));
    }

    #[test]
    fn a_key_rewritten_while_queued_for_removal_survives_the_sweep() {
        // Regression for the same staleness through `pending_removal`: with
        // more expired keys than one sweep's budget, a key can sit queued
        // across sweeps; rewriting it fresh in that window must not let the
        // stale queue entry delete the new value.
        let mut cache = Cache::new(UNBOUNDED);
        let now = Instant::now();

        for i in 0..(SWEEP_BUDGET + 1) {
            cache.set_with_ttl(
                Bytes::from(format!("key-{i}")),
                Bytes::from_static(b"old"),
                Duration::from_secs(1),
            );
        }

        let later = now + Duration::from_secs(60);
        assert_eq!(cache.sweep_at(later), SWEEP_BUDGET);

        // Exactly one expired key remains, and it is still queued. Rewrite
        // it with a fresh, unexpiring value before the next sweep round.
        let leftover = cache
            .entries
            .iter()
            .map(|(key, _)| key.clone())
            .next()
            .expect("one expired entry should remain after the budgeted sweep");
        cache.set(leftover.clone(), Bytes::from_static(b"fresh"));

        cache.sweep_at(later);
        assert_eq!(
            cache.get_at(&leftover, later),
            Some(Bytes::from_static(b"fresh"))
        );
    }

    #[test]
    fn unmark_migrated_keeps_sweep_from_removing_the_entry() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));
        cache.mark_migrated(b"name");
        cache.unmark_migrated(b"name");

        assert_eq!(cache.sweep(), 0);
        assert_eq!(cache.get(b"name"), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn unmark_migrated_is_a_no_op_for_a_key_that_was_never_marked() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));
        cache.unmark_migrated(b"name");

        assert_eq!(cache.get(b"name"), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn sweep_removes_a_marked_entry_and_reports_it_removed() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));
        cache.mark_migrated(b"name");

        assert_eq!(cache.sweep(), 1);
        assert_eq!(cache.get(b"name"), None);
    }

    #[test]
    fn sweep_does_not_touch_an_unmarked_entry() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));

        assert_eq!(cache.sweep(), 0);
        assert_eq!(cache.get(b"name"), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn sweep_frees_the_memory_a_marked_entry_used() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));
        cache.mark_migrated(b"name");
        cache.sweep();

        cache.set(Bytes::from_static(b"other"), Bytes::from_static(b"Bob"));

        assert_eq!(cache.get(b"other"), Some(Bytes::from_static(b"Bob")));
    }

    #[test]
    fn sweep_proactively_removes_an_expired_entry_without_being_read_first() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            Bytes::from_static(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        let removed = cache.sweep_at(Instant::now() + Duration::from_secs(6));

        assert_eq!(removed, 1);
    }

    #[test]
    fn sweep_does_not_remove_an_entry_that_has_not_expired() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            Bytes::from_static(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        assert_eq!(cache.sweep(), 0);
        assert_eq!(cache.get(b"name"), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn sweep_only_counts_each_entry_once_when_both_marked_and_expired() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            Bytes::from_static(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );
        cache.mark_migrated(b"name");

        let removed = cache.sweep_at(Instant::now() + Duration::from_secs(6));

        assert_eq!(removed, 1);
    }

    #[test]
    fn sweep_removes_at_most_sweep_budget_entries_per_call() {
        let mut cache = Cache::new(UNBOUNDED);

        for i in 0..(SWEEP_BUDGET + 500) {
            let key = format!("key-{i}").into_bytes();
            cache.set(Bytes::copy_from_slice(&key), Bytes::from_static(b"x"));
            cache.mark_migrated(&key);
        }

        assert_eq!(cache.sweep(), SWEEP_BUDGET);
        assert_eq!(cache.sweep(), 500);
        assert_eq!(cache.sweep(), 0);
    }

    #[test]
    #[ignore]
    fn perf_one_sweep_chunk_against_a_large_removal_backlog() {
        let mut cache = Cache::new(UNBOUNDED);

        for i in 0..1_000_000u32 {
            let key = Bytes::copy_from_slice(format!("key-{i}").as_bytes());
            let value = Bytes::copy_from_slice(format!("value-{i}").as_bytes());
            cache.set(key, value);
        }

        for i in 0..250_000u32 {
            let key = format!("key-{i}").into_bytes();
            cache.mark_migrated(&key);
        }

        // First call also pays the one-time refill scan (see
        // `pending_removal`); this is the worst-case single blocking call.
        let start = std::time::Instant::now();
        let removed = cache.sweep();
        let first_call = start.elapsed();

        let start = std::time::Instant::now();
        let mut total_removed = removed;
        while total_removed < 250_000 {
            total_removed += cache.sweep();
        }
        let total_elapsed = start.elapsed();

        eprintln!(
            "first sweep() call (refill + one {SWEEP_BUDGET}-entry chunk, {removed} removed) \
             took {first_call:?}; draining the remaining {} marked entries took {total_elapsed:?} more",
            250_000 - removed
        );
    }

    #[test]
    #[ignore]
    fn perf_a_ttl_only_sweep_scan_with_nothing_marked() {
        let mut cache = Cache::new(UNBOUNDED);

        for i in 0..1_000_000u32 {
            let key = Bytes::copy_from_slice(format!("key-{i}").as_bytes());
            let value = Bytes::copy_from_slice(format!("value-{i}").as_bytes());
            cache.set_with_ttl(key, value, Duration::from_secs(3600));
        }

        let start = std::time::Instant::now();
        let removed = cache.sweep();
        let elapsed = start.elapsed();

        eprintln!("scanned 1_000_000 non-expired TTL'd entries, removed {removed}, in {elapsed:?}");
    }
}
