use bytes::Bytes;
use lru::LruCache;
use rustc_hash::FxBuildHasher;
use std::collections::HashSet;
use std::time::{Duration, Instant};

struct Entry {
    value: Bytes,
    expires_at: Option<Instant>,
}

pub struct Cache {
    entries: LruCache<Bytes, Entry, FxBuildHasher>,
    used_bytes: usize,
    max_memory_bytes: usize,
    /// Keys handed off to another node during an ADR-0008 migration this
    /// node was the source for. Only ever added to today — the
    /// active-deletion sweep that reclaims marked entries is a separate,
    /// not-yet-implemented follow-up (see ADR-0008's Decision).
    migrated: HashSet<Bytes>,
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
        }
    }

    pub fn set(&mut self, key: Bytes, value: Bytes) {
        self.insert(key, value, None);
    }

    pub fn set_with_ttl(&mut self, key: Bytes, value: Bytes, ttl: Duration) {
        let expires_at = Instant::now() + ttl;
        self.insert(key, value, Some(expires_at));
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

    /// Marks `key` as handed off during an ADR-0008 migration this node
    /// was the source for. A no-op if the key is already marked or no
    /// longer present. See `migrated`'s doc comment for what this does
    /// and doesn't do yet.
    pub fn mark_migrated(&mut self, key: &[u8]) {
        if self.entries.contains(key) {
            self.migrated.insert(Bytes::copy_from_slice(key));
        }
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
    fn mark_migrated_is_a_no_op_for_a_missing_key() {
        let mut cache = Cache::new(UNBOUNDED);

        // Just needs to not panic; there is no observable state to assert
        // on yet (the sweep that consumes marks is a separate follow-up).
        cache.mark_migrated(b"missing");
    }

    #[test]
    fn mark_migrated_does_not_remove_or_change_the_entry() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));
        cache.mark_migrated(b"name");

        assert_eq!(cache.get(b"name"), Some(Bytes::from_static(b"Alice")));
    }
}
