use bytes::Bytes;
use lru::LruCache;
use rustc_hash::FxBuildHasher;
use std::time::{Duration, Instant};

struct Entry {
    value: Bytes,
    expires_at: Option<Instant>,
}

pub struct Cache {
    entries: LruCache<Bytes, Entry, FxBuildHasher>,
    used_bytes: usize,
    max_memory_bytes: usize,
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
            let Some((evicted_key, evicted_entry)) = self.entries.pop_lru() else {
                break;
            };

            self.used_bytes -= evicted_key.len() + evicted_entry.value.len();
        }
    }

    fn get_at(&mut self, key: &[u8], now: Instant) -> Option<Bytes> {
        let expired = self
            .entries
            .get(key)
            .is_some_and(|entry| entry.is_expired_at(now));

        if expired {
            self.remove_entry(key);
            return None;
        }

        self.entries.get(key).map(|entry| entry.value.clone())
    }

    fn delete_at(&mut self, key: &[u8], now: Instant) -> bool {
        let expired = self
            .entries
            .get(key)
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
}
