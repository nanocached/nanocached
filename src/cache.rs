use std::collections::HashMap;
use std::time::{Duration, Instant};

struct Entry {
    value: Vec<u8>,
    expires_at: Option<Instant>,
}

pub struct Cache {
    entries: HashMap<Vec<u8>, Entry>,
}

impl Entry {
    fn is_expired_at(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }
}

impl Cache {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let entry = Entry {
            value,
            expires_at: None,
        };
        self.entries.insert(key, entry);
    }

    pub fn set_with_ttl(&mut self, key: Vec<u8>, value: Vec<u8>, ttl: Duration) {
        let expires_at = Instant::now() + ttl;
        let entry = Entry {
            value,
            expires_at: Some(expires_at),
        };
        self.entries.insert(key, entry);
    }

    pub fn get(&mut self, key: &[u8]) -> Option<&[u8]> {
        self.get_at(key, Instant::now())
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        self.delete_at(key, Instant::now())
    }

    fn get_at(&mut self, key: &[u8], now: Instant) -> Option<&[u8]> {
        let expired = self
            .entries
            .get(key)
            .is_some_and(|entry| entry.is_expired_at(now));

        if expired {
            self.entries.remove(key);
            return None;
        }

        self.entries.get(key).map(|entry| entry.value.as_slice())
    }

    fn delete_at(&mut self, key: &[u8], now: Instant) -> bool {
        let expired = self
            .entries
            .get(key)
            .is_some_and(|entry| entry.is_expired_at(now));

        if expired {
            self.entries.remove(key);
            return false;
        }

        self.entries.remove(key).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gets_a_previously_set_value() {
        let mut cache = Cache::new();

        cache.set(b"name".to_vec(), b"Alice".to_vec());

        assert_eq!(cache.get(b"name"), Some(b"Alice".as_slice()));
    }

    #[test]
    fn get_returns_none_for_missing_key() {
        let mut cache = Cache::new();

        assert_eq!(cache.get(b"missing"), None);
    }

    #[test]
    fn delete_returns_true_for_existing_key() {
        let mut cache = Cache::new();

        cache.set(b"name".to_vec(), b"Alice".to_vec());

        assert!(cache.delete(b"name"));
    }

    #[test]
    fn delete_returns_false_for_missing_key() {
        let mut cache = Cache::new();

        assert!(!cache.delete(b"name"));
    }

    #[test]
    fn set_overwrites_existing_value() {
        let mut cache = Cache::new();

        cache.set(b"name".to_vec(), b"Alice".to_vec());
        cache.set(b"name".to_vec(), b"Bob".to_vec());

        assert_eq!(cache.get(b"name"), Some(b"Bob".as_slice()));
    }

    #[test]
    fn deleted_value_can_no_longer_be_retrieved() {
        let mut cache = Cache::new();

        cache.set(b"name".to_vec(), b"Alice".to_vec());
        cache.delete(b"name");

        assert_eq!(cache.get(b"name"), None);
    }

    #[test]
    fn gets_a_previously_set_value_with_ttl() {
        let mut cache = Cache::new();

        cache.set_with_ttl(b"name".to_vec(), b"Alice".to_vec(), Duration::from_secs(5));

        assert_eq!(cache.get(b"name"), Some(b"Alice".as_slice()));
    }

    #[test]
    fn delete_returns_true_before_expiration() {
        let mut cache = Cache::new();

        cache.set_with_ttl(b"name".to_vec(), b"Alice".to_vec(), Duration::from_secs(5));

        assert!(cache.delete(b"name"));
    }

    #[test]
    fn get_returns_value_before_expiration() {
        let mut cache = Cache::new();

        cache.set_with_ttl(b"name".to_vec(), b"Alice".to_vec(), Duration::from_secs(5));

        let future = Instant::now() + Duration::from_secs(4);

        assert_eq!(cache.get_at(b"name", future), Some(b"Alice".as_slice()));
    }

    #[test]
    fn get_returns_none_after_expiration() {
        let mut cache = Cache::new();

        cache.set_with_ttl(b"name".to_vec(), b"Alice".to_vec(), Duration::from_secs(5));

        let future = Instant::now() + Duration::from_secs(6);

        assert_eq!(cache.get_at(b"name", future), None);
    }

    #[test]
    fn get_removes_expired_entry() {
        let mut cache = Cache::new();

        cache.set_with_ttl(b"name".to_vec(), b"Alice".to_vec(), Duration::from_secs(5));

        let future = Instant::now() + Duration::from_secs(6);

        cache.get_at(b"name", future);

        assert!(!cache.entries.contains_key(b"name".as_slice()));
    }

    #[test]
    fn delete_returns_false_after_expiration() {
        let mut cache = Cache::new();

        cache.set_with_ttl(b"name".to_vec(), b"Alice".to_vec(), Duration::from_secs(5));

        let future = Instant::now() + Duration::from_secs(6);

        assert!(!cache.delete_at(b"name", future));
    }
}
