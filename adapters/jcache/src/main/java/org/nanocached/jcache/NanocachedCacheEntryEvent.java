package org.nanocached.jcache;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.EventType;

/**
 * A locally-fired {@link CacheEntryEvent} (issue #118) — see {@link
 * NanocachedCache}'s "local-only listeners" doc section for what "locally
 * fired" means and why.
 */
final class NanocachedCacheEntryEvent<K, V> extends CacheEntryEvent<K, V> {

    private final K key;
    private final V value;
    private final V oldValue;
    private final boolean oldValueAvailable;

    NanocachedCacheEntryEvent(
            Cache<K, V> source,
            EventType eventType,
            K key,
            V value,
            V oldValue,
            boolean oldValueAvailable) {
        super(source, eventType);
        this.key = key;
        this.value = value;
        this.oldValue = oldValue;
        this.oldValueAvailable = oldValueAvailable;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V getOldValue() {
        if (!oldValueAvailable) {
            throw new UnsupportedOperationException(
                    "nanocached-jcache: old value not available for this event");
        }
        return oldValue;
    }

    @Override
    public boolean isOldValueAvailable() {
        return oldValueAvailable;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException("nanocached-jcache: cannot unwrap event as " + clazz);
    }
}
