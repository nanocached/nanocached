package org.nanocached.spring;

import java.time.Duration;
import java.util.concurrent.Callable;
import org.nanocached.NanocachedClient;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.cache.support.AbstractValueAdaptingCache;

/**
 * One Spring {@link org.springframework.cache.Cache} backed by one
 * nanocached namespace (issue #105): the cache name <em>is</em> the
 * namespace, {@link #clear()} is the namespace's {@code CLEAR} (issue
 * #106), and get/put/evict are the SDK's namespaced get/set/delete.
 *
 * <p>Null caching is handled by {@link AbstractValueAdaptingCache}: with
 * {@code allowNullValues} (the default) a cached {@code null} is stored
 * as Spring's {@code NullValue} marker through the configured
 * {@link CacheValueSerializer}.
 *
 * <p>Consistency notes:
 * <ul>
 *   <li>{@link #putIfAbsent} is the wire's compare-and-set {@code
 *       putIfAbsent} (issue #141, add-if-absent) — genuinely atomic:
 *       exactly one of two racing writers for the same key stores its
 *       value, the other gets back the winner's;
 *   <li>{@link #get(Object, Callable)} computes on a miss under a
 *       <em>per-JVM</em> striped lock — one computation per process, but
 *       processes sharing the cluster may compute the same key
 *       concurrently (last write wins). That is the usual read-through
 *       cache-stampede trade-off, same as Spring's own non-distributed
 *       adapters.
 * </ul>
 */
public final class NanocachedCache extends AbstractValueAdaptingCache {

    /** Bounded per-JVM lock striping for {@link #get(Object, Callable)}:
     * no per-key lock objects to leak, at the cost of occasional
     * false sharing between keys on the same stripe. */
    private static final int LOCK_STRIPES = 64;

    private final String name;
    private final NanocachedClient.Namespace namespace;
    private final CacheValueSerializer serializer;
    private final CacheKeyConverter keyConverter;
    private final long ttlSeconds;
    private final Object[] loaderLocks;

    NanocachedCache(
            String name,
            NanocachedClient.Namespace namespace,
            CacheValueSerializer serializer,
            CacheKeyConverter keyConverter,
            Duration ttl,
            boolean allowNullValues) {
        super(allowNullValues);
        this.name = name;
        this.namespace = namespace;
        this.serializer = serializer;
        this.keyConverter = keyConverter;
        this.ttlSeconds = toTtlSeconds(ttl);
        this.loaderLocks = new Object[LOCK_STRIPES];
        for (int i = 0; i < LOCK_STRIPES; i++) {
            loaderLocks[i] = new Object();
        }
    }

    /** {@code Duration.ZERO} means "no expiry" (the SDK's own 0-TTL
     * convention); a positive sub-second TTL rounds up to 1s rather than
     * silently becoming eternal. */
    private static long toTtlSeconds(Duration ttl) {
        if (ttl.isZero()) {
            return 0;
        }
        return Math.max(1, ttl.toSeconds());
    }

    @Override
    public String getName() {
        return name;
    }

    /** The underlying SDK namespace handle — escape hatch for callers
     * that need the raw byte API alongside the Spring one. */
    @Override
    public NanocachedClient.Namespace getNativeCache() {
        return namespace;
    }

    @Override
    protected Object lookup(Object key) {
        return namespace
                .getBytes(keyConverter.toKeyBytes(key))
                .map(serializer::deserialize)
                .orElse(null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Object key, Callable<T> valueLoader) {
        Object cached = lookup(key);
        if (cached != null) {
            return (T) fromStoreValue(cached);
        }

        // One computation per JVM per stripe: the double-check under the
        // lock keeps a herd of same-key callers down to one loader call
        // without a lock object per key.
        synchronized (lockFor(key)) {
            cached = lookup(key);
            if (cached != null) {
                return (T) fromStoreValue(cached);
            }

            T loaded;
            try {
                loaded = valueLoader.call();
            } catch (Exception e) {
                throw new ValueRetrievalException(key, valueLoader, e);
            }
            put(key, loaded);
            return loaded;
        }
    }

    private Object lockFor(Object key) {
        return loaderLocks[Math.floorMod(key == null ? 0 : key.hashCode(), LOCK_STRIPES)];
    }

    @Override
    public void put(Object key, Object value) {
        byte[] payload = serializer.serialize(toStoreValue(value));
        namespace.set(keyConverter.toKeyBytes(key), payload, ttlSeconds);
    }

    /**
     * The wire's compare-and-set {@code putIfAbsent} (issue #141): one
     * conditioned write, not Spring's default get-then-put — two racing
     * callers for the same key can no longer both observe "absent" and
     * both write.
     *
     * <p>On a lost race (the key was already present) this follows up
     * with a plain read to report the existing value, purely for the
     * return value the {@link org.springframework.cache.Cache} contract
     * promises the caller; the write itself already atomically no-opped
     * against the wire before that read happens, so a value that changes
     * between the two only affects what this call <em>reports</em>, not
     * what it stores.
     */
    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        byte[] keyBytes = keyConverter.toKeyBytes(key);
        byte[] payload = serializer.serialize(toStoreValue(value));
        if (namespace.putIfAbsent(keyBytes, payload, ttlSeconds)) {
            return null;
        }
        return toValueWrapper(lookup(key));
    }

    @Override
    public void evict(Object key) {
        namespace.delete(keyConverter.toKeyBytes(key));
    }

    @Override
    public boolean evictIfPresent(Object key) {
        return namespace.delete(keyConverter.toKeyBytes(key));
    }

    @Override
    public void clear() {
        namespace.clear();
    }
}
