package org.nanocached.spring;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
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
 *   <li>{@link #get(Object, Callable)} computes on a miss once per key
 *       per JVM: concurrent callers for the same key wait for the one
 *       loader already running and share its result, while callers for
 *       other keys are never coupled to it. Processes sharing the
 *       cluster may still compute the same key concurrently (last write
 *       wins) — the usual read-through cache-stampede trade-off, same as
 *       Spring's own non-distributed adapters.
 * </ul>
 */
public final class NanocachedCache extends AbstractValueAdaptingCache {

    /** One loader run of {@link #get(Object, Callable)} in flight for a
     * key: the thread running it (so a loader that re-enters the same
     * key on the same thread is reported instead of waiting on itself)
     * and its outcome. */
    private record InFlightLoad(Thread owner, CompletableFuture<Object> outcome) {}

    private final String name;
    private final NanocachedClient.Namespace namespace;
    private final CacheValueSerializer serializer;
    private final CacheKeyConverter keyConverter;
    private final long ttlSeconds;
    /** The loader runs of {@link #get(Object, Callable)} in flight in
     * this JVM, by Spring key. No lock is held while a loader runs: a
     * lock held across user code coupled unrelated keys (a fixed number
     * of lock stripes shared by every key), so a sync loader that itself
     * read another sync-cached key could deadlock against a second
     * thread doing the reverse, with no logical cycle between the four
     * keys; and a monitor held across the network round trip pinned
     * virtual threads. Entries exist only while a load runs, so there is
     * nothing to leak. */
    private final ConcurrentHashMap<Object, InFlightLoad> loads = new ConcurrentHashMap<>();

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
    }

    /** {@code Duration.ZERO} means "no expiry" (the SDK's own 0-TTL
     * convention); any positive TTL rounds UP to whole seconds so an entry
     * never expires earlier than asked — a sub-second TTL becomes 1s, and
     * 2.5s becomes 3s, not 2s. Matches the ceiling every other adapter uses
     * (cache-manager/keyv {@code Math.ceil}, Django {@code math.ceil}, .NET
     * {@code Math.Ceiling}); {@code Duration.toSeconds()} floors, which
     * would expire a 2.5s entry up to half a second early. */
    private static long toTtlSeconds(Duration ttl) {
        if (ttl.isZero()) {
            return 0;
        }
        long seconds = ttl.getSeconds();
        if (ttl.getNano() > 0) {
            seconds++;
        }
        return Math.max(1, seconds);
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

    /**
     * Read-through: a miss runs {@code valueLoader} once per key per JVM
     * — a concurrent caller for the same key waits for that run and
     * shares its result (or, if the run failed, loads on its own) —
     * without holding any lock across the loader or the network, so
     * loaders that read other keys of this cache can never deadlock each
     * other and a virtual thread is never pinned. A loader that re-enters
     * {@code get} for its own key on the same thread is a programming
     * error and throws {@link IllegalStateException} rather than waiting
     * on itself.
     *
     * <p>Only the loader's own failure becomes a {@link
     * ValueRetrievalException}. A failure of the {@link #put} that
     * follows a successful load (the node went away between the miss and
     * the store, or the value is over the server's size limit) is thrown
     * as-is instead — exactly as Spring's own Redis cache does — because
     * the cache interceptor rethrows a {@code ValueRetrievalException}
     * without consulting the application's {@code CacheErrorHandler},
     * while any other runtime exception from this method reaches that
     * handler, which then decides (after which the interceptor invokes
     * the method again to produce the value). The non-sync path reports
     * the same failure through the handler's put-error callback.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Object key, Callable<T> valueLoader) {
        Object cached = lookup(key);
        if (cached != null) {
            return (T) fromStoreValue(cached);
        }

        Object loadKey = key == null ? NullKey.INSTANCE : key;
        while (true) {
            InFlightLoad mine = new InFlightLoad(Thread.currentThread(), new CompletableFuture<>());
            InFlightLoad running = loads.putIfAbsent(loadKey, mine);
            if (running == null) {
                try {
                    return (T) load(key, valueLoader, mine.outcome());
                } finally {
                    loads.remove(loadKey, mine);
                }
            }
            if (running.owner() == Thread.currentThread()) {
                throw new IllegalStateException(
                        "nanocached-spring: the value loader for key " + key
                                + " re-entered get(key, valueLoader) for the same key on the same thread");
            }
            try {
                return (T) fromStoreValue(running.outcome().join());
            } catch (CompletionException failed) {
                // The run we waited for failed; its caller got the
                // exception. Loading on our own is the honest outcome
                // for this caller — the loop re-checks the cache first.
                cached = lookup(key);
                if (cached != null) {
                    return (T) fromStoreValue(cached);
                }
            }
        }
    }

    /** The one loader run for a key: double-checks the cache, runs the
     * loader, stores the value, and settles {@code outcome} (the store
     * value, so waiters go through {@link #fromStoreValue} exactly like
     * a hit) for every caller waiting on it. */
    private Object load(Object key, Callable<?> valueLoader, CompletableFuture<Object> outcome) {
        try {
            Object cached = lookup(key);
            if (cached != null) {
                outcome.complete(cached);
                return fromStoreValue(cached);
            }
            Object loaded;
            try {
                loaded = valueLoader.call();
            } catch (Exception e) {
                throw new ValueRetrievalException(key, valueLoader, e);
            }
            put(key, loaded);
            outcome.complete(toStoreValue(loaded));
            return loaded;
        } catch (RuntimeException e) {
            outcome.completeExceptionally(e);
            throw e;
        }
    }

    /** Stand-in map key for a {@code null} Spring key (ConcurrentHashMap
     * rejects null keys; Spring allows them). */
    private enum NullKey {
        INSTANCE
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
     *
     * <p><b>Read-back race:</b> the follow-up read is a second, separate
     * round trip — it is not part of the same atomic operation as the
     * conditioned write. Between the write losing and the read
     * completing, another writer can {@link #evict}, {@link #clear}, or
     * overwrite the key (e.g. via its own {@code putIfAbsent} win racing
     * with a concurrent {@link #evict}, or a TTL expiry landing in that
     * window). Callers may therefore observe a returned {@link
     * ValueWrapper} that is stale (holds a value already superseded by
     * the time it's returned) or {@code null} for a key that was in fact
     * present at the moment the write lost — the wire truth at write time
     * is captured correctly, but what this method <em>reports</em> back
     * can lag it. This mirrors the read half of every other
     * read-after-write path in this adapter (e.g. {@link #lookup}); only
     * the conditioned write itself is atomic.
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
