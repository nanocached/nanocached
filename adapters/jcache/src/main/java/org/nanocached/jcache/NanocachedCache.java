package org.nanocached.jcache;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.management.JMException;
import javax.management.ObjectName;
import org.nanocached.NanocachedClient;

/**
 * One JCache {@link Cache}, backed by one nanocached namespace (issue
 * #118) — the cache name <em>is</em> the namespace, {@link #clear()} is
 * the namespace's {@code CLEAR}, and get/put/remove are the SDK's
 * namespaced get/set/delete.
 *
 * <p><b>Honest subset — what this cannot do:</b>
 *
 * <ul>
 *   <li>{@link #iterator()} always throws {@link
 *       UnsupportedOperationException}: the wire protocol has no key
 *       enumeration, so this cache cannot be listed;
 *   <li>{@link #invoke}/{@link #invokeAll} (entry processors) always
 *       throw {@link UnsupportedOperationException} — out of scope for
 *       this adapter;
 *   <li>read-through ({@code CacheLoader}) and write-through ({@code
 *       CacheWriter}) are rejected at {@link NanocachedCacheManager#createCache};
 *   <li>{@code getExpiryForUpdate()} returning {@code null} is supposed
 *       to mean "leave the current TTL unchanged", but the wire has no
 *       way to read a key's remaining TTL outside of {@code INCR}'s own
 *       response — a {@code null} update policy is treated as "no
 *       expiry" (eternal) instead, which is not faithful to the spec;
 *   <li>{@link CacheEntryListener}s only ever see mutations <em>this
 *       cache instance itself</em> performed — never another
 *       client/JVM's changes to the same namespace, and never {@code
 *       EXPIRED} events (server-side TTL expiry isn't observable).
 *       {@link #put}'s update branch and {@link #getAndPut}/{@link
 *       #getAndReplace}/{@link #getAndRemove}/3-argument {@link
 *       #replace(Object, Object, Object)}/{@link #remove(Object, Object)}
 *       fire {@code Updated}/{@code Removed} events with an accurate old
 *       value; the 2-argument {@link #replace(Object, Object)} fires
 *       {@code Updated} with the old value unavailable (a single
 *       CAS round trip, not a read-then-write); {@link #removeAll()}
 *       fires no per-entry events at all — enumerating what a bulk
 *       {@code CLEAR} actually removed would need a client-side key
 *       registry this adapter does not keep, so it behaves exactly like
 *       {@link #clear()}.
 * </ul>
 *
 * <p><b>{@code getAndPut}/{@code getAndReplace}/{@code getAndRemove}
 * are genuinely atomic</b>, unlike the caveats {@code nanocached-spring}
 * had to carry before issue #141 shipped compare-and-set: each is a
 * bounded compare-and-set retry loop (read a token, attempt the
 * conditioned write, retry on a concurrent change) — see {@link
 * #MAX_CAS_RETRIES}.
 */
final class NanocachedCache<K, V> implements Cache<K, V> {

    /** Bound on the {@code getAnd*} CAS retry loops — see the class doc.
     * Under pathological sustained contention on one key, the loop falls
     * back to a single unconditional write/delete so the call still
     * makes progress, documented in the README as a best-effort
     * fallback. */
    private static final int MAX_CAS_RETRIES = 10;

    private static final long NO_EXPIRY = 0L;

    private final NanocachedCacheManager manager;
    private final String name;
    private final NanocachedClient.Namespace namespace;
    private final CompleteConfiguration<K, V> configuration;
    private final ExpiryPolicy expiryPolicy;
    private final CopyOnWriteArrayList<RegisteredListener<K, V>> listeners = new CopyOnWriteArrayList<>();
    private final NanocachedCacheStatistics statistics = new NanocachedCacheStatistics();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile ObjectName statisticsObjectName;

    NanocachedCache(
            NanocachedCacheManager manager,
            String name,
            NanocachedClient.Namespace namespace,
            CompleteConfiguration<K, V> configuration) {
        this.manager = manager;
        this.name = name;
        this.namespace = namespace;
        this.configuration = configuration;
        this.expiryPolicy = configuration.getExpiryPolicyFactory().create();
        for (CacheEntryListenerConfiguration<K, V> listenerConfig :
                configuration.getCacheEntryListenerConfigurations()) {
            addListener(listenerConfig);
        }
        if (configuration.isStatisticsEnabled()) {
            setStatisticsEnabled(true);
        }
    }

    // ── reads ───────────────────────────────────────────────────────

    @Override
    public V get(K key) {
        requireNotClosed();
        Objects.requireNonNull(key, "key");
        byte[] keyBytes = KeyCodec.toKeyBytes(key);
        Optional<byte[]> raw = namespace.getBytes(keyBytes);
        if (raw.isEmpty()) {
            statistics.recordMiss();
            return null;
        }
        statistics.recordHit();
        refreshAccessExpiryIfConfigured(keyBytes, raw.get());
        return ValueCodec.deserialize(raw.get());
    }

    /**
     * Batches the wire round trip for keys the SDK's bulk {@code getManyBytes} can carry losslessly
     * (issue #152) — its keys are UTF-8 strings, and only {@link KeyCodec}'s typed-key branch ({@code
     * String}/{@code Number}/{@code Boolean}/{@code Character}/{@code UUID}, encoded there with {@code
     * getBytes(UTF_8)} already) is guaranteed to round-trip through a UTF-8 decode/re-encode unchanged.
     * A {@code byte[]} key is arbitrary opaque bytes the caller controls, and the JDK-serialization
     * fallback branch starts with non-UTF-8 magic bytes — both fall back to the per-key {@link #get}
     * path below, exactly as before this change, so no key type loses correctness for a batching win.
     */
    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        requireNotClosed();
        Objects.requireNonNull(keys, "keys");
        Map<K, V> result = new HashMap<>();
        if (keys.isEmpty()) {
            return result;
        }
        List<K> bulkKeys = new ArrayList<>();
        List<String> wireKeys = new ArrayList<>();
        for (K key : keys) {
            if (isBulkSafeKey(key)) {
                bulkKeys.add(key);
                wireKeys.add(new String(KeyCodec.toKeyBytes(key), StandardCharsets.UTF_8));
            }
        }
        if (!wireKeys.isEmpty()) {
            Map<String, byte[]> raw = namespace.getManyBytes(wireKeys);
            for (int i = 0; i < bulkKeys.size(); i++) {
                K key = bulkKeys.get(i);
                byte[] value = raw.get(wireKeys.get(i));
                if (value == null) {
                    statistics.recordMiss();
                    continue;
                }
                statistics.recordHit();
                refreshAccessExpiryIfConfigured(KeyCodec.toKeyBytes(key), value);
                result.put(key, ValueCodec.deserialize(value));
            }
        }
        for (K key : keys) {
            if (!isBulkSafeKey(key)) {
                V value = get(key);
                if (value != null) {
                    result.put(key, value);
                }
            }
        }
        return result;
    }

    /** See {@link #getAll(Set)}'s doc — whether {@code key}'s {@link KeyCodec#toKeyBytes} output is
     * guaranteed valid UTF-8 that round-trips through the bulk SDK's string-keyed wire API. */
    private static boolean isBulkSafeKey(Object key) {
        return key instanceof String
                || key instanceof Number
                || key instanceof Boolean
                || key instanceof Character
                || key instanceof UUID;
    }

    @Override
    public boolean containsKey(K key) {
        requireNotClosed();
        Objects.requireNonNull(key, "key");
        return namespace.getBytes(KeyCodec.toKeyBytes(key)).isPresent();
    }

    /** {@code getExpiryForAccess()} is evaluated on every {@code get} —
     * unlike the .NET adapter's per-call sliding expiration, JCache's
     * {@link ExpiryPolicy} is fixed per cache, so there is no per-entry
     * envelope to maintain: a non-null access policy just means
     * unconditionally re-setting the TTL to that fixed duration. This
     * re-set is a plain (non-conditional) write, so a concurrent writer
     * landing between the read and this refresh could, rarely, be
     * clobbered back to the value this {@code get} observed — documented
     * in the README. */
    private void refreshAccessExpiryIfConfigured(byte[] keyBytes, byte[] rawValue) {
        Duration accessExpiry = expiryPolicy.getExpiryForAccess();
        if (accessExpiry == null) {
            return;
        }
        OptionalLong ttl = wireTtlSeconds(accessExpiry);
        if (ttl.isEmpty()) {
            namespace.delete(keyBytes);
        } else {
            namespace.set(keyBytes, rawValue, ttl.getAsLong());
        }
    }

    // ── writes ──────────────────────────────────────────────────────

    @Override
    public void put(K key, V value) {
        requireNotClosed();
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        byte[] keyBytes = KeyCodec.toKeyBytes(key);
        byte[] valueBytes = ValueCodec.serialize(value);

        Optional<NanocachedClient.CasEntry> existing = namespace.getWithToken(keyBytes);
        Duration expiry =
                existing.isPresent() ? expiryPolicy.getExpiryForUpdate() : expiryPolicy.getExpiryForCreation();
        OptionalLong ttl = wireTtlSeconds(expiry);
        if (ttl.isEmpty()) {
            if (existing.isPresent()) {
                namespace.delete(keyBytes);
            }
            return;
        }
        namespace.set(keyBytes, valueBytes, ttl.getAsLong());
        statistics.recordPut();
        if (existing.isPresent()) {
            fireUpdated(key, value, ValueCodec.deserialize(existing.get().value()));
        } else {
            fireCreated(key, value);
        }
    }

    @Override
    public V getAndPut(K key, V value) {
        requireNotClosed();
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        byte[] keyBytes = KeyCodec.toKeyBytes(key);
        byte[] valueBytes = ValueCodec.serialize(value);

        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            Optional<NanocachedClient.CasEntry> existing = namespace.getWithToken(keyBytes);
            if (existing.isEmpty()) {
                OptionalLong ttl = wireTtlSeconds(expiryPolicy.getExpiryForCreation());
                if (ttl.isEmpty()) {
                    return null;
                }
                if (namespace.putIfAbsent(keyBytes, valueBytes, ttl.getAsLong())) {
                    statistics.recordPut();
                    fireCreated(key, value);
                    return null;
                }
                continue;
            }
            V oldValue = ValueCodec.deserialize(existing.get().value());
            OptionalLong ttl = wireTtlSeconds(expiryPolicy.getExpiryForUpdate());
            if (ttl.isEmpty()) {
                if (namespace.deleteIfMatches(keyBytes, existing.get().token())) {
                    statistics.recordRemove();
                    fireRemoved(key, oldValue);
                    return oldValue;
                }
                continue;
            }
            if (namespace.replace(keyBytes, existing.get().token(), valueBytes, ttl.getAsLong())) {
                statistics.recordPut();
                fireUpdated(key, value, oldValue);
                return oldValue;
            }
        }
        return fallbackOverwrite(keyBytes, valueBytes, key, value);
    }

    /** Pathological, sustained contention on one key: give up
     * compare-and-set and just overwrite unconditionally, so the call
     * still makes progress. The "previous value" this returns may be
     * stale by the time the overwrite lands — documented in the README. */
    private V fallbackOverwrite(byte[] keyBytes, byte[] valueBytes, K key, V value) {
        Optional<NanocachedClient.CasEntry> fallback = namespace.getWithToken(keyBytes);
        Duration expiry =
                fallback.isPresent() ? expiryPolicy.getExpiryForUpdate() : expiryPolicy.getExpiryForCreation();
        OptionalLong ttl = wireTtlSeconds(expiry);
        if (ttl.isEmpty()) {
            if (fallback.isPresent()) {
                namespace.delete(keyBytes);
            }
            return fallback.map(entry -> (V) ValueCodec.deserialize(entry.value())).orElse(null);
        }
        namespace.set(keyBytes, valueBytes, ttl.getAsLong());
        statistics.recordPut();
        V oldValue = fallback.map(entry -> (V) ValueCodec.deserialize(entry.value())).orElse(null);
        if (oldValue != null) {
            fireUpdated(key, value, oldValue);
        } else {
            fireCreated(key, value);
        }
        return oldValue;
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        requireNotClosed();
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        byte[] keyBytes = KeyCodec.toKeyBytes(key);
        byte[] valueBytes = ValueCodec.serialize(value);
        OptionalLong ttl = wireTtlSeconds(expiryPolicy.getExpiryForCreation());
        if (ttl.isEmpty()) {
            return false;
        }
        boolean stored = namespace.putIfAbsent(keyBytes, valueBytes, ttl.getAsLong());
        if (stored) {
            statistics.recordPut();
            fireCreated(key, value);
        }
        return stored;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        requireNotClosed();
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(oldValue, "oldValue");
        Objects.requireNonNull(newValue, "newValue");
        byte[] keyBytes = KeyCodec.toKeyBytes(key);
        String expectedToken = NanocachedClient.contentDigest(ValueCodec.serialize(oldValue));
        OptionalLong ttl = wireTtlSeconds(expiryPolicy.getExpiryForUpdate());
        if (ttl.isEmpty()) {
            boolean removed = namespace.deleteIfMatches(keyBytes, expectedToken);
            if (removed) {
                statistics.recordRemove();
                fireRemoved(key, oldValue);
            }
            return removed;
        }
        byte[] newValueBytes = ValueCodec.serialize(newValue);
        boolean replaced = namespace.replace(keyBytes, expectedToken, newValueBytes, ttl.getAsLong());
        if (replaced) {
            statistics.recordPut();
            fireUpdated(key, newValue, oldValue);
        }
        return replaced;
    }

    @Override
    public boolean replace(K key, V value) {
        requireNotClosed();
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        byte[] keyBytes = KeyCodec.toKeyBytes(key);
        OptionalLong ttl = wireTtlSeconds(expiryPolicy.getExpiryForUpdate());
        if (ttl.isEmpty()) {
            boolean removed = namespace.delete(keyBytes);
            if (removed) {
                statistics.recordRemove();
            }
            return removed;
        }
        byte[] valueBytes = ValueCodec.serialize(value);
        boolean replaced = namespace.replaceIfPresent(keyBytes, valueBytes, ttl.getAsLong());
        if (replaced) {
            statistics.recordPut();
            // No old value without an extra read — see the class doc's
            // "honest subset" list.
            fireUpdatedWithoutOldValue(key, value);
        }
        return replaced;
    }

    @Override
    public V getAndReplace(K key, V value) {
        requireNotClosed();
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        byte[] keyBytes = KeyCodec.toKeyBytes(key);
        byte[] valueBytes = ValueCodec.serialize(value);

        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            Optional<NanocachedClient.CasEntry> existing = namespace.getWithToken(keyBytes);
            if (existing.isEmpty()) {
                return null;
            }
            V oldValue = ValueCodec.deserialize(existing.get().value());
            OptionalLong ttl = wireTtlSeconds(expiryPolicy.getExpiryForUpdate());
            if (ttl.isEmpty()) {
                if (namespace.deleteIfMatches(keyBytes, existing.get().token())) {
                    statistics.recordRemove();
                    fireRemoved(key, oldValue);
                    return oldValue;
                }
                continue;
            }
            if (namespace.replace(keyBytes, existing.get().token(), valueBytes, ttl.getAsLong())) {
                statistics.recordPut();
                fireUpdated(key, value, oldValue);
                return oldValue;
            }
        }
        Optional<NanocachedClient.CasEntry> fallback = namespace.getWithToken(keyBytes);
        if (fallback.isEmpty()) {
            return null;
        }
        V oldValue = ValueCodec.deserialize(fallback.get().value());
        OptionalLong ttl = wireTtlSeconds(expiryPolicy.getExpiryForUpdate());
        if (ttl.isEmpty()) {
            namespace.delete(keyBytes);
            statistics.recordRemove();
            fireRemoved(key, oldValue);
        } else {
            namespace.set(keyBytes, valueBytes, ttl.getAsLong());
            statistics.recordPut();
            fireUpdated(key, value, oldValue);
        }
        return oldValue;
    }

    /**
     * Left as a per-key loop (issue #152 audit): unlike {@link #getAll}, batching this would need a
     * bulk write with one TTL per call ({@code setManyBytes}), but each entry here can resolve to a
     * different TTL ({@code expiryForCreation} vs. {@code expiryForUpdate}, themselves not necessarily
     * equal across entries) and {@link #put}'s CAS-metadata read/old-value tracking for listeners is
     * still per key — a correct batched form would need grouping by resolved TTL plus the same
     * bulk-safe-key split {@link #getAll} uses, for a write path this adapter's callers rarely batch
     * at meaningful size. Not attempted here; revisit if profiling shows it matters.
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        requireNotClosed();
        Objects.requireNonNull(map, "map");
        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    // ── removes ─────────────────────────────────────────────────────

    @Override
    public boolean remove(K key) {
        requireNotClosed();
        Objects.requireNonNull(key, "key");
        byte[] keyBytes = KeyCodec.toKeyBytes(key);
        if (listeners.isEmpty()) {
            boolean deleted = namespace.delete(keyBytes);
            if (deleted) {
                statistics.recordRemove();
            }
            return deleted;
        }
        // Only pay for the extra read when a listener could actually use
        // the old value.
        Optional<byte[]> raw = namespace.getBytes(keyBytes);
        boolean deleted = namespace.delete(keyBytes);
        if (deleted) {
            statistics.recordRemove();
            raw.ifPresent(bytes -> fireRemoved(key, ValueCodec.deserialize(bytes)));
        }
        return deleted;
    }

    @Override
    public boolean remove(K key, V oldValue) {
        requireNotClosed();
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(oldValue, "oldValue");
        byte[] keyBytes = KeyCodec.toKeyBytes(key);
        String expectedToken = NanocachedClient.contentDigest(ValueCodec.serialize(oldValue));
        boolean removed = namespace.deleteIfMatches(keyBytes, expectedToken);
        if (removed) {
            statistics.recordRemove();
            fireRemoved(key, oldValue);
        }
        return removed;
    }

    @Override
    public V getAndRemove(K key) {
        requireNotClosed();
        Objects.requireNonNull(key, "key");
        byte[] keyBytes = KeyCodec.toKeyBytes(key);

        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            Optional<NanocachedClient.CasEntry> existing = namespace.getWithToken(keyBytes);
            if (existing.isEmpty()) {
                return null;
            }
            V oldValue = ValueCodec.deserialize(existing.get().value());
            if (namespace.deleteIfMatches(keyBytes, existing.get().token())) {
                statistics.recordRemove();
                fireRemoved(key, oldValue);
                return oldValue;
            }
        }
        Optional<NanocachedClient.CasEntry> fallback = namespace.getWithToken(keyBytes);
        if (fallback.isEmpty()) {
            return null;
        }
        namespace.delete(keyBytes);
        V oldValue = ValueCodec.deserialize(fallback.get().value());
        statistics.recordRemove();
        fireRemoved(key, oldValue);
        return oldValue;
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        requireNotClosed();
        Objects.requireNonNull(keys, "keys");
        for (K key : keys) {
            remove(key);
        }
    }

    /** Maps to the namespace's bulk {@code CLEAR}, same as {@link
     * #clear()} — see the class doc's "honest subset" list for why this
     * cannot fire per-entry {@code Removed} events the way the spec
     * expects (no client-side key registry to enumerate what was
     * removed). */
    @Override
    public void removeAll() {
        requireNotClosed();
        namespace.clear();
    }

    @Override
    public void clear() {
        requireNotClosed();
        namespace.clear();
    }

    // ── unsupported (issue #118 scope) ─────────────────────────────

    @Override
    public Iterator<Cache.Entry<K, V>> iterator() {
        throw new UnsupportedOperationException(
                "nanocached-jcache: no key enumeration on the wire protocol — this cache cannot be"
                        + " iterated");
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments)
            throws EntryProcessorException {
        throw new UnsupportedOperationException("nanocached-jcache: entry processors are not supported");
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(
            Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        throw new UnsupportedOperationException("nanocached-jcache: entry processors are not supported");
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        requireNotClosed();
        // No CacheLoader is ever configured (rejected at createCache), so
        // per spec this completes immediately with nothing to do.
        if (completionListener != null) {
            completionListener.onCompletion();
        }
    }

    // ── configuration / lifecycle ───────────────────────────────────

    @Override
    @SuppressWarnings("unchecked")
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        if (clazz.isInstance(configuration)) {
            return (C) configuration;
        }
        throw new IllegalArgumentException("nanocached-jcache: configuration cannot be represented as " + clazz);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public CacheManager getCacheManager() {
        return manager;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            unregisterStatisticsMBean();
            manager.unregister(name);
        }
    }

    /** Called by {@link NanocachedCacheManager} when it (not this cache
     * directly) is the one tearing this cache down — already removed
     * from the manager's map by the caller, so this must not re-enter
     * {@link NanocachedCacheManager#unregister}. */
    void markClosed() {
        if (closed.compareAndSet(false, true)) {
            unregisterStatisticsMBean();
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return (T) this;
        }
        throw new IllegalArgumentException("nanocached-jcache: cannot unwrap Cache as " + clazz);
    }

    // ── listeners (local mutations only — see the class doc) ───────

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> config) {
        requireNotClosed();
        addListener(Objects.requireNonNull(config, "cacheEntryListenerConfiguration"));
    }

    private void addListener(CacheEntryListenerConfiguration<K, V> config) {
        CacheEntryListener<? super K, ? super V> listenerInstance = config.getCacheEntryListenerFactory().create();
        CacheEntryEventFilter<? super K, ? super V> filterInstance = config.getCacheEntryEventFilterFactory() != null
                ? config.getCacheEntryEventFilterFactory().create()
                : null;
        listeners.add(new RegisteredListener<>(config, listenerInstance, filterInstance));
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> config) {
        requireNotClosed();
        listeners.removeIf(registered -> registered.config().equals(config));
    }

    private void fireCreated(K key, V value) {
        fire(EventType.CREATED, key, value, null, false);
    }

    private void fireUpdated(K key, V value, V oldValue) {
        fire(EventType.UPDATED, key, value, oldValue, true);
    }

    private void fireUpdatedWithoutOldValue(K key, V value) {
        fire(EventType.UPDATED, key, value, null, false);
    }

    private void fireRemoved(K key, V oldValue) {
        fire(EventType.REMOVED, key, null, oldValue, true);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void fire(EventType type, K key, V value, V oldValue, boolean oldValueAvailable) {
        if (listeners.isEmpty()) {
            return;
        }
        NanocachedCacheEntryEvent<K, V> event =
                new NanocachedCacheEntryEvent<>(this, type, key, value, oldValue, oldValueAvailable);
        List<CacheEntryEvent<? extends K, ? extends V>> events = List.of(event);
        for (RegisteredListener<K, V> registered : listeners) {
            if (registered.filter() != null && !registered.filter().evaluate(event)) {
                continue;
            }
            try {
                switch (type) {
                    case CREATED -> {
                        if (registered.listener() instanceof CacheEntryCreatedListener<?, ?> listener) {
                            ((CacheEntryCreatedListener) listener).onCreated((Iterable) events);
                        }
                    }
                    case UPDATED -> {
                        if (registered.listener() instanceof CacheEntryUpdatedListener<?, ?> listener) {
                            ((CacheEntryUpdatedListener) listener).onUpdated((Iterable) events);
                        }
                    }
                    case REMOVED -> {
                        if (registered.listener() instanceof CacheEntryRemovedListener<?, ?> listener) {
                            ((CacheEntryRemovedListener) listener).onRemoved((Iterable) events);
                        }
                    }
                    default -> {
                        // EXPIRED is never fired — server-side TTL expiry
                        // isn't observable by this adapter (see the class doc).
                    }
                }
            } catch (CacheEntryListenerException e) {
                // A misbehaving listener must not corrupt the write that
                // already succeeded on this synchronous, local-mutation-only
                // path — log and keep going.
                System.err.println("nanocached-jcache: listener threw for " + name + ": " + e);
            }
        }
    }

    // ── statistics ──────────────────────────────────────────────────

    void setStatisticsEnabled(boolean enabled) {
        if (enabled) {
            registerStatisticsMBean();
        } else {
            unregisterStatisticsMBean();
        }
    }

    private void registerStatisticsMBean() {
        if (statisticsObjectName != null) {
            return;
        }
        try {
            ObjectName objectName = new ObjectName("javax.cache:type=CacheStatistics,CacheManager="
                    + ObjectName.quote(manager.getURI().toString()) + ",Cache=" + ObjectName.quote(name));
            ManagementFactory.getPlatformMBeanServer().registerMBean(statistics, objectName);
            statisticsObjectName = objectName;
        } catch (JMException e) {
            throw new CacheException("nanocached-jcache: failed to register statistics MBean", e);
        }
    }

    private void unregisterStatisticsMBean() {
        ObjectName objectName = statisticsObjectName;
        if (objectName == null) {
            return;
        }
        statisticsObjectName = null;
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(objectName);
        } catch (JMException ignored) {
            // Already gone — fine (e.g. the manager tore everything down first).
        }
    }

    // ── shared helpers ──────────────────────────────────────────────

    /** Converts a JSR-107 {@link Duration} to this wire's {@code
     * ttlSeconds} convention (0 = no expiry), rounding a positive
     * sub-second duration up to 1s (same rule {@code nanocached-spring}'s
     * {@code toTtlSeconds} uses) — or {@link OptionalLong#empty()} for
     * {@link Duration#ZERO}, whose JSR-107 meaning is "don't retain this
     * entry at all", which the caller must implement as "don't write /
     * delete instead", never as a wire TTL. */
    private static OptionalLong wireTtlSeconds(Duration duration) {
        if (duration == null || duration.isEternal()) {
            return OptionalLong.of(NO_EXPIRY);
        }
        if (duration.isZero()) {
            return OptionalLong.empty();
        }
        long seconds = duration.getTimeUnit().toSeconds(duration.getDurationAmount());
        return OptionalLong.of(Math.max(1L, seconds));
    }

    private void requireNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException("nanocached-jcache: this Cache is closed");
        }
    }

    private record RegisteredListener<K, V>(
            CacheEntryListenerConfiguration<K, V> config,
            CacheEntryListener<? super K, ? super V> listener,
            CacheEntryEventFilter<? super K, ? super V> filter) {}
}
