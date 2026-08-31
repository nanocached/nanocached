package org.nanocached.jcache;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;
import org.nanocached.NanocachedClient;

/**
 * One JSR-107 {@link CacheManager}, owning one {@link NanocachedClient}
 * (issue #118) — created and closed by this manager, unlike {@code
 * nanocached-spring}'s manager, which borrows an application-owned
 * client. Each named cache is created explicitly via {@link
 * #createCache}: unlike the Spring adapter's "any name works on first
 * use", JSR-107 requires {@link #getCache} to answer {@code null} for a
 * name that was never created (or has since been destroyed).
 */
public final class NanocachedCacheManager implements CacheManager {

    private final NanocachedCachingProvider provider;
    private final URI uri;
    private final ClassLoader classLoader;
    private final NanocachedClient client;
    private final ConcurrentHashMap<String, NanocachedCache<?, ?>> caches = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    NanocachedCacheManager(
            NanocachedCachingProvider provider, URI uri, ClassLoader classLoader, NanocachedClient client) {
        this.provider = provider;
        this.uri = uri;
        this.classLoader = classLoader;
        this.client = client;
    }

    @Override
    public CachingProvider getCachingProvider() {
        return provider;
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    @Override
    public Properties getProperties() {
        return new Properties();
    }

    @Override
    public <K, V, C extends Configuration<K, V>> javax.cache.Cache<K, V> createCache(
            String cacheName, C configuration) {
        requireNotClosed();
        if (cacheName == null) {
            throw new NullPointerException("cacheName");
        }
        if (configuration == null) {
            throw new NullPointerException("configuration");
        }

        CompleteConfiguration<K, V> complete = completeConfigurationOf(configuration);
        validateSupported(complete);

        NanocachedCache<K, V> cache = new NanocachedCache<>(this, cacheName, client.namespace(cacheName), complete);

        NanocachedCache<?, ?> raced = caches.putIfAbsent(cacheName, cache);
        if (raced != null) {
            throw new CacheException(
                    "nanocached-jcache: a cache named \"" + cacheName + "\" already exists");
        }
        // Issue #331: only register the statistics MBean once this cache
        // instance has actually won the putIfAbsent race above — doing it
        // in NanocachedCache's constructor meant a losing createCache call
        // for a duplicate name would register (or attempt to register) a
        // JMX MBean before failing with the CacheException above, an
        // unrelated side effect on the losing race.
        if (complete.isStatisticsEnabled()) {
            cache.setStatisticsEnabled(true);
        }
        return cache;
    }

    /** JSR-107 accepts a plain {@link Configuration} (just key/value
     * types) as well as a {@link CompleteConfiguration}; a plain one is
     * completed with {@link MutableConfiguration}'s own defaults
     * (eternal expiry, statistics/management off, no listeners) —
     * mirroring what {@code MutableConfiguration} itself defaults to. */
    @SuppressWarnings("unchecked")
    private static <K, V> CompleteConfiguration<K, V> completeConfigurationOf(Configuration<K, V> configuration) {
        if (configuration instanceof CompleteConfiguration<K, V> complete) {
            return complete;
        }
        return new MutableConfiguration<K, V>()
                .setTypes(configuration.getKeyType(), configuration.getValueType())
                .setStoreByValue(configuration.isStoreByValue());
    }

    /** Rejects configuration this adapter cannot honor, rather than
     * silently ignoring it — the "honest subset" this module documents
     * throughout. */
    private static void validateSupported(CompleteConfiguration<?, ?> configuration) {
        if (!configuration.isStoreByValue()) {
            throw new UnsupportedOperationException(
                    "nanocached-jcache: store-by-reference is not supported — every value crosses"
                            + " the wire as bytes");
        }
        if (configuration.getCacheLoaderFactory() != null) {
            throw new UnsupportedOperationException(
                    "nanocached-jcache: read-through (CacheLoader) is not supported");
        }
        if (configuration.getCacheWriterFactory() != null) {
            throw new UnsupportedOperationException(
                    "nanocached-jcache: write-through (CacheWriter) is not supported");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> javax.cache.Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
        requireNotClosed();
        NanocachedCache<?, ?> cache = caches.get(cacheName);
        if (cache == null) {
            return null;
        }
        Configuration<?, ?> configuration = cache.getConfiguration(Configuration.class);
        if (!configuration.getKeyType().equals(keyType) || !configuration.getValueType().equals(valueType)) {
            throw new ClassCastException(
                    "nanocached-jcache: cache \"" + cacheName + "\" was created with key type "
                            + configuration.getKeyType().getName() + " and value type "
                            + configuration.getValueType().getName() + ", not " + keyType.getName() + "/"
                            + valueType.getName());
        }
        return (javax.cache.Cache<K, V>) cache;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> javax.cache.Cache<K, V> getCache(String cacheName) {
        requireNotClosed();
        return (javax.cache.Cache<K, V>) caches.get(cacheName);
    }

    @Override
    public Iterable<String> getCacheNames() {
        requireNotClosed();
        return List.copyOf(caches.keySet());
    }

    @Override
    public void destroyCache(String cacheName) {
        requireNotClosed();
        if (cacheName == null) {
            throw new NullPointerException("cacheName");
        }
        NanocachedCache<?, ?> cache = caches.remove(cacheName);
        if (cache != null) {
            cache.clear();
            cache.markClosed();
        }
    }

    /** Called by {@link NanocachedCache#close()} when a cache is closed
     * directly rather than via {@link #destroyCache} — deregisters it
     * without touching its data (closing a {@code Cache} handle must not
     * delete what it holds; only {@link #destroyCache} does that). */
    void unregister(String cacheName) {
        caches.remove(cacheName);
    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {
        requireNotClosed();
        // Not implemented (issue #118 scope cut): CacheMXBean reports
        // static configuration, which offers little value here — see the
        // README's "Honest subset" section. isManagementEnabled() always
        // reports false, regardless of this call.
    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {
        requireNotClosed();
        NanocachedCache<?, ?> cache = caches.get(cacheName);
        if (cache != null) {
            cache.setStatisticsEnabled(enabled);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            List<NanocachedCache<?, ?>> snapshot = new ArrayList<>(caches.values());
            caches.clear();
            snapshot.forEach(NanocachedCache::markClosed);
            client.close();
            provider.forget(uri, classLoader);
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        }
        if (clazz.isInstance(client)) {
            return clazz.cast(client);
        }
        throw new IllegalArgumentException("nanocached-jcache: cannot unwrap CacheManager as " + clazz);
    }

    private void requireNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException("nanocached-jcache: this CacheManager is closed");
        }
    }
}
