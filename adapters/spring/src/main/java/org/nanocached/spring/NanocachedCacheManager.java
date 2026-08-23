package org.nanocached.spring;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.nanocached.NanocachedClient;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

/**
 * Spring {@link CacheManager} on top of a {@link NanocachedClient}
 * (issue #107): each named cache is one nanocached namespace, created on
 * first use — namespaces need no server-side setup, so by default any
 * cache name works ({@code @Cacheable("anything")}); restrict with
 * {@link Builder#cacheNames}. The client is borrowed, not owned: closing
 * it is the caller's job (typically a Spring {@code @Bean(destroyMethod
 * = "close")} on the client itself).
 *
 * <p>Policy note: framework adapters like this module are
 * ecosystem-specific and live <em>outside</em> the six-language SDK
 * parity policy (#25) — parity applies to the SDK core only.
 */
public final class NanocachedCacheManager implements CacheManager {

    private final NanocachedClient client;
    private final CacheValueSerializer serializer;
    private final CacheKeyConverter keyConverter;
    private final Duration defaultTtl;
    private final Map<String, Duration> ttlByCache;
    private final boolean allowNullValues;
    /** Null = dynamic mode (any name); non-null = the closed set of
     * allowed names, eagerly created so getCacheNames() is complete. */
    private final Set<String> fixedCacheNames;
    private final ConcurrentHashMap<String, NanocachedCache> caches = new ConcurrentHashMap<>();

    private NanocachedCacheManager(Builder builder) {
        this.client = builder.client;
        this.serializer = builder.serializer;
        this.keyConverter = builder.keyConverter;
        this.defaultTtl = builder.defaultTtl;
        this.ttlByCache = Map.copyOf(builder.ttlByCache);
        this.allowNullValues = builder.allowNullValues;
        this.fixedCacheNames =
                builder.fixedCacheNames == null
                        ? null
                        : Collections.unmodifiableSet(new LinkedHashSet<>(builder.fixedCacheNames));
        if (fixedCacheNames != null) {
            fixedCacheNames.forEach(name -> caches.put(name, createCache(name)));
        }
    }

    public static Builder builder(NanocachedClient client) {
        return new Builder(client);
    }

    @Override
    public Cache getCache(String name) {
        if (fixedCacheNames != null) {
            // The Spring contract for a fixed-name manager: unknown name
            // means "this manager doesn't handle it" (null), letting a
            // CompositeCacheManager fall through — not an error.
            return caches.get(name);
        }
        return caches.computeIfAbsent(name, this::createCache);
    }

    @Override
    public Collection<String> getCacheNames() {
        return fixedCacheNames != null
                ? fixedCacheNames
                : Collections.unmodifiableSet(caches.keySet());
    }

    private NanocachedCache createCache(String name) {
        return new NanocachedCache(
                name,
                client.namespace(name),
                serializer,
                keyConverter,
                ttlByCache.getOrDefault(name, defaultTtl),
                allowNullValues);
    }

    public static final class Builder {
        private final NanocachedClient client;
        private CacheValueSerializer serializer = new JdkCacheValueSerializer();
        private CacheKeyConverter keyConverter = new DefaultCacheKeyConverter();
        private Duration defaultTtl = Duration.ZERO;
        private final Map<String, Duration> ttlByCache = new HashMap<>();
        private boolean allowNullValues = true;
        private Set<String> fixedCacheNames;

        private Builder(NanocachedClient client) {
            this.client = Objects.requireNonNull(client, "client");
        }

        /** How the cached values become bytes; default JDK serialization
         * ({@link JdkCacheValueSerializer}). */
        public Builder serializer(CacheValueSerializer serializer) {
            this.serializer = Objects.requireNonNull(serializer, "serializer");
            return this;
        }

        /** How the cache keys become bytes; default
         * {@link DefaultCacheKeyConverter}. */
        public Builder keyConverter(CacheKeyConverter keyConverter) {
            this.keyConverter = Objects.requireNonNull(keyConverter, "keyConverter");
            return this;
        }

        /** TTL for caches without a per-cache override;
         * {@link Duration#ZERO} (the default) = entries live until
         * evicted or deleted. */
        public Builder defaultTtl(Duration ttl) {
            this.defaultTtl = requireNonNegative(ttl);
            return this;
        }

        /** Per-cache TTL override. */
        public Builder ttl(String cacheName, Duration ttl) {
            this.ttlByCache.put(
                    Objects.requireNonNull(cacheName, "cacheName"), requireNonNegative(ttl));
            return this;
        }

        /** Whether {@code null} is a cacheable value (stored as Spring's
         * {@code NullValue} marker). Default true, matching Spring's
         * other cache implementations. */
        public Builder allowNullValues(boolean allowNullValues) {
            this.allowNullValues = allowNullValues;
            return this;
        }

        /** Restricts the manager to exactly these cache names (created
         * eagerly); {@link #getCache} answers {@code null} for any other
         * name. Without this, caches are created on first use. */
        public Builder cacheNames(Collection<String> names) {
            this.fixedCacheNames = new LinkedHashSet<>(Objects.requireNonNull(names, "names"));
            return this;
        }

        public NanocachedCacheManager build() {
            return new NanocachedCacheManager(this);
        }

        private static Duration requireNonNegative(Duration ttl) {
            Objects.requireNonNull(ttl, "ttl");
            if (ttl.isNegative()) {
                throw new IllegalArgumentException("nanocached-spring: ttl must not be negative");
            }
            return ttl;
        }
    }
}
