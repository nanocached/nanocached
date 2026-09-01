package org.nanocached.spring.boot;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * Binds the {@code nanocached.*} namespace for {@link
 * NanocachedCacheAutoConfiguration}. Mirrors {@code
 * org.nanocached.NanocachedClient.Options} for the client-level settings,
 * and {@code org.nanocached.spring.NanocachedCacheManager.Builder} for the
 * {@code cache.*} group.
 */
@ConfigurationProperties("nanocached")
public class NanocachedProperties {

    /** {@code "host:port"} pairs. Required — the autoconfiguration is
     * inert (see {@link NanocachedCacheAutoConfiguration}) until this is
     * set, so a bare dependency changes nothing. */
    private List<String> addresses;

    /** Shared-secret auth; unset means anonymous auth, same as the SDK
     * default. */
    private String secret;

    /** Whether connections use TLS. Default {@code false}, same as the
     * SDK default. */
    private boolean tls;

    /** Path to a CA certificate file trusted for TLS, in addition to the
     * platform trust store. Only meaningful with {@code tls: true}. */
    private String ca;

    /** Whether values are transparently compressed. Unset defers to the
     * SDK default ({@code false}). */
    private Boolean compress;

    /** Minimum value size, in bytes, before compression kicks in. Unset
     * defers to the SDK default. Only meaningful with {@code compress:
     * true}. */
    private Integer compressionThreshold;

    /** Whether replica writes are dispatched fire-and-forget. Unset defers
     * to the SDK default ({@code false}). Mirrors {@code
     * Options.fireAndForgetReplicas}. */
    private Boolean fireAndForgetReplicas;

    /** Whether a clean miss probes the remaining owners and repairs the
     * primary in the background (read repair). Unset defers to the SDK
     * default ({@code false}). Mirrors {@code Options.readRepair}. */
    private Boolean readRepair;

    /** How long an address stays "down" after a failed reconnect dial.
     * Unset defers to the SDK default (1s). Mirrors {@code
     * Options.reconnectCooldown}. */
    private Duration reconnectCooldown;

    /** Send a read to the next owner once the primary has been silent this
     * long (hedged reads). Unset (the default) is off. Mirrors {@code
     * Options.readHedgeAfter}. */
    private Duration readHedgeAfter;

    @NestedConfigurationProperty
    private final Cache cache = new Cache();

    public List<String> getAddresses() {
        return addresses;
    }

    public void setAddresses(List<String> addresses) {
        this.addresses = addresses;
    }

    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public boolean isTls() {
        return tls;
    }

    public void setTls(boolean tls) {
        this.tls = tls;
    }

    public String getCa() {
        return ca;
    }

    public void setCa(String ca) {
        this.ca = ca;
    }

    public Boolean getCompress() {
        return compress;
    }

    public void setCompress(Boolean compress) {
        this.compress = compress;
    }

    public Integer getCompressionThreshold() {
        return compressionThreshold;
    }

    public void setCompressionThreshold(Integer compressionThreshold) {
        this.compressionThreshold = compressionThreshold;
    }

    public Boolean getFireAndForgetReplicas() {
        return fireAndForgetReplicas;
    }

    public void setFireAndForgetReplicas(Boolean fireAndForgetReplicas) {
        this.fireAndForgetReplicas = fireAndForgetReplicas;
    }

    public Boolean getReadRepair() {
        return readRepair;
    }

    public void setReadRepair(Boolean readRepair) {
        this.readRepair = readRepair;
    }

    public Duration getReconnectCooldown() {
        return reconnectCooldown;
    }

    public void setReconnectCooldown(Duration reconnectCooldown) {
        this.reconnectCooldown = reconnectCooldown;
    }

    public Duration getReadHedgeAfter() {
        return readHedgeAfter;
    }

    public void setReadHedgeAfter(Duration readHedgeAfter) {
        this.readHedgeAfter = readHedgeAfter;
    }

    public Cache getCache() {
        return cache;
    }

    /** {@code nanocached.cache.*} — passed straight to {@code
     * NanocachedCacheManager.Builder}. */
    public static class Cache {

        /** TTL for caches without a per-cache override. Default zero =
         * entries live until evicted or deleted, same as the manager's
         * own default. */
        private Duration defaultTtl = Duration.ZERO;

        /** Per-cache TTL override, keyed by cache (namespace) name. */
        private Map<String, Duration> ttl = new LinkedHashMap<>();

        /** Whether {@code null} is a cacheable value. Default {@code
         * true}, same as the manager's own default. */
        private boolean allowNullValues = true;

        /** Restricts the manager to exactly these cache names, created
         * eagerly. Unset (the default) creates caches on first use.
         * Aligned with {@code spring.cache.cache-names}. */
        private List<String> cacheNames;

        public Duration getDefaultTtl() {
            return defaultTtl;
        }

        public void setDefaultTtl(Duration defaultTtl) {
            this.defaultTtl = defaultTtl;
        }

        public Map<String, Duration> getTtl() {
            return ttl;
        }

        public void setTtl(Map<String, Duration> ttl) {
            this.ttl = ttl;
        }

        public boolean isAllowNullValues() {
            return allowNullValues;
        }

        public void setAllowNullValues(boolean allowNullValues) {
            this.allowNullValues = allowNullValues;
        }

        public List<String> getCacheNames() {
            return cacheNames;
        }

        public void setCacheNames(List<String> cacheNames) {
            this.cacheNames = cacheNames;
        }
    }
}
