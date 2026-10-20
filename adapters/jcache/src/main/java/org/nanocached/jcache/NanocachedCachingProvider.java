package org.nanocached.jcache;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import org.nanocached.NanocachedClient;

/**
 * JSR-107 entry point (issue #118): discovered by {@link
 * javax.cache.Caching#getCachingProvider()} via {@code
 * META-INF/services/javax.cache.spi.CachingProvider} — the standard JCache
 * discovery mechanism, nothing bespoke.
 *
 * <p>Unlike {@code nanocached-spring} (which borrows an application-owned
 * {@link NanocachedClient}), this provider <em>creates and owns</em> the
 * client for each {@link CacheManager} it hands out, since JSR-107's
 * {@code CacheManager} is expected to manage its own resources. Connection
 * settings come from the {@link Properties} passed to {@link
 * #getCacheManager(URI, ClassLoader, Properties)} — see {@link
 * NanocachedCacheManager} for the recognized {@code nanocached.*} keys.
 * The {@link URI} is used only as part of a manager's identity (JSR-107's
 * "same URI + ClassLoader returns the same manager" contract); it is
 * never parsed as a pointer to an external config resource.
 *
 * <p>Policy note: framework adapters like this module are
 * ecosystem-specific and live <em>outside</em> the six-language SDK
 * parity policy (issue #25) — parity applies to the SDK core only.
 */
public final class NanocachedCachingProvider implements CachingProvider {

    private static final URI DEFAULT_URI = URI.create("nanocached:jcache");

    private final ConcurrentHashMap<ManagerKey, NanocachedCacheManager> managers =
            new ConcurrentHashMap<>();

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        URI resolvedUri = uri != null ? uri : getDefaultURI();
        ClassLoader resolvedClassLoader = classLoader != null ? classLoader : getDefaultClassLoader();
        ManagerKey key = new ManagerKey(resolvedUri, resolvedClassLoader);

        NanocachedCacheManager existing = managers.get(key);
        if (existing != null && !existing.isClosed()) {
            return existing;
        }

        // issue #192: the blocking connect must not run inside
        // ConcurrentHashMap#compute below — its remapping function holds
        // that key's bucket lock for as long as it runs, so a slow or
        // hung dial used to stall every other getCacheManager/close call
        // that happened to land on the same bucket, not just this key.
        // Dial first, then atomically install-or-discard.
        NanocachedClient client = connect(properties != null ? properties : new Properties());
        NanocachedCacheManager created =
                new NanocachedCacheManager(this, resolvedUri, resolvedClassLoader, client);

        NanocachedCacheManager[] winner = new NanocachedCacheManager[1];
        managers.compute(key, (unused, current) -> {
            if (current != null && !current.isClosed()) {
                winner[0] = current;
                return current;
            }
            winner[0] = created;
            return created;
        });

        if (winner[0] != created) {
            // Lost the race to another thread's connect for the same
            // key: close the raw client directly, not created.close() —
            // that would also call provider.forget(uri, classLoader,
            // created) on the shared key. forget() is identity-checked,
            // so it would be a harmless no-op here (created never won
            // the key), but calling it would be misleading — this thread
            // isn't the owner of whatever manager now sits at that key.
            client.close();
        }
        return winner[0];
    }

    private static NanocachedClient connect(Properties properties) {
        String addresses = properties.getProperty("nanocached.addresses");
        if (addresses == null || addresses.isBlank()) {
            throw new CacheException(
                    "nanocached-jcache: \"nanocached.addresses\" is required (comma-separated"
                            + " host:port list) — pass it in the Properties given to"
                            + " Caching.getCachingProvider().getCacheManager(uri, classLoader,"
                            + " properties)");
        }

        NanocachedClient.Options options = new NanocachedClient.Options().addresses(parseAddresses(addresses));

        // Every property is parsed and applied under its own name: a
        // value the SDK or the parser rejects is a CacheException that
        // says which property, not a raw NumberFormatException or
        // IllegalArgumentException escaping getCacheManager.
        String secret = properties.getProperty("nanocached.secret");
        if (secret != null) {
            apply("nanocached.secret", () -> options.authSecret(secret));
        }
        Boolean tls = booleanProperty(properties, "nanocached.tls");
        if (tls != null) {
            apply("nanocached.tls", () -> options.tls(tls));
        }
        String ca = properties.getProperty("nanocached.ca");
        if (ca != null) {
            apply("nanocached.ca", () -> options.ca(ca));
        }
        Boolean compress = booleanProperty(properties, "nanocached.compress");
        if (compress != null) {
            apply("nanocached.compress", () -> options.compress(compress));
        }
        Long compressionThreshold = longProperty(properties, "nanocached.compression-threshold");
        if (compressionThreshold != null) {
            apply("nanocached.compression-threshold",
                    () -> options.compressionThreshold(Math.toIntExact(compressionThreshold)));
        }
        Boolean fireAndForgetReplicas = booleanProperty(properties, "nanocached.fire-and-forget-replicas");
        if (fireAndForgetReplicas != null) {
            apply("nanocached.fire-and-forget-replicas", () -> options.fireAndForgetReplicas(fireAndForgetReplicas));
        }
        Boolean readRepair = booleanProperty(properties, "nanocached.read-repair");
        if (readRepair != null) {
            apply("nanocached.read-repair", () -> options.readRepair(readRepair));
        }
        // Durations are given in whole milliseconds here — a plain
        // Properties value has no ISO-8601 binder like Boot's does.
        Long reconnectCooldownMillis = longProperty(properties, "nanocached.reconnect-cooldown-millis");
        if (reconnectCooldownMillis != null) {
            // Zero means "no cooldown", as the Spring Boot starter reads
            // it; the SDK's own Duration.ZERO means "use the default", so
            // zero must route through disableReconnectCooldown() to take
            // effect.
            apply("nanocached.reconnect-cooldown-millis", () -> {
                if (reconnectCooldownMillis == 0) {
                    options.disableReconnectCooldown();
                } else {
                    options.reconnectCooldown(Duration.ofMillis(reconnectCooldownMillis));
                }
            });
        }
        Long readHedgeAfterMillis = longProperty(properties, "nanocached.read-hedge-after-millis");
        if (readHedgeAfterMillis != null) {
            apply("nanocached.read-hedge-after-millis",
                    () -> options.readHedgeAfter(Duration.ofMillis(readHedgeAfterMillis)));
        }

        try {
            return NanocachedClient.connect(options);
        } catch (RuntimeException e) {
            throw new CacheException("nanocached-jcache: failed to connect to nanocached", e);
        }
    }

    /** Only {@code true} and {@code false} are accepted: {@code
     * Boolean.parseBoolean} reads anything else as {@code false}, so
     * {@code nanocached.tls=yes} silently connected in plaintext. */
    private static Boolean booleanProperty(Properties properties, String name) {
        String value = properties.getProperty(name);
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.equalsIgnoreCase("true")) {
            return Boolean.TRUE;
        }
        if (trimmed.equalsIgnoreCase("false")) {
            return Boolean.FALSE;
        }
        throw new CacheException(
                "nanocached-jcache: \"" + name + "\" must be true or false, got \"" + value + "\"");
    }

    private static Long longProperty(Properties properties, String name) {
        String value = properties.getProperty(name);
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new CacheException(
                    "nanocached-jcache: \"" + name + "\" must be a whole number, got \"" + value + "\"", e);
        }
    }

    private static void apply(String name, Runnable setter) {
        try {
            setter.run();
        } catch (IllegalArgumentException | ArithmeticException e) {
            throw new CacheException("nanocached-jcache: \"" + name + "\": " + e.getMessage(), e);
        }
    }

    private static List<NanocachedClient.Address> parseAddresses(String addresses) {
        try {
            return List.of(addresses.split(",")).stream()
                    .map(String::trim)
                    .map(NanocachedCachingProvider::parseAddress)
                    .toList();
        } catch (IllegalArgumentException e) {
            throw new CacheException("nanocached-jcache: \"nanocached.addresses\": " + e.getMessage(), e);
        }
    }

    /** One {@code host:port}; the last colon splits, so a bracketed IPv6 host works. */
    private static NanocachedClient.Address parseAddress(String address) {
        int colon = address.lastIndexOf(':');
        if (colon <= 0 || colon == address.length() - 1) {
            throw new IllegalArgumentException("expected host:port, got \"" + address + "\"");
        }
        String host = address.substring(0, colon);
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        int port;
        try {
            port = Integer.parseInt(address.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("port is not a number in \"" + address + "\"");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port out of range in \"" + address + "\"");
        }
        return new NanocachedClient.Address(host, port);
    }

    @Override
    public ClassLoader getDefaultClassLoader() {
        return getClass().getClassLoader();
    }

    @Override
    public URI getDefaultURI() {
        return DEFAULT_URI;
    }

    @Override
    public Properties getDefaultProperties() {
        return new Properties();
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        return getCacheManager(uri, classLoader, getDefaultProperties());
    }

    @Override
    public CacheManager getCacheManager() {
        return getCacheManager(getDefaultURI(), getDefaultClassLoader());
    }

    @Override
    public void close() {
        managers.values().forEach(NanocachedCacheManager::close);
        managers.clear();
    }

    @Override
    public void close(ClassLoader classLoader) {
        closeMatching(key -> key.classLoader().equals(classLoader));
    }

    @Override
    public void close(URI uri, ClassLoader classLoader) {
        closeMatching(key -> key.uri().equals(uri) && key.classLoader().equals(classLoader));
    }

    private void closeMatching(java.util.function.Predicate<ManagerKey> matches) {
        managers.keySet().stream()
                .filter(matches)
                .toList()
                .forEach(key -> {
                    NanocachedCacheManager manager = managers.remove(key);
                    if (manager != null) {
                        manager.close();
                    }
                });
    }

    /** Called by {@link NanocachedCacheManager#close()} so a manager
     * closed directly (rather than via this provider) still drops out of
     * the identity map instead of being handed out again as if live.
     *
     * <p>Removal is identity-checked ({@code managers.remove(key,
     * manager)}, not {@code managers.remove(key)}): the caller's {@code
     * close()} sets its {@code closed} flag and runs its own cleanup
     * (closing the client, closing every cache) before this call reaches
     * the registry. In that window another thread may already have
     * dialed and installed a new manager under the same key (permitted,
     * since the old one already reports {@code isClosed() == true}). A
     * key-only removal here would evict that new, live manager instead
     * of the stale one this call actually means to remove. */
    void forget(URI uri, ClassLoader classLoader, NanocachedCacheManager manager) {
        managers.remove(new ManagerKey(uri, classLoader), manager);
    }

    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {
        // Every optional JCache feature this enum names — store-by-reference
        // above all, since a value must cross the wire as bytes — is
        // unsupported here.
        return false;
    }

    private record ManagerKey(URI uri, ClassLoader classLoader) {}
}
