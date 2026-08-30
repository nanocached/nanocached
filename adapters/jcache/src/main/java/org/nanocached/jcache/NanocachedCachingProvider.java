package org.nanocached.jcache;

import java.net.URI;
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
            // that would also call provider.forget(uri, classLoader) on
            // the shared key and evict the winner this thread must not
            // touch.
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

        String secret = properties.getProperty("nanocached.secret");
        if (secret != null) {
            options.authSecret(secret);
        }
        String tls = properties.getProperty("nanocached.tls");
        if (tls != null) {
            options.tls(Boolean.parseBoolean(tls));
        }
        String ca = properties.getProperty("nanocached.ca");
        if (ca != null) {
            options.ca(ca);
        }
        String compress = properties.getProperty("nanocached.compress");
        if (compress != null) {
            options.compress(Boolean.parseBoolean(compress));
        }
        String compressionThreshold = properties.getProperty("nanocached.compression-threshold");
        if (compressionThreshold != null) {
            options.compressionThreshold(Integer.parseInt(compressionThreshold));
        }

        try {
            return NanocachedClient.connect(options);
        } catch (RuntimeException e) {
            throw new CacheException("nanocached-jcache: failed to connect to nanocached", e);
        }
    }

    private static List<NanocachedClient.Address> parseAddresses(String addresses) {
        return List.of(addresses.split(",")).stream()
                .map(String::trim)
                .map(address -> address.split(":", 2))
                .map(parts -> new NanocachedClient.Address(parts[0], Integer.parseInt(parts[1])))
                .toList();
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
     * the identity map instead of being handed out again as if live. */
    void forget(URI uri, ClassLoader classLoader) {
        managers.remove(new ManagerKey(uri, classLoader));
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
