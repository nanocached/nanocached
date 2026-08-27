package org.nanocached.jcache;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.net.URI;
import java.util.Properties;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Discovery and {@link CacheManager} identity — the SPI half of issue
 * #118, verified through {@link Caching}, the JCache consumption API
 * itself (not by constructing {@link NanocachedCachingProvider} directly).
 */
class NanocachedCachingProviderTest {

    private MockNode node;

    @BeforeEach
    void start() throws Exception {
        node = new MockNode();
    }

    @AfterEach
    void stop() throws Exception {
        node.close();
    }

    private Properties propertiesFor(MockNode node) {
        Properties properties = new Properties();
        properties.setProperty("nanocached.addresses", node.address());
        return properties;
    }

    @Test
    void isDiscoverableViaTheStandardServiceLoaderMechanism() {
        CachingProvider provider = Caching.getCachingProvider();
        assertTrue(
                provider instanceof NanocachedCachingProvider,
                "expected the ServiceLoader to find NanocachedCachingProvider, got "
                        + provider.getClass());
    }

    @Test
    void requiresAddressesInProperties() {
        CachingProvider provider = new NanocachedCachingProvider();
        CacheException error = assertThrows(
                CacheException.class,
                () -> provider.getCacheManager(URI.create("test:no-addresses"), null, new Properties()));
        assertTrue(error.getMessage().contains("nanocached.addresses"), error.getMessage());
    }

    @Test
    void theSameUriAndClassLoaderReturnTheSameManager() {
        try (CachingProvider provider = new NanocachedCachingProvider()) {
            URI uri = URI.create("test:identity");
            ClassLoader classLoader = getClass().getClassLoader();
            CacheManager first = provider.getCacheManager(uri, classLoader, propertiesFor(node));
            CacheManager second = provider.getCacheManager(uri, classLoader, propertiesFor(node));
            assertSame(first, second);
        }
    }

    @Test
    void aDifferentUriReturnsADifferentManager() throws Exception {
        try (CachingProvider provider = new NanocachedCachingProvider();
                MockNode other = new MockNode()) {
            ClassLoader classLoader = getClass().getClassLoader();
            CacheManager first =
                    provider.getCacheManager(URI.create("test:a"), classLoader, propertiesFor(node));
            CacheManager second =
                    provider.getCacheManager(URI.create("test:b"), classLoader, propertiesFor(other));
            assertNotSame(first, second);
        }
    }

    @Test
    void closingTheProviderClosesEveryManagerItHandedOut() {
        CachingProvider provider = new NanocachedCachingProvider();
        CacheManager manager =
                provider.getCacheManager(URI.create("test:close-all"), null, propertiesFor(node));
        assertTrue(!manager.isClosed());

        provider.close();

        assertTrue(manager.isClosed());
    }

    @Test
    void aRequestForANewManagerAfterCloseCreatesAFreshOne() {
        try (CachingProvider provider = new NanocachedCachingProvider()) {
            URI uri = URI.create("test:reopen");
            CacheManager first = provider.getCacheManager(uri, null, propertiesFor(node));
            first.close();

            CacheManager second = provider.getCacheManager(uri, null, propertiesFor(node));
            assertNotSame(first, second);
            assertTrue(!second.isClosed());
        }
    }

    @Test
    void theBareNoArgGetCacheManagerUsesEmptyDefaultPropertiesAndSoHasNoAddresses() {
        // This adapter only reads connection settings from the Properties
        // passed explicitly to getCacheManager(uri, classLoader,
        // properties); it never parses the URI as a config-file pointer
        // (documented scope cut). getDefaultProperties() is therefore
        // always empty, so the true no-arg convenience call always fails
        // fast unless the caller supplies properties some other way.
        try (CachingProvider provider = new NanocachedCachingProvider()) {
            assertNotNull(provider.getDefaultURI());
            assertThrows(CacheException.class, provider::getCacheManager);
        }
    }
}
