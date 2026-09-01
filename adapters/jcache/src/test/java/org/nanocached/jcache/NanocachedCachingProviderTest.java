package org.nanocached.jcache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
    void clientReliabilityOptionsAreParsedAndForwarded() {
        // Regression (pass-7 audit): the provider used to forward only
        // tls/ca/compress/compression-threshold. Setting the reliability
        // options proves they are parsed (Boolean/Long/Duration) and
        // forwarded to Options without error — a bad property name or a
        // parse slip would throw out of connect() and fail getCacheManager.
        try (CachingProvider provider = new NanocachedCachingProvider()) {
            Properties properties = propertiesFor(node);
            properties.setProperty("nanocached.fire-and-forget-replicas", "true");
            properties.setProperty("nanocached.read-repair", "true");
            properties.setProperty("nanocached.reconnect-cooldown-millis", "2000");
            properties.setProperty("nanocached.read-hedge-after-millis", "50");

            CacheManager manager =
                    provider.getCacheManager(URI.create("test:reliability-options"), null, properties);
            assertTrue(!manager.isClosed());
        }
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
    void racingRequestsForTheSameKeyConvergeOnOneManagerAndCloseEveryLosingClient() throws Exception {
        // issue #192: the blocking connect must run outside
        // ConcurrentHashMap#compute — otherwise every racer would
        // effectively serialize behind that key's bucket lock. Racing
        // several threads for the same (uri, classLoader) instead lets
        // each dial its own client concurrently; only one may end up
        // backing the manager everyone gets back, and every other
        // racer's client must be closed rather than leaked.
        try (CachingProvider provider = new NanocachedCachingProvider()) {
            URI uri = URI.create("test:race");
            ClassLoader classLoader = getClass().getClassLoader();
            int racers = 8;
            ExecutorService pool = Executors.newFixedThreadPool(racers);
            try {
                CountDownLatch ready = new CountDownLatch(racers);
                CountDownLatch go = new CountDownLatch(1);
                List<Future<CacheManager>> futures = new ArrayList<>();
                for (int i = 0; i < racers; i++) {
                    futures.add(pool.submit(() -> {
                        ready.countDown();
                        go.await();
                        return provider.getCacheManager(uri, classLoader, propertiesFor(node));
                    }));
                }
                ready.await();
                go.countDown();

                Set<CacheManager> managers = new HashSet<>();
                for (Future<CacheManager> future : futures) {
                    managers.add(future.get(10, TimeUnit.SECONDS));
                }

                assertEquals(1, managers.size(), "every racer must converge on the same manager");
                // A losing client's close() call returns once it has shut
                // its own socket down, but the mock node's server thread
                // still needs a moment to observe the resulting EOF and
                // decrement liveConnectionCount — bounded poll rather than
                // asserting immediately. Only the winner's connection
                // stays open in the end, however many raced.
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (node.liveConnectionCount.get() != 1 && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                assertEquals(
                        1,
                        node.liveConnectionCount.get(),
                        "every losing client must be closed, not leaked");
            } finally {
                pool.shutdownNow();
            }
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
