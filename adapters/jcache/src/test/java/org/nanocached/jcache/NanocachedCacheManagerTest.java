package org.nanocached.jcache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.Properties;
import java.util.Set;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.spi.CachingProvider;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NanocachedCacheManagerTest {

    record User(String name, int age) implements Serializable {}

    private MockNode node;
    private CachingProvider provider;
    private CacheManager manager;

    @BeforeEach
    void start() throws Exception {
        node = new MockNode();
        provider = new NanocachedCachingProvider();
        Properties properties = new Properties();
        properties.setProperty("nanocached.addresses", node.address());
        manager = provider.getCacheManager(URI.create("test:manager"), null, properties);
    }

    @AfterEach
    void stop() throws Exception {
        provider.close();
        node.close();
    }

    @Test
    void getCacheOnAnUncreatedNameReturnsNull() {
        assertNull(manager.getCache("never-created"));
    }

    @Test
    void createCacheThenGetCacheReturnsTheSameHandle() {
        MutableConfiguration<String, User> config = new MutableConfiguration<>();
        config.setTypes(String.class, User.class);
        manager.createCache("users", config);

        assertTrue(manager.getCache("users") != null);
        Set<String> names = new java.util.HashSet<>();
        manager.getCacheNames().forEach(names::add);
        assertEquals(Set.of("users"), names);
    }

    @Test
    void creatingTheSameCacheNameTwiceFails() {
        manager.createCache("users", new MutableConfiguration<String, User>().setTypes(String.class, User.class));

        assertThrows(
                CacheException.class,
                () -> manager.createCache(
                        "users", new MutableConfiguration<String, User>().setTypes(String.class, User.class)));
    }

    @Test
    void creatingADuplicateCacheNameWithStatisticsEnabledStillReportsTheDuplicateNameError() throws Exception {
        // Issue #331: the losing createCache used to construct its
        // NanocachedCache — which registered a statistics MBean under the
        // same ObjectName the winner already claimed — before the
        // duplicate-name putIfAbsent check ran. That meant a duplicate name
        // with statistics enabled failed with an MBean-registration
        // CacheException instead of the intended "already exists" one, and
        // left the platform MBean server in a confusing state.
        MutableConfiguration<String, User> first = new MutableConfiguration<>();
        first.setTypes(String.class, User.class);
        first.setStatisticsEnabled(true);
        javax.cache.Cache<String, User> winner = manager.createCache("dup-stats", first);

        MutableConfiguration<String, User> second = new MutableConfiguration<>();
        second.setTypes(String.class, User.class);
        second.setStatisticsEnabled(true);

        CacheException ex = assertThrows(CacheException.class, () -> manager.createCache("dup-stats", second));
        assertTrue(ex.getMessage().contains("already exists"), "unexpected message: " + ex.getMessage());

        // The winner's own MBean must still be registered exactly once, and
        // the winner's cache must remain fully usable.
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName objectName = new ObjectName("javax.cache:type=CacheStatistics,CacheManager="
                + ObjectName.quote(manager.getURI().toString()) + ",Cache=" + ObjectName.quote("dup-stats"));
        assertTrue(mbs.isRegistered(objectName), "the winner's statistics MBean must be registered");

        winner.put("a", new User("a", 1));
        assertEquals(new User("a", 1), winner.get("a"));
    }

    @Test
    void typedGetCacheRejectsAMismatchedValueType() {
        manager.createCache("users", new MutableConfiguration<String, User>().setTypes(String.class, User.class));

        assertThrows(ClassCastException.class, () -> manager.getCache("users", String.class, String.class));
    }

    @Test
    void destroyCacheRemovesItAndClearsItsData() {
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        javax.cache.Cache<String, String> cache = manager.createCache("sessions", config);
        cache.put("a", "1");
        assertEquals("1", cache.get("a"));

        manager.destroyCache("sessions");

        assertNull(manager.getCache("sessions"));
        assertEquals(1, node.clearCount.get(), "destroyCache must issue the namespace's CLEAR");
    }

    @Test
    void closingACacheHandleDirectlyDoesNotClearItsData() {
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        javax.cache.Cache<String, String> cache = manager.createCache("sessions", config);
        cache.put("a", "1");

        cache.close();

        assertNull(manager.getCache("sessions"), "a closed cache must no longer be returned by the manager");
        assertEquals(0, node.clearCount.get(), "closing a handle directly must not clear its data");
    }

    @Test
    void rejectsStoreByReference() {
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setStoreByValue(false);

        assertThrows(UnsupportedOperationException.class, () -> manager.createCache("x", config));
    }

    @Test
    void rejectsAConfiguredCacheLoader() {
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setReadThrough(true);
        config.setCacheLoaderFactory(FactoryBuilder.factoryOf(NoOpLoader.class));

        assertThrows(UnsupportedOperationException.class, () -> manager.createCache("x", config));
    }

    @Test
    void closingTheManagerClosesTheUnderlyingClientConnection() {
        manager.close();

        assertTrue(manager.isClosed());
        assertThrows(IllegalStateException.class, () -> manager.getCache("anything"));
    }

    static final class NoOpLoader implements CacheLoader<String, String>, Serializable {
        @Override
        public String load(String key) throws CacheLoaderException {
            return null;
        }

        @Override
        public java.util.Map<String, String> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
            return java.util.Map.of();
        }
    }
}
