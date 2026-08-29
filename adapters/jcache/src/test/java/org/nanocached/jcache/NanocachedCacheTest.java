package org.nanocached.jcache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.management.CacheStatisticsMXBean;
import javax.cache.processor.EntryProcessor;
import javax.cache.spi.CachingProvider;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NanocachedCacheTest {

    private MockNode node;
    private CachingProvider provider;
    private CacheManager manager;
    private Cache<String, String> cache;

    @BeforeEach
    void start() throws Exception {
        node = new MockNode();
        provider = new NanocachedCachingProvider();
        Properties properties = new Properties();
        properties.setProperty("nanocached.addresses", node.address());
        manager = provider.getCacheManager(URI.create("test:cache"), null, properties);
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setExpiryPolicyFactory(EternalExpiryPolicy.factoryOf());
        cache = manager.createCache("widgets", config);
    }

    @AfterEach
    void stop() throws Exception {
        provider.close();
        node.close();
    }

    // ── basic get/put ───────────────────────────────────────────────

    @Test
    void getOnAMissingKeyReturnsNull() {
        assertNull(cache.get("nope"));
    }

    @Test
    void putThenGetRoundTrips() {
        cache.put("a", "1");
        assertEquals("1", cache.get("a"));
    }

    @Test
    void putOverwritesAnExistingValue() {
        cache.put("a", "1");
        cache.put("a", "2");
        assertEquals("2", cache.get("a"));
    }

    @Test
    void containsKeyReflectsPresence() {
        assertFalse(cache.containsKey("a"));
        cache.put("a", "1");
        assertTrue(cache.containsKey("a"));
    }

    @Test
    void putAllAndGetAllRoundTrip() {
        cache.putAll(Map.of("a", "1", "b", "2", "c", "3"));
        Map<String, String> all = cache.getAll(Set.of("a", "b", "c", "missing"));
        assertEquals(Map.of("a", "1", "b", "2", "c", "3"), all);
    }

    // ── putIfAbsent / replace / remove(k, old) — CAS (#141) ────────

    @Test
    void putIfAbsentSucceedsWhenTheKeyIsAbsent() {
        assertTrue(cache.putIfAbsent("a", "1"));
        assertEquals("1", cache.get("a"));
    }

    @Test
    void putIfAbsentFailsWhenTheKeyIsPresent() {
        cache.put("a", "1");
        assertFalse(cache.putIfAbsent("a", "2"));
        assertEquals("1", cache.get("a"));
    }

    @Test
    void threeArgReplaceSucceedsWhenTheOldValueMatches() {
        cache.put("a", "1");
        assertTrue(cache.replace("a", "1", "2"));
        assertEquals("2", cache.get("a"));
    }

    @Test
    void threeArgReplaceFailsWhenTheOldValueDoesNotMatch() {
        cache.put("a", "1");
        assertFalse(cache.replace("a", "stale", "2"));
        assertEquals("1", cache.get("a"));
    }

    @Test
    void twoArgReplaceSucceedsWhenTheKeyIsPresent() {
        cache.put("a", "1");
        assertTrue(cache.replace("a", "2"));
        assertEquals("2", cache.get("a"));
    }

    @Test
    void twoArgReplaceFailsWhenTheKeyIsAbsent() {
        assertFalse(cache.replace("a", "2"));
        assertNull(cache.get("a"));
    }

    @Test
    void twoArgRemoveSucceedsWhenTheOldValueMatches() {
        cache.put("a", "1");
        assertTrue(cache.remove("a", "1"));
        assertFalse(cache.containsKey("a"));
    }

    @Test
    void twoArgRemoveFailsWhenTheOldValueDoesNotMatch() {
        cache.put("a", "1");
        assertFalse(cache.remove("a", "stale"));
        assertEquals("1", cache.get("a"));
    }

    // ── getAndPut / getAndReplace / getAndRemove — CAS retry loops ──

    @Test
    void getAndPutOnAnAbsentKeyStoresAndReturnsNull() {
        assertNull(cache.getAndPut("a", "1"));
        assertEquals("1", cache.get("a"));
    }

    @Test
    void getAndPutOnAPresentKeyReturnsTheOldValue() {
        cache.put("a", "1");
        assertEquals("1", cache.getAndPut("a", "2"));
        assertEquals("2", cache.get("a"));
    }

    @Test
    void getAndPutRetriesAfterAOneShotCasMismatchAndStillSucceeds() {
        cache.put("a", "1");
        node.forceCasMismatchOnce.set(true);
        assertEquals("1", cache.getAndPut("a", "2"));
        assertEquals("2", cache.get("a"));
        assertTrue(node.casSetCount.get() >= 2, "a forced mismatch must cause a retry");
    }

    @Test
    void getAndReplaceOnAnAbsentKeyReturnsNullAndWritesNothing() {
        assertNull(cache.getAndReplace("a", "1"));
        assertFalse(cache.containsKey("a"));
    }

    @Test
    void getAndReplaceOnAPresentKeyReturnsTheOldValue() {
        cache.put("a", "1");
        assertEquals("1", cache.getAndReplace("a", "2"));
        assertEquals("2", cache.get("a"));
    }

    @Test
    void getAndRemoveOnAnAbsentKeyReturnsNull() {
        assertNull(cache.getAndRemove("a"));
    }

    @Test
    void getAndRemoveOnAPresentKeyReturnsTheOldValueAndRemovesIt() {
        cache.put("a", "1");
        assertEquals("1", cache.getAndRemove("a"));
        assertFalse(cache.containsKey("a"));
    }

    @Test
    void getAndRemoveRetriesAfterAOneShotCasMismatchAndStillSucceeds() {
        cache.put("a", "1");
        node.forceCasMismatchOnce.set(true);
        assertEquals("1", cache.getAndRemove("a"));
        assertFalse(cache.containsKey("a"));
        assertTrue(node.casDeleteCount.get() >= 2, "a forced mismatch must cause a retry");
    }

    // ── batched getAll / putAll (issue #160) ─────────────────────────

    @Test
    void getAllBatchesEveryKeyTypeIntoOneBulkRead() {
        MutableConfiguration<Object, String> config = new MutableConfiguration<>();
        config.setTypes(Object.class, String.class);
        config.setExpiryPolicyFactory(EternalExpiryPolicy.factoryOf());
        Cache<Object, String> mixed = manager.createCache("mixed-keys", config);
        byte[] opaque = {(byte) 0xFF, (byte) 0xFE, 0, 1};
        SerializableKey serialized = new SerializableKey("s", 7);
        mixed.put("str", "1");
        mixed.put(42L, "2");
        mixed.put(opaque, "3");
        mixed.put(serialized, "4");
        node.multiGetCount.set(0);

        Map<Object, String> all = mixed.getAll(Set.of("str", 42L, opaque, serialized, "missing"));

        assertEquals(4, all.size());
        assertEquals("1", all.get("str"));
        assertEquals("2", all.get(42L));
        assertEquals("3", all.get(opaque));
        assertEquals("4", all.get(serialized));
        assertEquals(1, node.multiGetCount.get(), "one bulk read, no per-key fallback");
    }

    @Test
    void putAllIssuesOneBulkWritePerResolvedTtl() {
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new SplitExpiryPolicy()));
        Cache<String, String> split = manager.createCache("split-widgets", config);
        split.put("existing", "old");
        node.multiSetCount.set(0);
        node.multiGetCount.set(0);

        split.putAll(Map.of("existing", "new", "fresh1", "1", "fresh2", "2"));

        assertEquals(1, node.multiGetCount.get(), "one bulk read decides create-vs-update");
        assertEquals(2, node.multiSetCount.get(), "one bulk write per distinct TTL");
        assertEquals(30L, storedTtlSeconds("split-widgets", "existing"));
        assertEquals(60L, storedTtlSeconds("split-widgets", "fresh1"));
        assertEquals(60L, storedTtlSeconds("split-widgets", "fresh2"));
        assertEquals("new", split.get("existing"));
    }

    @Test
    void putAllFiresCreatedAndUpdatedEventsWithOldValues() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingListener<String, String> listener = new RecordingListener<>(events);
        cache.registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<>(
                FactoryBuilder.factoryOf(listener), null, true, true));
        cache.put("a", "1");
        events.clear();

        cache.putAll(new java.util.LinkedHashMap<>(Map.of("a", "2")) {{ put("b", "3"); }});

        assertEquals(List.of("UPDATED:a:2:old=1", "CREATED:b:3"), events);
    }

    @Test
    void putAllWithDurationZeroDeletesExistingAndSkipsNew() {
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ZERO));
        Cache<String, String> zeroCache = manager.createCache("zero-all", config);
        node.multiSetCount.set(0);

        zeroCache.putAll(Map.of("a", "1", "b", "2"));

        assertNull(zeroCache.get("a"));
        assertNull(zeroCache.get("b"));
        assertEquals(0, node.multiSetCount.get());
    }

    @Test
    void putAllOfAnEmptyMapIsANoOp() {
        cache.putAll(Map.of());
        assertEquals(0, node.multiGetCount.get());
        assertEquals(0, node.multiSetCount.get());
    }

    /** creation → 60 s, update → 30 s: two distinct TTLs within one putAll. */
    private static final class SplitExpiryPolicy implements ExpiryPolicy, java.io.Serializable {
        @Override
        public Duration getExpiryForCreation() {
            return new Duration(java.util.concurrent.TimeUnit.SECONDS, 60);
        }

        @Override
        public Duration getExpiryForAccess() {
            return null;
        }

        @Override
        public Duration getExpiryForUpdate() {
            return new Duration(java.util.concurrent.TimeUnit.SECONDS, 30);
        }
    }

    private record SerializableKey(String name, int id) implements java.io.Serializable {}

    // ── bulk removal / clear ────────────────────────────────────────

    @Test
    void removeAllWithKeysRemovesEachOne() {
        cache.putAll(Map.of("a", "1", "b", "2", "c", "3"));
        cache.removeAll(Set.of("a", "b"));
        assertFalse(cache.containsKey("a"));
        assertFalse(cache.containsKey("b"));
        assertEquals("3", cache.get("c"));
    }

    @Test
    void noArgRemoveAllMapsToTheNamespaceClear() {
        cache.put("a", "1");
        cache.removeAll();
        assertFalse(cache.containsKey("a"));
        assertEquals(1, node.clearCount.get());
    }

    @Test
    void clearMapsToTheNamespaceClear() {
        cache.put("a", "1");
        cache.clear();
        assertFalse(cache.containsKey("a"));
        assertEquals(1, node.clearCount.get());
    }

    // ── explicitly unsupported ──────────────────────────────────────

    @Test
    void iteratorAlwaysThrows() {
        assertThrows(UnsupportedOperationException.class, cache::iterator);
    }

    @Test
    void invokeAlwaysThrows() {
        EntryProcessor<String, String, Object> noOp = (entry, args) -> null;
        assertThrows(UnsupportedOperationException.class, () -> cache.invoke("a", noOp));
    }

    @Test
    void invokeAllAlwaysThrows() {
        EntryProcessor<String, String, Object> noOp = (entry, args) -> null;
        assertThrows(UnsupportedOperationException.class, () -> cache.invokeAll(Set.of("a"), noOp));
    }

    // ── expiry policy ────────────────────────────────────────────────

    @Test
    void createdExpiryPolicySetsATtlOnCreation() {
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(java.util.concurrent.TimeUnit.SECONDS, 60)));
        Cache<String, String> ttlCache = manager.createCache("ttl-widgets", config);

        ttlCache.put("a", "1");

        assertEquals(60L, storedTtlSeconds("ttl-widgets", "a"));
    }

    @Test
    void durationZeroOnCreationMeansTheEntryIsNeverRetained() {
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ZERO));
        Cache<String, String> zeroCache = manager.createCache("zero-widgets", config);

        zeroCache.put("a", "1");

        assertNull(zeroCache.get("a"));
    }

    @Test
    void accessedExpiryPolicyRefreshesTheTtlOnEveryGet() {
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setExpiryPolicyFactory(
                AccessedExpiryPolicy.factoryOf(new Duration(java.util.concurrent.TimeUnit.SECONDS, 30)));
        Cache<String, String> slidingCache = manager.createCache("sliding-widgets", config);

        slidingCache.put("a", "1");
        assertEquals(30L, storedTtlSeconds("sliding-widgets", "a"), "AccessedExpiryPolicy also sets the TTL on creation");

        // Simulate time having passed by rewriting the stored TTL directly,
        // bypassing the cache — then prove a plain get() resets it to the
        // fixed access duration rather than leaving it as-is.
        node.store("sliding-widgets")
                .put(
                        java.nio.ByteBuffer.wrap(KeyCodec.toKeyBytes("a")),
                        new MockNode.Entry(node.entry("sliding-widgets", KeyCodec.toKeyBytes("a")).value(), 5L));
        assertEquals(5L, storedTtlSeconds("sliding-widgets", "a"));

        slidingCache.get("a");

        assertEquals(30L, storedTtlSeconds("sliding-widgets", "a"), "a get must refresh the TTL via getExpiryForAccess");
    }

    private long storedTtlSeconds(String namespace, String key) {
        MockNode.Entry entry = node.entry(namespace, KeyCodec.toKeyBytes(key));
        return entry.ttlSeconds();
    }

    // ── statistics ───────────────────────────────────────────────────

    @Test
    void statisticsCountHitsMissesPutsAndRemovals() throws Exception {
        manager.enableStatistics("widgets", true);
        cache.put("a", "1");
        cache.get("a");
        cache.get("missing");
        cache.remove("a");

        CacheStatisticsMXBean statistics = statisticsMBean("widgets");
        assertEquals(1, statistics.getCacheHits());
        assertEquals(1, statistics.getCacheMisses());
        assertEquals(1, statistics.getCachePuts());
        assertEquals(1, statistics.getCacheRemovals());
    }

    private CacheStatisticsMXBean statisticsMBean(String cacheName) throws Exception {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName objectName = new ObjectName("javax.cache:type=CacheStatistics,CacheManager="
                + ObjectName.quote(manager.getURI().toString()) + ",Cache=" + ObjectName.quote(cacheName));
        return JMX.newMXBeanProxy(mbs, objectName, CacheStatisticsMXBean.class);
    }

    // ── local listeners ──────────────────────────────────────────────

    @Test
    void listenersFireForLocalCreateUpdateAndRemove() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingListener<String, String> listener = new RecordingListener<>(events);
        cache.registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<>(
                FactoryBuilder.factoryOf(listener), null, true, true));

        cache.put("a", "1"); // created
        cache.put("a", "2"); // updated
        cache.remove("a"); // removed

        assertEquals(List.of("CREATED:a:1", "UPDATED:a:2:old=1", "REMOVED:a:old=2"), events);
    }

    @Test
    void aFilterSuppressesNonMatchingEvents() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingListener<String, String> listener = new RecordingListener<>(events);
        OnlyKeyAFilter<String, String> onlyA = new OnlyKeyAFilter<>();
        cache.registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<>(
                FactoryBuilder.factoryOf(listener), FactoryBuilder.factoryOf(onlyA), true, true));

        cache.put("a", "1");
        cache.put("b", "1");

        assertEquals(List.of("CREATED:a:1"), events);
    }

    @Test
    void deregisteringAListenerStopsFutureEvents() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingListener<String, String> listener = new RecordingListener<>(events);
        CacheEntryListenerConfiguration<String, String> config = new MutableCacheEntryListenerConfiguration<>(
                FactoryBuilder.factoryOf(listener), null, true, true);
        cache.registerCacheEntryListener(config);
        cache.deregisterCacheEntryListener(config);

        cache.put("a", "1");

        assertTrue(events.isEmpty());
    }

    // ── configuration / lifecycle ────────────────────────────────────

    @Test
    void getConfigurationReturnsTheCompleteConfiguration() {
        javax.cache.configuration.CompleteConfiguration<String, String> configuration =
                cache.getConfiguration(javax.cache.configuration.CompleteConfiguration.class);
        assertEquals(String.class, configuration.getKeyType());
        assertEquals(String.class, configuration.getValueType());
    }

    @Test
    void aClosedCacheRejectsFurtherOperations() {
        cache.close();

        assertTrue(cache.isClosed());
        assertThrows(IllegalStateException.class, () -> cache.get("a"));
        assertNull(manager.getCache("widgets"));
    }

    private static final class OnlyKeyAFilter<K, V> implements CacheEntryEventFilter<K, V>, java.io.Serializable {
        @Override
        public boolean evaluate(CacheEntryEvent<? extends K, ? extends V> event) {
            return event.getKey().equals("a");
        }
    }

    private static final class RecordingListener<K, V>
            implements CacheEntryCreatedListener<K, V>,
                    CacheEntryUpdatedListener<K, V>,
                    CacheEntryRemovedListener<K, V>,
                    java.io.Serializable {

        private final transient List<String> events;

        RecordingListener(List<String> events) {
            this.events = events;
        }

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> iterable)
                throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends K, ? extends V> event : iterable) {
                events.add("CREATED:" + event.getKey() + ":" + event.getValue());
            }
        }

        @Override
        public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> iterable)
                throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends K, ? extends V> event : iterable) {
                String oldValue = event.isOldValueAvailable() ? String.valueOf(event.getOldValue()) : "?";
                events.add("UPDATED:" + event.getKey() + ":" + event.getValue() + ":old=" + oldValue);
            }
        }

        @Override
        public void onRemoved(Iterable<CacheEntryEvent<? extends K, ? extends V>> iterable)
                throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends K, ? extends V> event : iterable) {
                String oldValue = event.isOldValueAvailable() ? String.valueOf(event.getOldValue()) : "?";
                events.add("REMOVED:" + event.getKey() + ":old=" + oldValue);
            }
        }
    }
}
