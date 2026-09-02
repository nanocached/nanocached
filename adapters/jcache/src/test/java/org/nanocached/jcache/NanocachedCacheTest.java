package org.nanocached.jcache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
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
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.management.CacheStatisticsMXBean;
import javax.cache.processor.EntryProcessor;
import javax.cache.spi.CachingProvider;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nanocached.NanocachedException;

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

    // ── replace(k,old,new) / remove(k,old) compare by equals(), not a
    //    re-serialized digest (issue #186) ──────────────────────────
    //
    // A HashMap's serialized form isn't canonical: java.util.HashMap
    // writes its internal table's bucket count ahead of its entries, so
    // two HashMaps that are equals()-equal (same key/value pairs) but
    // built with different initial capacities serialize to different
    // bytes even though nothing about their contents differs. Before
    // issue #186's fix, replace/remove built the CAS token from a
    // digest of ValueCodec.serialize(oldValue) and compared it
    // byte-for-byte against what was actually stored — so passing an
    // equals()-equal HashMap with a different capacity than the one
    // that was put() silently failed the CAS (a no-op) even though
    // JSR-107 specifies equals() semantics here, not "identical wire
    // bytes".

    @Test
    void threeArgReplaceSucceedsWhenTheOldValueIsEqualButNotIdenticallySerialized() {
        MutableConfiguration<String, HashMap<String, String>> config = new MutableConfiguration<>();
        config.setTypes(String.class, (Class<HashMap<String, String>>) (Class<?>) HashMap.class);
        config.setExpiryPolicyFactory(EternalExpiryPolicy.factoryOf());
        Cache<String, HashMap<String, String>> mapCache = manager.createCache("map-widgets", config);

        HashMap<String, String> stored = new HashMap<>();
        stored.put("x", "1");
        stored.put("y", "2");
        stored.put("z", "3");
        mapCache.put("a", stored);

        HashMap<String, String> equalButDifferentCapacity = new HashMap<>(1024);
        equalButDifferentCapacity.put("x", "1");
        equalButDifferentCapacity.put("y", "2");
        equalButDifferentCapacity.put("z", "3");
        assertEquals(stored, equalButDifferentCapacity);

        HashMap<String, String> replacement = new HashMap<>();
        replacement.put("w", "4");
        assertTrue(mapCache.replace("a", equalButDifferentCapacity, replacement));
        assertEquals(replacement, mapCache.get("a"));
    }

    @Test
    void twoArgRemoveSucceedsWhenTheOldValueIsEqualButNotIdenticallySerialized() {
        MutableConfiguration<String, HashMap<String, String>> config = new MutableConfiguration<>();
        config.setTypes(String.class, (Class<HashMap<String, String>>) (Class<?>) HashMap.class);
        config.setExpiryPolicyFactory(EternalExpiryPolicy.factoryOf());
        Cache<String, HashMap<String, String>> mapCache = manager.createCache("map-widgets-remove", config);

        HashMap<String, String> stored = new HashMap<>();
        stored.put("x", "1");
        stored.put("y", "2");
        stored.put("z", "3");
        mapCache.put("a", stored);

        HashMap<String, String> equalButDifferentCapacity = new HashMap<>(1024);
        equalButDifferentCapacity.put("x", "1");
        equalButDifferentCapacity.put("y", "2");
        equalButDifferentCapacity.put("z", "3");
        assertEquals(stored, equalButDifferentCapacity);

        assertTrue(mapCache.remove("a", equalButDifferentCapacity));
        assertFalse(mapCache.containsKey("a"));
    }

    @Test
    void threeArgReplaceRetriesAfterAOneShotCasMismatchAndStillSucceeds() {
        cache.put("a", "1");
        node.forceCasMismatchOnce.set(true);
        assertTrue(cache.replace("a", "1", "2"));
        assertEquals("2", cache.get("a"));
        assertTrue(node.casSetCount.get() >= 2, "a forced mismatch must cause a retry");
    }

    @Test
    void twoArgRemoveRetriesAfterAOneShotCasMismatchAndStillSucceeds() {
        cache.put("a", "1");
        node.forceCasMismatchOnce.set(true);
        assertTrue(cache.remove("a", "1"));
        assertFalse(cache.containsKey("a"));
        assertTrue(node.casDeleteCount.get() >= 2, "a forced mismatch must cause a retry");
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
    void aByteArrayKeyDoesNotCollideWithATypePrefixedStringKey() {
        // Regression (pass-7 audit): byte[] keys used to pass through raw
        // while String keys became "String:<value>". A byte[] whose bytes
        // spell "String:x" would then share a wire key with the string "x"
        // and one would read back the other's value. The 0x00 marker on
        // byte[] keys keeps the two families apart.
        MutableConfiguration<Object, String> config = new MutableConfiguration<>();
        config.setTypes(Object.class, String.class);
        config.setExpiryPolicyFactory(EternalExpiryPolicy.factoryOf());
        Cache<Object, String> mixed = manager.createCache("collision", config);

        byte[] spellsStringX = "String:x".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        mixed.put(spellsStringX, "from-bytes");
        mixed.put("x", "from-string");

        assertEquals("from-bytes", mixed.get(spellsStringX));
        assertEquals("from-string", mixed.get("x"));
    }

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
    void getAllWithAnAccessExpiryPolicyRefreshesEveryHitInOneBulkWrite() {
        // issue #192: getAll used to refresh an access-based ExpiryPolicy
        // one key at a time (one wire round trip per hit); it must now
        // batch every hit's refresh into a single setManyBytes call,
        // exactly like putAll batches its writes.
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setExpiryPolicyFactory(
                AccessedExpiryPolicy.factoryOf(new Duration(java.util.concurrent.TimeUnit.SECONDS, 30)));
        Cache<String, String> slidingCache = manager.createCache("sliding-all", config);
        slidingCache.put("a", "1");
        slidingCache.put("b", "2");

        // Simulate time having passed, bypassing the cache, on both keys.
        node.store("sliding-all")
                .put(
                        java.nio.ByteBuffer.wrap(KeyCodec.toKeyBytes("a")),
                        new MockNode.Entry(node.entry("sliding-all", KeyCodec.toKeyBytes("a")).value(), 5L));
        node.store("sliding-all")
                .put(
                        java.nio.ByteBuffer.wrap(KeyCodec.toKeyBytes("b")),
                        new MockNode.Entry(node.entry("sliding-all", KeyCodec.toKeyBytes("b")).value(), 5L));
        node.multiSetCount.set(0);

        Map<String, String> all = slidingCache.getAll(Set.of("a", "b", "missing"));

        assertEquals(2, all.size());
        assertEquals("1", all.get("a"));
        assertEquals("2", all.get("b"));
        assertEquals(1, node.multiSetCount.get(), "one bulk write refreshes every hit's TTL");
        assertEquals(30L, storedTtlSeconds("sliding-all", "a"), "a getAll must refresh the TTL via getExpiryForAccess");
        assertEquals(30L, storedTtlSeconds("sliding-all", "b"), "a getAll must refresh the TTL via getExpiryForAccess");
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
        // ModifiedExpiryPolicy(ZERO), not CreatedExpiryPolicy(ZERO): per
        // JSR-107, CreatedExpiryPolicy.getExpiryForUpdate() always returns
        // null ("leave the current TTL alone"), so it can never actually
        // resolve an *update* to Duration.ZERO — this test's "deletes
        // existing" half went untested until issue #278. ModifiedExpiryPolicy
        // resolves both creation and update to the given duration, so ZERO
        // here really means "never retain," for a fresh key and an
        // already-present one alike.
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setExpiryPolicyFactory(ModifiedExpiryPolicy.factoryOf(Duration.ZERO));
        Cache<String, String> zeroCache = manager.createCache("zero-all", config);

        // Seed "a" as already-present, bypassing the cache — a ZERO policy
        // can never create an entry through the cache itself (same reason
        // accessedExpiryPolicyRefreshesTheTtlOnEveryGet seeds directly).
        node.store("zero-all")
                .put(
                        java.nio.ByteBuffer.wrap(KeyCodec.toKeyBytes("a")),
                        new MockNode.Entry(ValueCodec.serialize("old"), 60L));

        List<String> events = new CopyOnWriteArrayList<>();
        RecordingListener<String, String> listener = new RecordingListener<>(events);
        zeroCache.registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<>(
                FactoryBuilder.factoryOf(listener), null, true, true));
        node.multiSetCount.set(0);

        zeroCache.putAll(Map.of("a", "1", "b", "2"));

        assertNull(zeroCache.get("a"));
        assertNull(zeroCache.get("b"));
        assertEquals(0, node.multiSetCount.get());
        // Issue #278: an update resolving to Duration.ZERO must fire
        // Removed, exactly like every other removal path in this class.
        assertEquals(List.of("REMOVED:a:old=old"), events);
    }

    @Test
    void putAllOfAnEmptyMapIsANoOp() {
        cache.putAll(Map.of());
        assertEquals(0, node.multiGetCount.get());
        assertEquals(0, node.multiSetCount.get());
    }

    // ── getAll/putAll partial-wrong-node recovery (issue #415) ───────

    @Test
    void getAllRetriesAndMergesTheRemainderAfterAMidBatchWrongNode() {
        // A ring change mid-batch used to discard the whole getAll: the
        // SDK's getManyBytes throws NanocachedException.PartialWrongNodeRaw,
        // which carries every key that DID resolve, but getAll used to let
        // that exception propagate uncaught, losing the resolved data too.
        // It must now retry just the still-unresolved remainder and merge
        // the result back in.
        cache.putAll(Map.of("a", "1", "b", "2", "c", "3"));
        node.forceWrongNodeCountsForGet.put(
                java.nio.ByteBuffer.wrap(KeyCodec.toKeyBytes("b")), new AtomicInteger(1));
        node.multiGetCount.set(0);

        Map<String, String> all = cache.getAll(Set.of("a", "b", "c", "missing"));

        assertEquals(Map.of("a", "1", "b", "2", "c", "3"), all);
        assertEquals(2, node.multiGetCount.get(), "the forced W costs exactly one extra retry round");
    }

    @Test
    void getAllPropagatesWrongNodeOnceItsRetryBudgetIsExhausted() {
        // A key that never actually resolves (the ring never settles)
        // must not retry forever — it eventually propagates, the same as
        // it would if the SDK's own single-key withWrongNodeRetry gave up.
        cache.put("a", "1");
        node.forceWrongNodeCountsForGet.put(
                java.nio.ByteBuffer.wrap(KeyCodec.toKeyBytes("a")), new AtomicInteger(100));

        assertThrows(NanocachedException.WrongNode.class, () -> cache.getAll(Set.of("a")));
    }

    @Test
    void putAllRetriesTheGroupAndFiresEventsAfterAMidBatchWrongNode() {
        // Same regression as getAll's above, but for putAll: setManyBytes
        // has no per-key partial payload to retry a remainder from (see
        // NanocachedException.PartialWrongNode's own doc), so putAll
        // retries the whole TTL group instead — a mid-batch ring change
        // must not abort the write and drop its listener events.
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingListener<String, String> listener = new RecordingListener<>(events);
        cache.registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<>(
                FactoryBuilder.factoryOf(listener), null, true, true));
        node.forceWrongNodeCountsForSet.put(
                java.nio.ByteBuffer.wrap(KeyCodec.toKeyBytes("b")), new AtomicInteger(1));
        node.multiSetCount.set(0);

        cache.putAll(Map.of("a", "1", "b", "2"));

        assertEquals("1", cache.get("a"));
        assertEquals("2", cache.get("b"));
        assertEquals(2, node.multiSetCount.get(), "the forced W costs exactly one extra retry round");
        assertEquals(2, events.size());
        assertTrue(events.contains("CREATED:a:1"));
        assertTrue(events.contains("CREATED:b:2"));
    }

    @Test
    void putAllPropagatesWrongNodeOnceItsRetryBudgetIsExhausted() {
        node.forceWrongNodeCountsForSet.put(
                java.nio.ByteBuffer.wrap(KeyCodec.toKeyBytes("a")), new AtomicInteger(100));

        assertThrows(NanocachedException.WrongNode.class, () -> cache.putAll(Map.of("a", "1")));
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
    void removeAllOfAnEmptySetIsANoOp() {
        cache.removeAll(Set.of());
    }

    @Test
    void removeAllFansItsDeletesOutConcurrentlyRatherThanOneAtATime() {
        // Issue #415: removeAll(Set) used to be `for (K key : keys)
        // remove(key)` — one blocking RPC after another. With N keys and
        // each delete taking node.deleteDelayMillis to answer (answered
        // off the mock's read loop — see MockNode.delete's doc — so the
        // delay only measures how many round trips are actually in
        // flight together, not the mock's own processing order), the old
        // loop takes roughly N * delayMillis; a concurrent fan-out takes
        // roughly one delayMillis, however many keys there are.
        int keyCount = 20;
        long delayMillis = 150;
        Map<String, String> entries = new HashMap<>();
        for (int i = 0; i < keyCount; i++) {
            entries.put("k" + i, "v" + i);
        }
        cache.putAll(entries);
        node.deleteDelayMillis = delayMillis;
        long elapsedMillis;
        try {
            long start = System.nanoTime();
            cache.removeAll(entries.keySet());
            elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        } finally {
            node.deleteDelayMillis = 0;
        }

        assertTrue(
                elapsedMillis < keyCount * delayMillis / 2,
                "removeAll should fan its deletes out concurrently, not run them one at a time (took "
                        + elapsedMillis + "ms for " + keyCount + " keys at " + delayMillis + "ms each)");
        for (String key : entries.keySet()) {
            assertFalse(cache.containsKey(key));
        }
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
    void durationZeroOnUpdateDeletesTheExistingEntryAndFiresRemoved() {
        // See putAllWithDurationZeroDeletesExistingAndSkipsNew's comment for
        // why ModifiedExpiryPolicy, not CreatedExpiryPolicy, is what
        // actually resolves an *update* to Duration.ZERO.
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setExpiryPolicyFactory(ModifiedExpiryPolicy.factoryOf(Duration.ZERO));
        Cache<String, String> zeroCache = manager.createCache("zero-update", config);

        // Seed "a" as already-present, bypassing the cache — see the
        // putAll test above for why.
        node.store("zero-update")
                .put(
                        java.nio.ByteBuffer.wrap(KeyCodec.toKeyBytes("a")),
                        new MockNode.Entry(ValueCodec.serialize("old"), 60L));

        List<String> events = new CopyOnWriteArrayList<>();
        RecordingListener<String, String> listener = new RecordingListener<>(events);
        zeroCache.registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<>(
                FactoryBuilder.factoryOf(listener), null, true, true));

        zeroCache.put("a", "1"); // update resolves to Duration.ZERO -> deletes

        assertNull(zeroCache.get("a"));
        // Issue #278: put()'s Duration.ZERO-on-update branch must fire
        // Removed too, exactly like remove()/getAndRemove()/getAndPut().
        assertEquals(List.of("REMOVED:a:old=old"), events);
    }

    @Test
    void twoArgReplaceOnDurationZeroOnUpdateDeletesAndFiresRemoved() throws Exception {
        // Issue #331: replace(K,V)'s Duration.ZERO-on-update branch deleted
        // the entry but never fired Removed nor recorded the removal —
        // unlike put()/getAndPut()/replace(K,V,V)/getAndReplace's equivalent
        // zero-TTL branches (see the #278 comment on put()).
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setExpiryPolicyFactory(ModifiedExpiryPolicy.factoryOf(Duration.ZERO));
        Cache<String, String> zeroCache = manager.createCache("zero-replace", config);
        manager.enableStatistics("zero-replace", true);

        // Seed "a" as already-present, bypassing the cache — see the putAll
        // test above for why.
        node.store("zero-replace")
                .put(
                        java.nio.ByteBuffer.wrap(KeyCodec.toKeyBytes("a")),
                        new MockNode.Entry(ValueCodec.serialize("old"), 60L));

        List<String> events = new CopyOnWriteArrayList<>();
        RecordingListener<String, String> listener = new RecordingListener<>(events);
        zeroCache.registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<>(
                FactoryBuilder.factoryOf(listener), null, true, true));

        assertTrue(zeroCache.replace("a", "1")); // update resolves to Duration.ZERO -> deletes

        assertNull(zeroCache.get("a"));
        assertEquals(List.of("REMOVED:a:old=old"), events);
        assertEquals(1, statisticsMBean("zero-replace").getCacheRemovals());
    }

    @Test
    void getAndPutFallbackOverwriteOnDurationZeroDeletesAndFiresRemoved() throws Exception {
        // Issue #331: fallbackOverwrite's delete branch (reached once the CAS
        // retry budget is exhausted and the TTL resolves to Duration.ZERO)
        // deleted the entry but never recorded the removal in statistics nor
        // fired Removed — unlike getAndReplace's equivalent inline fallback
        // branch, which it mirrors.
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setExpiryPolicyFactory(ModifiedExpiryPolicy.factoryOf(Duration.ZERO));
        Cache<String, String> zeroCache = manager.createCache("zero-fallback", config);
        manager.enableStatistics("zero-fallback", true);

        // Seed "a" as already-present, bypassing the cache — see the putAll
        // test above for why.
        node.store("zero-fallback")
                .put(
                        java.nio.ByteBuffer.wrap(KeyCodec.toKeyBytes("a")),
                        new MockNode.Entry(ValueCodec.serialize("old"), 60L));

        List<String> events = new CopyOnWriteArrayList<>();
        RecordingListener<String, String> listener = new RecordingListener<>(events);
        zeroCache.registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<>(
                FactoryBuilder.factoryOf(listener), null, true, true));

        // Sustained CAS mismatch on every k/x request exhausts getAndPut's
        // retry budget, forcing it to fall through to fallbackOverwrite.
        node.forceCasMismatchCount.set(50);

        assertEquals("old", zeroCache.getAndPut("a", "1"));

        assertNull(zeroCache.get("a"));
        assertEquals(List.of("REMOVED:a:old=old"), events);
        assertEquals(1, statisticsMBean("zero-fallback").getCacheRemovals());
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
