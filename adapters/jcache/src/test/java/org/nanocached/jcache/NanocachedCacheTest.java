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
    void getAllWithAnAccessExpiryPolicyRefreshesEveryHitInOneBatchedTouch() {
        // issue #192 batched the refresh of an access-based ExpiryPolicy
        // into one setManyBytes; issue #871 replaces that re-set with one
        // batched touch that reads and refreshes every key in the same step.
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
        node.multiGetCount.set(0);

        Map<String, String> all = slidingCache.getAll(Set.of("a", "b", "missing"));

        assertEquals(2, all.size());
        assertEquals("1", all.get("a"));
        assertEquals("2", all.get("b"));
        assertEquals(1, node.multiTouchFrames.size(), "one batched touch reads and refreshes every key");
        assertTrue(node.multiTouchFrames.get(0).contains(" 30 G"), node.multiTouchFrames.get(0));
        assertEquals(0, node.multiGetCount.get(), "no separate batched read");
        assertEquals(0, node.multiSetCount.get(), "no re-set of the values");
        assertEquals(30L, storedTtlSeconds("sliding-all", "a"), "a getAll must refresh the TTL via getExpiryForAccess");
        assertEquals(30L, storedTtlSeconds("sliding-all", "b"), "a getAll must refresh the TTL via getExpiryForAccess");
        assertNull(node.entry("sliding-all", KeyCodec.toKeyBytes("missing")), "a miss is not created");
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

        javax.cache.CacheException error =
                assertThrows(javax.cache.CacheException.class, () -> cache.getAll(Set.of("a")));
        assertTrue(error.getCause() instanceof NanocachedException.WrongNode, "cause: " + error.getCause());
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

        javax.cache.CacheException error =
                assertThrows(javax.cache.CacheException.class, () -> cache.putAll(Map.of("a", "1")));
        assertTrue(error.getCause() instanceof NanocachedException.WrongNode, "cause: " + error.getCause());
    }

    // ── getAll/putAll partial-connection-loss recovery (issue #439) ───

    @Test
    void getAllRetriesAndMergesTheRemainderAfterAMidBatchConnectionDrop() {
        // A connection dying mid-batch used to discard the whole getAll
        // too, same as #415's ring-change regression above but for
        // NanocachedException.PartialConnectionFailedRaw instead of
        // PartialWrongNodeRaw: it carries every key the earlier,
        // already-succeeded chunk(s) resolved, but getAll used to let
        // that exception propagate uncaught, losing the resolved data
        // too. It must now make exactly one direct retry of the
        // still-unresolved remainder and merge the result back in.
        int keyCount = 401; // MAX_BATCH_KEYS(400) + 1 forces exactly two `m` sub-frames
        Map<String, String> seed = new HashMap<>();
        for (int i = 0; i < keyCount; i++) {
            seed.put("k" + i, "v" + i);
        }
        cache.putAll(seed);
        node.multiGetCount.set(0);
        // chunk 1 succeeds; both of chunk 2's attempts (the SDK's own
        // original attempt plus its one internal reconnect-and-retry) are
        // dropped, so the SDK throws PartialConnectionFailedRaw; the
        // adapter's own single follow-up retry (request #4) then
        // succeeds since the drop budget is exhausted by then.
        node.armMultiGetDrop(1, 2);

        Map<String, String> all = cache.getAll(seed.keySet());

        assertEquals(seed, all);
        assertEquals(
                4,
                node.multiGetCount.get(),
                "chunk1 + chunk2-attempt1 + chunk2-attempt2 + adapter-retry");
    }

    @Test
    void getAllPropagatesConnectionFailureOnceTheAdapterRetryAlsoFails() {
        // A connection that never recovers must not retry forever — it
        // eventually propagates, the same as it would if the SDK's own
        // built-in reconnect-and-retry gave up.
        int keyCount = 401;
        Map<String, String> seed = new HashMap<>();
        for (int i = 0; i < keyCount; i++) {
            seed.put("k" + i, "v" + i);
        }
        cache.putAll(seed);
        node.multiGetCount.set(0);
        // chunk 2's two attempts AND the adapter's own single retry's two
        // attempts (its own internal original + reconnect-retry) all drop.
        node.armMultiGetDrop(1, 4);

        javax.cache.CacheException error =
                assertThrows(javax.cache.CacheException.class, () -> cache.getAll(seed.keySet()));
        assertTrue(error.getCause() instanceof NanocachedException.ConnectionFailed, "cause: " + error.getCause());
    }

    @Test
    void getAllKeepsResolvingWhenTheFollowUpAfterAConnectionDropMeetsARingChange() {
        // Chunk 2's two attempts drop, so the SDK throws
        // PartialConnectionFailedRaw and the adapter makes its one
        // follow-up call for the remainder. That call meets a ring
        // change: the SDK throws PartialWrongNodeRaw, which used to
        // escape getAll raw (not a CacheException) with chunk 1's 400
        // resolved values discarded. The remainder now goes on through
        // the wrong-node retry and the whole map comes back.
        int keyCount = 401;
        Map<String, String> seed = new java.util.LinkedHashMap<>();
        for (int i = 0; i < keyCount; i++) {
            seed.put("k" + i, "v" + i);
        }
        cache.putAll(seed);
        node.multiGetCount.set(0);
        node.armMultiGetDrop(1, 2);
        node.forceWrongNodeCountsForGet.put(
                java.nio.ByteBuffer.wrap(KeyCodec.toKeyBytes("k400")), new AtomicInteger(2));

        Map<String, String> all = cache.getAll(new java.util.LinkedHashSet<>(seed.keySet()));

        assertEquals(seed, all);
    }

    @Test
    void putAllKeepsResolvingWhenTheFollowUpAfterAConnectionDropMeetsARingChange() {
        // The set-side twin: the adapter's one follow-up for the
        // positions not yet confirmed stored meets a W, which used to
        // escape putAll as the SDK's raw WrongNode with those keys never
        // stored. It is resent through the wrong-node retry instead.
        int keyCount = 401;
        Map<String, String> seed = new java.util.LinkedHashMap<>();
        for (int i = 0; i < keyCount; i++) {
            seed.put("k" + i, "v" + i);
        }
        node.armMultiSetDrop(1, 2);
        node.forceWrongNodeCountsForSet.put(
                java.nio.ByteBuffer.wrap(KeyCodec.toKeyBytes("k400")), new AtomicInteger(1));

        cache.putAll(seed);

        for (int i = 0; i < keyCount; i++) {
            assertEquals("v" + i, cache.get("k" + i), "key k" + i + " must have landed");
        }
    }

    @Test
    void putAllRetriesTheRemainderAfterAMidBatchConnectionDropOnSet() {
        // Analogous to getAllRetriesAndMergesTheRemainderAfterAMidBatchConnectionDrop
        // above, but for putAll/setManyBytes: NanocachedException.
        // PartialConnectionFailedSetRaw names the positions already
        // confirmed stored (the inverse of the get side's convention —
        // see the exception's own doc), so putAll must retry only the
        // still-unconfirmed remainder once rather than discarding it.
        int keyCount = 401;
        Map<String, String> seed = new HashMap<>();
        for (int i = 0; i < keyCount; i++) {
            seed.put("k" + i, "v" + i);
        }
        node.multiSetCount.set(0);
        node.armMultiSetDrop(1, 2);

        cache.putAll(seed);

        for (int i = 0; i < keyCount; i++) {
            assertEquals("v" + i, cache.get("k" + i), "key k" + i + " must have landed");
        }
        assertEquals(
                4,
                node.multiSetCount.get(),
                "chunk1 + chunk2-attempt1 + chunk2-attempt2 + adapter-retry");
    }

    @Test
    void putAllPropagatesConnectionFailureOnceTheAdapterRetryAlsoFailsOnSet() {
        int keyCount = 401;
        Map<String, String> seed = new HashMap<>();
        for (int i = 0; i < keyCount; i++) {
            seed.put("k" + i, "v" + i);
        }
        node.multiSetCount.set(0);
        node.armMultiSetDrop(1, 4);

        javax.cache.CacheException error =
                assertThrows(javax.cache.CacheException.class, () -> cache.putAll(seed));
        assertTrue(error.getCause() instanceof NanocachedException.ConnectionFailed, "cause: " + error.getCause());
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
    void removeAllWorkerThreadsTimeOutOnceTheFanOutIsDone() throws Exception {
        // The fan-out pool is static (JVM-wide) and never shut down, so its
        // core threads must be allowed to time out: otherwise one large
        // removeAll leaves up to 256 idle threads alive for the life of the
        // JVM, pinning this class's loader across redeploys.
        assertTrue(NanocachedCache.REMOVE_ALL_EXECUTOR.allowsCoreThreadTimeOut());
        int keyCount = 40;
        Map<String, String> entries = new HashMap<>();
        for (int i = 0; i < keyCount; i++) {
            entries.put("k" + i, "v" + i);
        }
        cache.putAll(entries);
        node.deleteDelayMillis = 50;
        try {
            cache.removeAll(entries.keySet());
        } finally {
            node.deleteDelayMillis = 0;
        }
        assertTrue(NanocachedCache.REMOVE_ALL_EXECUTOR.getPoolSize() > 1, "the fan-out should have spun up workers");

        long deadline = System.nanoTime() + 20_000_000_000L;
        while (NanocachedCache.REMOVE_ALL_EXECUTOR.getPoolSize() > 0 && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(0, NanocachedCache.REMOVE_ALL_EXECUTOR.getPoolSize(), "idle removeAll workers should exit");
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
    void getAndPutOnDurationZeroKeepsRetryingTheConditionedDeleteAndFiresRemoved() throws Exception {
        // The update expiry resolves to Duration.ZERO, so getAndPut's
        // write is a conditioned delete. Sustained CAS mismatches used to
        // exhaust a fixed budget and fall through to an unconditional
        // delete; now the loop keeps retrying until the mock stops
        // rejecting, and the removal is recorded and fired exactly once.
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

        node.forceCasMismatchCount.set(50);

        assertEquals("old", zeroCache.getAndPut("a", "1"));

        assertNull(zeroCache.get("a"));
        assertEquals(List.of("REMOVED:a:old=old"), events);
        assertEquals(1, statisticsMBean("zero-fallback").getCacheRemovals());
        assertEquals(0, node.deleteCount.get(), "no unconditional delete");
        assertTrue(node.casDeleteCount.get() > 50, "the conditioned delete was retried past the old budget");
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

    // ── access expiry through getAndTouch (issue #826) ──────────────

    private Cache<String, String> accessCache(String name, Duration access) {
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(access));
        return manager.createCache(name, config);
    }

    private void seedWithTtl(String namespace, String key, String value, long ttlSeconds) {
        node.store(namespace)
                .put(
                        java.nio.ByteBuffer.wrap(KeyCodec.toKeyBytes(key)),
                        new MockNode.Entry(ValueCodec.serialize(value), ttlSeconds));
    }

    @Test
    void anAccessedGetIsOneGetAndTouchNotAReadAndAReSet() {
        Cache<String, String> sliding =
                accessCache("touch-once", new Duration(java.util.concurrent.TimeUnit.SECONDS, 30));
        sliding.put("a", "1");
        seedWithTtl("touch-once", "a", "1", 5L);
        int gets = node.getCount.get();
        int sets = node.setCount.get();

        assertEquals("1", sliding.get("a"));

        assertEquals(1, node.touchFrames.size());
        assertTrue(node.touchFrames.get(0).contains(" 30 G"), node.touchFrames.get(0));
        assertEquals(gets, node.getCount.get(), "no plain get");
        assertEquals(sets, node.setCount.get(), "no re-set of the value");
        assertEquals(30L, storedTtlSeconds("touch-once", "a"));
    }

    @Test
    void anAccessedGetOfAMissingKeyIsAMissAndCreatesNothing() throws Exception {
        Cache<String, String> sliding =
                accessCache("touch-miss", new Duration(java.util.concurrent.TimeUnit.SECONDS, 30));
        manager.enableStatistics("touch-miss", true);

        assertNull(sliding.get("absent"));

        assertEquals(1, node.touchFrames.size());
        assertNull(node.entry("touch-miss", KeyCodec.toKeyBytes("absent")));
        assertEquals(1, statisticsMBean("touch-miss").getCacheMisses());
    }

    @Test
    void anEternalAccessPolicyRemovesTheTtlOnGet() {
        Cache<String, String> sliding = accessCache("touch-eternal", Duration.ETERNAL);
        seedWithTtl("touch-eternal", "a", "1", 5L);

        assertEquals("1", sliding.get("a"));

        assertEquals(0L, storedTtlSeconds("touch-eternal", "a"), "0 is the mock's no-TTL");
        assertFalse(node.touchFrames.get(0).contains(" 0 G"), "no TTL field, not a 0 that would expire it");
    }

    @Test
    void aZeroAccessPolicyReturnsTheValueAndExpiresTheEntryInOneStep() {
        Cache<String, String> sliding = accessCache("touch-zero", Duration.ZERO);
        seedWithTtl("touch-zero", "a", "1", 30L);

        assertEquals("1", sliding.get("a"));

        assertNull(node.entry("touch-zero", KeyCodec.toKeyBytes("a")));
        assertEquals(0, node.deleteCount.get(), "no separate delete");
        assertNull(sliding.get("a"));
    }

    @Test
    void aCachelessAccessPolicyStillReadsWithAPlainGet() {
        // Only a non-null getExpiryForAccess() touches: the default
        // policies never do, so their reads stay plain gets.
        cache.put("plain", "1");
        assertEquals("1", cache.get("plain"));
        assertEquals(0, node.touchFrames.size());
    }

    @Test
    void aWriteLandingDuringAnAccessedGetIsNotOverwritten() throws Exception {
        // issue #826: the old get() re-sent the value it had read, so a
        // write that landed between the read and the refresh was lost. The
        // concurrent write is injected where that gap was: after the node
        // has looked the value up for the read (the old code) or has seen
        // the touch (the new one), before answering. It goes over its own
        // raw connection, as a namespaced `s` frame.
        Cache<String, String> sliding =
                accessCache("touch-race", new Duration(java.util.concurrent.TimeUnit.SECONDS, 30));
        sliding.put("a", "old");
        java.util.concurrent.atomic.AtomicBoolean fired = new java.util.concurrent.atomic.AtomicBoolean();
        node.beforeAccessApplies = () -> {
            if (fired.compareAndSet(false, true)) {
                rawSet("touch-race", KeyCodec.toKeyBytes("a"), ValueCodec.serialize("new"), 30L);
            }
        };

        sliding.get("a");
        node.beforeAccessApplies = null;

        assertEquals("new", sliding.get("a"));
        assertEquals(
                "new",
                ValueCodec.deserialize(
                        node.entry("touch-race", KeyCodec.toKeyBytes("a")).value(),
                        getClass().getClassLoader()));
    }

    // ── getAll through the batched touch (issue #871) ───────────────

    @Test
    void getAllWithoutAnAccessPolicyStillReadsWithAPlainBatchedGet() {
        cache.put("plain", "1");
        node.multiGetCount.set(0);

        assertEquals(Map.of("plain", "1"), cache.getAll(Set.of("plain", "absent")));

        assertEquals(1, node.multiGetCount.get());
        assertEquals(0, node.multiTouchFrames.size());
    }

    @Test
    void anEternalAccessPolicyRemovesTheTtlOnGetAll() {
        Cache<String, String> sliding = accessCache("touch-all-eternal", Duration.ETERNAL);
        seedWithTtl("touch-all-eternal", "a", "1", 5L);
        seedWithTtl("touch-all-eternal", "b", "2", 5L);

        assertEquals(Map.of("a", "1", "b", "2"), sliding.getAll(Set.of("a", "b")));

        assertEquals(0L, storedTtlSeconds("touch-all-eternal", "a"), "0 is the mock's no-TTL");
        assertEquals(0L, storedTtlSeconds("touch-all-eternal", "b"), "0 is the mock's no-TTL");
        assertFalse(node.multiTouchFrames.get(0).contains(" 0 G"), "no TTL field, not a 0 that would expire them");
    }

    @Test
    void aZeroAccessPolicyReturnsTheValuesAndExpiresTheEntriesInOneStep() {
        Cache<String, String> sliding = accessCache("touch-all-zero", Duration.ZERO);
        seedWithTtl("touch-all-zero", "a", "1", 30L);
        seedWithTtl("touch-all-zero", "b", "2", 30L);

        assertEquals(Map.of("a", "1", "b", "2"), sliding.getAll(Set.of("a", "b")));

        assertNull(node.entry("touch-all-zero", KeyCodec.toKeyBytes("a")));
        assertNull(node.entry("touch-all-zero", KeyCodec.toKeyBytes("b")));
        assertEquals(0, node.deleteCount.get(), "no per-key delete");
        assertEquals(1, node.multiTouchFrames.size());
        assertTrue(sliding.getAll(Set.of("a", "b")).isEmpty());
    }

    @Test
    void getAllUnderAnAccessPolicyCountsHitsAndMisses() throws Exception {
        Cache<String, String> sliding =
                accessCache("touch-all-stats", new Duration(java.util.concurrent.TimeUnit.SECONDS, 30));
        manager.enableStatistics("touch-all-stats", true);
        sliding.put("a", "1");

        sliding.getAll(Set.of("a", "absent1", "absent2"));

        assertEquals(1, statisticsMBean("touch-all-stats").getCacheHits());
        assertEquals(2, statisticsMBean("touch-all-stats").getCacheMisses());
    }

    @Test
    void aWriteLandingDuringAnAccessedGetAllIsNotOverwritten() throws Exception {
        // The race issue #871 closes: getAll used to re-send the values it
        // had read, so a write that landed between the read and the
        // refresh was lost. The concurrent write is injected after the node
        // has seen the batched touch and before it answers.
        Cache<String, String> sliding =
                accessCache("touch-all-race", new Duration(java.util.concurrent.TimeUnit.SECONDS, 30));
        sliding.put("a", "old");
        sliding.put("b", "other");
        java.util.concurrent.atomic.AtomicBoolean fired = new java.util.concurrent.atomic.AtomicBoolean();
        node.beforeAccessApplies = () -> {
            if (fired.compareAndSet(false, true)) {
                rawSet("touch-all-race", KeyCodec.toKeyBytes("a"), ValueCodec.serialize("new"), 30L);
            }
        };

        sliding.getAll(Set.of("a", "b"));
        node.beforeAccessApplies = null;

        assertEquals("new", sliding.get("a"));
        assertEquals(
                "new",
                ValueCodec.deserialize(
                        node.entry("touch-all-race", KeyCodec.toKeyBytes("a")).value(),
                        getClass().getClassLoader()));
    }

    @Test
    void getAllUnderAnAccessPolicyRetriesOnlyTheRemainderAfterAMidBatchWrongNode() {
        Cache<String, String> sliding =
                accessCache("touch-all-w", new Duration(java.util.concurrent.TimeUnit.SECONDS, 30));
        sliding.putAll(Map.of("a", "1", "b", "2", "c", "3"));
        seedWithTtl("touch-all-w", "b", "2", 5L);
        node.forceWrongNodeCountsForGet.put(
                java.nio.ByteBuffer.wrap(KeyCodec.toKeyBytes("b")), new AtomicInteger(1));

        Map<String, String> all = sliding.getAll(Set.of("a", "b", "c", "missing"));

        assertEquals(Map.of("a", "1", "b", "2", "c", "3"), all);
        assertEquals(2, node.multiTouchFrames.size(), "the forced W costs exactly one extra retry round");
        assertTrue(node.multiTouchFrames.get(1).startsWith("y " + "touch-all-w".length() + " 1 "),
                "the retry carries only the unresolved key: " + node.multiTouchFrames.get(1));
        assertEquals(30L, storedTtlSeconds("touch-all-w", "b"));
    }

    @Test
    void getAllAgainstAServerWithoutTheBatchedTouchFailsWithServerTooOld() {
        Cache<String, String> sliding =
                accessCache("touch-all-old", new Duration(java.util.concurrent.TimeUnit.SECONDS, 30));
        sliding.put("a", "1");
        node.rejectMultiTouch = true;

        javax.cache.CacheException error =
                assertThrows(javax.cache.CacheException.class, () -> sliding.getAll(Set.of("a")));

        assertTrue(error.getCause() instanceof NanocachedException.ServerTooOld, "cause: " + error.getCause());
        assertEquals("y", ((NanocachedException.ServerTooOld) error.getCause()).command());
        // A cache without an access policy never sends `y`, so it keeps working.
        cache.put("plain", "1");
        assertEquals(Map.of("plain", "1"), cache.getAll(Set.of("plain")));
    }

    private void rawSet(String namespace, byte[] key, byte[] value, long ttlSeconds) {
        byte[] ns = namespace.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (java.net.Socket socket = new java.net.Socket("127.0.0.1", node.port())) {
            java.io.OutputStream out = socket.getOutputStream();
            java.io.InputStream in = socket.getInputStream();
            out.write("A 1\nx".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            readLine(in); // On
            out.write(("s " + ns.length + " " + key.length + " " + value.length + " " + ttlSeconds + "\n")
                    .getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            out.write(ns);
            out.write(key);
            out.write(value);
            out.flush();
            readLine(in); // S
        } catch (java.io.IOException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static String readLine(java.io.InputStream in) throws java.io.IOException {
        StringBuilder line = new StringBuilder();
        for (int b = in.read(); b != '\n' && b != -1; b = in.read()) {
            line.append((char) b);
        }
        return line.toString();
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

    // ── compare-and-set loops never fall back to an unconditional write ──

    @Test
    void getAndPutKeepsRetryingUnderSustainedContentionInsteadOfOverwriting() {
        // Thirty forced mismatches in a row: a fixed budget of ten used to
        // end in an unconditional set whose "previous value" could already
        // be stale and was handed to more than one caller.
        cache.put("a", "old");
        node.forceCasMismatchCount.set(30);
        assertEquals("old", cache.getAndPut("a", "new"));
        assertEquals("new", cache.get("a"));
        assertEquals(1, node.setCount.get(), "only the seeding put sent an unconditional set");
        assertTrue(node.casSetCount.get() > 30, "the compare-and-set was retried past the old budget");
    }

    @Test
    void getAndRemoveNeverFallsBackToAnUnconditionalDelete() {
        cache.put("a", "old");
        node.forceCasMismatchCount.set(30);
        assertEquals("old", cache.getAndRemove("a"));
        assertNull(cache.get("a"));
        assertEquals(0, node.deleteCount.get(), "no unconditional delete");
        assertTrue(node.casDeleteCount.get() > 30);
    }

    @Test
    void aCompareAndSetLoopThatKeepsLosingFailsLoudlyAtTheDeadline() throws Exception {
        // The deadline is reached through reflection so this test still
        // compiles against an adapter without it and then fails (rather
        // than being silently skipped by a compilation error).
        java.lang.reflect.Field deadline = NanocachedCache.class.getDeclaredField("casRetryDeadlineMillis");
        deadline.setAccessible(true);
        long saved = deadline.getLong(null);
        deadline.setLong(null, 200);
        try {
            cache.put("a", "old");
            node.forceCasMismatchCount.set(1_000_000);
            javax.cache.CacheException error =
                    assertThrows(javax.cache.CacheException.class, () -> cache.getAndPut("a", "new"));
            assertTrue(error.getMessage().contains("gave up"), error.getMessage());
            assertEquals(1, node.setCount.get(), "no unconditional overwrite after giving up");
            assertEquals("old", cache.get("a"), "the stored value is untouched");
        } finally {
            deadline.setLong(null, saved);
            node.forceCasMismatchCount.set(0);
        }
    }

    @Test
    void concurrentGetAndPutReturnsEveryPreviousValueExactlyOnce() throws Exception {
        // Eight threads, each a hundred getAndPut with unique values on one
        // key: every value written must come back as a previous value
        // exactly once (plus the initial null), which only holds when
        // every getAndPut is atomic.
        int threads = 8, perThread = 100;
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        try {
            List<java.util.concurrent.Future<List<String>>> results = new java.util.ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int thread = t;
                results.add(pool.submit(() -> {
                    List<String> previous = new java.util.ArrayList<>();
                    for (int i = 0; i < perThread; i++) {
                        previous.add(cache.getAndPut("hot", "t" + thread + "-" + i));
                    }
                    return previous;
                }));
            }
            List<String> all = new java.util.ArrayList<>();
            for (java.util.concurrent.Future<List<String>> result : results) all.addAll(result.get());
            long nulls = all.stream().filter(java.util.Objects::isNull).count();
            assertEquals(1, nulls, "exactly one caller saw the empty cache");
            Set<String> distinct = new java.util.HashSet<>(all);
            assertEquals(all.size(), distinct.size(), "a previous value was returned to more than one caller");
            assertEquals(threads * perThread, all.size());
        } finally {
            pool.shutdownNow();
        }
    }

    // ── backend failures are CacheExceptions ────────────────────────

    @Test
    void backendFailuresAreReportedAsCacheExceptions() throws Exception {
        cache.put("a", "1");
        node.close();
        Map<String, Runnable> operations = new java.util.LinkedHashMap<>();
        operations.put("get", () -> cache.get("a"));
        operations.put("put", () -> cache.put("a", "2"));
        operations.put("containsKey", () -> cache.containsKey("a"));
        operations.put("getAll", () -> cache.getAll(Set.of("a", "b")));
        operations.put("putAll", () -> cache.putAll(Map.of("a", "2", "b", "3")));
        operations.put("remove", () -> cache.remove("a"));
        operations.put("clear", () -> cache.clear());
        operations.put("putIfAbsent", () -> cache.putIfAbsent("c", "3"));
        operations.put("getAndPut", () -> cache.getAndPut("a", "4"));
        for (Map.Entry<String, Runnable> operation : operations.entrySet()) {
            javax.cache.CacheException error = assertThrows(javax.cache.CacheException.class,
                    operation.getValue()::run, operation.getKey() + " must throw CacheException");
            assertTrue(error.getCause() instanceof NanocachedException,
                    operation.getKey() + " cause: " + error.getCause());
        }
    }

    @Test
    void anOversizedValueIsACacheException() {
        javax.cache.CacheException error = assertThrows(
                javax.cache.CacheException.class, () -> cache.put("big", "x".repeat(1_200_000)));
        assertTrue(error.getCause() instanceof IllegalArgumentException, "cause: " + error.getCause());
    }

    // ── the manager's class loader resolves value classes ───────────

    @Test
    void theManagerClassLoaderIsUsedForValueClasses() throws Exception {
        // The application's value class lives in a child loader handed to
        // getCacheManager(uri, classLoader, props), as JSR-107 asks; a
        // plain ObjectInputStream resolved with the adapter's loader and
        // failed every hit with ClassNotFoundException.
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("nanocached-jcache-child");
        java.nio.file.Path source = dir.resolve("app").resolve("JcUser.java");
        java.nio.file.Files.createDirectories(source.getParent());
        java.nio.file.Files.writeString(
                source, "package app; public record JcUser(String name) implements java.io.Serializable {}");
        assertEquals(0, javax.tools.ToolProvider.getSystemJavaCompiler()
                .run(null, null, null, "-d", dir.toString(), source.toString()));
        try (java.net.URLClassLoader child = new java.net.URLClassLoader(
                new java.net.URL[] {dir.toUri().toURL()}, NanocachedCacheTest.class.getClassLoader())) {
            Class<?> type = child.loadClass("app.JcUser");
            Object user = type.getConstructor(String.class).newInstance("alice");
            Properties properties = new Properties();
            properties.setProperty("nanocached.addresses", node.address());
            CacheManager childManager = provider.getCacheManager(URI.create("test:child-loader"), child, properties);
            MutableConfiguration<String, Object> config = new MutableConfiguration<>();
            config.setTypes(String.class, Object.class);
            config.setExpiryPolicyFactory(EternalExpiryPolicy.factoryOf());
            Cache<String, Object> users = childManager.createCache("child-users", config);
            users.put("alice", user);
            assertEquals(user, users.get("alice"));
        }
    }

    // ── a String key with a lone surrogate is rejected ──────────────

    @Test
    void aKeyWithALoneSurrogateIsRejectedInsteadOfCollidingWithAnotherKey() {
        cache.put("x?", "question");
        assertThrows(IllegalArgumentException.class, () -> cache.put("x\uD800", "surrogate"));
        assertThrows(IllegalArgumentException.class, () -> cache.get("x\uD800"));
        assertEquals("question", cache.get("x?"));
    }

    @Test
    void aCharacterKeyThatIsALoneSurrogateIsRejectedLikeAStringHoldingOne() {
        // `Character:` + the char, UTF-8 encoded, turns every lone
        // surrogate into '?', so '\uD800' and '\uD801' addressed one entry.
        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> KeyCodec.toKeyBytes('\uD800'));
        assertTrue(error.getMessage().contains("lone surrogate"), error.getMessage());
        assertEquals("Character:x", new String(KeyCodec.toKeyBytes('x'), java.nio.charset.StandardCharsets.UTF_8));
    }
}
