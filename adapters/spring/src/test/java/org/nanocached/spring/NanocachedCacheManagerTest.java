package org.nanocached.spring;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nanocached.NanocachedClient;
import org.springframework.cache.Cache;
import org.springframework.cache.interceptor.SimpleKey;

class NanocachedCacheManagerTest {

    /** A typical cached domain value: Serializable, value semantics. */
    record User(String name, int age) implements Serializable {}

    private MockNode node;
    private NanocachedClient client;

    @BeforeEach
    void start() throws Exception {
        node = new MockNode();
        client = NanocachedClient.connect(new NanocachedClient.Options()
                .addresses(List.of(new NanocachedClient.Address("127.0.0.1", node.port()))));
    }

    @AfterEach
    void stop() throws Exception {
        client.close();
        node.close();
    }

    private NanocachedCacheManager.Builder manager() {
        return NanocachedCacheManager.builder(client);
    }

    @Test
    void putAndGetRoundTripThroughTheSpringApi() {
        Cache cache = manager().build().getCache("users");

        cache.put("alice", new User("Alice", 30));

        Cache.ValueWrapper wrapper = cache.get("alice");
        assertNotNull(wrapper);
        assertEquals(new User("Alice", 30), wrapper.get());
        assertEquals(new User("Alice", 30), cache.get("alice", User.class));
        assertNull(cache.get("missing"));
    }

    @Test
    void aCacheIsOneNamespaceNamedAfterIt() {
        Cache cache = manager().build().getCache("users");
        cache.put("alice", new User("Alice", 30));

        // The entry landed in the "users" namespace, keyed by the
        // default key converter's canonical form.
        assertNotNull(node.entry("users", "String:alice".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void cachesWithTheSameKeyNameDoNotCollide() {
        NanocachedCacheManager manager = manager().build();
        Cache users = manager.getCache("users");
        Cache orders = manager.getCache("orders");

        users.put("k", new User("Alice", 30));
        orders.put("k", new User("Bob", 40));

        assertEquals(new User("Alice", 30), users.get("k", User.class));
        assertEquals(new User("Bob", 40), orders.get("k", User.class));
    }

    @Test
    void clearWipesOnlyThatCache() {
        NanocachedCacheManager manager = manager().build();
        Cache users = manager.getCache("users");
        Cache orders = manager.getCache("orders");
        users.put("k", new User("Alice", 30));
        orders.put("k", new User("Bob", 40));

        users.clear();

        assertNull(users.get("k"));
        assertEquals(new User("Bob", 40), orders.get("k", User.class));
        assertEquals(1, node.clearCount.get());
    }

    @Test
    void evictDeletesTheOneEntry() {
        Cache cache = manager().build().getCache("users");
        cache.put("a", new User("Alice", 30));
        cache.put("b", new User("Bob", 40));

        cache.evict("a");

        assertNull(cache.get("a"));
        assertNotNull(cache.get("b"));
        assertTrue(cache.evictIfPresent("b"));
        assertFalse(cache.evictIfPresent("b"));
    }

    @Test
    void perCacheTtlOverridesTheDefaultAndReachesTheWire() {
        NanocachedCacheManager manager = manager()
                .defaultTtl(Duration.ofMinutes(5))
                .ttl("sessions", Duration.ofSeconds(30))
                .build();

        manager.getCache("users").put("k", new User("Alice", 30));
        manager.getCache("sessions").put("k", new User("Bob", 40));
        manager.getCache("eternal");

        byte[] key = "String:k".getBytes(StandardCharsets.UTF_8);
        assertEquals(300, node.entry("users", key).ttlSeconds());
        assertEquals(30, node.entry("sessions", key).ttlSeconds());
    }

    @Test
    void zeroTtlMeansNoExpiryAndPositiveTtlRoundsUp() {
        NanocachedCacheManager manager = manager()
                .ttl("forever", Duration.ZERO)
                .ttl("blink", Duration.ofMillis(200))
                // Regression (pass-7 audit): a fractional TTL above 1s must
                // round UP, not floor. 2.5s used to reach the wire as 2s
                // (Duration.toSeconds floors), expiring the entry up to half
                // a second early.
                .ttl("fractional", Duration.ofMillis(2500))
                .build();
        manager.getCache("forever").put("k", new User("Alice", 30));
        manager.getCache("blink").put("k", new User("Bob", 40));
        manager.getCache("fractional").put("k", new User("Carol", 50));

        byte[] key = "String:k".getBytes(StandardCharsets.UTF_8);
        assertEquals(0, node.entry("forever", key).ttlSeconds());
        assertEquals(1, node.entry("blink", key).ttlSeconds());
        assertEquals(3, node.entry("fractional", key).ttlSeconds());
    }

    @Test
    void foreverTtlSaturatesInsteadOfWrappingToOneSecond() {
        // Issue #1886: ChronoUnit.FOREVER is Duration.ofSeconds(Long.MAX_VALUE,
        // 999_999_999); rounding its nano part up used to wrap the seconds to
        // Long.MIN_VALUE, which Math.max(1, ..) turned into a 1s TTL.
        NanocachedCacheManager manager =
                manager().ttl("forever", ChronoUnit.FOREVER.getDuration()).build();
        manager.getCache("forever").put("k", new User("Alice", 30));

        byte[] key = "String:k".getBytes(StandardCharsets.UTF_8);
        assertEquals(Long.MAX_VALUE, node.entry("forever", key).ttlSeconds());
    }

    @Test
    void nullValuesAreCacheableByDefault() {
        Cache cache = manager().build().getCache("users");

        cache.put("nobody", null);

        // A cached null is a HIT whose value is null — distinguishable
        // from the miss for a key never written.
        Cache.ValueWrapper wrapper = cache.get("nobody");
        assertNotNull(wrapper);
        assertNull(wrapper.get());
        assertNull(cache.get("never-written"));
    }

    @Test
    void nullValuesCanBeDisallowed() {
        Cache cache = manager().allowNullValues(false).build().getCache("users");

        assertThrows(IllegalArgumentException.class, () -> cache.put("nobody", null));
    }

    @Test
    void getWithLoaderComputesOnceAndCaches() {
        Cache cache = manager().build().getCache("users");
        AtomicInteger loads = new AtomicInteger();

        User first = cache.get("alice", () -> {
            loads.incrementAndGet();
            return new User("Alice", 30);
        });
        User second = cache.get("alice", () -> {
            loads.incrementAndGet();
            return new User("SHOULD NOT LOAD", 0);
        });

        assertEquals(new User("Alice", 30), first);
        assertEquals(new User("Alice", 30), second);
        assertEquals(1, loads.get());
    }

    @Test
    void getWithLoaderHerdsConcurrentCallersOfTheSameKey() throws Exception {
        Cache cache = manager().build().getCache("users");
        AtomicInteger loads = new AtomicInteger();
        int callers = 8;
        CountDownLatch ready = new CountDownLatch(callers);
        CountDownLatch go = new CountDownLatch(1);
        Thread[] threads = new Thread[callers];
        User[] results = new User[callers];

        for (int i = 0; i < callers; i++) {
            int slot = i;
            threads[i] = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                results[slot] = cache.get("alice", () -> {
                    loads.incrementAndGet();
                    return new User("Alice", 30);
                });
            });
            threads[i].start();
        }
        ready.await();
        go.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(1, loads.get(), "one JVM must compute a herded key once");
        for (User result : results) {
            assertEquals(new User("Alice", 30), result);
        }
    }

    @Test
    void getWithLoaderWrapsLoaderFailures() {
        Cache cache = manager().build().getCache("users");

        Cache.ValueRetrievalException thrown = assertThrows(
                Cache.ValueRetrievalException.class,
                () -> cache.get("alice", () -> {
                    throw new IllegalStateException("db down");
                }));
        assertInstanceOf(IllegalStateException.class, thrown.getCause());
        // The failure was not cached: a later loader run does compute.
        assertEquals(new User("Alice", 30), cache.get("alice", () -> new User("Alice", 30)));
    }

    @Test
    void getWithLoaderThrowsAStoreFailureAsIsAfterASuccessfulLoad() {
        // A store-side failure for an otherwise-successful load (a
        // non-Serializable value with the default serializer, here) is
        // thrown as-is, not as a ValueRetrievalException: Spring's cache
        // interceptor rethrows a ValueRetrievalException without
        // consulting the application's CacheErrorHandler, while any
        // other runtime exception reaches the handler — the same shape
        // Spring's Redis cache has. (This used to be wrapped, which hid
        // a failed put from the handler under sync = true.)
        Cache cache = manager().build().getCache("users");
        Object notSerializable = new Object();

        assertThrows(IllegalArgumentException.class, () -> cache.get("k", () -> notSerializable));
    }

    @Test
    void getWithLoaderThrowsTheNullStoreFailureAsIsWhenDisallowed() {
        Cache cache = manager().allowNullValues(false).build().getCache("users");

        assertThrows(IllegalArgumentException.class, () -> cache.get("k", () -> null));
        // The failed store must not have left a half-written entry behind.
        assertNull(cache.get("k"));
    }

    @Test
    void putIfAbsentKeepsTheFirstValue() {
        Cache cache = manager().build().getCache("users");

        assertNull(cache.putIfAbsent("k", new User("Alice", 30)));
        Cache.ValueWrapper existing = cache.putIfAbsent("k", new User("Bob", 40));
        assertNotNull(existing);
        assertEquals(new User("Alice", 30), existing.get());
        assertEquals(new User("Alice", 30), cache.get("k", User.class));
    }

    @Test
    void putIfAbsentUsesTheWireCompareAndSetNotGetThenPut() {
        Cache cache = manager().build().getCache("users");

        assertNull(cache.putIfAbsent("k", new User("Alice", 30)));
        assertEquals(1, node.casSetCount.get(), "a winning putIfAbsent must be one CAS write");

        Cache.ValueWrapper existing = cache.putIfAbsent("k", new User("Bob", 40));
        assertNotNull(existing);
        assertEquals(
                2,
                node.casSetCount.get(),
                "a losing putIfAbsent must still be one CAS write, not a get followed by a put");
    }

    @Test
    void putIfAbsentIsAtomicUnderConcurrentWriters() throws Exception {
        Cache cache = manager().build().getCache("users");
        int callers = 8;
        CountDownLatch ready = new CountDownLatch(callers);
        CountDownLatch go = new CountDownLatch(1);
        Thread[] threads = new Thread[callers];
        Cache.ValueWrapper[] results = new Cache.ValueWrapper[callers];

        for (int i = 0; i < callers; i++) {
            int slot = i;
            threads[i] = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                results[slot] = cache.putIfAbsent("k", new User("writer-" + slot, slot));
            });
            threads[i].start();
        }
        ready.await();
        go.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        long winners = Arrays.stream(results).filter(Objects::isNull).count();
        assertEquals(1, winners, "exactly one racing putIfAbsent must win");
        User stored = cache.get("k", User.class);
        for (Cache.ValueWrapper result : results) {
            if (result != null) {
                assertEquals(stored, result.get(), "every loser must observe the winner's value");
            }
        }
    }

    @Test
    void putIfAbsentTreatsACachedNullAsPresent() {
        Cache cache = manager().build().getCache("users");
        cache.put("nobody", null);

        Cache.ValueWrapper existing = cache.putIfAbsent("nobody", new User("Alice", 30));

        assertNotNull(existing, "a cached null is a present entry, not absent");
        assertNull(existing.get());
        assertNull(cache.get("nobody", User.class), "the cached null must not have been overwritten");
    }

    @Test
    void putIfAbsentRejectsNullWhenNullValuesAreDisallowed() {
        Cache cache = manager().allowNullValues(false).build().getCache("users");

        assertThrows(IllegalArgumentException.class, () -> cache.putIfAbsent("k", null));
    }

    @Test
    void dynamicManagerCreatesCachesOnFirstUseAndReusesThem() {
        NanocachedCacheManager manager = manager().build();

        assertTrue(manager.getCacheNames().isEmpty());
        Cache cache = manager.getCache("users");
        assertSame(cache, manager.getCache("users"));
        assertEquals(List.of("users"), List.copyOf(manager.getCacheNames()));
    }

    @Test
    void fixedNameManagerRejectsUnknownNamesWithNull() {
        NanocachedCacheManager manager =
                manager().cacheNames(List.of("users", "orders")).build();

        assertNotNull(manager.getCache("users"));
        assertNull(manager.getCache("surprise"));
        assertEquals(List.of("users", "orders"), List.copyOf(manager.getCacheNames()));
    }

    @Test
    void differentKeyTypesWithTheSameTextStayDistinct() {
        Cache cache = manager().build().getCache("users");
        cache.put("42", new User("string", 1));
        cache.put(42L, new User("long", 2));
        cache.put(new SimpleKey("a", "b"), new User("simple", 3));

        assertEquals(new User("string", 1), cache.get("42", User.class));
        assertEquals(new User("long", 2), cache.get(42L, User.class));
        assertEquals(new User("simple", 3), cache.get(new SimpleKey("a", "b"), User.class));
        assertNull(cache.get(new SimpleKey("a,b")));
    }

    @Test
    void equalMultiArgumentKeysHaveTheSameBytesWhicheverInstancesTheyHold() {
        // JDK serialization writes a repeated instance as a back-reference,
        // so SimpleKey(x, x) and SimpleKey(x, copyOf(x)) — equal keys —
        // used to serialize differently (#1952).
        DefaultCacheKeyConverter converter = new DefaultCacheKeyConverter();
        Long id = 1000L; // outside the Long box cache: Long.valueOf(1000) is another instance
        String sku = "sku-1";
        User user = new User("Alice", 30);
        List<SimpleKey[]> equalPairs = List.of(
                new SimpleKey[] {new SimpleKey(id, id), new SimpleKey(id, Long.valueOf(1000L))},
                new SimpleKey[] {new SimpleKey(sku, sku), new SimpleKey(sku, new String("sku-1"))},
                new SimpleKey[] {new SimpleKey(user, user), new SimpleKey(user, new User("Alice", 30))},
                // A varargs method's key: SimpleKey compares nested arrays with deepEquals.
                new SimpleKey[] {
                    new SimpleKey((Object) new Long[] {id, id}),
                    new SimpleKey((Object) new Long[] {id, Long.valueOf(1000L)})
                });
        for (SimpleKey[] pair : equalPairs) {
            assertEquals(pair[0], pair[1]);
            assertArrayEquals(converter.toKeyBytes(pair[0]), converter.toKeyBytes(pair[1]), pair[0].toString());
        }
    }

    @Test
    void evictWithAnEqualMultiArgumentKeyBuiltFromOtherInstancesRemovesTheEntry() {
        Cache cache = manager().build().getCache("prices");
        Long from = 1000L;
        cache.put(new SimpleKey(from, from), new User("cached", 1)); // @Cacheable prices(from, from)

        cache.evict(new SimpleKey(1000L, Long.valueOf("1000"))); // @CacheEvict with separately parsed arguments

        assertNull(cache.get(new SimpleKey(from, from)));
    }

    /** A key object holding two values: the shape a composite {@code @Cacheable(key = ...)} builds. */
    record Pair(String a, String b) implements Serializable {}

    @Test
    void aKeyObjectHoldingOneStringTwiceMatchesAnEqualOneHoldingTwoCopies() {
        // JDK serialization writes the second reference to one String as a
        // back-reference, so Pair(s, s) and Pair(s, copyOf(s)) — equal keys —
        // used to be different wire keys: the lookup missed and the evict
        // left the entry in place.
        Cache cache = manager().build().getCache("pairs");
        String s = "x";
        cache.put(new Pair(s, s), "v");
        cache.put(new SimpleKey(new Pair(s, s), 1), "w");

        Pair copies = new Pair("x", new String("x"));
        assertEquals("v", cache.get(copies, String.class));
        assertEquals("w", cache.get(new SimpleKey(copies, 1), String.class));
        assertTrue(cache.evictIfPresent(copies));
        assertTrue(cache.evictIfPresent(new SimpleKey(copies, 1)));
        assertNull(cache.get(new Pair(s, s)));
        assertNull(cache.get(new SimpleKey(new Pair(s, s), 1)));
    }

    @Test
    void equalValuesOfDifferentTypesInAKeyObjectStayDistinct() {
        DefaultCacheKeyConverter converter = new DefaultCacheKeyConverter();
        List<Object> asLong = new java.util.ArrayList<>(List.of(42L, 42L));
        List<Object> mixed = new java.util.ArrayList<>(List.of(42L, 42));

        assertFalse(java.util.Arrays.equals(converter.toKeyBytes(asLong), converter.toKeyBytes(mixed)));
    }

    @Test
    void aKeyObjectWithoutRepeatedValuesKeepsPlainJdkSerialization() throws Exception {
        Pair key = new Pair("a", "b");
        java.io.ByteArrayOutputStream plain = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(plain)) {
            out.writeObject(key);
        }

        assertArrayEquals(plain.toByteArray(), new DefaultCacheKeyConverter().toKeyBytes(key));
    }

    @Test
    void differentMultiArgumentKeysStayDistinct() {
        DefaultCacheKeyConverter converter = new DefaultCacheKeyConverter();
        List<Object> keys = List.of(
                SimpleKey.EMPTY,
                new SimpleKey((Object) null),
                new SimpleKey(null, null),
                new SimpleKey("a", "b"),
                new SimpleKey("b", "a"),
                new SimpleKey("ab"),
                new SimpleKey("a", null),
                new SimpleKey(null, "a"),
                new SimpleKey(new SimpleKey("a", "b")),
                new SimpleKey((Object) new String[] {"a", "b"}),
                new SimpleKey((Object) new byte[] {1, 2}),
                new SimpleKey(1L, 2L),
                new SimpleKey(1, 2),
                "a",
                new byte[0]);
        long distinct = keys.stream()
                .map(converter::toKeyBytes)
                .map(Arrays::toString)
                .distinct()
                .count();
        assertEquals(keys.size(), distinct);
    }

    @Test
    void customSerializerIsUsedForValues() {
        CacheValueSerializer utf8 = new CacheValueSerializer() {
            @Override
            public byte[] serialize(Object value) {
                return ((String) value).getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public Object deserialize(byte[] bytes) {
                return new String(bytes, StandardCharsets.UTF_8);
            }
        };
        Cache cache = manager().serializer(utf8).allowNullValues(false).build().getCache("users");

        cache.put("k", "plain text");

        // The bytes on the wire are the serializer's own output, no
        // JDK-serialization envelope.
        assertArrayEquals(
                "plain text".getBytes(StandardCharsets.UTF_8),
                node.entry("users", "String:k".getBytes(StandardCharsets.UTF_8)).value());
        assertEquals("plain text", cache.get("k", String.class));
    }

    @Test
    void customKeyConverterIsUsedForKeys() {
        CacheKeyConverter idOnly = key -> ("user-" + ((User) key).age()).getBytes(StandardCharsets.UTF_8);
        Cache cache = manager().keyConverter(idOnly).build().getCache("users");

        cache.put(new User("Alice", 30), new User("Alice", 30));

        assertNotNull(node.entry("users", "user-30".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void nonSerializableValuesFailFastWithTheDefaultSerializer() {
        Cache cache = manager().build().getCache("users");
        Object notSerializable = new Object();

        assertThrows(IllegalArgumentException.class, () -> cache.put("k", notSerializable));
    }

    @Test
    void nativeCacheIsTheSdkNamespaceHandle() {
        NanocachedCacheManager manager = manager().build();
        Cache cache = manager.getCache("users");

        Object nativeCache = cache.getNativeCache();
        assertInstanceOf(NanocachedClient.Namespace.class, nativeCache);
        assertArrayEquals(
                "users".getBytes(StandardCharsets.UTF_8), ((NanocachedClient.Namespace) nativeCache).namespace());
    }

    @Test
    void builderRejectsNegativeTtlAndNullClient() {
        assertThrows(IllegalArgumentException.class, () -> manager().defaultTtl(Duration.ofSeconds(-1)));
        assertThrows(NullPointerException.class, () -> NanocachedCacheManager.builder(null));
    }
}
