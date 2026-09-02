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
        assertEquals(
                new User("Alice", 30),
                cache.get("alice", () -> new User("Alice", 30)));
    }

    @Test
    void getWithLoaderWrapsStoreFailuresFromTheSuccessfulLoad() {
        // Regression (issue #417): put(key, loaded) used to sit outside
        // the try/catch that wraps the loader call, so a store-side
        // failure for an otherwise-successful load (a non-Serializable
        // value with the default serializer, here) escaped unwrapped
        // instead of being reported as Spring's own ConcurrentMapCache
        // reports it: a ValueRetrievalException.
        Cache cache = manager().build().getCache("users");
        Object notSerializable = new Object();

        Cache.ValueRetrievalException thrown = assertThrows(
                Cache.ValueRetrievalException.class,
                () -> cache.get("k", () -> notSerializable));
        assertInstanceOf(IllegalArgumentException.class, thrown.getCause());
    }

    @Test
    void getWithLoaderWrapsStoreFailuresFromANullValueWhenDisallowed() {
        Cache cache = manager().allowNullValues(false).build().getCache("users");

        Cache.ValueRetrievalException thrown = assertThrows(
                Cache.ValueRetrievalException.class,
                () -> cache.get("k", () -> null));
        assertInstanceOf(IllegalArgumentException.class, thrown.getCause());
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
        NanocachedCacheManager manager = manager()
                .cacheNames(List.of("users", "orders"))
                .build();

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
        Cache cache = manager()
                .serializer(utf8)
                .allowNullValues(false)
                .build()
                .getCache("users");

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
        CacheKeyConverter idOnly = key -> ("user-" + ((User) key).age())
                .getBytes(StandardCharsets.UTF_8);
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
                "users".getBytes(StandardCharsets.UTF_8),
                ((NanocachedClient.Namespace) nativeCache).namespace());
    }

    @Test
    void builderRejectsNegativeTtlAndNullClient() {
        assertThrows(IllegalArgumentException.class,
                () -> manager().defaultTtl(Duration.ofSeconds(-1)));
        assertThrows(NullPointerException.class, () -> NanocachedCacheManager.builder(null));
    }
}
