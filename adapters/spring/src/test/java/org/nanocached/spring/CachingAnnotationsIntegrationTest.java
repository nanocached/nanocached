package org.nanocached.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nanocached.NanocachedClient;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The adapter driven the way applications actually use it: a real Spring
 * context with {@code @EnableCaching}, a proxied service bean, and the
 * caching annotations — not direct {@code Cache} calls. Every annotation
 * lands on the {@link NanocachedCacheManager} and from there on the wire
 * (asserted against {@link MockNode}'s stores where it matters).
 */
class CachingAnnotationsIntegrationTest {

    record User(String name, int age) implements Serializable {}

    /** The "expensive" backend the annotations are supposed to shield:
     * every real invocation is counted, so the tests can distinguish a
     * cache hit (count unchanged) from a recomputation. */
    static class UserService {
        private final AtomicInteger dbCalls = new AtomicInteger();

        /** Read through a method, not the field: the bean the tests hold
         * is a CGLIB proxy, and only method calls delegate to the target
         * instance that owns the real counter. */
        public int dbCalls() {
            return dbCalls.get();
        }

        @Cacheable("users")
        public User findUser(String name) {
            dbCalls.incrementAndGet();
            return new User(name, name.length());
        }

        @Cacheable("users")
        public User findUser(String name, int age) {
            dbCalls.incrementAndGet();
            return new User(name, age);
        }

        @Cacheable(cacheNames = "users", sync = true)
        public User findUserSync(String name) {
            dbCalls.incrementAndGet();
            return new User(name, name.length());
        }

        @Cacheable("users")
        public User findMissingUser(String name) {
            dbCalls.incrementAndGet();
            return null;
        }

        @Cacheable(cacheNames = "users", unless = "#result.age > 100")
        public User findUnlessOld(String name, int age) {
            dbCalls.incrementAndGet();
            return new User(name, age);
        }

        @CachePut(cacheNames = "users", key = "#user.name")
        public User saveUser(User user) {
            dbCalls.incrementAndGet();
            return user;
        }

        @CacheEvict("users")
        public void deleteUser(String name) {}

        @CacheEvict(cacheNames = "users", allEntries = true)
        public void deleteEveryUser() {}
    }

    @Configuration
    @EnableCaching
    static class CacheConfig {
        static NanocachedClient client;

        @Bean
        CacheManager cacheManager() {
            return NanocachedCacheManager.builder(client)
                    .defaultTtl(Duration.ofMinutes(5))
                    .build();
        }

        @Bean
        UserService userService() {
            return new UserService();
        }
    }

    private MockNode node;
    private NanocachedClient client;
    private AnnotationConfigApplicationContext context;
    private UserService service;

    @BeforeEach
    void start() throws Exception {
        node = new MockNode();
        client = NanocachedClient.connect(new NanocachedClient.Options()
                .addresses(List.of(new NanocachedClient.Address("127.0.0.1", node.port()))));
        CacheConfig.client = client;
        context = new AnnotationConfigApplicationContext(CacheConfig.class);
        service = context.getBean(UserService.class);
    }

    @AfterEach
    void stop() throws Exception {
        context.close();
        client.close();
        node.close();
    }

    @Test
    void cacheableCallsTheMethodOnceAndServesRepeatsFromTheCluster() {
        User first = service.findUser("alice");
        User repeat = service.findUser("alice");
        User other = service.findUser("bob");

        assertEquals(first, repeat);
        assertEquals(2, service.dbCalls(), "alice must be computed once, bob once");
        assertEquals(new User("bob", 3), other);

        // The hit really came from the cluster: the entry is in the
        // "users" namespace under the single-argument key.
        assertEquals(
                1,
                node.store("users").keySet().stream()
                        .filter(key -> new String(key.array(), StandardCharsets.UTF_8)
                                .equals("String:alice"))
                        .count());
    }

    @Test
    void multiArgumentMethodsCacheUnderSimpleKeys() {
        service.findUser("alice", 30);
        service.findUser("alice", 30);
        service.findUser("alice", 31);

        assertEquals(2, service.dbCalls(), "same-args repeat must hit, new args must miss");
    }

    @Test
    void syncCacheableGoesThroughTheLoaderPathAndStillCaches() {
        service.findUserSync("alice");
        service.findUserSync("alice");

        assertEquals(1, service.dbCalls());
    }

    @Test
    void cacheableNullResultsAreCachedAsHits() {
        assertNull(service.findMissingUser("ghost"));
        assertNull(service.findMissingUser("ghost"));

        assertEquals(1, service.dbCalls(), "a cached null is a hit, not a recompute");
    }

    @Test
    void unlessSkipsCachingWhenTheConditionMatches() {
        service.findUnlessOld("elder", 120);
        service.findUnlessOld("elder", 120);

        assertEquals(2, service.dbCalls(), "unless=#result... must keep the value uncached");
    }

    @Test
    void cachePutOverwritesWhatCacheableThenServes() {
        service.findUser("alice"); // caches ("alice", 5)
        service.saveUser(new User("alice", 99));

        assertEquals(new User("alice", 99), service.findUser("alice"));
        assertEquals(2, service.dbCalls(), "findUser must not recompute after the @CachePut");
    }

    @Test
    void cacheEvictForcesTheNextCallToRecompute() {
        service.findUser("alice");
        service.deleteUser("alice");
        service.findUser("alice");

        assertEquals(2, service.dbCalls());
    }

    @Test
    void cacheEvictAllEntriesIsTheNamespaceClearOnTheWire() {
        service.findUser("alice");
        service.findUser("bob");

        service.deleteEveryUser();

        assertEquals(1, node.clearCount.get(), "allEntries=true must be one CLEAR, not N deletes");
        service.findUser("alice");
        assertEquals(3, service.dbCalls());
    }
}
