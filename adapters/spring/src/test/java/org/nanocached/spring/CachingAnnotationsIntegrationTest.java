package org.nanocached.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.tools.ToolProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.annotation.CachingConfigurer;
import org.springframework.cache.interceptor.CacheErrorHandler;
import org.springframework.context.annotation.Lazy;
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

        /** Nested sync loads: the loader of an "outer" key reads an
         * "inner" key of the same cache through the proxy. A barrier
         * shared by two threads makes both outer loaders hold their
         * in-flight state before either nests, which is the deadlock
         * choreography. */
        @Autowired @Lazy UserService self;
        private volatile CyclicBarrier bothOuterLoadersStarted;

        /** Through a method, not the field: the bean the tests hold is a
         * CGLIB proxy whose fields are not the target's. */
        public void bothOuterLoadersStarted(CyclicBarrier barrier) {
            bothOuterLoadersStarted = barrier;
        }

        @Cacheable(cacheNames = "users", sync = true)
        public User loadSync(String key, String nested) {
            dbCalls.incrementAndGet();
            if (nested != null) {
                try {
                    bothOuterLoadersStarted.await(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
                // The same arguments again re-enter the same Spring key;
                // anything else nests a different key.
                self.loadSync(nested, nested.equals(key) ? nested : null);
            }
            return new User(key, key.length());
        }

        /** A value over the server's 1 MiB limit: the load succeeds, the
         * put that follows does not. */
        @Cacheable(cacheNames = "users", sync = true)
        public byte[] loadOversizedSync(String name) {
            dbCalls.incrementAndGet();
            return new byte[1_200_000];
        }

        /** The cached type lives in a class loader this adapter cannot
         * see; the service creates it reflectively. */
        private volatile Class<?> childClass;

        public void childClass(Class<?> type) {
            childClass = type;
        }

        @Cacheable("users")
        public Object findChildObject(String name) {
            dbCalls.incrementAndGet();
            try {
                return childClass.getConstructor(String.class).newInstance(name);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
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

    /** Records what the application's CacheErrorHandler is told and,
     * when {@code swallow} is set, swallows it — the "degrade to the
     * method" policy; otherwise rethrows like Spring's default handler. */
    static class RecordingErrorHandler implements CacheErrorHandler {
        final List<RuntimeException> getErrors = new java.util.concurrent.CopyOnWriteArrayList<>();
        final List<RuntimeException> putErrors = new java.util.concurrent.CopyOnWriteArrayList<>();
        volatile boolean swallow;

        @Override
        public void handleCacheGetError(RuntimeException exception, Cache cache, Object key) {
            getErrors.add(exception);
            if (!swallow) throw exception;
        }

        @Override
        public void handleCachePutError(RuntimeException exception, Cache cache, Object key, Object value) {
            putErrors.add(exception);
            if (!swallow) throw exception;
        }

        @Override
        public void handleCacheEvictError(RuntimeException exception, Cache cache, Object key) {}

        @Override
        public void handleCacheClearError(RuntimeException exception, Cache cache) {}
    }

    @Configuration
    @EnableCaching
    static class CacheConfig implements CachingConfigurer {
        static NanocachedClient client;
        static final RecordingErrorHandler errorHandler = new RecordingErrorHandler();

        @Bean
        CacheManager nanocachedCacheManager() {
            return NanocachedCacheManager.builder(client)
                    .defaultTtl(Duration.ofMinutes(5))
                    .build();
        }

        @Override
        public CacheErrorHandler errorHandler() {
            return errorHandler;
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
        CacheConfig.errorHandler.getErrors.clear();
        CacheConfig.errorHandler.putErrors.clear();
        CacheConfig.errorHandler.swallow = false;
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

    // ── sync loaders never couple unrelated keys ────────────────────

    @Test
    void nestedSyncLoadersOnDifferentKeysDoNotDeadlock() throws Exception {
        // Thread 1 loads A and, inside the loader, B; thread 2 loads C
        // and, inside, D. Four distinct keys, no logical cycle. The old
        // 64-stripe lock coupled A with D and C with B whenever their
        // hash codes shared a stripe, so the two loaders waited on each
        // other forever; the keys below are chosen to collide that way
        // (the outer Spring key is SimpleKey(outer, inner), the nested
        // one SimpleKey(inner, null)).
        String b = "b-0", d = "d-0";
        int stripeOfB = stripeOf(new org.springframework.cache.interceptor.SimpleKey(b, null));
        int stripeOfD = stripeOf(new org.springframework.cache.interceptor.SimpleKey(d, null));
        String a = outerKeyOnStripe("a", b, stripeOfD);
        String c = outerKeyOnStripe("c", d, stripeOfB);
        service.bothOuterLoadersStarted(new CyclicBarrier(2));

        CompletableFuture<User> first = CompletableFuture.supplyAsync(() -> service.loadSync(a, b));
        CompletableFuture<User> second = CompletableFuture.supplyAsync(() -> service.loadSync(c, d));
        assertEquals(a, first.get(8, TimeUnit.SECONDS).name());
        assertEquals(c, second.get(8, TimeUnit.SECONDS).name());
        assertEquals(4, service.dbCalls());
    }

    private static int stripeOf(Object springKey) {
        return Math.floorMod(springKey.hashCode(), 64);
    }

    /** An outer key whose Spring cache key {@code SimpleKey(outer,
     * inner)} hashes onto {@code stripe} of a 64-stripe lock. */
    private static String outerKeyOnStripe(String prefix, String inner, int stripe) {
        for (int i = 0; ; i++) {
            String candidate = prefix + "-" + i;
            if (stripeOf(new org.springframework.cache.interceptor.SimpleKey(candidate, inner)) == stripe) {
                return candidate;
            }
        }
    }

    @Test
    void syncLoaderReenteringItsOwnKeyIsReportedNotDeadlocked() {
        // The loader of (self, self) calls loadSync(self, self) again on
        // its own thread: the inner call is told so instead of waiting
        // for the outer load that is waiting for it. The report surfaces
        // as the outer loader's own failure.
        // as the outer loader's own failure, which Spring's cache aspect
        // unwraps back to the original exception for the caller.
        service.bothOuterLoadersStarted(new CyclicBarrier(1));
        String key = "self";
        RuntimeException error = assertThrows(RuntimeException.class, () -> service.loadSync(key, key));
        Throwable cause = error;
        while (cause != null && !(cause.getMessage() != null && cause.getMessage().contains("re-entered"))) {
            cause = cause.getCause();
        }
        assertNotNull(cause, "want the re-entrance report in the cause chain, got " + error);
    }

    // ── a failing put after a successful sync load reaches the handler ──

    @Test
    void syncLoadWhosePutFailsReturnsTheValueAndReachesTheErrorHandler() {
        // The loader succeeded; only the store of its 1.2 MB result failed
        // (over the server's limit). The non-sync path reports that
        // through the handler and returns the value; the sync path used
        // to wrap it in ValueRetrievalException, which the interceptor
        // rethrows without consulting the handler.
        CacheConfig.errorHandler.swallow = true;
        byte[] value = service.loadOversizedSync("big");
        assertEquals(1_200_000, value.length);
        assertEquals(1, CacheConfig.errorHandler.getErrors.size() + CacheConfig.errorHandler.putErrors.size(),
                "the error handler must see the failed put exactly once");
        RuntimeException seen = CacheConfig.errorHandler.getErrors.isEmpty()
                ? CacheConfig.errorHandler.putErrors.get(0)
                : CacheConfig.errorHandler.getErrors.get(0);
        assertTrue(seen instanceof IllegalArgumentException, "unexpected error: " + seen);
    }

    // ── application classes in a child class loader ─────────────────

    @Test
    void cachedValuesWhoseClassLivesInAChildClassLoaderAreReadBack() throws Exception {
        // The adapter sits in a parent loader; the application's classes
        // are in a child (a devtools restart loader, a server's shared
        // library directory). A plain ObjectInputStream resolved classes
        // with the adapter's loader and failed every cache hit with
        // ClassNotFoundException.
        Path dir = Files.createTempDirectory("nanocached-child-loader");
        Path source = dir.resolve("app").resolve("ChildUser.java");
        Files.createDirectories(source.getParent());
        Files.writeString(source, "package app; public record ChildUser(String name) implements java.io.Serializable {}");
        int compiled = ToolProvider.getSystemJavaCompiler().run(null, null, null, "-d", dir.toString(), source.toString());
        assertEquals(0, compiled, "javac");
        try (URLClassLoader child = new URLClassLoader(new java.net.URL[] {dir.toUri().toURL()},
                CachingAnnotationsIntegrationTest.class.getClassLoader())) {
            service.childClass(child.loadClass("app.ChildUser"));
            Object first = service.findChildObject("alice");
            ClassLoader original = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(child);
            try {
                Object again = service.findChildObject("alice");
                assertEquals(first, again);
                assertEquals(1, service.dbCalls(), "the second call must be a cache hit");
            } finally {
                Thread.currentThread().setContextClassLoader(original);
            }
        }
    }

    // ── a String key with a lone surrogate is rejected ──────────────

    @Test
    void aKeyWithALoneSurrogateIsRejectedInsteadOfCollidingWithAnotherKey() {
        // The key conversion rejects the lone surrogate; the error
        // handler configured here swallows that, so the method runs
        // uncached and nothing is written under "x?"'s bytes.
        CacheConfig.errorHandler.swallow = true;
        service.findUser("x?");
        service.findUser("x\uD800");
        assertEquals(2, service.dbCalls());
        assertTrue(CacheConfig.errorHandler.getErrors.stream().anyMatch(
                error -> error instanceof IllegalArgumentException && error.getMessage().contains("lone surrogate")),
                "the lookup under the malformed key must have been rejected: " + CacheConfig.errorHandler.getErrors);
        service.findUser("x?");
        assertEquals(2, service.dbCalls(), "\"x?\" must still be a hit, untouched by the rejected key");
    }
}
