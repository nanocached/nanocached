package org.nanocached.spring.boot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nanocached.NanocachedClient;
import org.nanocached.spring.NanocachedCacheManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The starter's whole point end to end, against real Boot autoconfiguration
 * (issue #119): unlike {@code nanocached-spring}'s own {@code
 * BootAutoConfigurationInteractionTest}, none of the configs here declare
 * a {@code NanocachedClient} or {@code CacheManager} bean by hand — the
 * starter is the thing under test, not a manual wiring against it.
 */
class NanocachedCacheAutoConfigurationTest {

    static class UserService {
        private final AtomicInteger calls = new AtomicInteger();

        public int calls() {
            return calls.get();
        }

        @Cacheable("users")
        public String findUser(String name) {
            calls.incrementAndGet();
            return name.toUpperCase();
        }

        @Cacheable(cacheNames = "nullable", sync = false)
        public String findNullable(String name) {
            calls.incrementAndGet();
            return null;
        }
    }

    /** Yaml-only: nothing but the service and the two annotations the
     * README asks Boot apps for. No client/manager bean anywhere. */
    @Configuration
    @EnableAutoConfiguration
    @EnableCaching
    static class YamlOnlyConfig {
        @Bean
        UserService userService() {
            return new UserService();
        }
    }

    /** An app that wires its own client and manager: the starter must
     * back off both beans and leave these in place. */
    @Configuration
    @EnableAutoConfiguration
    @EnableCaching
    static class ExplicitBeansConfig {
        @Bean(destroyMethod = "close")
        NanocachedClient nanocachedClient(@Value("${nanocached.addresses}") String address) {
            String[] hostPort = address.split(":", 2);
            return NanocachedClient.connect(new NanocachedClient.Options()
                    .addresses(List.of(
                            new NanocachedClient.Address(hostPort[0], Integer.parseInt(hostPort[1])))));
        }

        @Bean
        CacheManager cacheManager(NanocachedClient client) {
            return NanocachedCacheManager.builder(client).build();
        }

        @Bean
        UserService userService() {
            return new UserService();
        }
    }

    private MockNode node;
    private ConfigurableApplicationContext context;

    @BeforeEach
    void start() throws Exception {
        node = new MockNode();
    }

    @AfterEach
    void stop() {
        if (context != null) {
            context.close();
        }
        try {
            node.close();
        } catch (Exception ignored) {
            // best-effort teardown
        }
    }

    private ConfigurableApplicationContext boot(Class<?> config, String... properties) {
        SpringApplicationBuilder builder = new SpringApplicationBuilder(config)
                .web(WebApplicationType.NONE)
                .properties("nanocached.addresses=127.0.0.1:" + node.port());
        SpringApplication application = builder.properties(properties).build();
        context = application.run();
        return context;
    }

    @Test
    void yamlOnlyBringsUpTheNanocachedCacheManagerWithNoJavaConfig() {
        boot(YamlOnlyConfig.class);

        assertInstanceOf(NanocachedCacheManager.class, context.getBean(CacheManager.class));

        UserService service = context.getBean(UserService.class);
        service.findUser("alice");
        service.findUser("alice");
        assertEquals(1, service.calls());
        assertEquals(
                1,
                node.store("users").keySet().stream()
                        .filter(key -> new String(key.array(), StandardCharsets.UTF_8)
                                .equals("String:alice"))
                        .count());
    }

    @Test
    void withoutNanocachedAddressesTheStarterIsInert() {
        SpringApplicationBuilder builder = new SpringApplicationBuilder(YamlOnlyConfig.class)
                .web(WebApplicationType.NONE);
        context = builder.build().run();

        assertEquals(
                0,
                context.getBeanNamesForType(NanocachedClient.class).length,
                "no nanocached.addresses means the starter must not attempt a connection");
    }

    // issue #388: the idiomatic YAML list form binds as indexed keys
    // (nanocached.addresses[0], ...) and never produces the literal
    // nanocached.addresses key — @ConditionalOnProperty on the literal
    // key silently skipped the whole autoconfiguration for such configs,
    // handing @Cacheable to Boot's in-memory default with no error. The
    // Binder-based condition must treat both forms as configured.
    @Test
    void indexedYamlListAddressesActivateTheStarter() {
        SpringApplicationBuilder builder = new SpringApplicationBuilder(YamlOnlyConfig.class)
                .web(WebApplicationType.NONE)
                .properties("nanocached.addresses[0]=127.0.0.1:" + node.port());
        context = builder.build().run();

        assertInstanceOf(NanocachedCacheManager.class, context.getBean(CacheManager.class));

        UserService service = context.getBean(UserService.class);
        service.findUser("alice");
        service.findUser("alice");
        assertEquals(1, service.calls(), "the second lookup must come from the cluster cache");
    }

    @Test
    void aStraySpringCacheTypeRedisDoesNotOverrideTheStarter() {
        boot(YamlOnlyConfig.class, "spring.cache.type=redis");

        assertInstanceOf(NanocachedCacheManager.class, context.getBean(CacheManager.class));
    }

    @Test
    void explicitClientAndManagerBeansAreRespected() {
        boot(ExplicitBeansConfig.class);

        NanocachedClient appClient =
                context.getBean(ExplicitBeansConfig.class).nanocachedClient("unused:0");
        assertSame(
                appClient,
                context.getBean(NanocachedClient.class),
                "the starter must not create a second client alongside the app's own");
        assertEquals(1, context.getBeanNamesForType(CacheManager.class).length);
    }

    @Test
    void defaultTtlAndPerCacheTtlFlowFromPropertiesToTheWire() {
        boot(
                YamlOnlyConfig.class,
                "nanocached.cache.default-ttl=7m",
                "nanocached.cache.ttl.nullable=30s");

        context.getBean(UserService.class).findUser("alice");

        MockNode.Entry entry = node.entry("users", "String:alice".getBytes(StandardCharsets.UTF_8));
        assertEquals(420, entry.ttlSeconds(), "the default TTL must reach the wire");
    }

    @Test
    void cacheNamesRestrictsTheManagerToAFixedSet() {
        boot(YamlOnlyConfig.class, "nanocached.cache.cache-names=users,sessions");

        CacheManager manager = context.getBean(CacheManager.class);
        assertEquals(Set.of("users", "sessions"), Set.copyOf(manager.getCacheNames()));
        assertNull(
                manager.getCache("unlisted"),
                "an unlisted name must fall through, not be created on demand");
    }

    @Test
    void allowNullValuesFalseRejectsANullResultInsteadOfCachingIt() {
        boot(YamlOnlyConfig.class, "nanocached.cache.allow-null-values=false");

        // Matches every other Spring Cache implementation's contract for
        // allowNullValues(false) (AbstractValueAdaptingCache.toStoreValue):
        // this is not "skip caching a null", it is "a null result is a
        // configuration error" — proof the property reached the cache.
        UserService service = context.getBean(UserService.class);
        assertThrows(IllegalArgumentException.class, () -> service.findNullable("alice"));
    }

    @Test
    void allowNullValuesDefaultsToTrueAndCachesANullResult() {
        boot(YamlOnlyConfig.class);

        UserService service = context.getBean(UserService.class);
        service.findNullable("alice");
        service.findNullable("alice");
        assertEquals(1, service.calls(), "a null result must be cached by default");
    }

    @Test
    void clientReliabilityOptionsBindAndReachTheClient() {
        // Regression (pass-7 audit): the starter used to forward only
        // tls/ca/compress/compressionThreshold, so a properties-only app had
        // no way to set fireAndForgetReplicas/readRepair/reconnectCooldown/
        // readHedgeAfter without hand-writing the client bean the starter
        // exists to avoid. Booting with them set proves they bind (asserted
        // on NanocachedProperties) and that the client bean is still built
        // from them without error (the forwarding path runs).
        boot(
                YamlOnlyConfig.class,
                "nanocached.fire-and-forget-replicas=true",
                "nanocached.read-repair=true",
                "nanocached.reconnect-cooldown=2s",
                "nanocached.read-hedge-after=50ms");

        NanocachedProperties props = context.getBean(NanocachedProperties.class);
        assertEquals(Boolean.TRUE, props.getFireAndForgetReplicas());
        assertEquals(Boolean.TRUE, props.getReadRepair());
        assertEquals(java.time.Duration.ofSeconds(2), props.getReconnectCooldown());
        assertEquals(java.time.Duration.ofMillis(50), props.getReadHedgeAfter());
        // The client bean built from these properties exists (no exception
        // thrown while forwarding them).
        assertInstanceOf(NanocachedClient.class, context.getBean(NanocachedClient.class));
    }
}
