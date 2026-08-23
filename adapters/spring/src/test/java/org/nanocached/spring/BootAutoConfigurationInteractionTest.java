package org.nanocached.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nanocached.NanocachedClient;
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
 * The setup story against real Spring Boot autoconfiguration, with the
 * Redis starter on the classpath as the competing backend — the exact
 * situation a Boot app migrating from Redis is in:
 *
 * <ul>
 *   <li>without the adapter's beans, Boot picks its own manager (Redis,
 *       since the starter is present) — adding this module's dependency
 *       alone changes nothing;
 *   <li>with the two beans from the README, the explicit {@code
 *       CacheManager} wins with <em>zero</em> {@code spring.cache.*}
 *       configuration — Boot's {@code CacheAutoConfiguration} is
 *       {@code @ConditionalOnMissingBean(CacheManager)};
 *   <li>even a stray {@code spring.cache.type=redis} does not override
 *       the explicit bean (that key only steers Boot's own
 *       autoconfiguration, which has already backed off);
 *   <li>the README's {@code nanocached.*} property binding works: the
 *       address and TTL flow from properties to the wire.
 * </ul>
 */
class BootAutoConfigurationInteractionTest {

    record User(String name, int age) implements Serializable {}

    static class UserService {
        private final AtomicInteger dbCalls = new AtomicInteger();

        public int dbCalls() {
            return dbCalls.get();
        }

        @Cacheable("users")
        public User findUser(String name) {
            dbCalls.incrementAndGet();
            return new User(name, name.length());
        }
    }

    /** The README's Boot setup, verbatim: two beans bound from
     * {@code nanocached.*} properties, plus the service under test. */
    @Configuration
    @EnableAutoConfiguration
    @EnableCaching
    static class NanocachedBootConfig {

        @Bean(destroyMethod = "close")
        NanocachedClient nanocachedClient(
                @Value("${nanocached.addresses}") List<String> addresses) {
            return NanocachedClient.connect(new NanocachedClient.Options()
                    .addresses(addresses.stream()
                            .map(address -> address.split(":", 2))
                            .map(address -> new NanocachedClient.Address(
                                    address[0], Integer.parseInt(address[1])))
                            .toList()));
        }

        @Bean
        CacheManager cacheManager(
                NanocachedClient client,
                @Value("${nanocached.default-ttl:0s}") Duration defaultTtl) {
            return NanocachedCacheManager.builder(client).defaultTtl(defaultTtl).build();
        }

        @Bean
        UserService userService() {
            return new UserService();
        }
    }

    /** No adapter beans: what a Boot app is like before this module's
     * setup — only the service and whatever Boot autoconfigures. */
    @Configuration
    @EnableAutoConfiguration
    @EnableCaching
    static class NoAdapterConfig {
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
    void stop() throws Exception {
        if (context != null) {
            context.close();
        }
        node.close();
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
    void withoutTheAdapterBeansBootPicksItsOwnManager() {
        boot(NoAdapterConfig.class);

        CacheManager manager = context.getBean(CacheManager.class);
        assertFalse(
                manager instanceof NanocachedCacheManager,
                "adding the dependency alone must not switch caching to nanocached");
        // The Redis starter is on the classpath, so Boot's pick is its
        // Redis manager — the backend this adapter is displacing.
        assertTrue(
                manager.getClass().getName().contains("Redis"),
                "expected Boot's Redis manager, got " + manager.getClass().getName());
    }

    @Test
    void theExplicitBeansWinWithZeroSpringCacheConfiguration() {
        boot(NanocachedBootConfig.class);

        assertInstanceOf(NanocachedCacheManager.class, context.getBean(CacheManager.class));

        // And it is live end-to-end: @Cacheable traffic lands on the
        // nanocached node, not on Redis (which isn't even running).
        UserService service = context.getBean(UserService.class);
        service.findUser("alice");
        service.findUser("alice");
        assertEquals(1, service.dbCalls());
        assertEquals(
                1,
                node.store("users").keySet().stream()
                        .filter(key -> new String(key.array(), StandardCharsets.UTF_8)
                                .equals("String:alice"))
                        .count());
    }

    @Test
    void aStraySpringCacheTypeRedisDoesNotOverrideTheExplicitBean() {
        boot(NanocachedBootConfig.class, "spring.cache.type=redis");

        assertInstanceOf(NanocachedCacheManager.class, context.getBean(CacheManager.class));
    }

    @Test
    void nanocachedPropertiesFlowFromYamlBindingToTheWire() {
        // default-ttl comes in Boot's duration syntax; the address list
        // already flows through every test via boot()'s base property.
        boot(NanocachedBootConfig.class, "nanocached.default-ttl=7m");

        context.getBean(UserService.class).findUser("alice");

        MockNode.Entry entry =
                node.entry("users", "String:alice".getBytes(StandardCharsets.UTF_8));
        assertEquals(420, entry.ttlSeconds(), "the bound TTL must reach the wire");
    }
}
