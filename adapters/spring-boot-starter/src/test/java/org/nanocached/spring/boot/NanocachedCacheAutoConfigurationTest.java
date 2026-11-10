package org.nanocached.spring.boot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.nanocached.NanocachedClient;
import org.nanocached.spring.NanocachedCacheManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
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

    /** A service whose cache name contains dots, which {@code
     * nanocached.cache.ttl.<name>} cannot spell without brackets. */
    static class DottedNameService {
        @Cacheable("users.by.id")
        public String findUser(String name) {
            return name.toUpperCase();
        }
    }

    @Configuration
    @EnableAutoConfiguration
    @EnableCaching
    static class DottedNameConfig {
        @Bean
        DottedNameService dottedNameService() {
            return new DottedNameService();
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

    @Test
    void malformedAddressesFailStartupWithAMessageNamingTheOffendingEntry() {
        for (String bad : new String[] {"localhost", "localhost:", ":8357", "localhost:abc", "localhost:0",
            "localhost:70000"}) {
            Exception error = assertThrows(Exception.class, () -> boot(YamlOnlyConfig.class, "nanocached.addresses=" + bad), bad);
            Throwable cause = error;
            while (cause != null && !(cause instanceof IllegalArgumentException)) {
                cause = cause.getCause();
            }
            assertTrue(cause != null, "no IllegalArgumentException in the cause chain for " + bad);
            assertTrue(cause.getMessage().contains("nanocached.addresses"), cause.getMessage());
        }
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

    private long dottedNameTtlSeconds() {
        context.getBean(DottedNameService.class).findUser("alice");
        return node.entry("users.by.id", "String:alice".getBytes(StandardCharsets.UTF_8))
                .ttlSeconds();
    }

    private static String messagesOf(Throwable error) {
        StringBuilder messages = new StringBuilder();
        for (Throwable t = error; t != null; t = t.getCause()) {
            messages.append(t.getMessage()).append('\n');
        }
        return messages.toString();
    }

    @Test
    void bracketedPerCacheTtlForADottedCacheNameReachesTheWire() {
        boot(DottedNameConfig.class, "nanocached.cache.default-ttl=7m", "nanocached.cache.ttl.[users.by.id]=40s");

        assertEquals(40, dottedNameTtlSeconds(), "the bracketed key must bind to the cache named users.by.id");
    }

    @Test
    void bracketedPerCacheTtlInYamlReachesTheWire(@TempDir Path dir) throws Exception {
        Path yaml = dir.resolve("application.yaml");
        Files.writeString(yaml, """
                nanocached:
                  cache:
                    default-ttl: 7m
                    ttl:
                      "[users.by.id]": 40s
                """);

        boot(DottedNameConfig.class, "spring.config.additional-location=" + yaml.toUri());

        assertEquals(40, dottedNameTtlSeconds());
    }

    @Test
    void unbracketedPerCacheTtlForADottedCacheNameFailsStartupNamingTheBracketForm() {
        // Spring Boot's binder reads the unbracketed dotted key as a nested
        // path and binds nothing, so the cache would silently have used the
        // default TTL (no expiry unless configured).
        Exception error = assertThrows(
                Exception.class,
                () -> boot(
                        DottedNameConfig.class,
                        "nanocached.cache.default-ttl=7m",
                        "nanocached.cache.ttl.users.by.id=40s"));

        String messages = messagesOf(error);
        assertTrue(messages.contains("nanocached.cache.ttl.users.by.id"), messages);
        assertTrue(messages.contains("nanocached.cache.ttl.[users.by.id]"), messages);
    }

    @Test
    void unbracketedPerCacheTtlInYamlFailsStartupToo(@TempDir Path dir) throws Exception {
        Path yaml = dir.resolve("application.yaml");
        Files.writeString(yaml, """
                nanocached:
                  cache:
                    ttl:
                      users.by.id: 40s
                """);

        Exception error = assertThrows(
                Exception.class,
                () -> boot(DottedNameConfig.class, "spring.config.additional-location=" + yaml.toUri()));

        assertTrue(messagesOf(error).contains("nanocached.cache.ttl.[users.by.id]"), messagesOf(error));
    }

    @Test
    void perCacheTtlKeysWithoutADotStillBindWithoutBrackets() {
        boot(YamlOnlyConfig.class, "nanocached.cache.ttl.users=40s", "nanocached.cache.ttl.some-cache=10s");

        context.getBean(UserService.class).findUser("alice");

        MockNode.Entry entry = node.entry("users", "String:alice".getBytes(StandardCharsets.UTF_8));
        assertEquals(40, entry.ttlSeconds());
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

    @Test
    void bareDurationNumbersDoNotDefaultToMilliseconds() {
        // Regression (issue #417): none of these Duration fields declared
        // @DurationUnit, so a bare number (no "s"/"ms" suffix) silently
        // bound as milliseconds — nanocached.cache.default-ttl: 10 meant
        // 10ms, not the 10s an operator would expect. Bind straight
        // through a Binder (same mechanism NanocachedProperties itself
        // uses via @ConfigurationProperties) rather than booting a full
        // context, since this is purely about the unit each field binds
        // bare numbers with.
        MapConfigurationPropertySource source = new MapConfigurationPropertySource(Map.of(
                "nanocached.reconnect-cooldown", "2",
                "nanocached.read-hedge-after", "50",
                "nanocached.cache.default-ttl", "10"));
        NanocachedProperties properties = new Binder(source)
                .bind("nanocached", Bindable.of(NanocachedProperties.class))
                .get();

        assertEquals(Duration.ofSeconds(2), properties.getReconnectCooldown());
        assertEquals(Duration.ofMillis(50), properties.getReadHedgeAfter());
        assertEquals(Duration.ofSeconds(10), properties.getCache().getDefaultTtl());
    }

    @Test
    void anEnvironmentVariableCannotNameACacheWithAnUnderscoreAndIsRejectedRatherThanIgnored() {
        // Boot maps every underscore of an environment variable name to a
        // name element, so NANOCACHED_CACHE_TTL_USER_SESSIONS is the
        // unbindable nanocached.cache.ttl.user.sessions, not the cache
        // user_sessions; NANOCACHED_CACHE_TTL_PLAIN binds.
        StandardEnvironment environment = new StandardEnvironment();
        ConfigurationPropertySources.attach(environment);
        environment
                .getPropertySources()
                .replace(
                        StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                        new SystemEnvironmentPropertySource(
                                StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                                Map.<String, Object>of(
                                        "NANOCACHED_CACHE_TTL_USER_SESSIONS", "30s",
                                        "NANOCACHED_CACHE_TTL_PLAIN", "20s")));

        assertEquals(
                Map.of("plain", Duration.ofSeconds(20)),
                Binder.get(environment)
                        .bind("nanocached", Bindable.of(NanocachedProperties.class))
                        .get()
                        .getCache()
                        .getTtl());
        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> NanocachedCacheAutoConfiguration.rejectUnbindablePerCacheTtlKeys(environment));
        assertTrue(error.getMessage().contains("nanocached.cache.ttl.user.sessions"), error.getMessage());
        assertFalse(error.getMessage().contains("nanocached.cache.ttl.plain"), error.getMessage());
    }

    @Test
    void perCacheTtlKeysBindOnlyWhenTheCacheNameIsASingleElement() {
        // Pins what Spring Boot's binder does with nanocached.cache.ttl.*:
        // Duration is not a java.lang type, so MapBinder takes one element
        // after "ttl" as the key and looks for the value at exactly that
        // name; a dotted name written without brackets has no value there
        // and binds nothing.
        MapConfigurationPropertySource source = new MapConfigurationPropertySource(Map.of(
                "nanocached.cache.ttl.user_sessions", "1s",
                "nanocached.cache.ttl.UserCache", "2s",
                "nanocached.cache.ttl.my-cache", "3s",
                "nanocached.cache.ttl.[a_b.c]", "4s",
                "nanocached.cache.ttl.[users.by.id]", "5s",
                "nanocached.cache.ttl.users.by.id", "6s"));
        Map<String, Duration> ttl = new Binder(source)
                .bind("nanocached", Bindable.of(NanocachedProperties.class))
                .get()
                .getCache()
                .getTtl();

        assertEquals(
                Map.of(
                        "user_sessions", Duration.ofSeconds(1),
                        "UserCache", Duration.ofSeconds(2),
                        "my-cache", Duration.ofSeconds(3),
                        "a_b.c", Duration.ofSeconds(4),
                        "users.by.id", Duration.ofSeconds(5)),
                ttl);
    }

    @Test
    void perCacheTtlMapBareNumbersStayMillisecondsUnlikeDefaultTtl() {
        // Spring Boot's MapBinder does not forward a field's
        // @DurationUnit to the per-entry Bindable it builds for each map
        // value (confirmed against spring-boot 3.5.3's MapBinder.EntryBinder,
        // which constructs value Bindables via plain Bindable.of(valueType)
        // with no annotations) — so cache.ttl.<name> cannot get the same
        // bare-number-is-seconds treatment as cache.default-ttl. Documented
        // in NanocachedProperties.Cache#ttl and the README; this test pins
        // that behavior so a future Spring Boot upgrade that starts
        // propagating the annotation doesn't silently change the meaning
        // of an existing bare-number config value out from under an
        // operator.
        MapConfigurationPropertySource source =
                new MapConfigurationPropertySource(Map.of("nanocached.cache.ttl.sessions", "30"));
        NanocachedProperties properties = new Binder(source)
                .bind("nanocached", Bindable.of(NanocachedProperties.class))
                .get();

        assertEquals(Duration.ofMillis(30), properties.getCache().getTtl().get("sessions"));
    }

    @Test
    void zeroReconnectCooldownDisablesItInsteadOfFallingBackToTheSdkDefault() throws Exception {
        // Regression (issue #417): Options.reconnectCooldown(Duration.ZERO)
        // means "use the SDK default", so forwarding a zero property value
        // straight through used to silently keep the default cooldown
        // instead of actually disabling it — operators had no way to turn
        // this off via configuration.
        boot(YamlOnlyConfig.class, "nanocached.reconnect-cooldown=0s");

        assertTrue(
                reconnectCooldownDisabled(context.getBean(NanocachedClient.class)),
                "a zero reconnect-cooldown must call disableReconnectCooldown()");
    }

    @Test
    void nonZeroReconnectCooldownIsForwardedWithoutDisablingIt() throws Exception {
        boot(YamlOnlyConfig.class, "nanocached.reconnect-cooldown=2s");

        assertFalse(
                reconnectCooldownDisabled(context.getBean(NanocachedClient.class)),
                "a non-zero reconnect-cooldown must not disable the cooldown");
    }

    @Test
    void orderedBeforeBootsCacheAutoConfigurationByANameThatResolves() throws Exception {
        // The ordering is declared by string (so the starter still loads
        // without spring-boot-starter-cache on the classpath), which means
        // the compiler cannot catch a name that stopped resolving — the
        // exact way the Boot 3 -> 4 package move would have gone unnoticed.
        // Boot only sorts alphabetically as a fallback, so a stale name
        // would not fail the context tests above; check it here.
        String[] before =
                NanocachedCacheAutoConfiguration.class.getAnnotation(AutoConfiguration.class).beforeName();

        assertEquals(1, before.length);
        assertEquals(
                "org.springframework.boot.cache.autoconfigure.CacheAutoConfiguration",
                Class.forName(before[0]).getName());
    }

    private static boolean reconnectCooldownDisabled(NanocachedClient client) throws Exception {
        Field field = NanocachedClient.class.getDeclaredField("reconnectCooldownDisabled");
        field.setAccessible(true);
        return field.getBoolean(client);
    }
}
