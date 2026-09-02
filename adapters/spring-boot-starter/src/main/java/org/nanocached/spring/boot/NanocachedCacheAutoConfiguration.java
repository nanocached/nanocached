package org.nanocached.spring.boot;

import java.util.List;
import org.nanocached.NanocachedClient;
import org.nanocached.spring.NanocachedCacheManager;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.cache.CacheAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;

/**
 * Registers a {@link NanocachedClient} and a {@link NanocachedCacheManager}
 * from {@code nanocached.*} properties (issue #119), so a Boot app needs
 * only the dependency plus {@code @EnableCaching} and {@code
 * nanocached.addresses} — no manual {@code @Bean} methods, unlike
 * {@code nanocached-spring} on its own (see that module's README).
 *
 * <p>Inert until {@code nanocached.addresses} is set — in either the
 * comma-string or the YAML-list form, see {@link
 * OnNanocachedAddressesCondition} (issue #388): adding this starter's
 * dependency alone changes nothing, matching Boot's own
 * opt-in-by-configuration convention. Each bean also backs off
 * individually ({@link ConditionalOnMissingBean}) so an app can supply
 * its own client or manager and keep the other.
 *
 * <p>{@link AutoConfigureBefore} Boot's own {@link CacheAutoConfiguration}
 * so this configuration's {@code CacheManager} exists by the time Boot's
 * runs — otherwise {@code @ConditionalOnMissingBean(CacheManager.class)}
 * would race the other way and Boot's default manager would win instead
 * (the same ordering {@code nanocached-spring}'s manual two-bean setup gets
 * for free from being declared directly in the app's own configuration).
 *
 * <p>{@code nanocached.reconnect-cooldown: 0s} (or any zero duration)
 * disables the reconnect cooldown outright ({@code
 * Options.disableReconnectCooldown()}) rather than mapping to the SDK's
 * own default cooldown, which is what a bare {@code
 * Options.reconnectCooldown(Duration.ZERO)} call means — see {@link
 * NanocachedProperties#getReconnectCooldown()}.
 */
@AutoConfiguration
@AutoConfigureBefore(CacheAutoConfiguration.class)
@EnableConfigurationProperties(NanocachedProperties.class)
@ConditionalOnClass({NanocachedClient.class, CacheManager.class})
@Conditional(OnNanocachedAddressesCondition.class)
public class NanocachedCacheAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(NanocachedClient.class)
    public NanocachedClient nanocachedClient(NanocachedProperties properties) {
        NanocachedClient.Options options =
                new NanocachedClient.Options().addresses(parseAddresses(properties.getAddresses()));
        if (properties.getSecret() != null) {
            options.authSecret(properties.getSecret());
        }
        options.tls(properties.isTls());
        if (properties.getCa() != null) {
            options.ca(properties.getCa());
        }
        if (properties.getCompress() != null) {
            options.compress(properties.getCompress());
        }
        if (properties.getCompressionThreshold() != null) {
            options.compressionThreshold(properties.getCompressionThreshold());
        }
        if (properties.getFireAndForgetReplicas() != null) {
            options.fireAndForgetReplicas(properties.getFireAndForgetReplicas());
        }
        if (properties.getReadRepair() != null) {
            options.readRepair(properties.getReadRepair());
        }
        if (properties.getReconnectCooldown() != null) {
            // A zero duration means "disable the cooldown", not "use the
            // SDK default" — Options.reconnectCooldown(Duration.ZERO)
            // itself means the latter (issue #417), so a zero property
            // value (e.g. reconnect-cooldown: 0s) has to route through
            // disableReconnectCooldown() to actually take effect.
            if (properties.getReconnectCooldown().isZero()) {
                options.disableReconnectCooldown();
            } else {
                options.reconnectCooldown(properties.getReconnectCooldown());
            }
        }
        if (properties.getReadHedgeAfter() != null) {
            options.readHedgeAfter(properties.getReadHedgeAfter());
        }
        return NanocachedClient.connect(options);
    }

    @Bean
    @ConditionalOnMissingBean(CacheManager.class)
    public CacheManager nanocachedCacheManager(
            NanocachedClient client, NanocachedProperties properties) {
        NanocachedProperties.Cache cache = properties.getCache();
        NanocachedCacheManager.Builder builder =
                NanocachedCacheManager.builder(client)
                        .defaultTtl(cache.getDefaultTtl())
                        .allowNullValues(cache.isAllowNullValues());
        cache.getTtl().forEach(builder::ttl);
        if (cache.getCacheNames() != null) {
            builder.cacheNames(cache.getCacheNames());
        }
        return builder.build();
    }

    /** Same {@code "host:port"} split used by the manual setup in
     * nanocached-spring's own README/tests — IPv6 literals aren't
     * supported by that convention any more than they are there. */
    private static List<NanocachedClient.Address> parseAddresses(List<String> addresses) {
        return addresses.stream()
                .map(address -> address.split(":", 2))
                .map(parts -> new NanocachedClient.Address(parts[0], Integer.parseInt(parts[1])))
                .toList();
    }
}
