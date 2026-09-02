package org.nanocached.spring.boot;

import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionMessage;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Matches when {@code nanocached.addresses} is configured, in either form
 * Boot's relaxed binding accepts for a {@code List<String>} property
 * (issue #388): the comma-separated string ({@code
 * nanocached.addresses=a:1,b:2}) produces the literal key, but the
 * idiomatic YAML list form produces only indexed keys ({@code
 * nanocached.addresses[0]}, …) — so {@code
 * ConditionalOnProperty("nanocached.addresses")}, which checks the
 * literal key alone, silently skipped the whole autoconfiguration for
 * YAML-list users and let Boot's default in-memory manager take over.
 * Binding through {@link Binder}, exactly as {@link NanocachedProperties}
 * itself later will, keeps this condition and the eventual binding in
 * agreement by construction.
 */
class OnNanocachedAddressesCondition extends SpringBootCondition {

    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
        List<String> addresses = Binder.get(context.getEnvironment())
                .bind("nanocached.addresses", Bindable.listOf(String.class))
                .orElse(null);
        ConditionMessage.Builder message = ConditionMessage.forCondition("Nanocached addresses");
        if (addresses == null || addresses.isEmpty()) {
            return ConditionOutcome.noMatch(
                    message.didNotFind("property").items("nanocached.addresses"));
        }
        return ConditionOutcome.match(message.found("property").items("nanocached.addresses"));
    }
}
