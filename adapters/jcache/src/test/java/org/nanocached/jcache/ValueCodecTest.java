package org.nanocached.jcache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ObjectInputFilter;
import java.io.Serializable;
import org.junit.jupiter.api.Test;

/**
 * Proves {@link ValueCodec#deserialize} applies the JVM-wide {@link ObjectInputFilter}
 * (issue #232's trust-boundary fix) rather than silently ignoring it.
 *
 * <p>Unlike {@code nanocached-spring}'s {@code JdkCacheValueSerializer}, this package-private
 * codec has no constructor to inject a scoped filter, and {@link
 * ObjectInputFilter.Config#setSerialFilter} can only succeed once per JVM — calling it here
 * with a rejecting filter would permanently poison every other test's deserialization in the
 * same test JVM, order-dependent and irreversible. So this test instead proves the safe half
 * of the same code path directly: with no {@code -Djdk.serialFilter} configured for this test
 * JVM (the default, actually asserted below), {@code
 * ObjectInputFilter.Config.getSerialFilter()} is {@code null} and {@link
 * ValueCodec#deserialize}'s {@code if (filter != null)} guard is a no-op, so a normal
 * round-trip still works exactly as before this fix.
 */
class ValueCodecTest {

    record User(String name, int age) implements Serializable {}

    @Test
    void withNoGlobalSerialFilterConfiguredRoundTripStillWorks() {
        assertNull(
                ObjectInputFilter.Config.getSerialFilter(),
                "this test assumes no -Djdk.serialFilter/global filter is set in the test JVM;"
                        + " if some other test configured one, this assumption (and the"
                        + " ValueCodec.deserialize null-check it exercises) needs revisiting");

        byte[] bytes = ValueCodec.serialize(new User("Alice", 30));

        User roundTripped = ValueCodec.deserialize(bytes);
        assertEquals(new User("Alice", 30), roundTripped);
    }
}
