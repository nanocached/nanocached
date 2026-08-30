package org.nanocached.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.InvalidClassException;
import java.io.ObjectInputFilter;
import java.io.Serializable;
import org.junit.jupiter.api.Test;

/**
 * Proves {@link JdkCacheValueSerializer#deserialize} actually applies an {@link
 * ObjectInputFilter} (issue #232's trust-boundary fix), using the constructor overload rather
 * than {@link ObjectInputFilter.Config#setSerialFilter} — that JVM-wide setter can only
 * succeed once per process, so mutating it here would be order-dependent and could break any
 * other test in the same JVM that deserializes afterward.
 */
class JdkCacheValueSerializerTest {

    record User(String name, int age) implements Serializable {}

    @Test
    void defaultConstructorRoundTripsNormally() {
        // The JVM under test starts with no -Djdk.serialFilter, so
        // ObjectInputFilter.Config.getSerialFilter() is null and the
        // no-arg constructor behaves exactly like unfiltered JDK
        // serialization.
        JdkCacheValueSerializer serializer = new JdkCacheValueSerializer();

        byte[] bytes = serializer.serialize(new User("Alice", 30));

        assertEquals(new User("Alice", 30), serializer.deserialize(bytes));
    }

    @Test
    void explicitFilterThatRejectsEverythingBlocksDeserialization() {
        ObjectInputFilter rejectAll = filterInfo -> ObjectInputFilter.Status.REJECTED;
        JdkCacheValueSerializer serializer = new JdkCacheValueSerializer(rejectAll);

        byte[] bytes = serializer.serialize(new User("Alice", 30));

        IllegalStateException thrown =
                assertThrows(IllegalStateException.class, () -> serializer.deserialize(bytes));
        assertInstanceOf(InvalidClassException.class, thrown.getCause());
    }

    @Test
    void explicitFilterThatAllowsEverythingStillRoundTrips() {
        ObjectInputFilter allowAll = filterInfo -> ObjectInputFilter.Status.ALLOWED;
        JdkCacheValueSerializer serializer = new JdkCacheValueSerializer(allowAll);

        byte[] bytes = serializer.serialize(new User("Bob", 25));

        assertEquals(new User("Bob", 25), serializer.deserialize(bytes));
    }
}
