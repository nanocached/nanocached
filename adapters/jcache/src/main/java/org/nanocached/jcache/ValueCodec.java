package org.nanocached.jcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Cache values to wire bytes — plain JDK serialization, the same default
 * {@code nanocached-spring}'s {@code JdkCacheValueSerializer} uses,
 * reimplemented independently here (see this module's README policy
 * note). Unlike Spring's cache abstraction, JCache never stores {@code
 * null} (see {@link javax.cache.Cache#put}'s contract), so there is no
 * null-marker concern here.
 */
final class ValueCodec {

    private ValueCodec() {}

    static byte[] serialize(Object value) {
        if (!(value instanceof Serializable)) {
            throw new IllegalArgumentException(
                    "nanocached-jcache: value of type "
                            + value.getClass().getName()
                            + " is not Serializable");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        } catch (IOException e) {
            throw new IllegalStateException("nanocached-jcache: failed to serialize cache value", e);
        }
        return bytes.toByteArray();
    }

    @SuppressWarnings("unchecked")
    static <V> V deserialize(byte[] bytes) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (V) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("nanocached-jcache: failed to deserialize cache value", e);
        }
    }
}
