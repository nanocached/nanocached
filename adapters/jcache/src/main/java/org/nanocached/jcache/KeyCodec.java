package org.nanocached.jcache;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Cache keys to wire bytes — the same canonical scheme
 * {@code nanocached-spring}'s {@code DefaultCacheKeyConverter} uses,
 * reimplemented independently here (adapters don't depend on each other;
 * see the policy note in this module's README): {@code byte[]} passes
 * through untouched; {@code String}, boxed numbers, {@code Boolean},
 * {@code Character} and {@code UUID} become a type-prefixed canonical
 * string (so {@code "42"} and {@code 42L} stay distinct keys); everything
 * else goes through JDK serialization, which must be canonical across
 * every JVM sharing the cache.
 */
final class KeyCodec {

    private KeyCodec() {}

    static byte[] toKeyBytes(Object key) {
        if (key instanceof byte[] raw) {
            return raw;
        }
        if (key instanceof String
                || key instanceof Number
                || key instanceof Boolean
                || key instanceof Character
                || key instanceof UUID) {
            return (key.getClass().getSimpleName() + ":" + key).getBytes(StandardCharsets.UTF_8);
        }
        if (!(key instanceof Serializable)) {
            throw new IllegalArgumentException(
                    "nanocached-jcache: cache key of type "
                            + key.getClass().getName()
                            + " is neither a simple type nor Serializable");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(key);
        } catch (IOException e) {
            throw new IllegalStateException("nanocached-jcache: failed to serialize cache key", e);
        }
        return bytes.toByteArray();
    }
}
