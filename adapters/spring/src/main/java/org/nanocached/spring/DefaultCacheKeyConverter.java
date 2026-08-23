package org.nanocached.spring;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * The default {@link CacheKeyConverter}:
 *
 * <ul>
 *   <li>{@code byte[]} keys pass through untouched;
 *   <li>{@code String}, boxed numbers, {@code Boolean}, {@code Character}
 *       and {@code UUID} become their canonical string form in UTF-8,
 *       prefixed with the value's simple type name ({@code "String:v"},
 *       {@code "Long:42"}) so {@code "42"} and {@code 42L} stay distinct
 *       keys instead of silently sharing an entry;
 *   <li>everything else (Spring's {@code SimpleKey} included) is JDK
 *       serialization output — stable across JVMs for the simple
 *       parameter types method keys are made of, but bulky; prefer
 *       simple keys, or plug in a converter that knows the key type.
 * </ul>
 */
public final class DefaultCacheKeyConverter implements CacheKeyConverter {

    @Override
    public byte[] toKeyBytes(Object key) {
        if (key instanceof byte[] raw) {
            return raw;
        }
        if (key instanceof String
                || key instanceof Number
                || key instanceof Boolean
                || key instanceof Character
                || key instanceof UUID) {
            return (key.getClass().getSimpleName() + ":" + key)
                    .getBytes(StandardCharsets.UTF_8);
        }
        if (!(key instanceof Serializable)) {
            throw new IllegalArgumentException(
                    "nanocached-spring: cache key of type "
                            + key.getClass().getName()
                            + " is neither a simple type nor Serializable; configure a custom"
                            + " CacheKeyConverter");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(key);
        } catch (IOException e) {
            throw new IllegalStateException("nanocached-spring: failed to serialize cache key", e);
        }
        return bytes.toByteArray();
    }
}
