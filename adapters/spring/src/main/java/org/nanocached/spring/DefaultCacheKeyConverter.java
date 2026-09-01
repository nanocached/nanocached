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
 *   <li>{@code byte[]} keys are prefixed with {@link #RAW_BYTES_MARKER}
 *       (0x00) and passed through;
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
 *
 * <p>The three families never collide because of their leading byte: a
 * type-prefixed string starts with an ASCII uppercase letter (the class
 * SimpleName), a JDK-serialized stream starts with {@code 0xAC}
 * (STREAM_MAGIC), and a {@code byte[]} carries the {@code 0x00} marker,
 * which neither of the others can begin with. Without it a {@code byte[]}
 * whose bytes spelled {@code "String:foo"} would share a wire key with the
 * string {@code "foo"} and one would read back the other's value.
 */
public final class DefaultCacheKeyConverter implements CacheKeyConverter {

    /** Leading byte distinguishing a raw {@code byte[]} key from the
     * type-prefixed-string and JDK-serialized families — see the class
     * doc. 0x00 can begin neither of those. */
    static final byte RAW_BYTES_MARKER = 0x00;

    @Override
    public byte[] toKeyBytes(Object key) {
        if (key instanceof byte[] raw) {
            byte[] marked = new byte[raw.length + 1];
            marked[0] = RAW_BYTES_MARKER;
            System.arraycopy(raw, 0, marked, 1, raw.length);
            return marked;
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
