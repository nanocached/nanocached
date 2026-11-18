package org.nanocached.spring;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.springframework.cache.interceptor.SimpleKey;

/**
 * The default {@link CacheKeyConverter}:
 *
 * <ul>
 *   <li>{@code byte[]} keys are prefixed with {@link #RAW_BYTES_MARKER} (0x00) and passed through;
 *   <li>{@code String}, boxed numbers, {@code Boolean}, {@code Character} and {@code UUID} become
 *       their canonical string form in UTF-8, prefixed with the value's simple type name ({@code
 *       "String:v"}, {@code "Long:42"}) so {@code "42"} and {@code 42L} stay distinct keys instead
 *       of silently sharing an entry;
 *   <li>Spring's multi-argument {@code SimpleKey} is {@link #SIMPLE_KEY_MARKER} (0x01) followed by
 *       each argument as a 4-byte big-endian length and that argument's own encoding by these rules
 *       ({@code -1} and no bytes for {@code null}; an {@code Object[]} argument, which {@code
 *       SimpleKey} compares element by element, as {@link #ARRAY_ARGUMENT_MARKER} (0x02) and the same
 *       length-prefixed list) — so the bytes depend only on the arguments' values. JDK serialization
 *       of the whole key writes an instance it has already written as a back-reference, so the equal
 *       keys {@code SimpleKey(x, x)} and {@code SimpleKey(x, copyOf(x))} would be different wire
 *       keys and a {@code @CacheEvict} built from separately obtained arguments would miss;
 *   <li>everything else is JDK serialization output — stable across JVMs for the simple parameter
 *       types method keys are made of, but bulky; prefer simple keys, or plug in a converter that
 *       knows the key type.
 * </ul>
 *
 * <p>The four families never collide because of their leading byte: a type-prefixed string starts
 * with an ASCII uppercase letter (the class SimpleName), a JDK-serialized stream starts with {@code
 * 0xAC} (STREAM_MAGIC), a {@code byte[]} carries the {@code 0x00} marker and a {@code SimpleKey} the
 * {@code 0x01} marker, which none of the others can begin with. Without it a {@code byte[]} whose
 * bytes spelled {@code "String:foo"} would share a wire key with the string {@code "foo"} and one
 * would read back the other's value.
 */
public final class DefaultCacheKeyConverter implements CacheKeyConverter {

    /**
     * Leading byte distinguishing a raw {@code byte[]} key from the type-prefixed-string,
     * JDK-serialized and {@code SimpleKey} families — see the class doc. 0x00 can begin none of those.
     */
    static final byte RAW_BYTES_MARKER = 0x00;

    /** Leading byte of a {@code SimpleKey}'s encoding — see the class doc. */
    static final byte SIMPLE_KEY_MARKER = 0x01;

    /** Leading byte of an {@code Object[]} argument inside a {@code SimpleKey} — see the class doc. */
    static final byte ARRAY_ARGUMENT_MARKER = 0x02;

    /** Length written for a {@code null} argument; every non-null argument encodes to at least one byte. */
    private static final int NULL_ARGUMENT = -1;

    /**
     * {@code SimpleKey}'s private {@code Object[] params}: Spring offers no accessor for a key's
     * arguments, and the key generator that builds them is the application's, not this adapter's.
     * {@code null} if a Spring release renamed it: encoding a {@code SimpleKey} then fails instead
     * of silently falling back to a different key format, while every other key (and a custom
     * converter, the builder creating this one as its default) keeps working.
     */
    private static final Field SIMPLE_KEY_PARAMS = simpleKeyParamsField();

    @Override
    public byte[] toKeyBytes(Object key) {
        if (key instanceof SimpleKey simpleKey) {
            return argumentsBytes(SIMPLE_KEY_MARKER, simpleKeyParams(simpleKey));
        }
        if (key instanceof byte[] raw) {
            byte[] marked = new byte[raw.length + 1];
            marked[0] = RAW_BYTES_MARKER;
            System.arraycopy(raw, 0, marked, 1, raw.length);
            return marked;
        }
        if (key instanceof String text) {
            requireWellFormed(text);
        }
        if (key instanceof Character c) {
            // A lone surrogate char has the same '?' problem as a String
            // holding one: every such key would address one entry.
            requireWellFormed(String.valueOf(c));
        }
        if (key instanceof String
                || key instanceof Number
                || key instanceof Boolean
                || key instanceof Character
                || key instanceof UUID) {
            return (key.getClass().getSimpleName() + ":" + key).getBytes(StandardCharsets.UTF_8);
        }
        if (!(key instanceof Serializable)) {
            throw new IllegalArgumentException("nanocached-spring: cache key of type "
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

    /**
     * {@code marker}, then each argument as a 4-byte big-endian length and its bytes. Each argument
     * is encoded on its own, so no argument can be written as a back-reference to another, and the
     * length prefixes keep {@code ("a", "b")} apart from {@code ("ab")}.
     */
    private byte[] argumentsBytes(byte marker, Object[] arguments) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write(marker);
        for (Object argument : arguments) {
            if (argument == null) {
                writeLength(bytes, NULL_ARGUMENT);
                continue;
            }
            // SimpleKey compares an Object[] argument with Arrays.deepEquals, so its elements
            // need the same value-only encoding (a varargs method's arguments arrive this way).
            byte[] encoded = argument instanceof Object[] array
                    ? argumentsBytes(ARRAY_ARGUMENT_MARKER, array)
                    : toKeyBytes(argument);
            writeLength(bytes, encoded.length);
            bytes.writeBytes(encoded);
        }
        return bytes.toByteArray();
    }

    private static void writeLength(ByteArrayOutputStream bytes, int length) {
        bytes.write(length >>> 24);
        bytes.write(length >>> 16);
        bytes.write(length >>> 8);
        bytes.write(length);
    }

    private static Object[] simpleKeyParams(SimpleKey key) {
        if (SIMPLE_KEY_PARAMS == null) {
            throw new IllegalStateException("nanocached-spring: this Spring version's SimpleKey has no readable"
                    + " params field; configure a custom CacheKeyConverter");
        }
        try {
            return (Object[]) SIMPLE_KEY_PARAMS.get(key);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("nanocached-spring: cannot read the arguments of a SimpleKey", e);
        }
    }

    private static Field simpleKeyParamsField() {
        try {
            Field params = SimpleKey.class.getDeclaredField("params");
            params.setAccessible(true);
            return params.getType() == Object[].class ? params : null;
        } catch (NoSuchFieldException | RuntimeException e) {
            return null;
        }
    }

    /**
     * {@code String.getBytes(UTF_8)} silently replaces a lone surrogate (half of a UTF-16 pair with
     * no partner) with {@code '?'}, so two different keys ({@code "x\uD800"} and {@code "x\uD801"},
     * or either and {@code "x?"}) would address one cache entry and a lookup under one would return
     * the other's value. Such a key is rejected instead.
     */
    private static void requireWellFormed(String text) {
        for (int i = 0, n = text.length(); i < n; i++) {
            char c = text.charAt(i);
            if (!Character.isSurrogate(c)) continue;
            if (Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(text.charAt(i + 1))) {
                i++;
                continue;
            }
            throw new IllegalArgumentException(
                    "nanocached-spring: cache key is not well-formed UTF-16 (a lone surrogate at index "
                            + i
                            + " cannot be encoded as UTF-8)");
        }
    }
}
