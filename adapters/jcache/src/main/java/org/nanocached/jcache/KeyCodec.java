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
 * see the policy note in this module's README): {@code byte[]} is marked
 * and passed through; {@code String}, boxed numbers, {@code Boolean},
 * {@code Character} and {@code UUID} become a type-prefixed canonical
 * string (so {@code "42"} and {@code 42L} stay distinct keys); everything
 * else goes through JDK serialization, which must be canonical across
 * every JVM sharing the cache.
 *
 * <p>The three families are kept from colliding by their leading byte: a
 * type-prefixed string starts with an ASCII uppercase letter (the class
 * SimpleName), a JDK-serialized stream starts with {@code 0xAC}
 * (STREAM_MAGIC), and a {@code byte[]} is prefixed with {@link
 * #RAW_BYTES_MARKER} (0x00), which neither of the others can begin with.
 * Without that marker a {@code byte[]} whose bytes happened to spell, say,
 * {@code "String:foo"} would map to the same wire key as the string
 * {@code "foo"} and one would read back the other's value.
 */
final class KeyCodec {

    private KeyCodec() {}

    /** Leading byte distinguishing a raw {@code byte[]} key from the
     * type-prefixed-string and JDK-serialized families — see the class
     * doc. 0x00 can begin neither of those. */
    static final byte RAW_BYTES_MARKER = 0x00;

    static byte[] toKeyBytes(Object key) {
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

    /** {@code String.getBytes(UTF_8)} silently replaces a lone surrogate
     * (half of a UTF-16 pair with no partner) with {@code '?'}, so two
     * different keys ({@code "x\uD800"} and {@code "x\uD801"}, or either
     * and {@code "x?"}) would address one cache entry and a lookup under
     * one would return the other's value. Such a key is rejected
     * instead. */
    private static void requireWellFormed(String text) {
        for (int i = 0, n = text.length(); i < n; i++) {
            char c = text.charAt(i);
            if (!Character.isSurrogate(c)) continue;
            if (Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(text.charAt(i + 1))) {
                i++;
                continue;
            }
            throw new IllegalArgumentException(
                    "nanocached-jcache: cache key is not well-formed UTF-16 (a lone surrogate at index " + i
                            + " cannot be encoded as UTF-8)");
        }
    }
}
