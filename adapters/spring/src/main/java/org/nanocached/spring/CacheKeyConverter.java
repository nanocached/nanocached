package org.nanocached.spring;

/**
 * Turns Spring cache keys (arbitrary objects — strings, numbers, SpEL
 * results, {@code SimpleKey}s) into the key bytes the wire carries.
 *
 * <p>Unlike value serialization this must be <em>canonical</em>: two keys
 * that are {@code equals()} must map to the same bytes on every JVM that
 * shares the cache, or the same logical entry splits per producer. The
 * default is {@link DefaultCacheKeyConverter}.
 */
public interface CacheKeyConverter {

    byte[] toKeyBytes(Object key);
}
