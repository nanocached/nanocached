package org.nanocached.spring;

/**
 * Turns the objects Spring caches into the bytes the wire carries, and
 * back. The default is {@link JdkCacheValueSerializer} — Spring's cache
 * abstraction traditionally assumes {@code Serializable} values, and JDK
 * serialization round-trips every such value (including Spring's internal
 * {@code NullValue} marker for cached nulls) with no configuration. Plug
 * in a JSON/Kryo/etc. implementation when values are shared with non-JVM
 * readers or classes change between deployments; such an implementation
 * must be able to round-trip {@code org.springframework.cache.support.NullValue}
 * if null caching is left enabled.
 */
public interface CacheValueSerializer {

    byte[] serialize(Object value);

    Object deserialize(byte[] bytes);
}
