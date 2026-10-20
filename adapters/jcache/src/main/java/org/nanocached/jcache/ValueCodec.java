package org.nanocached.jcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import javax.cache.CacheException;

/**
 * Cache values to wire bytes — plain JDK serialization, the same default
 * {@code nanocached-spring}'s {@code JdkCacheValueSerializer} uses,
 * reimplemented independently here (see this module's README policy
 * note). Unlike Spring's cache abstraction, JCache never stores {@code
 * null} (see {@link javax.cache.Cache#put}'s contract), so there is no
 * null-marker concern here.
 *
 * <p><b>Trust boundary.</b> {@link #deserialize} runs {@link ObjectInputStream#readObject()}
 * over whatever bytes are read back from the namespace — see this module's README
 * "Trust boundary / deserialization" section for what that means for anyone who can write to
 * that namespace. To bound what this class is willing to reconstruct, every {@code
 * ObjectInputStream} it creates has the JVM-wide serial filter applied, if one is configured
 * ({@code -Djdk.serialFilter}, or {@link ObjectInputFilter.Config#setSerialFilter}) — set
 * explicitly here because {@code ObjectInputStream} does not always pick up the process-wide
 * filter on its own.
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
            throw new CacheException("nanocached-jcache: failed to serialize cache value", e);
        }
        return bytes.toByteArray();
    }

    /** Resolves classes with the calling thread's context class loader —
     * see {@link #deserialize(byte[], ClassLoader)}. */
    static <V> V deserialize(byte[] bytes) {
        return deserialize(bytes, null);
    }

    /**
     * Resolves the value's classes with {@code classLoader} — the loader
     * the {@code CacheManager} was created with, which JSR-107 says is the
     * one for loading the cache's underlying classes — or, when it is
     * {@code null}, the calling thread's context class loader, falling
     * back to this class's own. A plain {@code ObjectInputStream} uses
     * the loader that defined this adapter, which cannot see application
     * classes in a child loader (a web application, a devtools restart):
     * every cache hit then failed with {@code ClassNotFoundException}.
     * A failure to read the value back is a {@link CacheException}, as
     * the {@code Cache} contract asks.
     */
    @SuppressWarnings("unchecked")
    static <V> V deserialize(byte[] bytes, ClassLoader classLoader) {
        ClassLoader loader = classLoader;
        if (loader == null) {
            loader = Thread.currentThread().getContextClassLoader();
        }
        if (loader == null) {
            loader = ValueCodec.class.getClassLoader();
        }
        try (ObjectInputStream in = new LoaderObjectInputStream(new ByteArrayInputStream(bytes), loader)) {
            ObjectInputFilter filter = ObjectInputFilter.Config.getSerialFilter();
            if (filter != null) {
                in.setObjectInputFilter(filter);
            }
            return (V) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new CacheException("nanocached-jcache: failed to deserialize cache value", e);
        }
    }

    private static final class LoaderObjectInputStream extends ObjectInputStream {
        private final ClassLoader loader;

        LoaderObjectInputStream(ByteArrayInputStream in, ClassLoader loader) throws IOException {
            super(in);
            this.loader = loader;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            try {
                return Class.forName(desc.getName(), false, loader);
            } catch (ClassNotFoundException notInLoader) {
                return super.resolveClass(desc);
            }
        }
    }
}
