package org.nanocached.spring;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * The default {@link CacheValueSerializer}: plain JDK serialization.
 *
 * <p><b>Trust boundary.</b> {@link #deserialize} runs {@link ObjectInputStream#readObject()}
 * over whatever bytes are read back from the namespace — see this module's README
 * "Trust boundary / deserialization" section for what that means for anyone who can write
 * to that namespace. To bound what this class is willing to reconstruct, every {@code
 * ObjectInputStream} it creates has an {@link ObjectInputFilter} applied: by default the
 * JVM-wide filter, if one is configured (via {@code -Djdk.serialFilter} or {@link
 * ObjectInputFilter.Config#setSerialFilter}) — set explicitly here because {@code
 * ObjectInputStream} does not always pick up the process-wide filter on its own — or, when a
 * more specific policy than the process default is wanted, the filter passed to {@link
 * #JdkCacheValueSerializer(ObjectInputFilter)}.
 */
public final class JdkCacheValueSerializer implements CacheValueSerializer {

    private final ObjectInputFilter filter;

    /** Uses the JVM-wide serial filter ({@link ObjectInputFilter.Config#getSerialFilter()}), if any. */
    public JdkCacheValueSerializer() {
        this(ObjectInputFilter.Config.getSerialFilter());
    }

    /**
     * Uses {@code filter} instead of (or in addition to configuring) the JVM-wide filter.
     * Pass {@code null} to explicitly deserialize without a filter — not recommended; see the
     * class javadoc and the README's trust boundary section.
     */
    public JdkCacheValueSerializer(ObjectInputFilter filter) {
        this.filter = filter;
    }

    @Override
    public byte[] serialize(Object value) {
        if (!(value instanceof Serializable)) {
            throw new IllegalArgumentException(
                    "nanocached-spring: value of type "
                            + value.getClass().getName()
                            + " is not Serializable; configure a custom CacheValueSerializer"
                            + " or make the cached type Serializable");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        } catch (IOException e) {
            throw new IllegalStateException("nanocached-spring: failed to serialize cache value", e);
        }
        return bytes.toByteArray();
    }

    @Override
    public Object deserialize(byte[] bytes) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            if (filter != null) {
                in.setObjectInputFilter(filter);
            }
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(
                    "nanocached-spring: failed to deserialize cache value", e);
        }
    }
}
